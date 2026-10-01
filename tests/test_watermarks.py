"""顺序水位与截止查询的自动化验证。

覆盖移交审计要求的五类场景：
1. 正常推进——不同业务时刻按时间有序，水位单调；
2. 同刻度多版本——同一业务时刻的多个版本拥有稳定水位，
   first/last/指定水位三种截止查询行为明确；
3. 旧库迁移——历史记录按内容确定性回填顺序，与插入/回放顺序无关，
   迁移后新写入从水位 1 开始，旧审计链仍可校验；
4. 并发提交——多线程同刻写入拿到唯一、稠密的水位；
5. 重启回放——重新打开后历史、快照、审计链返回完全一致的结果；
另外验证幂等重试不产生新水位、字段裁剪使用同一排序语义。
"""

from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.cases import CaseService
from civicflow.evidence import EvidenceService
from civicflow.errors import NotFoundError, ValidationError
from civicflow.jsonutil import canonical_json
from civicflow.security import AccessContext

T1 = "2026-10-01T09:00:00+08:00"
T1Z = "2026-10-01T01:00:00Z"
T2 = "2026-10-01T10:00:00+08:00"
T3 = "2026-10-01T11:00:00+08:00"

# 水位改造前的旧表结构（无 watermark 列），用于构造"旧数据库"。
LEGACY_SCHEMA = r"""
CREATE TABLE entities (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id)
);
CREATE TABLE entity_versions (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id, version)
);
CREATE TABLE idempotency_keys (
    scope TEXT NOT NULL,
    request_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, request_key)
);
CREATE TABLE audit_entries (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    detail_json TEXT NOT NULL,
    previous_digest TEXT NOT NULL,
    entry_digest TEXT NOT NULL
);
"""


def legacy_digest(*, occurred_at, actor_id, action, entity_type, entity_id, version, detail, previous):
    body = canonical_json({"occurred_at": occurred_at, "actor_id": actor_id, "action": action,
                           "entity_type": entity_type, "entity_id": entity_id, "version": version,
                           "detail": detail, "previous": previous})
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def build_legacy_database(path: Path, *, reverse_insert: bool) -> dict[str, int]:
    """写入一批同刻度的旧记录，返回每个 (entity_id, version) 的行号次序无关期望水位。"""
    connection = sqlite3.connect(path)
    connection.executescript(LEGACY_SCHEMA)
    # 两个案件、各两版，全部使用同一业务时刻——旧库里它们的先后只能靠猜。
    versions = [
        ("cases", "cases:A", 1, "draft"),
        ("cases", "cases:A", 2, "open"),
        ("cases", "cases:B", 1, "draft"),
        ("cases", "cases:B", 2, "open"),
    ]
    ordered = versions if not reverse_insert else list(reversed(versions))
    previous = "0" * 64
    for entity_type, entity_id, version, state in ordered:
        payload = {"state": state, "subject": f"事项{entity_id[-1]}", "priority": "high"}
        connection.execute(
            "INSERT INTO entity_versions(entity_type,entity_id,version,state,payload_json,valid_from,actor_id,request_key)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (entity_type, entity_id, version, state, canonical_json(payload), T1Z, "tester", f"req-{entity_id}-{version}"),
        )
        digest = legacy_digest(occurred_at=T1Z, actor_id="tester", action=("create" if version == 1 else "update"),
                               entity_type=entity_type, entity_id=entity_id, version=version,
                               detail=payload, previous=previous)
        connection.execute(
            "INSERT INTO audit_entries(occurred_at,actor_id,action,entity_type,entity_id,version,detail_json,previous_digest,entry_digest)"
            " VALUES(?,?,?,?,?,?,?,?,?)",
            (T1Z, "tester", "create" if version == 1 else "update", entity_type, entity_id, version,
             canonical_json(payload), previous, digest),
        )
        previous = digest
    # entities 每个实体只有一行，存放当前（最高）版本。
    for entity_id in ("cases:A", "cases:B"):
        payload = {"state": "open", "subject": f"事项{entity_id[-1]}", "priority": "high"}
        connection.execute(
            "INSERT INTO entities(entity_type,entity_id,version,state,payload_json,created_at,updated_at,created_by,updated_by)"
            " VALUES(?,?,?,?,?,?,?,?,?)",
            ("cases", entity_id, 2, "open", canonical_json(payload), T1Z, T1Z, "tester", "tester"),
        )
    connection.commit()
    connection.close()

    # 期望水位只由内容键 (valid_from, entity_type, entity_id, version) 决定。
    ranked = sorted(versions, key=lambda v: (T1Z, v[0], v[1], v[2]))
    total = len(versions)
    return {(entity_id, version): rank - total - 1 for rank, (_, entity_id, version, _) in enumerate(ranked, start=1)}


class WatermarkTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "test.sqlite3"
        self.app = CivicFlow.open(self.db_path, fixed_now=T1)
        self.system = AccessContext.system("tester")

    def tearDown(self):
        self.temp.cleanup()

    def reopen(self, fixed_now=T1):
        return CivicFlow.open(self.db_path, fixed_now=fixed_now)

    def replace_with_legacy_database(self, *, reverse_insert: bool):
        """丢弃新库，构造一个水位改造前的旧库。"""
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_path) + suffix)
            if path.exists():
                path.unlink()
        return build_legacy_database(self.db_path, reverse_insert=reverse_insert)

    def make_case(self, service, key="c1", subject="知识产权案件", priority="high"):
        return service.create(self.system, {"case_type": "联合处置", "subject": subject,
                                            "owner_org": "org:a", "priority": priority,
                                            "opened_at": T1}, request_key=key)

    # 1. 正常推进 ------------------------------------------------------
    def test_normal_progression_monotonic_watermark(self):
        service = CaseService(self.app.repository)
        created = self.make_case(service)
        self.assertEqual(created["order_watermark"], 1)

        app2 = self.reopen(T2)
        service = CaseService(app2.repository)
        v2 = service.revise(self.system, created["entity_id"], {"priority": "normal"},
                            expected_version=1, request_key="c2")
        app3 = self.reopen(T3)
        service = CaseService(app3.repository)
        v3 = service.revise(self.system, created["entity_id"], {"priority": "low"},
                            expected_version=2, request_key="c3")
        self.assertEqual([v2["order_watermark"], v3["order_watermark"]], [2, 3])

        history = service.history(self.system, created["entity_id"])
        self.assertEqual([(v["version"], v["order_watermark"]) for v in history],
                         [(1, 1), (2, 2), (3, 3)])
        self.assertEqual([v["valid_from"] for v in history], [T1Z, "2026-10-01T02:00:00Z", "2026-10-01T03:00:00Z"])

        # 不同时刻的截止查询：T2 时刻只能看到前两版。
        self.assertEqual(service.snapshot(self.system, created["entity_id"], as_of=T2)["version"], 2)
        self.assertEqual(service.snapshot(self.system, created["entity_id"], as_of=T2, bound="first")["version"], 1)
        self.assertEqual(service.snapshot(self.system, created["entity_id"], as_of=T3, bound="last")["version"], 3)
        with self.assertRaises(NotFoundError):
            service.snapshot(self.system, created["entity_id"], as_of="2026-10-01T08:59:59+08:00")

    # 2. 同刻度多版本 ---------------------------------------------------
    def test_same_instant_multiple_versions_have_stable_watermarks(self):
        # 固定时钟不变：连续三版共用同一业务时刻。
        service = CaseService(self.app.repository)
        created = self.make_case(service)
        v2 = service.revise(self.system, created["entity_id"], {"priority": "normal"},
                            expected_version=1, request_key="c2")
        v3 = service.revise(self.system, created["entity_id"], {"priority": "low"},
                            expected_version=2, request_key="c3")
        marks = [created["order_watermark"], v2["order_watermark"], v3["order_watermark"]]
        self.assertEqual(marks, [1, 2, 3])

        history = service.history(self.system, created["entity_id"])
        self.assertEqual([v["version"] for v in history], [1, 2, 3])
        self.assertTrue(all(v["valid_from"] == T1Z for v in history))

        # 截止查询明确区分最先可见、最后可见。
        self.assertEqual(service.snapshot(self.system, created["entity_id"], as_of=T1, bound="first")["version"], 1)
        self.assertEqual(service.snapshot(self.system, created["entity_id"], as_of=T1, bound="last")["version"], 3)
        self.assertEqual(service.snapshot(self.system, created["entity_id"], as_of=T1)["version"], 3)

        # 指定水位：精确复核某次移交时刻的材料边界。
        self.assertEqual(service.snapshot(self.system, created["entity_id"], as_of=T1,
                                          bound="watermark", watermark=2)["version"], 2)
        self.assertEqual(service.snapshot(self.system, created["entity_id"], as_of=T1,
                                          bound="watermark", watermark=1)["version"], 1)
        with self.assertRaises(NotFoundError):
            service.snapshot(self.system, created["entity_id"], as_of=T1,
                             bound="watermark", watermark=0)
        with self.assertRaises(ValidationError):
            service.snapshot(self.system, created["entity_id"], as_of=T1, bound="middle")
        with self.assertRaises(ValidationError):
            service.snapshot(self.system, created["entity_id"], as_of=T1, bound="watermark")

    def test_evidence_visibility_at_watermark_for_handover(self):
        # 移交人员用返回的水位说明某份证据在当时是否可见。
        evidence = EvidenceService(self.app.repository)
        # 水位 1：先有案件无关的其他写入，确保证据拿到更高水位。
        self.make_case(CaseService(self.app.repository), key="case-before")
        record = evidence.create(self.system, {"case_id": "cases:A", "kind": "交易凭证",
                                               "digest": "sha256:abc", "source": "柜台A",
                                               "occurred_at": T1}, request_key="ev1")
        mark = record["order_watermark"]
        self.assertEqual(mark, 2)

        # 截止到上一水位：证据尚不可见。
        with self.assertRaises(NotFoundError):
            evidence.snapshot(self.system, record["entity_id"], as_of=T1,
                              bound="watermark", watermark=mark - 1)
        # 截止到证据自身水位：可见。
        visible = evidence.snapshot(self.system, record["entity_id"], as_of=T1,
                                    bound="watermark", watermark=mark)
        self.assertEqual(visible["entity_id"], record["entity_id"])
        self.assertEqual(visible["order_watermark"], mark)
        # last/first 在同刻单点上也一致。
        self.assertEqual(evidence.snapshot(self.system, record["entity_id"], as_of=T1, bound="first")["order_watermark"], mark)

    def test_redaction_uses_same_ordering(self):
        service = CaseService(self.app.repository)
        created = self.make_case(service, subject="敏感主体")
        service.revise(self.system, created["entity_id"], {"priority": "normal"},
                       expected_version=1, request_key="c2")
        service.revise(self.system, created["entity_id"], {"priority": "low"},
                       expected_version=2, request_key="c3")
        reader = AccessContext(actor_id="reader",
                               permissions=frozenset({"history:cases"}))
        # 排序先于裁剪：first/last 分别定位到 v1/v3，subject 均已被裁剪。
        first = service.snapshot(reader, created["entity_id"], as_of=T1, bound="first")
        last = service.snapshot(reader, created["entity_id"], as_of=T1, bound="last")
        self.assertEqual(first["version"], 1)
        self.assertEqual(last["version"], 3)
        self.assertEqual(first["subject"], "***")
        self.assertEqual(last["subject"], "***")

    # 3. 旧库迁移 -------------------------------------------------------
    def test_legacy_database_backfills_deterministic_order(self):
        expected = self.replace_with_legacy_database(reverse_insert=False)

        app = self.reopen()  # 触发迁移
        service = CaseService(app.repository)
        with app.database.connect() as conn:
            rows = conn.execute("SELECT entity_id,version,watermark FROM entity_versions").fetchall()
        for row in rows:
            self.assertEqual(row["watermark"], expected[(row["entity_id"], row["version"])],
                             msg="回填顺序必须由内容键决定")
        self.assertEqual(app.repository.current_watermark(), 0)

        # 同刻历史按 (valid_from, watermark) 稳定排序；A 的两版排在 B 之前。
        history_a = service.history(self.system, "cases:A")
        self.assertEqual([(v["version"], v["order_watermark"]) for v in history_a],
                         [(1, expected[("cases:A", 1)]), (2, expected[("cases:A", 2)])])
        self.assertEqual(service.snapshot(self.system, "cases:A", as_of=T1, bound="last")["version"], 2)
        self.assertEqual(service.snapshot(self.system, "cases:A", as_of=T1, bound="first")["version"], 1)

        # 旧审计链（哈希体不含水位）仍可校验。
        self.assertEqual(app.verify()["audit_entries"], 4)

        # 迁移后新写入从水位 1 开始，与负水位历史不交叠，混合审计链依然连续。
        created = service.create(self.system, {"case_type": "协作", "subject": "迁移后新案",
                                               "owner_org": "org:b", "priority": "high",
                                               "opened_at": T2}, request_key="new-1")
        self.assertEqual(created["order_watermark"], 1)
        self.assertEqual(app.verify()["audit_entries"], 5)
        # 截止水位 0 恰好只能看到迁移前的材料。
        self.assertEqual(service.snapshot(self.system, "cases:A", as_of=T2,
                                          bound="watermark", watermark=0)["version"], 2)
        with self.assertRaises(NotFoundError):
            service.snapshot(self.system, created["entity_id"], as_of=T2,
                             bound="watermark", watermark=0)
        self.assertEqual(service.snapshot(self.system, created["entity_id"], as_of=T2,
                                          bound="watermark", watermark=1)["version"], 1)

    def test_legacy_backfill_independent_of_insertion_order(self):
        # 用相反的物理插入次序构造内容相同的旧库：回填结果必须一致。
        expected = self.replace_with_legacy_database(reverse_insert=True)
        app = self.reopen()
        with app.database.connect() as conn:
            rows = conn.execute("SELECT entity_id,version,watermark FROM entity_versions").fetchall()
        for row in rows:
            self.assertEqual(row["watermark"], expected[(row["entity_id"], row["version"])])

        # 迁移幂等：再次初始化（模拟重启）水位不变化。
        app2 = self.reopen()
        with app2.database.connect() as conn:
            rows2 = conn.execute("SELECT entity_id,version,watermark FROM entity_versions").fetchall()
        self.assertEqual(sorted((r["entity_id"], r["version"], r["watermark"]) for r in rows),
                         sorted((r["entity_id"], r["version"], r["watermark"]) for r in rows2))

    # 4. 幂等重试不产生新水位 --------------------------------------------
    def test_idempotent_retry_reuses_watermark(self):
        service = CaseService(self.app.repository)
        values = {"case_type": "协作", "subject": "幂等", "owner_org": "org:a",
                  "priority": "high", "opened_at": T1}
        first = service.create(self.system, values, request_key="same-key")
        cursor_before = self.app.repository.current_watermark()
        second = service.create(self.system, values, request_key="same-key")
        cursor_after = self.app.repository.current_watermark()

        self.assertEqual(first["entity_id"], second["entity_id"])
        self.assertEqual(first["order_watermark"], second["order_watermark"])
        self.assertEqual(cursor_before, cursor_after)

        updated = service.revise(self.system, first["entity_id"], {"priority": "low"},
                                 expected_version=1, request_key="upd-key")
        cursor_before = self.app.repository.current_watermark()
        retried = service.revise(self.system, first["entity_id"], {"priority": "low"},
                                 expected_version=1, request_key="upd-key")
        self.assertEqual(updated["order_watermark"], retried["order_watermark"])
        self.assertEqual(cursor_before, self.app.repository.current_watermark())

        # 版本行、审计项都只写一次。
        with self.app.database.connect() as conn:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) AS n FROM entity_versions WHERE entity_id=?",
                (first["entity_id"],)).fetchone()["n"], 2)
            self.assertEqual(conn.execute("SELECT COUNT(*) AS n FROM audit_entries").fetchone()["n"], 2)

    # 5. 并发提交 -------------------------------------------------------
    def test_concurrent_writers_get_unique_dense_watermarks(self):
        writers = 8
        per_writer = 5

        def write_case(worker):
            # 每个线程独立打开应用（各自连接），且共用同一固定时刻。
            app = CivicFlow.open(self.db_path, fixed_now=T1)
            service = CaseService(app.repository)
            marks = []
            for i in range(per_writer):
                row = service.create(AccessContext.system(f"worker-{worker}"),
                                     {"case_type": "协作", "subject": f"并发{worker}-{i}",
                                      "owner_org": f"org:{worker}", "priority": "high",
                                      "opened_at": T1}, request_key=f"w{worker}-{i}")
                marks.append(row["order_watermark"])
            return marks

        with ThreadPoolExecutor(max_workers=writers) as pool:
            results = list(pool.map(write_case, range(writers)))

        all_marks = [mark for marks in results for mark in marks]
        expected_total = writers * per_writer
        self.assertEqual(len(all_marks), expected_total)
        self.assertEqual(len(set(all_marks)), expected_total)  # 全局唯一
        self.assertEqual(set(all_marks), set(range(1, expected_total + 1)))  # 稠密无洞

    # 6. 重启回放一致 ----------------------------------------------------
    def test_restart_replay_returns_identical_results(self):
        service = CaseService(self.app.repository)
        created = self.make_case(service)
        service.revise(self.system, created["entity_id"], {"priority": "normal"},
                       expected_version=1, request_key="c2")
        service.revise(self.system, created["entity_id"], {"priority": "low"},
                       expected_version=2, request_key="c3")

        def snapshot_view(app):
            svc = CaseService(app.repository)
            return {
                "history": [(v["version"], v["order_watermark"]) for v in svc.history(self.system, created["entity_id"])],
                "first": svc.snapshot(self.system, created["entity_id"], as_of=T1, bound="first")["order_watermark"],
                "last": svc.snapshot(self.system, created["entity_id"], as_of=T1, bound="last")["order_watermark"],
                "at2": svc.snapshot(self.system, created["entity_id"], as_of=T1, bound="watermark", watermark=2)["order_watermark"],
                "cursor": app.repository.current_watermark(),
                "audit": app.verify()["audit_entries"],
            }

        before = snapshot_view(self.app)
        # 连续"重启"两个新进程视角，结果必须逐字节一致。
        app2 = self.reopen()
        app3 = self.reopen()
        self.assertEqual(before, snapshot_view(app2))
        self.assertEqual(before, snapshot_view(app3))
        self.assertEqual(before["history"], [(1, 1), (2, 2), (3, 3)])
        self.assertEqual(before["first"], 1)
        self.assertEqual(before["last"], 3)
        self.assertEqual(before["at2"], 2)

    def test_concurrent_then_restart_is_consistent(self):
        # 并发场景 + 重启的组合验证：重新打开后水位唯一性、审计链仍然成立。
        def write_case(worker):
            app = CivicFlow.open(self.db_path, fixed_now=T1)
            service = CaseService(app.repository)
            return service.create(AccessContext.system(f"worker-{worker}"),
                                  {"case_type": "协作", "subject": f"并发{worker}",
                                   "owner_org": "org:x", "priority": "high",
                                   "opened_at": T1}, request_key=f"w{worker}")["order_watermark"]

        threads = [threading.Thread(target=write_case, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        reopened = self.reopen()
        self.assertEqual(reopened.verify()["audit_entries"], 6)
        with reopened.database.connect() as conn:
            marks = [r["watermark"] for r in conn.execute("SELECT watermark FROM entity_versions")]
        self.assertEqual(len(marks), 6)
        self.assertEqual(len(set(marks)), 6)
        self.assertEqual(reopened.repository.current_watermark(), max(marks))


if __name__ == "__main__":
    unittest.main()
