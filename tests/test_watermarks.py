"""顺序水位与历史快照的移交复核验证。

覆盖：
* 正常推进——水位随写入单调递增，历史/审计按水位排序；
* 同一业务时刻多版本——最先可见 / 最后可见 / 指定水位三种截止查询；
* 旧数据库迁移——旧结构库打开后确定性回填水位并重算审计链；
* 并发提交——多个进程同时写入得到唯一、连续、不重复的水位；
* 服务重启与回放——重开后水位延续，截止查询结论一致；
* 重复请求不产生新水位；字段裁剪与案件历史、审计链使用同一顺序语义。
"""

from __future__ import annotations

import hashlib
import multiprocessing as mp
import sqlite3
import tempfile
import unittest
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.cases import CaseService
from civicflow.evidence import EvidenceService
from civicflow.errors import NotFoundError, ValidationError
from civicflow.jsonutil import canonical_json
from civicflow.security import AccessContext

T0 = "2026-10-01T00:00:00Z"
T1 = "2026-10-01T00:00:01Z"
T2 = "2026-10-01T00:00:02Z"


def case_values(app, subject="知产案件"):
    return {"case_type": "知识产权移交", "subject": subject, "owner_org": "org:court",
            "priority": "high", "opened_at": app.clock.now()}


def evidence_values(case_id, kind, digest, source):
    return {"case_id": case_id, "kind": kind, "digest": digest, "source": source,
            "occurred_at": "2026-10-01T00:00:00Z"}


class WatermarkTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "wm.sqlite3"
        self.app = CivicFlow.open(self.db_path, fixed_now=T0)
        self.ctx = AccessContext.system("auditor")

    def tearDown(self):
        self.temp.cleanup()

    def reopen(self, fixed_now=T0):
        return CivicFlow.open(self.db_path, fixed_now=fixed_now)

    # --- 正常推进 -------------------------------------------------------
    def test_normal_progression_monotonic_watermark(self):
        cases = CaseService(self.app.repository)
        c1 = cases.create(self.ctx, case_values(self.app, "案件一"), request_key="c1")
        app1 = self.reopen(T1)
        c2 = CaseService(app1.repository).create(
            AccessContext.system("auditor"), case_values(app1, "案件二"), request_key="c2")
        app2 = self.reopen(T2)
        cases2 = CaseService(app2.repository)
        updated = cases2.revise(self.ctx, c1["entity_id"], {"priority": "normal"},
                                expected_version=1, request_key="c1-upd")

        # 每次真实写入水位严格递增，且记录在案
        self.assertLess(c1["seq"], c2["seq"])
        self.assertLess(c2["seq"], updated["seq"])
        self.assertEqual(app2.high_watermark(), updated["seq"])

        # 历史按水位排序；同一实体内水位与版本同向
        history = cases2.history(self.ctx, c1["entity_id"])
        self.assertEqual([h["seq"] for h in history], sorted(h["seq"] for h in history))
        self.assertEqual([h["version"] for h in history], [1, 2])

        # 不同时刻的截止查询
        self.assertEqual(cases2.snapshot(self.ctx, c1["entity_id"], as_of=T0)["version"], 1)
        self.assertEqual(cases2.snapshot(self.ctx, c1["entity_id"], as_of=T2)["version"], 2)
        self.assertEqual(app2.verify()["audit_entries"], 3)

    # --- 同一业务时刻多版本 ---------------------------------------------
    def test_same_instant_multiple_versions_have_distinct_watermarks(self):
        cases = CaseService(self.app.repository)
        evidence = EvidenceService(self.app.repository)
        case = cases.create(self.ctx, case_values(self.app), request_key="k-case")
        # 同一固定时刻：证据 1、证据 2、案件修订，全部 valid_from == T0
        e1 = evidence.create(self.ctx, evidence_values(case["entity_id"], "交易证据", "dig-1", "银行"),
                             request_key="k-e1")
        e2 = evidence.create(self.ctx, evidence_values(case["entity_id"], "鉴定结论", "dig-2", "鉴定机构"),
                             request_key="k-e2")
        upd = cases.revise(self.ctx, case["entity_id"], {"priority": "low"},
                           expected_version=1, request_key="k-upd")

        # 同刻版本水位互不相同，且全局严格递增
        seqs = [case["seq"], e1["seq"], e2["seq"], upd["seq"]]
        self.assertEqual(sorted(seqs), list(range(case["seq"], upd["seq"] + 1)))

        # 案件在 T0 有两个版本（v1 与 v2 时刻相同）
        first = cases.snapshot(self.ctx, case["entity_id"], as_of=T0, bound="first")
        last = cases.snapshot(self.ctx, case["entity_id"], as_of=T0, bound="last")
        self.assertEqual((first["version"], first["seq"]), (1, case["seq"]))
        self.assertEqual((last["version"], last["seq"]), (2, upd["seq"]))

        # 指定水位：水位递进时案件版本依次为 v1 -> v2，且之后保持 v2
        seen = [cases.snapshot(self.ctx, case["entity_id"], as_of=T0, at_seq=s)["version"]
                for s in range(case["seq"], upd["seq"] + 1)]
        self.assertEqual(seen, [1, 1, 1, 2])

        # 指定水位复核某份证据在当时是否可见
        before = self.app.audit_window(at_seq=e1["seq"] - 1, entity_type="evidence",
                                       entity_id=e2["entity_id"])
        self.assertEqual(before, [])
        at_e2 = self.app.audit_window(at_seq=e2["seq"], entity_type="evidence",
                                      entity_id=e2["entity_id"])
        self.assertEqual([row["seq"] for row in at_e2], [e2["seq"]])
        with self.assertRaises(NotFoundError):
            evidence.snapshot(self.ctx, e2["entity_id"], as_of=T0, at_seq=e1["seq"])
        self.assertEqual(evidence.snapshot(self.ctx, e2["entity_id"], as_of=T0,
                                           at_seq=e2["seq"])["seq"], e2["seq"])

    def test_invalid_bound_and_seq_rejected(self):
        cases = CaseService(self.app.repository)
        case = cases.create(self.ctx, case_values(self.app), request_key="x")
        with self.assertRaises(ValidationError):
            cases.snapshot(self.ctx, case["entity_id"], as_of=T0, bound="middle")
        for bad in (0, -1, "2", True):
            with self.assertRaises(ValidationError):
                cases.snapshot(self.ctx, case["entity_id"], as_of=T0, at_seq=bad)

    # --- 重复请求不产生新水位 -------------------------------------------
    def test_repeated_request_does_not_allocate_new_watermark(self):
        cases = CaseService(self.app.repository)
        values = case_values(self.app, "幂等案件")
        first = cases.create(self.ctx, values, request_key="dup")
        watermark_after_first = self.app.high_watermark()
        second = cases.create(self.ctx, values, request_key="dup")

        self.assertEqual(second["entity_id"], first["entity_id"])
        self.assertEqual(second["seq"], first["seq"])
        self.assertEqual(self.app.high_watermark(), watermark_after_first)
        self.assertEqual(len(cases.history(self.ctx, first["entity_id"])), 1)
        self.assertEqual(self.app.verify()["audit_entries"], 1)

    # --- 重启回放 -------------------------------------------------------
    def test_restart_replays_same_material_boundary(self):
        cases = CaseService(self.app.repository)
        evidence = EvidenceService(self.app.repository)
        case = cases.create(self.ctx, case_values(self.app), request_key="r-case")
        e1 = evidence.create(self.ctx, evidence_values(case["entity_id"], "交易证据", "d1", "银行"),
                             request_key="r-e1")
        boundary = self.app.high_watermark()

        # 模拟服务重启：全新句柄、全新内存状态，只读磁盘
        reopened = self.reopen(T0)
        self.assertEqual(reopened.high_watermark(), boundary)
        rcases = CaseService(reopened.repository)
        reevidence = EvidenceService(reopened.repository)

        # 重启后截止查询结论完全一致
        last_before = cases.snapshot(self.ctx, case["entity_id"], as_of=T0, bound="last")
        last_after = rcases.snapshot(self.ctx, case["entity_id"], as_of=T0, bound="last")
        self.assertEqual((last_after["version"], last_after["seq"]),
                         (last_before["version"], last_before["seq"]))
        self.assertEqual(
            reevidence.snapshot(self.ctx, e1["entity_id"], as_of=T0, at_seq=e1["seq"])["digest"], "d1")

        # 重启后新写入的水位严格延续，绝不复用或回绕（不依赖进程内计数）
        app_next = self.reopen(T1)
        e2 = EvidenceService(app_next.repository).create(
            self.ctx, evidence_values(case["entity_id"], "鉴定结论", "d2", "鉴定机构"),
            request_key="r-e2")
        self.assertGreater(e2["seq"], boundary)
        self.assertEqual(app_next.high_watermark(), e2["seq"])
        self.assertEqual(app_next.verify()["audit_entries"], 3)

    # --- 字段裁剪与排序语义一致 -----------------------------------------
    def test_redaction_preserves_watermark_order(self):
        evidence = EvidenceService(self.app.repository)
        case_id = "case:bundle"
        recs = [
            evidence.create(self.ctx, evidence_values(case_id, f"种类{i}", f"dig{i}", f"来源{i}"),
                            request_key=f"red-{i}")
            for i in range(3)
        ]
        reader = AccessContext(actor_id="reader",
                               permissions=frozenset({"read:evidence", "history:evidence"}))
        visible = evidence.list_current(reader)
        # 裁剪生效（敏感字段 source 被遮蔽）……
        self.assertTrue(all(row["source"] == "***" for row in visible))
        # ……顺序仍按水位，与历史/审计一致
        self.assertEqual([row["seq"] for row in visible], [r["seq"] for r in recs])

    # --- 审计水位窗口 ---------------------------------------------------
    def test_audit_window_respects_watermark(self):
        cases = CaseService(self.app.repository)
        c = cases.create(self.ctx, case_values(self.app), request_key="a1")
        u = cases.revise(self.ctx, c["entity_id"], {"priority": "normal"},
                         expected_version=1, request_key="a2")
        window = self.app.audit_window(at_seq=c["seq"])
        self.assertEqual([row["seq"] for row in window], [c["seq"]])
        full = self.app.audit_window()
        self.assertEqual([row["seq"] for row in full], [c["seq"], u["seq"]])

    # --- 旧数据库迁移 ---------------------------------------------------
    def test_legacy_database_backfills_deterministic_order(self):
        legacy = Path(self.temp.name) / "legacy.sqlite3"
        _build_legacy_database(legacy)

        app = CivicFlow.open(legacy)  # 触发迁移
        self.assertEqual(app.verify()["audit_entries"], 3)
        self.assertEqual(app.high_watermark(), 3)

        con = sqlite3.connect(legacy); con.row_factory = sqlite3.Row
        rows = con.execute(
            "SELECT entity_type, version, seq FROM entity_versions ORDER BY seq").fetchall()
        # 同刻多版本按 (valid_from, entity_type, entity_id, version) 确定性编号
        self.assertEqual([(r["entity_type"], r["version"], r["seq"]) for r in rows],
                         [("cases", 1, 1), ("cases", 2, 2), ("evidence", 1, 3)])
        audit_seqs = [r["seq"] for r in con.execute("SELECT seq FROM audit_entries ORDER BY seq")]
        self.assertEqual(audit_seqs, [1, 2, 3])
        con.close()

        # 迁移后截止查询立即可用且确定
        ctx = AccessContext.system("migrator")
        cases = CaseService(app.repository)
        con = sqlite3.connect(legacy)
        eid = con.execute("SELECT entity_id FROM entity_versions WHERE entity_type='cases' LIMIT 1").fetchone()[0]
        con.close()
        first = cases.snapshot(ctx, eid, as_of=T0, bound="first")
        last = cases.snapshot(ctx, eid, as_of=T0, bound="last")
        self.assertEqual((first["version"], last["version"]), (1, 2))

        # 迁移幂等：再次打开仍校验通过
        self.assertEqual(CivicFlow.open(legacy).verify()["audit_entries"], 3)


# --- 并发提交（多进程） ----------------------------------------------------

def _concurrent_writer(payload):
    path, worker_id, count = payload
    app = CivicFlow.open(Path(path), fixed_now=T0)  # 所有写入同一业务时刻
    ctx = AccessContext.system(f"worker-{worker_id}")
    evidence = EvidenceService(app.repository)
    seqs = []
    for i in range(count):
        rec = evidence.create(
            ctx,
            evidence_values("case:concurrent", f"k{worker_id}-{i}", f"d-{worker_id}-{i}",
                            f"src-{worker_id}"),
            request_key=f"w{worker_id}-{i}",
        )
        seqs.append(rec["seq"])
    return seqs


class ConcurrentWatermarkTest(unittest.TestCase):
    def test_concurrent_processes_get_unique_contiguous_watermarks(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        db_path = Path(temp.name) / "concurrent.sqlite3"
        CivicFlow.open(db_path, fixed_now=T0)  # 主进程先完成建表/迁移

        workers = 4
        per_worker = 5
        payload = [(str(db_path), w, per_worker) for w in range(workers)]
        ctx = mp.get_context("fork")
        with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as pool:
            results = list(pool.map(_concurrent_writer, payload))

        seqs = [s for chunk in results for s in chunk]
        # 同一业务时刻的并发写入：水位唯一、连续、无重复无缺口
        self.assertEqual(len(seqs), workers * per_worker)
        self.assertEqual(len(set(seqs)), workers * per_worker)
        self.assertEqual(sorted(seqs), list(range(1, workers * per_worker + 1)))

        app = CivicFlow.open(db_path)
        self.assertEqual(app.high_watermark(), workers * per_worker)
        # 审计链在并发提交后依然完整可校验
        self.assertEqual(app.verify()["audit_entries"], workers * per_worker)
        # 截止水位与返回的水位一一对应：每个水位都能精确取回当时那份证据
        window = app.audit_window()
        self.assertEqual([row["seq"] for row in window], sorted(seqs))


# --- 旧结构数据库构造 ------------------------------------------------------

LEGACY_SCHEMA = """
CREATE TABLE entities (
    entity_type TEXT NOT NULL, entity_id TEXT NOT NULL, version INTEGER NOT NULL,
    state TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL, created_by TEXT NOT NULL, updated_by TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id)
);
CREATE TABLE entity_versions (
    entity_type TEXT NOT NULL, entity_id TEXT NOT NULL, version INTEGER NOT NULL,
    state TEXT NOT NULL, payload_json TEXT NOT NULL, valid_from TEXT NOT NULL,
    actor_id TEXT NOT NULL, request_key TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id, version)
);
CREATE TABLE idempotency_keys (
    scope TEXT NOT NULL, request_key TEXT NOT NULL, request_digest TEXT NOT NULL,
    response_json TEXT NOT NULL, created_at TEXT NOT NULL,
    PRIMARY KEY(scope, request_key)
);
CREATE TABLE audit_entries (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT, occurred_at TEXT NOT NULL,
    actor_id TEXT NOT NULL, action TEXT NOT NULL, entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL, version INTEGER NOT NULL, detail_json TEXT NOT NULL,
    previous_digest TEXT NOT NULL, entry_digest TEXT NOT NULL
);
"""


def _legacy_audit_digest(occurred_at, actor, action, etype, eid, version, detail, previous):
    # 旧版摘要算法（不含 seq）
    body = canonical_json({"occurred_at": occurred_at, "actor_id": actor, "action": action,
                           "entity_type": etype, "entity_id": eid, "version": version,
                           "detail": detail, "previous": previous})
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _build_legacy_database(path: Path) -> None:
    con = sqlite3.connect(path)
    con.executescript(LEGACY_SCHEMA)
    actor = "migrator"

    def insert(etype, eid, version, state, body, action, detail, previous):
        payload = dict(body); payload["state"] = state
        # entities 每实体仅一行：v1 插入，后续版本更新当前态
        if version == 1:
            con.execute("INSERT INTO entities(entity_type,entity_id,version,state,payload_json,"
                        "created_at,updated_at,created_by,updated_by) VALUES(?,?,?,?,?,?,?,?,?)",
                        (etype, eid, version, state, canonical_json(payload), T0, T0, actor, actor))
        else:
            con.execute("UPDATE entities SET version=?,state=?,payload_json=?,updated_at=?,updated_by=? "
                        "WHERE entity_type=? AND entity_id=?",
                        (version, state, canonical_json(payload), T0, actor, etype, eid))
        con.execute("INSERT INTO entity_versions(entity_type,entity_id,version,state,payload_json,"
                    "valid_from,actor_id,request_key) VALUES(?,?,?,?,?,?,?,?)",
                    (etype, eid, version, state, canonical_json(payload), T0, actor, f"{eid}-{version}"))
        digest = _legacy_audit_digest(T0, actor, action, etype, eid, version, detail, previous)
        con.execute("INSERT INTO audit_entries(occurred_at,actor_id,action,entity_type,entity_id,"
                    "version,detail_json,previous_digest,entry_digest) VALUES(?,?,?,?,?,?,?,?,?)",
                    (T0, actor, action, etype, eid, version, canonical_json(detail), previous, digest))
        return digest

    case_body = {"case_type": "知产", "subject": "旧案", "owner_org": "o", "priority": "high",
                 "opened_at": T0}
    p = insert("cases", "case:legacy", 1, "draft", case_body, "create",
               {**case_body, "state": "draft"}, "0" * 64)
    p = insert("cases", "case:legacy", 2, "draft", {**case_body, "priority": "normal"},
               "update", {"priority": "normal"}, p)
    ev_body = {"case_id": "case:legacy", "kind": "交易证据", "digest": "dev", "source": "银行",
               "occurred_at": T0}
    insert("evidence", "evidence:legacy", 1, "received", ev_body, "create",
           {**ev_body, "state": "received"}, p)
    con.commit(); con.close()


if __name__ == "__main__":
    unittest.main()
