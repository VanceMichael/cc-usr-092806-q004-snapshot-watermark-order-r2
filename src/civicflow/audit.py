"""追加式审计链。

每条审计记录都带顺序水位 ``seq``，该水位与对应实体版本的水位一致；
摘要把 ``seq`` 纳入哈希，链的串联与校验一律按 ``seq`` 排序，从而与
案件历史、截止快照、字段裁剪使用完全相同的顺序语义，而不依赖
``AUTOINCREMENT`` 的插入编号或进程内计数。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass

from .errors import InvariantViolation
from .jsonutil import canonical_json
from .timeutil import Clock


def entry_digest(*, seq: int, occurred_at: str, actor_id: str, action: str, entity_type: str,
                 entity_id: str, version: int, detail: object, previous: str) -> str:
    body = canonical_json({
        "seq": seq,
        "occurred_at": occurred_at,
        "actor_id": actor_id,
        "action": action,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "version": version,
        "detail": detail,
        "previous": previous,
    })
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AuditLog:
    clock: Clock

    def append(self, connection: sqlite3.Connection, *, seq: int, actor_id: str, action: str,
               entity_type: str, entity_id: str, version: int, detail: dict) -> str:
        row = connection.execute("SELECT entry_digest FROM audit_entries ORDER BY seq DESC LIMIT 1").fetchone()
        previous = row["entry_digest"] if row else "0" * 64
        occurred_at = self.clock.now()
        digest = entry_digest(seq=seq, occurred_at=occurred_at, actor_id=actor_id, action=action,
                              entity_type=entity_type, entity_id=entity_id, version=version,
                              detail=detail, previous=previous)
        connection.execute(
            "INSERT INTO audit_entries(seq,occurred_at,actor_id,action,entity_type,entity_id,version,detail_json,previous_digest,entry_digest) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (seq, occurred_at, actor_id, action, entity_type, entity_id, version,
             canonical_json(detail), previous, digest),
        )
        return digest

    def verify(self, connection: sqlite3.Connection) -> int:
        previous = "0" * 64
        count = 0
        for row in connection.execute("SELECT * FROM audit_entries ORDER BY seq"):
            expected = entry_digest(
                seq=row["seq"], occurred_at=row["occurred_at"], actor_id=row["actor_id"],
                action=row["action"], entity_type=row["entity_type"], entity_id=row["entity_id"],
                version=row["version"], detail=json.loads(row["detail_json"]), previous=previous,
            )
            if row["previous_digest"] != previous or row["entry_digest"] != expected:
                raise InvariantViolation(f"审计链在水位 {row['seq']} 处不连续")
            previous = expected
            count += 1
        return count

    def entries(self, connection: sqlite3.Connection, *, at_seq: int | None = None,
                entity_type: str | None = None, entity_id: str | None = None) -> list[dict]:
        """按与历史相同的水位顺序返回审计记录。

        给定 ``at_seq`` 时只返回该水位（含）之前已可见的记录，移交人员
        据此说明"在该水位能看到哪些证据与结论"；可选按实体过滤。
        """
        sql = "SELECT * FROM audit_entries"; clauses: list[str] = []; params: list[object] = []
        if at_seq is not None:
            if isinstance(at_seq, bool) or not isinstance(at_seq, int) or at_seq < 1:
                raise ValueError("水位必须是不小于 1 的整数")
            clauses.append("seq<=?"); params.append(at_seq)
        if entity_type is not None:
            clauses.append("entity_type=?"); params.append(entity_type)
        if entity_id is not None:
            clauses.append("entity_id=?"); params.append(entity_id)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY seq"
        return [
            {
                "seq": row["seq"], "occurred_at": row["occurred_at"], "actor_id": row["actor_id"],
                "action": row["action"], "entity_type": row["entity_type"],
                "entity_id": row["entity_id"], "version": row["version"],
                "detail": json.loads(row["detail_json"]), "entry_digest": row["entry_digest"],
            }
            for row in connection.execute(sql, params)
        ]
