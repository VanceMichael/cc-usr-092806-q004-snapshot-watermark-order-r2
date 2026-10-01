"""追加式审计链。

审计项与实体版本共用同一排序语义：每条审计项记录对应版本的持久水位
``watermark``，链路按 ``(watermark, audit_id)`` 回放。这样审计人员可以明确
说明某份证据在某次移交的截止水位上是否已经可见，而审计链给出的先后次序
与案件历史、字段裁剪后的快照完全一致，且重启或跨机器回放结果不变。

水位改造前写入的旧审计项没有水位（以 0 落库），其哈希体也不含水位字段；
验证时按落库时的格式重算，因此历史链仍然可校验。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass

from .errors import InvariantViolation
from .jsonutil import canonical_json
from .timeutil import Clock


@dataclass(frozen=True)
class AuditLog:
    clock: Clock

    def append(self, connection: sqlite3.Connection, *, actor_id: str, action: str, entity_type: str, entity_id: str, version: int, detail: dict, watermark: int) -> str:
        row = connection.execute("SELECT entry_digest FROM audit_entries ORDER BY watermark DESC, audit_id DESC LIMIT 1").fetchone()
        previous = row["entry_digest"] if row else "0" * 64
        occurred_at = self.clock.now()
        body_dict = {"occurred_at": occurred_at, "actor_id": actor_id, "action": action, "entity_type": entity_type, "entity_id": entity_id, "version": version, "detail": detail, "watermark": watermark, "previous": previous}
        body = canonical_json(body_dict)
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        connection.execute("INSERT INTO audit_entries(occurred_at,actor_id,action,entity_type,entity_id,version,detail_json,previous_digest,entry_digest,watermark) VALUES(?,?,?,?,?,?,?,?,?,?)", (occurred_at, actor_id, action, entity_type, entity_id, version, canonical_json(detail), previous, digest, watermark))
        return digest

    def verify(self, connection: sqlite3.Connection) -> int:
        previous = "0" * 64
        count = 0
        # 与历史/快照相同的排序键：先按持久水位，再以自增 audit_id 兜底
        # （旧条目水位为 0 时仍保持其落库次序）。
        rows = connection.execute("SELECT * FROM audit_entries ORDER BY watermark, audit_id").fetchall()
        for row in rows:
            detail = json.loads(row["detail_json"])
            entry = {"occurred_at": row["occurred_at"], "actor_id": row["actor_id"], "action": row["action"], "entity_type": row["entity_type"], "entity_id": row["entity_id"], "version": row["version"], "detail": detail}
            if row["watermark"] != 0:
                # 新格式：水位参与哈希，顺序不可被时间戳碰撞或回放打乱。
                entry["watermark"] = row["watermark"]
            entry["previous"] = previous
            expected = hashlib.sha256(canonical_json(entry).encode("utf-8")).hexdigest()
            if row["previous_digest"] != previous or row["entry_digest"] != expected:
                raise InvariantViolation(f"审计链在 {row['audit_id']} 处不连续")
            previous = expected
            count += 1
        return count
