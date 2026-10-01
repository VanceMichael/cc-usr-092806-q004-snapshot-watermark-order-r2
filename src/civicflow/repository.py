"""版本化实体持久化。

排序语义（历史、字段裁剪后的快照、审计链共用同一套）：

    排序键 = (valid_from, watermark)

``valid_from`` 是业务时刻；``watermark`` 是写入时在事务内从持久游标分配的
全局单调水位（见 :mod:`civicflow.watermarks`），它让同一业务时刻的多个版本
拥有稳定、持久的先后次序，且与进程、机器、回放时机无关。旧库迁移而来的
版本使用非正水位，同样落在这个全序上。

截止查询（snapshot）支持三种边界：

* ``first``     —— 该时点最先可见的版本；
* ``last``      —— 该时点最后可见的版本（默认）；
* ``watermark`` —— 在指定水位截止，返回水位不超过它的最新可见版本。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, NotFoundError, ValidationError
from .identifiers import new_id, require_safe
from .idempotency import IdempotencyStore
from .jsonutil import canonical_json
from .timeutil import Clock, canonical_instant
from .watermarks import next_watermark

SNAPSHOT_BOUNDS = ("first", "last", "watermark")


@dataclass(frozen=True)
class EntityRepository:
    database: Database
    clock: Clock
    audit: AuditLog
    idempotency: IdempotencyStore

    def create(self, entity_type: str, payload: dict, *, actor: str, request_key: str) -> dict:
        require_safe(entity_type, "实体类型")
        with self.database.transaction() as connection:
            def operation() -> dict:
                entity_id = new_id(entity_type)
                now = self.clock.now()
                state = str(payload.get("state", "draft"))
                body = dict(payload)
                body["state"] = state
                watermark = next_watermark(connection)
                connection.execute("INSERT INTO entities(entity_type,entity_id,version,state,payload_json,created_at,updated_at,created_by,updated_by,watermark) VALUES(?,?,?,?,?,?,?,?,?,?)", (entity_type, entity_id, 1, state, canonical_json(body), now, now, actor, actor, watermark))
                connection.execute("INSERT INTO entity_versions(entity_type,entity_id,version,state,payload_json,valid_from,actor_id,request_key,watermark) VALUES(?,?,?,?,?,?,?,?,?)", (entity_type, entity_id, 1, state, canonical_json(body), now, actor, request_key, watermark))
                self.audit.append(connection, actor_id=actor, action="create", entity_type=entity_type, entity_id=entity_id, version=1, detail=body, watermark=watermark)
                return self._row_to_dict(connection.execute("SELECT * FROM entities WHERE entity_type=? AND entity_id=?", (entity_type, entity_id)).fetchone())
            return self.idempotency.execute(connection, scope=f"create:{entity_type}", request_key=request_key, request=payload, operation=operation)

    def update(self, entity_type: str, entity_id: str, changes: dict, *, actor: str, expected_version: int, request_key: str) -> dict:
        if not changes:
            raise ValidationError("修改内容不能为空")
        with self.database.transaction() as connection:
            def operation() -> dict:
                row = connection.execute("SELECT * FROM entities WHERE entity_type=? AND entity_id=?", (entity_type, entity_id)).fetchone()
                if not row:
                    raise NotFoundError(f"{entity_type}/{entity_id} 不存在")
                if row["version"] != expected_version:
                    raise ConflictError(f"版本冲突，当前为 {row['version']}")
                payload = json.loads(row["payload_json"]); payload.update(changes)
                version = expected_version + 1
                state = str(payload.get("state", row["state"]))
                now = self.clock.now()
                changed = connection.execute("UPDATE entities SET version=?,state=?,payload_json=?,updated_at=?,updated_by=? WHERE entity_type=? AND entity_id=? AND version=?", (version, state, canonical_json(payload), now, actor, entity_type, entity_id, expected_version)).rowcount
                if changed != 1:
                    raise ConflictError("并发修改导致版本变化")
                # 水位在乐观锁判定通过后分配，且与版本行、审计项处于同一事务：
                # 提交后一起持久可见，回滚则全部消失。
                watermark = next_watermark(connection)
                connection.execute("UPDATE entities SET watermark=? WHERE entity_type=? AND entity_id=?", (watermark, entity_type, entity_id))
                connection.execute("INSERT INTO entity_versions(entity_type,entity_id,version,state,payload_json,valid_from,actor_id,request_key,watermark) VALUES(?,?,?,?,?,?,?,?,?)", (entity_type, entity_id, version, state, canonical_json(payload), now, actor, request_key, watermark))
                self.audit.append(connection, actor_id=actor, action="update", entity_type=entity_type, entity_id=entity_id, version=version, detail=changes, watermark=watermark)
                return self._row_to_dict(connection.execute("SELECT * FROM entities WHERE entity_type=? AND entity_id=?", (entity_type, entity_id)).fetchone())
            return self.idempotency.execute(connection, scope=f"update:{entity_type}:{entity_id}", request_key=request_key, request={"changes": changes, "expected_version": expected_version}, operation=operation)

    def get(self, entity_type: str, entity_id: str) -> dict:
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM entities WHERE entity_type=? AND entity_id=?", (entity_type, entity_id)).fetchone()
            if not row:
                raise NotFoundError(f"{entity_type}/{entity_id} 不存在")
            return self._row_to_dict(row)

    def list(self, entity_type: str, *, state: str | None = None, limit: int = 100) -> list[dict]:
        if limit < 1 or limit > 500:
            raise ValidationError("limit 必须在 1 到 500 之间")
        sql = "SELECT * FROM entities WHERE entity_type=?"; params: list[object] = [entity_type]
        if state is not None:
            sql += " AND state=?"; params.append(state)
        sql += " ORDER BY updated_at, watermark, entity_id LIMIT ?"; params.append(limit)
        with self.database.connect() as connection:
            return [self._row_to_dict(row) for row in connection.execute(sql, params)]

    def search(self, entity_type: str, field: str, value: object, *, limit: int = 100) -> list[dict]:
        rows = self.list(entity_type, limit=500)
        return [row for row in rows if row.get(field) == value][:limit]

    def history(self, entity_type: str, entity_id: str) -> list[dict]:
        """按统一排序键 (valid_from, watermark) 返回该实体的全部版本。"""
        with self.database.connect() as connection:
            rows = connection.execute("SELECT * FROM entity_versions WHERE entity_type=? AND entity_id=? ORDER BY valid_from, watermark, version", (entity_type, entity_id)).fetchall()
            return [self._version_to_dict(row) for row in rows]

    def snapshot(
        self,
        entity_type: str,
        entity_id: str,
        *,
        as_of: str,
        bound: str = "last",
        watermark: int | None = None,
    ) -> dict:
        """按业务时刻 as_of 做截止查询。

        ``bound`` 选择同一时刻的版本边界：

        * ``"first"``：最先可见（valid_from 最早、水位最小）；
        * ``"last"``（默认）：最后可见（valid_from 最晚、水位最大）；
        * ``"watermark"``：额外要求版本水位不超过传入的 ``watermark``，
          再取最后可见的一版，用于精确复核某次移交时的材料边界。
        """
        instant = canonical_instant(as_of)
        if bound not in SNAPSHOT_BOUNDS:
            raise ValidationError("bound 必须是 first、last 或 watermark")
        if bound == "watermark":
            if not isinstance(watermark, int) or isinstance(watermark, bool):
                raise ValidationError("指定水位时必须提供整数 watermark")
            sql = (
                "SELECT * FROM entity_versions "
                "WHERE entity_type=? AND entity_id=? AND valid_from<=? AND watermark<=? "
                "ORDER BY valid_from DESC, watermark DESC LIMIT 1"
            )
            params: tuple[object, ...] = (entity_type, entity_id, instant, watermark)
        elif bound == "first":
            sql = (
                "SELECT * FROM entity_versions "
                "WHERE entity_type=? AND entity_id=? AND valid_from<=? "
                "ORDER BY valid_from ASC, watermark ASC LIMIT 1"
            )
            params = (entity_type, entity_id, instant)
        else:
            sql = (
                "SELECT * FROM entity_versions "
                "WHERE entity_type=? AND entity_id=? AND valid_from<=? "
                "ORDER BY valid_from DESC, watermark DESC LIMIT 1"
            )
            params = (entity_type, entity_id, instant)
        with self.database.connect() as connection:
            row = connection.execute(sql, params).fetchone()
            if not row:
                raise NotFoundError("指定时点没有可见版本")
            return self._version_to_dict(row)

    def current_watermark(self) -> int:
        """返回持久游标当前的最高水位（供移交说明与校验使用）。"""
        with self.database.connect() as connection:
            row = connection.execute("SELECT value FROM write_cursor WHERE cursor_key='global'").fetchone()
            return int(row["value"]) if row else 0

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict:
        payload = json.loads(row["payload_json"]); payload.update({"entity_type": row["entity_type"], "entity_id": row["entity_id"], "version": row["version"], "state": row["state"], "created_at": row["created_at"], "updated_at": row["updated_at"], "created_by": row["created_by"], "updated_by": row["updated_by"], "order_watermark": row["watermark"]}); return payload

    @staticmethod
    def _version_to_dict(row: sqlite3.Row) -> dict:
        payload = json.loads(row["payload_json"]); payload.update({"entity_type": row["entity_type"], "entity_id": row["entity_id"], "version": row["version"], "state": row["state"], "valid_from": row["valid_from"], "actor_id": row["actor_id"], "request_key": row["request_key"], "order_watermark": row["watermark"]}); return payload
