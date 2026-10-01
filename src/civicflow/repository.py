"""版本化实体持久化。

排序语义（全平台唯一）：每个实体版本在提交事务内获得一个持久化全局
顺序水位 ``seq``。历史、截止快照、列表与审计链都以水位为先后依据；
同一业务时刻（``valid_from`` 相同）的多个版本靠水位区分先后，因此
最先可见 / 最后可见 / 指定水位三种截止查询都有确定且可复现的答案。
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, NotFoundError, ValidationError
from .identifiers import new_id, require_safe
from .idempotency import IdempotencyStore
from .jsonutil import canonical_json
from .timeutil import Clock, canonical_instant
from . import watermarks

# 截止查询的可见边界：
#   first —— 该业务时刻最先可见的版本（水位最低）
#   last  —— 该业务时刻最后可见的版本（水位最高，向后兼容的默认值）
SNAPSHOT_BOUNDS = ("first", "last")


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
                seq = watermarks.allocate(connection)
                connection.execute("INSERT INTO entities(entity_type,entity_id,version,state,payload_json,created_at,updated_at,created_by,updated_by) VALUES(?,?,?,?,?,?,?,?,?)", (entity_type, entity_id, 1, state, canonical_json(body), now, now, actor, actor))
                connection.execute("INSERT INTO entity_versions(entity_type,entity_id,version,state,payload_json,valid_from,actor_id,request_key,seq) VALUES(?,?,?,?,?,?,?,?,?)", (entity_type, entity_id, 1, state, canonical_json(body), now, actor, request_key, seq))
                self.audit.append(connection, seq=seq, actor_id=actor, action="create", entity_type=entity_type, entity_id=entity_id, version=1, detail=body)
                return self._load_current(connection, entity_type, entity_id)
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
                seq = watermarks.allocate(connection)
                connection.execute("INSERT INTO entity_versions(entity_type,entity_id,version,state,payload_json,valid_from,actor_id,request_key,seq) VALUES(?,?,?,?,?,?,?,?,?)", (entity_type, entity_id, version, state, canonical_json(payload), now, actor, request_key, seq))
                self.audit.append(connection, seq=seq, actor_id=actor, action="update", entity_type=entity_type, entity_id=entity_id, version=version, detail=changes)
                return self._load_current(connection, entity_type, entity_id)
            return self.idempotency.execute(connection, scope=f"update:{entity_type}:{entity_id}", request_key=request_key, request={"changes": changes, "expected_version": expected_version}, operation=operation)

    def get(self, entity_type: str, entity_id: str) -> dict:
        with self.database.connect() as connection:
            row = self._select_current(connection, entity_type, entity_id).fetchone()
            if not row:
                raise NotFoundError(f"{entity_type}/{entity_id} 不存在")
            return self._row_to_dict(row)

    def list(self, entity_type: str, *, state: str | None = None, limit: int = 100) -> list[dict]:
        if limit < 1 or limit > 500:
            raise ValidationError("limit 必须在 1 到 500 之间")
        sql = ("SELECT e.*, v.seq AS seq FROM entities e "
               "JOIN entity_versions v ON v.entity_type=e.entity_type "
               "AND v.entity_id=e.entity_id AND v.version=e.version "
               "WHERE e.entity_type=?")
        params: list[object] = [entity_type]
        if state is not None:
            sql += " AND e.state=?"; params.append(state)
        # 按当前版本水位排序：与历史、审计完全一致的先后语义，时刻相同也稳定。
        sql += " ORDER BY v.seq LIMIT ?"; params.append(limit)
        with self.database.connect() as connection:
            return [self._row_to_dict(row) for row in connection.execute(sql, params)]

    def search(self, entity_type: str, field: str, value: object, *, limit: int = 100) -> list[dict]:
        rows = self.list(entity_type, limit=500)
        return [row for row in rows if row.get(field) == value][:limit]

    def history(self, entity_type: str, entity_id: str) -> list[dict]:
        with self.database.connect() as connection:
            # 水位即权威先后：同一实体内 seq 与 version 同向，且跨实体可比。
            rows = connection.execute("SELECT * FROM entity_versions WHERE entity_type=? AND entity_id=? ORDER BY seq", (entity_type, entity_id)).fetchall()
            return [self._version_to_dict(row) for row in rows]

    def snapshot(self, entity_type: str, entity_id: str, *, as_of: str,
                 bound: str = "last", at_seq: int | None = None) -> dict:
        """读取业务时刻 ``as_of`` 的截止版本。

        ``at_seq`` 给定时返回该水位（含）之前可见的版本，用于复核"某份
        证据在当时是否可见"；否则按 ``bound`` 选择该时刻最先/最后可见
        的版本。同一业务时刻的多个版本以水位定先后，结论持久且可复现。
        """
        instant = canonical_instant(as_of)
        with self.database.connect() as connection:
            if at_seq is not None:
                at_seq = self._require_seq(at_seq)
                row = connection.execute(
                    "SELECT * FROM entity_versions WHERE entity_type=? AND entity_id=? "
                    "AND valid_from<=? AND seq<=? ORDER BY seq DESC LIMIT 1",
                    (entity_type, entity_id, instant, at_seq),
                ).fetchone()
            else:
                if bound not in SNAPSHOT_BOUNDS:
                    raise ValidationError("bound 只能是 first 或 last")
                # last：时刻 T 那一组里水位最高；first：同一组里水位最低，
                # 二者都先锁定不晚于 T 的最晚业务时刻，再用水位定先后。
                seq_direction = "DESC" if bound == "last" else "ASC"
                row = connection.execute(
                    f"SELECT * FROM entity_versions WHERE entity_type=? AND entity_id=? "
                    f"AND valid_from<=? ORDER BY valid_from DESC, seq {seq_direction} LIMIT 1",
                    (entity_type, entity_id, instant),
                ).fetchone()
            if not row:
                raise NotFoundError("指定时点没有可见版本")
            return self._version_to_dict(row)

    @staticmethod
    def _require_seq(value: object) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValidationError("水位必须是不小于 1 的整数")
        return value

    @staticmethod
    def _select_current(connection, entity_type: str, entity_id: str):
        return connection.execute(
            "SELECT e.*, v.seq AS seq FROM entities e "
            "JOIN entity_versions v ON v.entity_type=e.entity_type "
            "AND v.entity_id=e.entity_id AND v.version=e.version "
            "WHERE e.entity_type=? AND e.entity_id=?",
            (entity_type, entity_id),
        )

    def _load_current(self, connection, entity_type: str, entity_id: str) -> dict:
        return self._row_to_dict(self._select_current(connection, entity_type, entity_id).fetchone())

    @staticmethod
    def _row_to_dict(row) -> dict:
        payload = json.loads(row["payload_json"]); payload.update({"entity_type": row["entity_type"], "entity_id": row["entity_id"], "version": row["version"], "state": row["state"], "seq": row["seq"], "created_at": row["created_at"], "updated_at": row["updated_at"], "created_by": row["created_by"], "updated_by": row["updated_by"]}); return payload

    @staticmethod
    def _version_to_dict(row) -> dict:
        payload = json.loads(row["payload_json"]); payload.update({"entity_type": row["entity_type"], "entity_id": row["entity_id"], "version": row["version"], "state": row["state"], "seq": row["seq"], "valid_from": row["valid_from"], "actor_id": row["actor_id"], "request_key": row["request_key"]}); return payload
