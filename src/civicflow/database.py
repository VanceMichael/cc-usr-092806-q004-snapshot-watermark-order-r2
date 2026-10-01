"""SQLite 连接、事务和数据库初始化。

所有版本化写入在提交事务内从 ``sequence_watermarks`` 取一个严格递增、
持久化的全局顺序水位 ``seq``。历史快照、字段裁剪前的读取和审计链一律
按 ``(valid_from, seq)`` 这同一语义排序，避免同一业务时刻（同一
``valid_from``）的多个版本被时间戳坍缩成一个结果，也不依赖任何进程内
计数器。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .jsonutil import canonical_json


SCHEMA = r"""
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS entities (
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
CREATE TABLE IF NOT EXISTS entity_versions (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    seq INTEGER NOT NULL,
    PRIMARY KEY(entity_type, entity_id, version)
);
-- 全局持久化顺序水位。单行计数器，写入在 IMMEDIATE 事务内完成，
-- 因而跨进程、跨重启单调递增；不使用任何进程内计数。
CREATE TABLE IF NOT EXISTS sequence_watermarks (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK(singleton = 1),
    last_seq INTEGER NOT NULL
);
INSERT OR IGNORE INTO sequence_watermarks(singleton, last_seq) VALUES(1, 0);
CREATE TABLE IF NOT EXISTS idempotency_keys (
    scope TEXT NOT NULL,
    request_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, request_key)
);
CREATE TABLE IF NOT EXISTS audit_entries (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    seq INTEGER NOT NULL,
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
CREATE TABLE IF NOT EXISTS inbox_messages (
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    payload_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY(source, source_key, sequence)
);
CREATE TABLE IF NOT EXISTS inbox_conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    existing_digest TEXT NOT NULL,
    incoming_digest TEXT NOT NULL,
    received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox_messages (
    message_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    available_at TEXT NOT NULL,
    lease_until TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    delivered_at TEXT
);
CREATE INDEX IF NOT EXISTS outbox_ready ON outbox_messages(status, available_at, lease_until);
CREATE TABLE IF NOT EXISTS journal_entries (
    entry_id TEXT PRIMARY KEY,
    journal_key TEXT NOT NULL,
    account TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    direction TEXT NOT NULL,
    reference TEXT NOT NULL,
    reversed_entry_id TEXT,
    occurred_at TEXT NOT NULL,
    posted_by TEXT NOT NULL,
    FOREIGN KEY(reversed_entry_id) REFERENCES journal_entries(entry_id)
);
CREATE INDEX IF NOT EXISTS journal_reference ON journal_entries(journal_key, reference, occurred_at);
CREATE TABLE IF NOT EXISTS resource_reservations (
    reservation_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_by TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reservation_window ON resource_reservations(resource_id, start_at, end_at, status);
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    run_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    lease_until TEXT,
    last_error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS jobs_due ON scheduled_jobs(status, run_at, lease_until);
"""

# 历史快照、列表和审计共同使用的确定性排序：先业务时刻，再顺序水位。
ASOF_ORDER = "valid_from, seq"


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}


def _rebuild_audit_chain(connection: sqlite3.Connection) -> None:
    """按顺序水位重算审计链的前后摘要。"""
    previous = "0" * 64
    rows = connection.execute("SELECT * FROM audit_entries ORDER BY seq").fetchall()
    for row in rows:
        detail = json.loads(row["detail_json"])
        body = canonical_json({
            "seq": row["seq"],
            "occurred_at": row["occurred_at"],
            "actor_id": row["actor_id"],
            "action": row["action"],
            "entity_type": row["entity_type"],
            "entity_id": row["entity_id"],
            "version": row["version"],
            "detail": detail,
            "previous": previous,
        })
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        connection.execute(
            "UPDATE audit_entries SET previous_digest=?, entry_digest=? WHERE audit_id=?",
            (previous, digest, row["audit_id"]),
        )
        previous = digest


def _backfill_entity_seq(connection: sqlite3.Connection) -> bool:
    """旧库 entity_versions 没有 seq 列时补列并确定性回填。返回是否发生迁移。"""
    if "seq" in _columns(connection, "entity_versions"):
        return False
    connection.execute("ALTER TABLE entity_versions ADD COLUMN seq INTEGER")
    # 先按业务时刻 valid_from，时刻相同再按 (实体类型, 实体标识, 版本) 这一
    # 与机器、回放顺序无关的稳定全序键排列，依次编号。
    connection.execute(
        """
        UPDATE entity_versions
           SET seq = migrated.seq
          FROM (
            SELECT entity_type, entity_id, version,
                   ROW_NUMBER() OVER (
                     ORDER BY valid_from, entity_type, entity_id, version
                   ) AS seq
              FROM entity_versions
          ) AS migrated
         WHERE entity_versions.entity_type = migrated.entity_type
           AND entity_versions.entity_id = migrated.entity_id
           AND entity_versions.version = migrated.version
        """
    )
    connection.execute(
        """
        UPDATE sequence_watermarks
           SET last_seq = max(last_seq,
                 (SELECT COALESCE(MAX(seq), 0) FROM entity_versions))
         WHERE singleton = 1
        """
    )
    return True


def _backfill_audit_seq(connection: sqlite3.Connection) -> bool:
    """旧库 audit_entries 没有 seq 列时补列、对齐水位并重算审计链。"""
    if "seq" in _columns(connection, "audit_entries"):
        return False
    connection.execute("ALTER TABLE audit_entries ADD COLUMN seq INTEGER")
    # 审计水位优先与对应版本水位对齐：(类型,标识,版本) 关联 entity_versions。
    connection.execute(
        """
        UPDATE audit_entries
           SET seq = matched.seq
          FROM (
            SELECT a.audit_id, v.seq
              FROM audit_entries a
              JOIN entity_versions v
                ON v.entity_type = a.entity_type
               AND v.entity_id = a.entity_id
               AND v.version = a.version
          ) AS matched
         WHERE audit_entries.audit_id = matched.audit_id
        """
    )
    # 极少数关联不到版本的审计行，按稳定键排在已知水位之后继续编号。
    if connection.execute("SELECT COUNT(*) AS n FROM audit_entries WHERE seq IS NULL").fetchone()["n"]:
        gap = connection.execute("SELECT COALESCE(MAX(seq), 0) AS m FROM audit_entries").fetchone()["m"]
        connection.execute(
            """
            UPDATE audit_entries
               SET seq = tail.seq
              FROM (
                SELECT audit_id,
                       ? + ROW_NUMBER() OVER (
                         ORDER BY occurred_at, entity_type, entity_id, version, audit_id
                       ) AS seq
                  FROM audit_entries WHERE seq IS NULL
              ) AS tail
             WHERE audit_entries.audit_id = tail.audit_id
            """,
            (gap,),
        )
    connection.execute(
        """
        UPDATE sequence_watermarks
           SET last_seq = max(last_seq,
                 (SELECT COALESCE(MAX(seq), 0) FROM audit_entries))
         WHERE singleton = 1
        """
    )
    _rebuild_audit_chain(connection)
    return True


def _migrate(connection: sqlite3.Connection) -> None:
    _backfill_entity_seq(connection)
    _backfill_audit_seq(connection)
    connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS entity_versions_seq ON entity_versions(seq)")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS entity_versions_asof "
        "ON entity_versions(entity_type, entity_id, valid_from, seq)"
    )
    connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS audit_entries_seq ON audit_entries(seq)")


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def initialize(self) -> None:
        # executescript 会隐式提交，先以自动提交方式建表，再在单一事务内迁移。
        with self.connect() as connection:
            connection.executescript(SCHEMA)
        with self.transaction() as connection:
            _migrate(connection)

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
