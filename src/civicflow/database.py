"""SQLite 连接、事务和数据库初始化。"""

from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


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
    watermark INTEGER NOT NULL,
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
    watermark INTEGER NOT NULL,
    PRIMARY KEY(entity_type, entity_id, version)
);
CREATE INDEX IF NOT EXISTS entity_versions_asof ON entity_versions(entity_type, entity_id, valid_from, watermark);
CREATE INDEX IF NOT EXISTS entity_versions_visible ON entity_versions(entity_type, entity_id, valid_from DESC, watermark DESC);
CREATE UNIQUE INDEX IF NOT EXISTS entity_versions_watermark ON entity_versions(watermark);
CREATE TABLE IF NOT EXISTS write_cursor (
    cursor_key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
INSERT OR IGNORE INTO write_cursor(cursor_key, value) VALUES('global', 0);
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
    occurred_at TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    detail_json TEXT NOT NULL,
    previous_digest TEXT NOT NULL,
    entry_digest TEXT NOT NULL,
    watermark INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS audit_entries_order ON audit_entries(watermark, audit_id);
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

class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        # busy_timeout 必须先于 WAL 切换：多进程/多机器并发首次打开时，
        # journal_mode=WAL 需要短暂排他锁，否则会立即抛 "database is locked"。
        connection.execute("PRAGMA busy_timeout=30000")
        self._set_wal_mode(connection)
        return connection

    @staticmethod
    def _set_wal_mode(connection: sqlite3.Connection) -> None:
        for attempt in range(30):
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                return
            except sqlite3.OperationalError:
                if attempt == 29:
                    raise
                time.sleep(0.05)

    def initialize(self) -> None:
        connection = self.connect()
        try:
            # 迁移放在一个立即写事务中：多进程同时升级同一旧库时，后来者会在
            # 锁上等待，待先行者提交后看到最终结构，避免重复 ALTER 竞态。
            connection.execute("BEGIN IMMEDIATE")
            self._migrate_legacy_watermarks(connection)
            connection.commit()
            # 新表/索引补齐必须在回填之后，否则唯一索引会在全 0 临时列上失败。
            connection.executescript(SCHEMA)
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _migrate_legacy_watermarks(self, connection: sqlite3.Connection) -> None:
        """把水位改造前的旧库补齐水位列并确定性回填。

        判断依据是表结构本身，因此重复执行（或升级后再次启动）是幂等的：
        加列幂等、回填只处理水位为 0 的行、且在唯一索引创建之前运行。
        """
        def columns(table: str) -> set[str]:
            return {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}

        # 全新数据库，或另一个进程正在用新 SCHEMA 逐条建表（此刻只看到部分
        # 新表）：两种情况都交给 SCHEMA 的 CREATE TABLE IF NOT EXISTS 处理，
        # 绝不进入旧库迁移。完整旧库必定同时包含下列三张表。
        existing = {
            row["name"]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if not {"entities", "entity_versions", "audit_entries"}.issubset(existing):
            return

        version_columns = columns("entity_versions")
        entity_columns = columns("entities")
        audit_columns = columns("audit_entries")

        if "watermark" not in entity_columns:
            connection.execute("ALTER TABLE entities ADD COLUMN watermark INTEGER NOT NULL DEFAULT 0")
        if "watermark" not in version_columns:
            connection.execute("ALTER TABLE entity_versions ADD COLUMN watermark INTEGER NOT NULL DEFAULT 0")
        if "watermark" not in audit_columns:
            connection.execute("ALTER TABLE audit_entries ADD COLUMN watermark INTEGER NOT NULL DEFAULT 0")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS write_cursor ("
            "cursor_key TEXT PRIMARY KEY, value INTEGER NOT NULL)"
        )
        connection.execute("INSERT OR IGNORE INTO write_cursor(cursor_key, value) VALUES('global', 0)")

        # 新代码写入的版本水位恒为正数，因此存在 0 水位版本行只可能意味着
        # 旧库尚未完成回填（也覆盖加列后、回填前崩溃的恢复场景）。此时
        # 不可能已有正数水位的新写入，因此对全表重新做确定性编号是安全的。
        needs_backfill = connection.execute(
            "SELECT COUNT(*) AS n FROM entity_versions WHERE watermark = 0"
        ).fetchone()["n"] > 0
        if needs_backfill:
            # 按完全确定的键 (valid_from, entity_type, entity_id, version, rowid)
            # 统一回放，回填为非正水位 -M..-1（M 为版本总数）。这个顺序只依赖
            # 已持久化的数据，与进程、机器或回放时机无关。
            connection.execute(
                """
                UPDATE entity_versions
                SET watermark = (
                    SELECT ranked.rnk - totals.cnt - 1
                    FROM (
                        SELECT rowid AS rid,
                               ROW_NUMBER() OVER (
                                   ORDER BY valid_from, entity_type, entity_id, version, rowid
                               ) AS rnk
                        FROM entity_versions
                    ) AS ranked,
                    (SELECT COUNT(*) AS cnt FROM entity_versions) AS totals
                    WHERE ranked.rid = entity_versions.rowid
                )
                """
            )
            # 当前表水位对齐该实体的最后一个版本。
            connection.execute(
                """
                UPDATE entities
                SET watermark = COALESCE((
                        SELECT ev.watermark
                        FROM entity_versions AS ev
                        WHERE ev.entity_type = entities.entity_type
                          AND ev.entity_id = entities.entity_id
                        ORDER BY ev.version DESC
                        LIMIT 1
                    ), 0)
                """
            )
            # 旧版本水位全部为负，游标保持 0：之后的第一次写入拿到水位 1，
            # 与历史回填区间永不交叠。
            connection.execute("UPDATE write_cursor SET value = 0 WHERE cursor_key = 'global'")

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
