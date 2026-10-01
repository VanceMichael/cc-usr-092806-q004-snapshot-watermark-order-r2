"""应用装配。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .audit import AuditLog
from .database import Database
from .idempotency import IdempotencyStore
from .inbox import Inbox
from .jobs import JobQueue
from .ledger import Ledger
from .outbox import Outbox
from .repository import EntityRepository
from .reservations import ReservationBook
from .timeutil import Clock
from . import watermarks


@dataclass(frozen=True)
class CivicFlow:
    database: Database
    clock: Clock
    repository: EntityRepository
    inbox: Inbox
    outbox: Outbox
    ledger: Ledger
    reservations: ReservationBook
    jobs: JobQueue

    @classmethod
    def open(cls, path: str | Path, *, fixed_now: str | None = None) -> "CivicFlow":
        database = Database(path); database.initialize(); clock = Clock(fixed_now)
        audit = AuditLog(clock); idempotency = IdempotencyStore(clock)
        repository = EntityRepository(database, clock, audit, idempotency)
        return cls(database, clock, repository, Inbox(database, clock), Outbox(database, clock), Ledger(database, clock), ReservationBook(database), JobQueue(database, clock))

    def verify(self) -> dict:
        with self.database.connect() as connection:
            audit_count = AuditLog(self.clock).verify(connection)
            entity_count = connection.execute("SELECT COUNT(*) AS n FROM entities").fetchone()["n"]
            conflict_count = connection.execute("SELECT COUNT(*) AS n FROM inbox_conflicts").fetchone()["n"]
        return {"audit_entries": audit_count, "entities": entity_count, "inbox_conflicts": conflict_count}

    def high_watermark(self) -> int:
        """返回已持久化的最高顺序水位（供移交时声明材料边界）。"""
        with self.database.connect() as connection:
            return watermarks.high_watermark(connection)

    def audit_window(self, *, at_seq: int | None = None, entity_type: str | None = None,
                     entity_id: str | None = None) -> list[dict]:
        """按水位顺序返回审计记录，可限定到某水位（含）或某实体。"""
        with self.database.connect() as connection:
            return AuditLog(self.clock).entries(connection, at_seq=at_seq,
                                               entity_type=entity_type, entity_id=entity_id)
