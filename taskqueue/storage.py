"""SQLite persistence layer. A single file DB simulates the broker:
WAL + ``BEGIN IMMEDIATE`` transactions plus conditional UPDATEs provide
exclusive lease ownership even with threads/processes contending.

All conditional mutation methods return an outcome code so the queue layer can
map failures to distinguishable reasons instead of dropping anything.
"""

from __future__ import annotations

import os
import sqlite3
import threading

from .models import GroupOffset, Task, TaskStatus

# Outcome codes shared by ack/nack paths.
ACKED = "ACKED"
ALREADY_SUCCEEDED = "ALREADY_SUCCEEDED"
NACKED = "NACKED"
STALE_EPOCH = "STALE_EPOCH"
NOT_INFLIGHT = "NOT_INFLIGHT"
UNKNOWN = "UNKNOWN"

_TASK_COLUMNS = (
    "id", "dedup_key", "task_type", "payload", "status", "attempts",
    "max_attempts", "epoch", "lease_owner", "lease_expires_at_ms",
    "available_at_ms", "terminal_reason", "last_error",
    "created_at_ms", "updated_at_ms",
)


class Storage:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self.init_schema()

    def init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dedup_key TEXT UNIQUE,
                    task_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 5,
                    epoch INTEGER NOT NULL DEFAULT 0,
                    lease_owner TEXT,
                    lease_expires_at_ms INTEGER,
                    available_at_ms INTEGER NOT NULL,
                    terminal_reason TEXT,
                    last_error TEXT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_deliver
                    ON tasks(status, available_at_ms);

                CREATE TABLE IF NOT EXISTS consumer_groups (
                    name TEXT PRIMARY KEY,
                    committed_offset INTEGER NOT NULL DEFAULT 0,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                """
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ rows
    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> Task:
        return Task(
            id=row["id"],
            dedup_key=row["dedup_key"],
            task_type=row["task_type"],
            payload=row["payload"],
            status=TaskStatus(row["status"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            epoch=row["epoch"],
            lease_owner=row["lease_owner"],
            lease_expires_at_ms=row["lease_expires_at_ms"],
            available_at_ms=row["available_at_ms"],
            created_at_ms=row["created_at_ms"],
            updated_at_ms=row["updated_at_ms"],
            terminal_reason=row["terminal_reason"],
            last_error=row["last_error"],
        )

    def _get_task_conn(self, conn: sqlite3.Connection, task_id: int) -> sqlite3.Row | None:
        cur = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
        return cur.fetchone()

    # ----------------------------------------------------------------- tasks
    def find_task_by_dedup_key(self, dedup_key: str) -> Task | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE dedup_key = ?", (dedup_key,)
            ).fetchone()
            return self._row_to_task(row) if row else None

    def insert_task(
        self,
        dedup_key: str | None,
        task_type: str,
        payload: str,
        status: TaskStatus,
        terminal_reason: str | None,
        max_attempts: int,
        now_ms: int,
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO tasks (dedup_key, task_type, payload, status,
                                   terminal_reason, max_attempts, available_at_ms,
                                   created_at_ms, updated_at_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (dedup_key, task_type, payload, status.value, terminal_reason,
                 max_attempts, now_ms, now_ms, now_ms),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def get_task(self, task_id: int) -> Task | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            return self._row_to_task(row) if row else None

    def max_task_id(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COALESCE(MAX(id), 0) AS m FROM tasks").fetchone()
            return int(row["m"])

    def reclaim_expired(self, now_ms: int, lease_timeout_ms: int) -> int:
        """Release inflight leases whose visibility timeout has passed.

        The row goes back to ``pending`` and becomes deliverable again; its
        ``epoch`` is *not* bumped here (a fresh epoch is granted on redelivery),
        which is exactly what invalidates a late ack from the dead consumer.
        """
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE tasks
                   SET status = 'pending', lease_owner = NULL,
                       lease_expires_at_ms = NULL, updated_at_ms = ?
                 WHERE status = 'inflight' AND lease_expires_at_ms <= ?
                """,
                (now_ms, now_ms),
            )
            self._conn.commit()
            return cur.rowcount

    def lease_available(
        self, limit: int, now_ms: int, lease_timeout_ms: int, owner: str
    ) -> list[Task]:
        """Atomically grant leases for due, non-leased tasks.

        ``epoch`` is the fencing token: every grant bumps it and records a new
        delivery attempt. Two concurrent polls serialise on BEGIN IMMEDIATE, so
        any task is held by at most one consumer at any instant.
        """
        if limit <= 0:
            return []
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                ids = [
                    r["id"]
                    for r in self._conn.execute(
                        """
                        SELECT id FROM tasks
                         WHERE status = 'pending' AND available_at_ms <= ?
                         ORDER BY id ASC
                         LIMIT ?
                        """,
                        (now_ms, limit),
                    ).fetchall()
                ]
                leased: list[Task] = []
                if ids:
                    placeholders = ",".join("?" * len(ids))
                    rows = self._conn.execute(
                        f"""
                        UPDATE tasks
                           SET status = 'inflight', lease_owner = ?,
                               lease_expires_at_ms = ?, epoch = epoch + 1,
                               attempts = attempts + 1, updated_at_ms = ?
                         WHERE id IN ({placeholders})
                        RETURNING {",".join(_TASK_COLUMNS)}
                        """,
                        (owner, now_ms + lease_timeout_ms, now_ms, *ids),
                    ).fetchall()
                    leased = [self._row_to_task(r) for r in rows]
                self._conn.commit()
                return leased
            except Exception:
                self._conn.rollback()
                raise

    def ack_task(self, task_id: int, epoch: int, now_ms: int) -> tuple[Task | None, str]:
        """Conditional ack. Returns (task, outcome code)."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._get_task_conn(self._conn, task_id)
                if row is None:
                    self._conn.commit()
                    return None, UNKNOWN
                # Idempotent: the exact epoch that completed the task may ack again.
                if row["status"] == TaskStatus.SUCCEEDED.value and row["epoch"] == epoch:
                    self._conn.commit()
                    return self._row_to_task(row), ALREADY_SUCCEEDED
                if row["status"] != TaskStatus.INFLIGHT.value:
                    self._conn.commit()
                    return self._row_to_task(row), NOT_INFLIGHT
                if row["epoch"] != epoch:
                    self._conn.commit()
                    return self._row_to_task(row), STALE_EPOCH
                updated = self._conn.execute(
                    """
                    UPDATE tasks
                       SET status = 'succeeded', lease_owner = NULL,
                           lease_expires_at_ms = NULL, terminal_reason = NULL,
                           updated_at_ms = ?
                     WHERE id = ? AND status = 'inflight' AND epoch = ?
                    RETURNING *
                    """,
                    (now_ms, task_id, epoch),
                ).fetchone()
                self._conn.commit()
                return self._row_to_task(updated), ACKED
            except Exception:
                self._conn.rollback()
                raise

    def nack_task(
        self,
        task_id: int,
        epoch: int,
        reason: str,
        retry_at_ms: int | None,
        now_ms: int,
        failed: bool,
    ) -> tuple[Task | None, str]:
        """Conditional negative-ack. Either schedule a retry (back to pending with
        backoff) or move the task to the terminal ``failed`` state."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._get_task_conn(self._conn, task_id)
                if row is None:
                    self._conn.commit()
                    return None, UNKNOWN
                if row["status"] != TaskStatus.INFLIGHT.value:
                    self._conn.commit()
                    return self._row_to_task(row), NOT_INFLIGHT
                if row["epoch"] != epoch:
                    self._conn.commit()
                    return self._row_to_task(row), STALE_EPOCH
                if failed:
                    updated = self._conn.execute(
                        """
                        UPDATE tasks
                           SET status = 'failed', lease_owner = NULL,
                               lease_expires_at_ms = NULL, terminal_reason = ?,
                               last_error = ?, updated_at_ms = ?
                         WHERE id = ? AND status = 'inflight' AND epoch = ?
                        RETURNING *
                        """,
                        (reason, reason, now_ms, task_id, epoch),
                    ).fetchone()
                else:
                    updated = self._conn.execute(
                        """
                        UPDATE tasks
                           SET status = 'pending', lease_owner = NULL,
                               lease_expires_at_ms = NULL, available_at_ms = ?,
                               last_error = ?, updated_at_ms = ?
                         WHERE id = ? AND status = 'inflight' AND epoch = ?
                        RETURNING *
                        """,
                        (retry_at_ms, reason, now_ms, task_id, epoch),
                    ).fetchone()
                self._conn.commit()
                return self._row_to_task(updated), NACKED
            except Exception:
                self._conn.rollback()
                raise

    def count_tasks_by_status(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS c FROM tasks GROUP BY status"
            ).fetchall()
            return {r["status"]: r["c"] for r in rows}

    # -------------------------------------------------------- groups/offsets
    def ensure_group(self, name: str, now_ms: int) -> GroupOffset:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO consumer_groups (name, committed_offset, created_at_ms, updated_at_ms)
                VALUES (?, 0, ?, ?)
                ON CONFLICT(name) DO UPDATE SET name = name
                """,
                (name, now_ms, now_ms),
            )
            self._conn.commit()
            return self.get_group(name)  # type: ignore[return-value]

    def get_group(self, name: str) -> GroupOffset | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM consumer_groups WHERE name = ?", (name,)
            ).fetchone()
            if not row:
                return None
            return GroupOffset(
                group_name=row["name"],
                committed_offset=row["committed_offset"],
                created_at_ms=row["created_at_ms"],
                updated_at_ms=row["updated_at_ms"],
            )

    def advance_offset(self, name: str, new_offset: int, now_ms: int) -> bool:
        """Database-level guard: offsets only move forward. Returns False on a
        lost race (someone else committed a >= offset in the meantime)."""
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE consumer_groups
                   SET committed_offset = ?, updated_at_ms = ?
                 WHERE name = ? AND committed_offset < ?
                """,
                (new_offset, now_ms, name, new_offset),
            )
            self._conn.commit()
            return cur.rowcount == 1
