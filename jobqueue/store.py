"""基于本地 SQLite 的持久化存储（模拟消息代理）。

所有写操作在同一把进程锁内以 ``BEGIN IMMEDIATE`` 事务执行，
配合 WAL 与 busy_timeout，保证多线程并发下的原子占用与状态守恒。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from jobqueue.config import Settings
from jobqueue.errors import ErrorCode, QueueError
from jobqueue.models import Delivery, GroupOffset, Job, JobState

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id           TEXT PRIMARY KEY,
    dedupe_key   TEXT UNIQUE,
    payload      TEXT NOT NULL,
    state        TEXT NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_retries  INTEGER NOT NULL,
    available_at REAL NOT NULL,
    last_error   TEXT,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS groups (
    name   TEXT PRIMARY KEY,
    "offset" INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS group_jobs (
    "group"     TEXT NOT NULL,
    job_id      TEXT NOT NULL,
    delivery_id INTEGER,
    status      TEXT NOT NULL DEFAULT 'pending',
    attempts    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY ("group", job_id)
);
CREATE TABLE IF NOT EXISTS deliveries (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    "group"       TEXT NOT NULL,
    job_id        TEXT NOT NULL,
    consumer      TEXT NOT NULL,
    attempt       INTEGER NOT NULL,
    delivered_at  REAL NOT NULL,
    visible_after REAL NOT NULL,
    state         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_group_jobs_group ON group_jobs("group");
CREATE INDEX IF NOT EXISTS idx_deliveries_group ON deliveries("group", state);
"""


class JobStore:
    def __init__(
        self,
        settings: Settings,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._settings = settings
        self._now = clock or time.time
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            settings.db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        """对早期版本的数据库做幂等加列迁移（CREATE TABLE IF NOT EXISTS 不会补列）。"""
        cols = {
            r["name"]
            for r in self._conn.execute("PRAGMA table_info(group_jobs)").fetchall()
        }
        if "status" not in cols:
            # 旧库用 terminal INTEGER 表达终态：先加列再据此回填每组分发状态
            self._conn.execute(
                "ALTER TABLE group_jobs ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'"
            )
            self._conn.execute(
                "UPDATE group_jobs SET status=CASE WHEN terminal=1 THEN 'succeeded' "
                "ELSE CASE WHEN delivery_id IS NULL THEN 'pending' ELSE 'inflight' END END"
            )
        if "attempts" not in cols:
            # 旧库投递次数以 jobs.attempts 为准，回填到每组
            self._conn.execute(
                "ALTER TABLE group_jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0"
            )
            self._conn.execute(
                "UPDATE group_jobs SET attempts=COALESCE("
                "(SELECT attempts FROM jobs WHERE jobs.id=group_jobs.job_id), 0)"
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------- 映射辅助 ----------

    def _rollup_job(
        self, conn: sqlite3.Connection, job_id: str, now: float
    ) -> None:
        """根据所有组的分发状态汇总任务全局状态。

        优先级：存在未终结的组 -> inflight/retrying（有 active 则 inflight）；
        全部终结 -> 任一组 succeeded 即 succeeded；否则 rejected 优先于 failed。
        """
        row = conn.execute(
            "SELECT "
            "SUM(CASE WHEN status='inflight' THEN 1 ELSE 0 END) AS inflight_n, "
            "SUM(CASE WHEN status='retrying' THEN 1 ELSE 0 END) AS retrying_n, "
            "SUM(CASE WHEN status='succeeded' THEN 1 ELSE 0 END) AS ok_n, "
            "SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END) AS rej_n, "
            "SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS fail_n, "
            "COUNT(*) AS total "
            "FROM group_jobs WHERE job_id=?",
            (job_id,),
        ).fetchone()
        if row is None or row["total"] == 0:
            state = JobState.PENDING.value
        elif row["inflight_n"]:
            state = JobState.INFLIGHT.value
        elif row["retrying_n"]:
            state = JobState.RETRYING.value
        elif row["ok_n"]:
            state = JobState.SUCCEEDED.value
        elif row["rej_n"]:
            state = JobState.REJECTED.value
        elif row["fail_n"]:
            state = JobState.FAILED.value
        else:
            state = JobState.PENDING.value
        conn.execute(
            "UPDATE jobs SET state=?, attempts=COALESCE(("
            "SELECT MAX(attempts) FROM group_jobs WHERE job_id=?), 0), "
            "updated_at=? WHERE id=?",
            (state, job_id, now, job_id),
        )

    def _row_to_job(self, row: sqlite3.Row) -> Job:
        return Job(
            id=row["id"],
            dedupe_key=row["dedupe_key"],
            payload=json.loads(row["payload"]),
            state=JobState(row["state"]),
            attempts=row["attempts"],
            max_retries=row["max_retries"],
            available_at=row["available_at"],
            last_error=row["last_error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_delivery(self, row: sqlite3.Row) -> Delivery:
        return Delivery(
            id=row["id"],
            job_id=row["job_id"],
            group=row["group"],
            consumer=row["consumer"],
            attempt=row["attempt"],
            delivered_at=row["delivered_at"],
            visible_after=row["visible_after"],
            state=row["state"],
        )

    # ---------- 入队 ----------

    def enqueue(self, payload: dict[str, Any], dedupe_key: str | None) -> Job:
        if not isinstance(payload, dict):
            raise QueueError(
                ErrorCode.INVALID_PAYLOAD, "payload 必须是 JSON 对象"
            )
        now = self._now()
        job_id = uuid.uuid4().hex
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if dedupe_key is not None:
                    row = self._conn.execute(
                        "SELECT id FROM jobs WHERE dedupe_key=?", (dedupe_key,)
                    ).fetchone()
                    if row is not None:
                        raise QueueError(
                            ErrorCode.DUPLICATE_JOB,
                            f"dedupe_key={dedupe_key!r} 已存在，"
                            f"既有任务 id={row['id']}",
                        )
                self._conn.execute(
                    "INSERT INTO jobs (id, dedupe_key, payload, state, attempts, "
                    "max_retries, available_at, last_error, created_at, updated_at) "
                    "VALUES (?,?,?,?,0,?,?,NULL,?,?)",
                    (
                        job_id,
                        dedupe_key,
                        body,
                        JobState.PENDING.value,
                        self._settings.max_retries,
                        now,
                        now,
                        now,
                    ),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        job = self.get_job(job_id)
        assert job is not None
        return job

    # ---------- 占用 / 回收 / 投递 ----------

    def _reclaim_expired(
        self, conn: sqlite3.Connection, group: str, now: float
    ) -> None:
        """回收该组内占用超时的 active 投递，并推进任务重试/终态。"""
        expired = conn.execute(
            "SELECT d.id AS d_id, d.job_id AS job_id, "
            "COALESCE(gj.attempts, 0) AS attempts, j.max_retries AS max_retries "
            "FROM deliveries d JOIN jobs j ON j.id=d.job_id "
            "JOIN group_jobs gj ON gj.delivery_id=d.id "
            "WHERE d.state='active' AND d.\"group\"=? AND d.visible_after<=?",
            (group, now),
        ).fetchall()
        for row in expired:
            attempts = row["attempts"]
            if attempts >= row["max_retries"] + 1:
                new_state = JobState.FAILED.value
                error = f"组 {group!r} 占用超时且重试已耗尽（第 {attempts} 次投递未确认）"
                available_at = now
            else:
                new_state = JobState.RETRYING.value
                error = f"组 {group!r} 占用超时，等待第 {attempts + 1} 次投递"
                available_at = now + self._settings.backoff_delay(attempts)
            conn.execute(
                "UPDATE deliveries SET state='timed_out' WHERE id=?", (row["d_id"],)
            )
            conn.execute(
                "UPDATE group_jobs SET delivery_id=NULL, status=? "
                "WHERE \"group\"=? AND job_id=?",
                (new_state, group, row["job_id"]),
            )
            conn.execute(
                "UPDATE jobs SET available_at=?, last_error=?, updated_at=? "
                "WHERE id=?",
                (available_at, error, now, row["job_id"]),
            )
            self._rollup_job(conn, row["job_id"], now)

    def claim(
        self, group: str, consumer: str, visibility_timeout: float | None = None
    ) -> Delivery | None:
        if not group or not consumer:
            raise QueueError(
                ErrorCode.INVALID_PAYLOAD, "group 与 consumer 不能为空"
            )
        timeout = (
            visibility_timeout
            if visibility_timeout is not None
            else self._settings.visibility_timeout
        )
        if timeout <= 0:
            raise QueueError(
                ErrorCode.INVALID_PAYLOAD, "visibility_timeout 必须为正数"
            )
        now = self._now()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                conn = self._conn
                conn.execute(
                    "INSERT INTO groups (name, \"offset\") VALUES (?,0) "
                    "ON CONFLICT(name) DO NOTHING",
                    (group,),
                )
                self._reclaim_expired(conn, group, now)
                # 懒补齐 group_jobs：新组也能看到建组之前的任务
                conn.execute(
                    "INSERT INTO group_jobs (\"group\", job_id, delivery_id, status) "
                    "SELECT ?, j.id, NULL, 'pending' FROM jobs j "
                    "WHERE NOT EXISTS (SELECT 1 FROM group_jobs gj "
                    "WHERE gj.\"group\"=? AND gj.job_id=j.id)",
                    (group, group),
                )
                row = conn.execute(
                    "SELECT j.*, gj.status AS g_status "
                    "FROM jobs j JOIN group_jobs gj "
                    "ON gj.job_id=j.id AND gj.\"group\"=? "
                    "WHERE gj.status IN ('pending','retrying') "
                    "AND j.available_at<=? AND gj.delivery_id IS NULL "
                    "ORDER BY j.created_at, j.rowid LIMIT 1",
                    (group, now),
                ).fetchone()
                if row is None:
                    conn.execute("COMMIT")
                    return None
                job = self._row_to_job(row)
                gattempts = conn.execute(
                    "SELECT attempts FROM group_jobs WHERE \"group\"=? AND job_id=?",
                    (group, job.id),
                ).fetchone()["attempts"]
                attempt = gattempts + 1
                cur = conn.execute(
                    "INSERT INTO deliveries "
                    "(\"group\", job_id, consumer, attempt, delivered_at, "
                    "visible_after, state) VALUES (?,?,?,?,?,?,'active')",
                    (group, job.id, consumer, attempt, now, now + timeout),
                )
                delivery_id = cur.lastrowid
                conn.execute(
                    "UPDATE group_jobs SET delivery_id=?, status='inflight', "
                    "attempts=attempts+1 "
                    "WHERE \"group\"=? AND job_id=?",
                    (delivery_id, group, job.id),
                )
                conn.execute(
                    "UPDATE jobs SET updated_at=? WHERE id=?", (now, job.id)
                )
                self._rollup_job(conn, job.id, now)
                conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            drow = self._conn.execute(
                "SELECT * FROM deliveries WHERE id=?", (delivery_id,)
            ).fetchone()
            return self._row_to_delivery(drow)

    # ---------- 确认 ----------

    def _load_active_delivery(
        self, conn: sqlite3.Connection, delivery_id: int, consumer: str
    ) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM deliveries WHERE id=?", (delivery_id,)
        ).fetchone()
        if row is None:
            raise QueueError(
                ErrorCode.DELIVERY_NOT_FOUND,
                f"投递 id={delivery_id} 不存在",
            )
        if row["state"] != "active":
            raise QueueError(
                ErrorCode.DELIVERY_TERMINAL,
                f"投递 id={delivery_id} 已处于 {row['state']} 终态，"
                "重复确认不会重复执行",
            )
        if row["consumer"] != consumer:
            raise QueueError(
                ErrorCode.DELIVERY_OWNER_MISMATCH,
                f"投递属于消费者 {row['consumer']!r}，拒绝由 {consumer!r} 确认",
            )
        if self._now() > row["visible_after"]:
            raise QueueError(
                ErrorCode.DELIVERY_EXPIRED,
                f"投递 id={delivery_id} 占用已超时，"
                "请等待重新投递后再确认",
            )
        return row

    def ack(self, delivery_id: int, consumer: str) -> Job:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                conn = self._conn
                row = self._load_active_delivery(conn, delivery_id, consumer)
                now = self._now()
                conn.execute(
                    "UPDATE deliveries SET state='acked' WHERE id=?", (delivery_id,)
                )
                conn.execute(
                    "UPDATE group_jobs SET delivery_id=NULL, status='succeeded' "
                    "WHERE \"group\"=? AND job_id=?",
                    (row["group"], row["job_id"]),
                )
                self._rollup_job(conn, row["job_id"], now)
                conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        job = self.get_job(row["job_id"])
        assert job is not None
        return job

    def nack(
        self,
        delivery_id: int,
        consumer: str,
        reason: str,
        *,
        unprocessable: bool = False,
    ) -> Job:
        if not reason or not reason.strip():
            raise QueueError(
                ErrorCode.INVALID_PAYLOAD, "nack 必须提供非空失败原因 reason"
            )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                conn = self._conn
                row = self._load_active_delivery(conn, delivery_id, consumer)
                now = self._now()
                job_row = conn.execute(
                    "SELECT gj.attempts AS attempts, j.max_retries AS max_retries "
                    "FROM group_jobs gj JOIN jobs j ON j.id=gj.job_id "
                    "WHERE gj.\"group\"=? AND gj.job_id=?",
                    (row["group"], row["job_id"]),
                ).fetchone()
                attempts = job_row["attempts"]
                if unprocessable:
                    new_state = JobState.REJECTED.value
                    available_at = now
                    error = f"组 {row['group']!r} 判定不可处理（拒绝重试）：{reason}"
                elif attempts >= job_row["max_retries"] + 1:
                    new_state = JobState.FAILED.value
                    available_at = now
                    error = f"组 {row['group']!r} 重试已耗尽：{reason}"
                else:
                    new_state = JobState.RETRYING.value
                    available_at = now + self._settings.backoff_delay(attempts)
                    error = f"组 {row['group']!r} 第 {attempts} 次投递失败：{reason}"
                conn.execute(
                    "UPDATE deliveries SET state='nacked' WHERE id=?", (delivery_id,)
                )
                conn.execute(
                    "UPDATE group_jobs SET delivery_id=NULL, status=? "
                    "WHERE \"group\"=? AND job_id=?",
                    (new_state, row["group"], row["job_id"]),
                )
                conn.execute(
                    "UPDATE jobs SET available_at=?, last_error=?, updated_at=? "
                    "WHERE id=?",
                    (available_at, error, now, row["job_id"]),
                )
                self._rollup_job(conn, row["job_id"], now)
                conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        job = self.get_job(row["job_id"])
        assert job is not None
        return job

    # ---------- 查询 ----------

    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
        return self._row_to_job(row) if row is not None else None

    def get_job_by_dedupe_key(self, dedupe_key: str) -> Job | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE dedupe_key=?", (dedupe_key,)
            ).fetchone()
        return self._row_to_job(row) if row is not None else None

    def list_jobs(self, state: str | None = None) -> list[Job]:
        with self._lock:
            if state is None:
                rows = self._conn.execute(
                    "SELECT * FROM jobs ORDER BY created_at, rowid"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM jobs WHERE state=? ORDER BY created_at, rowid",
                    (state,),
                ).fetchall()
        return [self._row_to_job(r) for r in rows]

    def list_deliveries(self, job_id: str) -> list[Delivery]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM deliveries WHERE job_id=? ORDER BY id", (job_id,)
            ).fetchall()
        return [self._row_to_delivery(r) for r in rows]

    # ---------- 消费者组与位点 ----------

    def create_group(self, group: str) -> None:
        if not group:
            raise QueueError(ErrorCode.INVALID_PAYLOAD, "group 不能为空")
        with self._lock:
            self._conn.execute(
                "INSERT INTO groups (name, \"offset\") VALUES (?,0) "
                "ON CONFLICT(name) DO NOTHING",
                (group,),
            )

    def list_groups(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute("SELECT name FROM groups ORDER BY name").fetchall()
        return [r["name"] for r in rows]

    def get_offset(self, group: str) -> GroupOffset | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT name, \"offset\" FROM groups WHERE name=?", (group,)
            ).fetchone()
        return GroupOffset(group=row["name"], offset=row["offset"]) if row else None

    def commit_offset(self, group: str, new_offset: int) -> GroupOffset:
        if not isinstance(new_offset, int) or new_offset < 0:
            raise QueueError(
                ErrorCode.INVALID_PAYLOAD, "new_offset 必须是非负整数"
            )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                conn = self._conn
                row = conn.execute(
                    "SELECT \"offset\" FROM groups WHERE name=?", (group,)
                ).fetchone()
                if row is None:
                    raise QueueError(
                        ErrorCode.GROUP_NOT_FOUND,
                        f"消费者组 {group!r} 不存在，请先注册",
                    )
                current = row["offset"]
                if new_offset <= current:
                    raise QueueError(
                        ErrorCode.OFFSET_ROLLBACK,
                        f"位点只能前进：当前={current}，"
                        f"拒绝提交 {new_offset}（回退或原地提交）",
                    )
                total = conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"]
                if new_offset > total:
                    raise QueueError(
                        ErrorCode.OFFSET_OUT_OF_RANGE,
                        f"位点越界：队列中仅有 {total} 条任务，"
                        f"拒绝提交 {new_offset}",
                    )
                conn.execute(
                    "UPDATE groups SET \"offset\"=? WHERE name=?",
                    (new_offset, group),
                )
                conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        result = self.get_offset(group)
        assert result is not None
        return result
