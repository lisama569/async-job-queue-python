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
from jobqueue.models import (
    CheckpointJob,
    Delivery,
    GroupCheckpoint,
    GroupJobView,
    GroupOffset,
    GroupProgress,
    GroupStatus,
    GROUP_TERMINAL_STATUSES,
    Job,
    JobState,
)

# 检查点快照格式版本
CHECKPOINT_VERSION = 1

# 组账本中的终态字符串值
GROUP_TERMINAL_VALUES: frozenset[str] = frozenset(
    {s.value for s in GROUP_TERMINAL_STATUSES}
)

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
    "group"      TEXT NOT NULL,
    job_id       TEXT NOT NULL,
    delivery_id  INTEGER,
    status       TEXT NOT NULL DEFAULT 'pending',
    attempts     INTEGER NOT NULL DEFAULT 0,
    available_at REAL NOT NULL DEFAULT 0,
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
        if "available_at" not in cols:
            # 旧库退避时间只存在 jobs.available_at（全局），回填到每组账本，
            # 使退避按组独立计时
            self._conn.execute(
                "ALTER TABLE group_jobs ADD COLUMN available_at REAL NOT NULL DEFAULT 0"
            )
            self._conn.execute(
                "UPDATE group_jobs SET available_at=COALESCE("
                "(SELECT available_at FROM jobs WHERE jobs.id=group_jobs.job_id), 0)"
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------- 映射辅助 ----------

    def _rollup_job(
        self, conn: sqlite3.Connection, job_id: str, now: float
    ) -> None:
        """根据所有组的分发状态汇总任务全局状态。

        终态粘滞：只要任一组已有最终结论，全局状态就定格为终态
        （succeeded 优先于 rejected 优先于 failed），其它组后续的
        投递 / 退避都不能把它改回 inflight/retrying/pending；
        尚无任何组定论时：有 active -> inflight；有 retrying -> retrying。
        ``available_at`` 取各组账本中的最早到期时间，使退避按组独立，
        A 组退避不会拖住 B 组取件。
        """
        row = conn.execute(
            "SELECT "
            "SUM(CASE WHEN status='inflight' THEN 1 ELSE 0 END) AS inflight_n, "
            "SUM(CASE WHEN status='retrying' THEN 1 ELSE 0 END) AS retrying_n, "
            "SUM(CASE WHEN status='succeeded' THEN 1 ELSE 0 END) AS ok_n, "
            "SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END) AS rej_n, "
            "SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS fail_n, "
            "MIN(available_at) AS min_available, "
            "COUNT(*) AS total "
            "FROM group_jobs WHERE job_id=?",
            (job_id,),
        ).fetchone()
        if row is None or row["total"] == 0:
            state = JobState.PENDING.value
        elif row["ok_n"]:
            state = JobState.SUCCEEDED.value
        elif row["rej_n"]:
            state = JobState.REJECTED.value
        elif row["fail_n"]:
            state = JobState.FAILED.value
        elif row["inflight_n"]:
            state = JobState.INFLIGHT.value
        elif row["retrying_n"]:
            state = JobState.RETRYING.value
        else:
            state = JobState.PENDING.value
        conn.execute(
            "UPDATE jobs SET state=?, attempts=COALESCE(("
            "SELECT MAX(attempts) FROM group_jobs WHERE job_id=?), 0), "
            "available_at=COALESCE(("
            "SELECT MIN(available_at) FROM group_jobs WHERE job_id=?), 0), "
            "updated_at=? WHERE id=?",
            (state, job_id, job_id, now, job_id),
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
                group_available = now
            else:
                new_state = JobState.RETRYING.value
                error = f"组 {group!r} 占用超时，等待第 {attempts + 1} 次投递"
                group_available = now + self._settings.backoff_delay(attempts)
            conn.execute(
                "UPDATE deliveries SET state='timed_out' WHERE id=?", (row["d_id"],)
            )
            conn.execute(
                "UPDATE group_jobs SET delivery_id=NULL, status=?, available_at=? "
                "WHERE \"group\"=? AND job_id=?",
                (new_state, group_available, group, row["job_id"]),
            )
            conn.execute(
                "UPDATE jobs SET last_error=?, updated_at=? WHERE id=?",
                (error, now, row["job_id"]),
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
                self._ensure_group_exists(conn, group)
                self._reclaim_expired(conn, group, now)
                # 懒补齐 group_jobs：新组也能看到建组之前的任务
                self._backfill_group_jobs(conn, group)
                row = conn.execute(
                    "SELECT j.*, gj.status AS g_status "
                    "FROM jobs j JOIN group_jobs gj "
                    "ON gj.job_id=j.id AND gj.\"group\"=? "
                    "WHERE gj.status IN ('pending','retrying') "
                    "AND gj.available_at<=? AND gj.delivery_id IS NULL "
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
                    "attempts=attempts+1, available_at=0 "
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
                    "UPDATE group_jobs SET delivery_id=NULL, status='succeeded', "
                    "available_at=0 "
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
                    group_available = now
                    error = f"组 {row['group']!r} 判定不可处理（拒绝重试）：{reason}"
                elif attempts >= job_row["max_retries"] + 1:
                    new_state = JobState.FAILED.value
                    group_available = now
                    error = f"组 {row['group']!r} 重试已耗尽：{reason}"
                else:
                    new_state = JobState.RETRYING.value
                    group_available = now + self._settings.backoff_delay(attempts)
                    error = f"组 {row['group']!r} 第 {attempts} 次投递失败：{reason}"
                conn.execute(
                    "UPDATE deliveries SET state='nacked' WHERE id=?", (delivery_id,)
                )
                conn.execute(
                    "UPDATE group_jobs SET delivery_id=NULL, status=?, available_at=? "
                    "WHERE \"group\"=? AND job_id=?",
                    (new_state, group_available, row["group"], row["job_id"]),
                )
                conn.execute(
                    "UPDATE jobs SET last_error=?, updated_at=? WHERE id=?",
                    (error, now, row["job_id"]),
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

    # ---------- 进度对账 ----------

    def _ensure_group_exists(self, conn: sqlite3.Connection, group: str) -> None:
        conn.execute(
            "INSERT INTO groups (name, \"offset\") VALUES (?,0) "
            "ON CONFLICT(name) DO NOTHING",
            (group,),
        )

    def _backfill_group_jobs(self, conn: sqlite3.Connection, group: str) -> None:
        """懒补齐该组账本：让新组看到建组前已存在的全部任务。"""
        conn.execute(
            "INSERT INTO group_jobs (\"group\", job_id, delivery_id, status, "
            "attempts, available_at) "
            "SELECT ?, j.id, NULL, 'pending', 0, 0 FROM jobs j "
            "WHERE NOT EXISTS (SELECT 1 FROM group_jobs gj "
            "WHERE gj.\"group\"=? AND gj.job_id=j.id)",
            (group, group),
        )

    def _group_rows(
        self, conn: sqlite3.Connection, group: str
    ) -> list[sqlite3.Row]:
        """按入队顺序返回该组账本的全部行。"""
        return conn.execute(
            "SELECT j.id AS job_id, gj.status AS status, gj.attempts AS attempts, "
            "gj.delivery_id AS delivery_id "
            "FROM jobs j JOIN group_jobs gj "
            "ON gj.job_id=j.id AND gj.\"group\"=? "
            "ORDER BY j.created_at, j.rowid",
            (group,),
        ).fetchall()

    @staticmethod
    def _safe_offset(rows: list[sqlite3.Row]) -> int:
        """连续定论前缀长度：遇到第一条非终态任务即停止。"""
        safe = 0
        for r in rows:
            if r["status"] in (
                JobState.SUCCEEDED.value,
                JobState.FAILED.value,
                JobState.REJECTED.value,
            ):
                safe += 1
            else:
                break
        return safe

    def group_progress(self, group: str) -> GroupProgress:
        """导出该组的进度对账结果（位点 + 五类任务桶 + 安全位点）。"""
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
                self._backfill_group_jobs(conn, group)
                rows = self._group_rows(conn, group)
                offset = row["offset"]
                conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        buckets: dict[str, list[str]] = {
            JobState.SUCCEEDED.value: [],
            JobState.FAILED.value: [],
            JobState.REJECTED.value: [],
            JobState.INFLIGHT.value: [],
            JobState.RETRYING.value: [],
            JobState.PENDING.value: [],
        }
        for r in rows:
            buckets[r["status"]].append(r["job_id"])
        return GroupProgress(
            group=group,
            offset=offset,
            safe_offset=self._safe_offset(rows),
            total=len(rows),
            succeeded=buckets[JobState.SUCCEEDED.value],
            failed=buckets[JobState.FAILED.value],
            rejected=buckets[JobState.REJECTED.value],
            inflight=buckets[JobState.INFLIGHT.value],
            retrying=buckets[JobState.RETRYING.value],
            pending=buckets[JobState.PENDING.value],
        )

    def list_group_jobs(self, group: str) -> list[GroupJobView]:
        """按入队顺序列出该组每个任务的组内定论（运维对账用）。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                conn = self._conn
                if conn.execute(
                    "SELECT 1 FROM groups WHERE name=?", (group,)
                ).fetchone() is None:
                    raise QueueError(
                        ErrorCode.GROUP_NOT_FOUND,
                        f"消费者组 {group!r} 不存在，请先注册",
                    )
                self._backfill_group_jobs(conn, group)
                rows = self._group_rows(conn, group)
                conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return [
            GroupJobView(
                job_id=r["job_id"],
                group=group,
                status=GroupStatus(r["status"]),
                attempts=r["attempts"],
                delivery_id=r["delivery_id"],
            )
            for r in rows
        ]

    def get_job_group_views(self, job_id: str) -> list[GroupJobView]:
        """查询单个任务在各个组里的处理结论（组间隔离的对账单）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT gj.\"group\" AS \"group\", gj.job_id AS job_id, "
                "gj.status AS status, gj.attempts AS attempts, "
                "gj.delivery_id AS delivery_id "
                "FROM group_jobs gj WHERE gj.job_id=? ORDER BY gj.\"group\"",
                (job_id,),
            ).fetchall()
        return [
            GroupJobView(
                job_id=r["job_id"],
                group=r["group"],
                status=GroupStatus(r["status"]),
                attempts=r["attempts"],
                delivery_id=r["delivery_id"],
            )
            for r in rows
        ]

    # ---------- 检查点导出 / 恢复 ----------

    def export_checkpoint(self, group: str) -> GroupCheckpoint:
        """把某组当前对账结果导出为可持久化的检查点快照。

        快照包含该组视角下每个任务的定论、投递次数与退避到期时间，
        以及组位点；inflight 任务在恢复侧会被规范化为 pending 重新投递，
        因此检查点天然兼容至少一次语义。
        """
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
                self._backfill_group_jobs(conn, group)
                srows = conn.execute(
                    "SELECT j.id AS id, j.dedupe_key AS dedupe_key, "
                    "j.payload AS payload, j.max_retries AS max_retries, "
                    "j.created_at AS created_at, gj.status AS status, "
                    "gj.attempts AS attempts, gj.available_at AS available_at "
                    "FROM jobs j JOIN group_jobs gj "
                    "ON gj.job_id=j.id AND gj.\"group\"=? "
                    "ORDER BY j.created_at, j.rowid",
                    (group,),
                ).fetchall()
                offset = row["offset"]
                conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        jobs = [
            CheckpointJob(
                job_id=r["id"],
                dedupe_key=r["dedupe_key"],
                payload=json.loads(r["payload"]),
                status=GroupStatus(r["status"]),
                attempts=r["attempts"],
                max_retries=r["max_retries"],
                available_at=0.0
                if r["status"] in GROUP_TERMINAL_VALUES
                or r["status"] == JobState.INFLIGHT.value
                else r["available_at"],
                created_at=r["created_at"],
            )
            for r in srows
        ]
        return GroupCheckpoint(
            version=CHECKPOINT_VERSION,
            group=group,
            offset=offset,
            created_at=self._now(),
            jobs=jobs,
        )

    def restore_checkpoint(self, checkpoint: GroupCheckpoint) -> GroupProgress:
        """依据检查点让该组在本库接着消费。

        - 快照中已定论（succeeded/failed/rejected）的任务补写为该组终态，
          不会再被投递执行；
        - inflight 规范化为 pending、retrying 保留退避到期时间，未完成的不跳过；
        - 仅影响快照所属组，其它组账本一行不动；
        - 该组已经是终态的条目不会被改回非终态（终态粘滞）；
        - 位点按恢复后的连续定论前缀钳制，绝不产生「位点在前、定论在后有缺口」。
        """
        self._validate_checkpoint(checkpoint)
        group = checkpoint.group
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                conn = self._conn
                self._ensure_group_exists(conn, group)
                for cj in checkpoint.jobs:
                    existing = conn.execute(
                        "SELECT id, payload, state FROM jobs WHERE id=?",
                        (cj.job_id,),
                    ).fetchone()
                    if existing is None:
                        if cj.dedupe_key is not None:
                            clash = conn.execute(
                                "SELECT id FROM jobs WHERE dedupe_key=?",
                                (cj.dedupe_key,),
                            ).fetchone()
                            if clash is not None:
                                raise QueueError(
                                    ErrorCode.CHECKPOINT_INVALID,
                                    f"检查点任务 {cj.job_id} 的 dedupe_key="
                                    f"{cj.dedupe_key!r} 与库内任务 {clash['id']} "
                                    "冲突，拒绝恢复",
                                )
                        body = json.dumps(
                            cj.payload, ensure_ascii=False, sort_keys=True
                        )
                        # 先以 pending 落库，再由组账本 rollup 汇总出真实状态
                        conn.execute(
                            "INSERT INTO jobs (id, dedupe_key, payload, state, "
                            "attempts, max_retries, available_at, last_error, "
                            "created_at, updated_at) "
                            "VALUES (?,?,?,?,?,?,?,NULL,?,?)",
                            (
                                cj.job_id,
                                cj.dedupe_key,
                                body,
                                JobState.PENDING.value,
                                cj.attempts,
                                cj.max_retries,
                                cj.available_at,
                                cj.created_at,
                                self._now(),
                            ),
                        )
                    desired_status = cj.status
                    desired_available = 0.0
                    if desired_status is GroupStatus.RETRYING:
                        desired_available = cj.available_at
                    elif desired_status is GroupStatus.INFLIGHT:
                        # 导出时在途：原消费者已失联，按至少一次语义重新投递
                        desired_status = GroupStatus.PENDING
                    grow = conn.execute(
                        "SELECT status FROM group_jobs "
                        "WHERE \"group\"=? AND job_id=?",
                        (group, cj.job_id),
                    ).fetchone()
                    if grow is not None and grow["status"] in GROUP_TERMINAL_VALUES:
                        # 本组已有最终结论：终态不被检查点改回
                        continue
                    conn.execute(
                        "INSERT INTO group_jobs (\"group\", job_id, delivery_id, "
                        "status, attempts, available_at) VALUES (?,?,NULL,?,?,?) "
                        "ON CONFLICT(\"group\", job_id) DO UPDATE SET "
                        "delivery_id=NULL, status=excluded.status, "
                        "attempts=MAX(group_jobs.attempts, excluded.attempts), "
                        "available_at=excluded.available_at",
                        (
                            group,
                            cj.job_id,
                            desired_status.value,
                            cj.attempts,
                            desired_available,
                        ),
                    )
                    self._rollup_job(conn, cj.job_id, self._now())
                # 快照外可能还有库内已有任务，补齐后再钳制位点
                self._backfill_group_jobs(conn, group)
                rows = self._group_rows(conn, group)
                safe = self._safe_offset(rows)
                cur = conn.execute(
                    "SELECT \"offset\" FROM groups WHERE name=?", (group,)
                ).fetchone()["offset"]
                restored = max(cur, min(checkpoint.offset, safe))
                conn.execute(
                    "UPDATE groups SET \"offset\"=? WHERE name=?",
                    (restored, group),
                )
                conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return self.group_progress(group)

    @staticmethod
    def _validate_checkpoint(checkpoint: GroupCheckpoint) -> None:
        if not isinstance(checkpoint, GroupCheckpoint):
            raise QueueError(
                ErrorCode.CHECKPOINT_INVALID, "检查点必须是 GroupCheckpoint 实例"
            )
        if checkpoint.version != CHECKPOINT_VERSION:
            raise QueueError(
                ErrorCode.CHECKPOINT_INVALID,
                f"检查点版本 {checkpoint.version} 不受支持，"
                f"当前仅支持版本 {CHECKPOINT_VERSION}",
            )
        if not checkpoint.group:
            raise QueueError(
                ErrorCode.CHECKPOINT_INVALID, "检查点缺少消费者组名"
            )
        if checkpoint.offset < 0:
            raise QueueError(
                ErrorCode.CHECKPOINT_INVALID, "检查点位点不能为负数"
            )
        seen: set[str] = set()
        for cj in checkpoint.jobs:
            if not cj.job_id or cj.job_id in seen:
                raise QueueError(
                    ErrorCode.CHECKPOINT_INVALID,
                    f"检查点任务 id 非法或重复：{cj.job_id!r}",
                )
            seen.add(cj.job_id)
            if not isinstance(cj.status, GroupStatus):
                raise QueueError(
                    ErrorCode.CHECKPOINT_INVALID,
                    f"检查点任务 {cj.job_id} 状态非法：{cj.status!r}",
                )
            if not isinstance(cj.payload, dict):
                raise QueueError(
                    ErrorCode.CHECKPOINT_INVALID,
                    f"检查点任务 {cj.job_id} 的 payload 必须是 JSON 对象",
                )

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
                self._backfill_group_jobs(conn, group)
                total = conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"]
                if new_offset > total:
                    raise QueueError(
                        ErrorCode.OFFSET_OUT_OF_RANGE,
                        f"位点越界：队列中仅有 {total} 条任务，"
                        f"拒绝提交 {new_offset}",
                    )
                rows = self._group_rows(conn, group)
                safe = self._safe_offset(rows)
                if new_offset > safe:
                    blockers: list[str] = []
                    for r in rows[safe:]:
                        if len(blockers) >= 3:
                            break
                        blockers.append(f"{r['job_id']}({r['status']})")
                    raise QueueError(
                        ErrorCode.OFFSET_GAP,
                        f"位点不能跳过本组尚未定论的任务：前 {safe} 条已有最终"
                        f"结论，第 {safe + 1} 条起仍在途/待处理（如 "
                        f"{', '.join(blockers)}），拒绝把位点提交到 {new_offset}",
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
