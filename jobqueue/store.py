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
    TERMINAL_GROUP_STATUSES,
    Checkpoint,
    CheckpointJob,
    Delivery,
    GroupOffset,
    GroupProgress,
    Job,
    JobState,
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
    "group"     TEXT NOT NULL,
    job_id      TEXT NOT NULL,
    delivery_id INTEGER,
    status      TEXT NOT NULL DEFAULT 'pending',
    attempts    INTEGER NOT NULL DEFAULT 0,
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
            # 旧库退避时间挂在 jobs 上（全局），回填为每组各自的退避起点
            self._conn.execute(
                "ALTER TABLE group_jobs ADD COLUMN available_at REAL NOT NULL DEFAULT 0"
            )
            self._conn.execute(
                "UPDATE group_jobs SET available_at=COALESCE("
                "(SELECT available_at FROM jobs WHERE jobs.id=group_jobs.job_id), 0) "
                "WHERE status IN ('pending','retrying','inflight')"
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------- 映射辅助 ----------

    def _rollup_job(
        self, conn: sqlite3.Connection, job_id: str, now: float
    ) -> None:
        """根据所有组的分发状态汇总任务全局状态。

        终态一旦由任一组给出即锁定（不会被其它组后续的在途 / 重试改回）：
        任一组终态 -> succeeded 优先于 rejected 优先于 failed；
        尚无任何组给出终态 -> 有 active 则 inflight，其次 retrying，否则 pending。
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
            "SELECT MIN(available_at) FROM group_jobs WHERE job_id=? "
            "AND status IN ('pending','retrying','inflight')), 0), "
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

    def _backfill_group_jobs(self, conn: sqlite3.Connection, group: str) -> None:
        """懒补齐 group_jobs：新组也能看到建组之前的任务。"""
        conn.execute(
            "INSERT INTO group_jobs (\"group\", job_id, delivery_id, status) "
            "SELECT ?, j.id, NULL, 'pending' FROM jobs j "
            "WHERE NOT EXISTS (SELECT 1 FROM group_jobs gj "
            "WHERE gj.\"group\"=? AND gj.job_id=j.id)",
            (group, group),
        )

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
                "UPDATE group_jobs SET delivery_id=NULL, status=?, available_at=? "
                "WHERE \"group\"=? AND job_id=?",
                (new_state, available_at, group, row["job_id"]),
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
                conn.execute(
                    "INSERT INTO groups (name, \"offset\") VALUES (?,0) "
                    "ON CONFLICT(name) DO NOTHING",
                    (group,),
                )
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
                    "UPDATE group_jobs SET delivery_id=NULL, status=?, available_at=? "
                    "WHERE \"group\"=? AND job_id=?",
                    (new_state, available_at, row["group"], row["job_id"]),
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
                # 位点必须与该组实际处理进度对齐：前 new_offset 条任务
                # （按入队顺序）在该组内必须全部已有终态结论，
                # 不允许跳过仍在途 / 未投递 / 退避中的任务
                self._backfill_group_jobs(conn, group)
                undecided = [
                    r["id"]
                    for r in conn.execute(
                        "SELECT j.id AS id, gj.status AS st FROM jobs j "
                        "JOIN group_jobs gj ON gj.job_id=j.id AND gj.\"group\"=? "
                        "ORDER BY j.created_at, j.rowid LIMIT ?",
                        (group, new_offset),
                    ).fetchall()
                    if r["st"] not in TERMINAL_GROUP_STATUSES
                ]
                if undecided:
                    raise QueueError(
                        ErrorCode.OFFSET_GAP,
                        f"位点跳过未定论任务：组 {group!r} 在前 {new_offset} 条中"
                        f"仍有 {len(undecided)} 条没有终态结论"
                        f"（如 {undecided[:3]}），拒绝提交 {new_offset}",
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

    # ---------- 进度对账 ----------

    def group_progress(self, group: str) -> GroupProgress:
        """导出某个消费者组的进度对账结果。

        返回该组位点，以及 pending / inflight / retrying / succeeded /
        failed / rejected 各桶的任务 id 列表（按入队顺序）。
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
                offset = row["offset"]
                # 对账前补齐账本，保证建组前的历史任务也计入
                self._backfill_group_jobs(conn, group)
                rows = conn.execute(
                    "SELECT j.id AS id, gj.status AS st FROM jobs j "
                    "JOIN group_jobs gj ON gj.job_id=j.id AND gj.\"group\"=? "
                    "ORDER BY j.created_at, j.rowid",
                    (group,),
                ).fetchall()
                conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        buckets: dict[str, list[str]] = {
            "pending": [],
            "inflight": [],
            "retrying": [],
            "succeeded": [],
            "failed": [],
            "rejected": [],
        }
        for r in rows:
            buckets.setdefault(r["st"], []).append(r["id"])
        return GroupProgress(
            group=group,
            offset=offset,
            total_jobs=len(rows),
            buckets=buckets,
        )

    def job_group_statuses(self, job_id: str) -> dict[str, str]:
        """查询一个任务在各个消费者组内的处理结论（组名 -> 组内状态）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT \"group\" AS g, status AS st FROM group_jobs "
                "WHERE job_id=? ORDER BY \"group\"",
                (job_id,),
            ).fetchall()
        return {r["g"]: r["st"] for r in rows}

    # ---------- 检查点（进度快照） ----------

    def export_checkpoint(self, group: str) -> Checkpoint:
        """导出某组当前的进度快照：位点 + 该组内所有终态结论 + 全量任务。

        快照自足：除结论外还带上导出时队列中的全部任务（按入队顺序），
        因此可搬运到另一进程、甚至一份完全空白的队列数据上恢复续跑。
        """
        progress = self.group_progress(group)
        conclusions: dict[str, str] = {}
        for status in sorted(TERMINAL_GROUP_STATUSES):
            for job_id in progress.buckets.get(status, []):
                conclusions[job_id] = status
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, payload, dedupe_key, rowid AS seq, created_at, "
                "max_retries FROM jobs ORDER BY created_at, rowid"
            ).fetchall()
        jobs = [
            CheckpointJob(
                id=row["id"],
                payload=json.loads(row["payload"]),
                dedupe_key=row["dedupe_key"],
                seq=row["seq"],
                created_at=row["created_at"],
                max_retries=row["max_retries"],
            )
            for row in rows
        ]
        return Checkpoint(
            group=group,
            offset=progress.offset,
            created_at=self._now(),
            conclusions=conclusions,
            jobs=jobs,
        )

    def restore_checkpoint(
        self, checkpoint: Checkpoint, group: str | None = None
    ) -> GroupOffset:
        """把快照恢复到指定组（默认快照原组名），供重启/换进程后接着消费。

        快照是自足的：除终态结论外还携带全量任务，因此可恢复进另一个进程、
        甚至一份完全空白的队列数据：

        - 快照携带但目标库缺失的任务会被原样补齐（保留 id / payload /
          dedupe_key / 入队顺序），快照未收录的库内既有任务保持 pending；
        - 快照中的终态结论写入目标组：已定论任务不会再被投递；未定论任务
          （含源组当时在途 / 退避的任务）保持 pending，不会被跳过或丢失；
        - 目标组已有不同终态结论时拒绝（checkpoint_conflict），不覆盖；
        - 位点只前进不回退，且恢复后位点始终与该组真实进度对齐：先纠正到
          “前 n 条任务全部已定论”的最长连续前缀，再与当前位点取较大者；
        - 快照位点超过恢复后队列任务总数时拒绝（checkpoint_invalid）；
        - 只写目标组的账本，其它组不受影响。
        """
        target = group or checkpoint.group
        if not target:
            raise QueueError(ErrorCode.INVALID_PAYLOAD, "检查点恢复目标组不能为空")
        for job_id, status in checkpoint.conclusions.items():
            if status not in TERMINAL_GROUP_STATUSES:
                raise QueueError(
                    ErrorCode.CHECKPOINT_INVALID,
                    f"检查点含非终态结论：job={job_id} status={status!r}",
                )
        now = self._now()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                conn = self._conn
                conn.execute(
                    "INSERT INTO groups (name, \"offset\") VALUES (?,0) "
                    "ON CONFLICT(name) DO NOTHING",
                    (target,),
                )

                # 1) 自足恢复：把快照携带、但目标库缺失的任务原样补齐。
                #    旧版无 jobs 的快照无法补齐，引用缺失任务仍按非法快照拒绝。
                materialized: list[str] = []
                for cp_job in sorted(checkpoint.jobs, key=lambda j: j.seq):
                    row = conn.execute(
                        "SELECT dedupe_key FROM jobs WHERE id=?", (cp_job.id,)
                    ).fetchone()
                    if row is not None:
                        if (
                            cp_job.dedupe_key is not None
                            and row["dedupe_key"] != cp_job.dedupe_key
                        ):
                            dup = conn.execute(
                                "SELECT id FROM jobs WHERE dedupe_key=?",
                                (cp_job.dedupe_key,),
                            ).fetchone()
                            if dup is not None and dup["id"] != cp_job.id:
                                raise QueueError(
                                    ErrorCode.CHECKPOINT_INVALID,
                                    f"快照任务 {cp_job.id} 的 dedupe_key="
                                    f"{cp_job.dedupe_key!r} 与库中任务 "
                                    f"{dup['id']} 冲突，拒绝恢复",
                                )
                        continue
                    if cp_job.dedupe_key is not None:
                        dup = conn.execute(
                            "SELECT id FROM jobs WHERE dedupe_key=?",
                            (cp_job.dedupe_key,),
                        ).fetchone()
                        if dup is not None:
                            raise QueueError(
                                ErrorCode.CHECKPOINT_INVALID,
                                f"快照任务 {cp_job.id} 的 dedupe_key="
                                f"{cp_job.dedupe_key!r} 已被库中任务 "
                                f"{dup['id']} 占用，拒绝恢复",
                            )
                    body = json.dumps(
                        cp_job.payload, ensure_ascii=False, sort_keys=True
                    )
                    # 目标库该序号位置空闲时按原 seq 落位，完整保留入队顺序；
                    # 位置已被库内任务占用（混跑场景）则交给 SQLite 分配新 rowid，
                    # 顺序退化为按 created_at 排序。
                    rowid_free = conn.execute(
                        "SELECT 1 FROM jobs WHERE rowid=?", (cp_job.seq,)
                    ).fetchone() is None
                    if rowid_free:
                        conn.execute(
                            "INSERT INTO jobs (rowid, id, dedupe_key, payload, state, "
                            "attempts, max_retries, available_at, last_error, "
                            "created_at, updated_at) "
                            "VALUES (?,?,?,?,?,0,?,?,NULL,?,?)",
                            (
                                cp_job.seq,
                                cp_job.id,
                                cp_job.dedupe_key,
                                body,
                                JobState.PENDING.value,
                                cp_job.max_retries,
                                cp_job.created_at,
                                cp_job.created_at,
                                now,
                            ),
                        )
                    else:
                        conn.execute(
                            "INSERT INTO jobs (id, dedupe_key, payload, state, "
                            "attempts, max_retries, available_at, last_error, "
                            "created_at, updated_at) "
                            "VALUES (?,?,?,?,?,0,?,?,NULL,?,?)",
                            (
                                cp_job.id,
                                cp_job.dedupe_key,
                                body,
                                JobState.PENDING.value,
                                cp_job.max_retries,
                                cp_job.created_at,
                                cp_job.created_at,
                                now,
                            ),
                        )
                    materialized.append(cp_job.id)

                unknown = [
                    job_id
                    for job_id in checkpoint.conclusions
                    if conn.execute(
                        "SELECT 1 FROM jobs WHERE id=?", (job_id,)
                    ).fetchone()
                    is None
                ]
                if unknown:
                    raise QueueError(
                        ErrorCode.CHECKPOINT_INVALID,
                        f"检查点引用了队列中不存在的任务：{unknown[:3]}"
                        f"（共 {len(unknown)} 条）；旧版不含任务清单的快照"
                        "只能恢复进包含这些任务的队列数据",
                    )

                # 2) 把补齐的 / 库内既有的任务登记到目标组账本（pending），
                #    再覆盖写入快照终态结论；与已有不同终态冲突则整体拒绝。
                self._backfill_group_jobs(conn, target)
                conflicts: list[str] = []
                changed: list[str] = []
                for job_id, status in checkpoint.conclusions.items():
                    row = conn.execute(
                        "SELECT status FROM group_jobs "
                        "WHERE \"group\"=? AND job_id=?",
                        (target, job_id),
                    ).fetchone()
                    if row is None:
                        conn.execute(
                            "INSERT INTO group_jobs "
                            "(\"group\", job_id, delivery_id, status) "
                            "VALUES (?,?,NULL,?)",
                            (target, job_id, status),
                        )
                        changed.append(job_id)
                    elif row["status"] in TERMINAL_GROUP_STATUSES:
                        if row["status"] != status:
                            conflicts.append(
                                f"{job_id}: 现有={row['status']} 快照={status}"
                            )
                    else:
                        conn.execute(
                            "UPDATE group_jobs SET delivery_id=NULL, status=?, "
                            "available_at=0 WHERE \"group\"=? AND job_id=?",
                            (status, target, job_id),
                        )
                        changed.append(job_id)
                if conflicts:
                    raise QueueError(
                        ErrorCode.CHECKPOINT_CONFLICT,
                        f"检查点与组 {target!r} 现有终态结论冲突：{conflicts[:3]}"
                        f"（共 {len(conflicts)} 条），未做任何修改",
                    )

                # 3) 位点对齐：不能超过任务总数，且必须等于真实进度
                #    （前 n 条全部已定论的最长连续前缀），只前进不回退。
                total = conn.execute(
                    "SELECT COUNT(*) AS c FROM jobs"
                ).fetchone()["c"]
                if checkpoint.offset > total:
                    raise QueueError(
                        ErrorCode.CHECKPOINT_INVALID,
                        f"检查点位点越界：恢复后队列中仅有 {total} 条任务，"
                        f"快照位点={checkpoint.offset}，拒绝恢复",
                    )
                rows = conn.execute(
                    "SELECT j.id AS id, gj.status AS st FROM jobs j "
                    "JOIN group_jobs gj ON gj.job_id=j.id AND gj.\"group\"=? "
                    "ORDER BY j.created_at, j.rowid",
                    (target,),
                ).fetchall()
                aligned = 0
                for r in rows:
                    if r["st"] not in TERMINAL_GROUP_STATUSES:
                        break
                    aligned += 1
                effective = min(checkpoint.offset, aligned)
                cur_row = conn.execute(
                    "SELECT \"offset\" FROM groups WHERE name=?", (target,)
                ).fetchone()
                new_offset = max(cur_row["offset"], effective)
                conn.execute(
                    "UPDATE groups SET \"offset\"=? WHERE name=?",
                    (new_offset, target),
                )

                # 4) 汇总受影响任务的全局状态（补齐任务初始为 pending，
                #    随后按各组账本汇总出正确的终态）。
                for job_id in materialized:
                    self._rollup_job(conn, job_id, now)
                for job_id in changed:
                    self._rollup_job(conn, job_id, now)
                conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        result = self.get_offset(target)
        assert result is not None
        return result
