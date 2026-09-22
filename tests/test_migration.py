"""早期 schema（group_jobs 无 status/attempts 列）打开时自动迁移。"""

from __future__ import annotations

import logging
import sqlite3

from jobqueue.config import Settings
from jobqueue.models import JobState
from jobqueue.queue import JobQueue

logger = logging.getLogger("tests.test_migration")


def test_open_legacy_schema_and_continue(settings, tmp_path) -> None:
    db = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE jobs (
            id TEXT PRIMARY KEY, dedupe_key TEXT UNIQUE, payload TEXT NOT NULL,
            state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL, available_at REAL NOT NULL,
            last_error TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL);
        CREATE TABLE groups (name TEXT PRIMARY KEY, "offset" INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE group_jobs (
            "group" TEXT NOT NULL, job_id TEXT NOT NULL, delivery_id INTEGER,
            terminal INTEGER NOT NULL DEFAULT 0, PRIMARY KEY ("group", job_id));
        CREATE TABLE deliveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT, "group" TEXT NOT NULL,
            job_id TEXT NOT NULL, consumer TEXT NOT NULL, attempt INTEGER NOT NULL,
            delivered_at REAL NOT NULL, visible_after REAL NOT NULL, state TEXT NOT NULL);
        INSERT INTO groups VALUES ('g', 1);
        """
    )
    conn.execute(
        "INSERT INTO jobs VALUES ('j1',NULL,'{}','succeeded',1,3,0,NULL,0,0)"
    )
    conn.execute("INSERT INTO group_jobs ('group',job_id,delivery_id,terminal) "
                 "VALUES ('g','j1',NULL,1)")
    conn.commit()
    conn.close()

    logger.info("[输入] 打开旧版表结构数据库 [判定] 自动补列且不丢失数据/位点")
    q = JobQueue(Settings(db_path=db, base_backoff=0.0))
    assert q.get_offset("g").offset == 1
    assert q.get_job("j1").state is JobState.SUCCEEDED
    assert q.claim("g", "c") is None  # 已终态不复活
    j2 = q.enqueue({"fresh": True})
    d = q.claim("g", "c")
    assert d is not None and d.job_id == j2.id and d.attempt == 1
    q.ack(d.id, "c")
    assert q.get_job(j2.id).state is JobState.SUCCEEDED
    q.close()
