"""进程重启后：持久化恢复、任务不丢失、inflight 可被重新投递、位点保留。"""

from __future__ import annotations

import logging
import time
from dataclasses import replace

from jobqueue.config import Settings
from jobqueue.models import JobState
from jobqueue.queue import JobQueue

logger = logging.getLogger("tests.test_restart")


def test_pending_and_offset_survive_restart(settings, tmp_path) -> None:
    q1 = JobQueue(settings)
    q1.create_group("g")
    j1 = q1.enqueue({"n": 1})
    j2 = q1.enqueue({"n": 2})
    q1.claim("g", "c1")  # j1 占用中
    q1.commit_offset("g", 1)
    logger.info(
        "[输入] 进程重启前：j1 inflight、j2 pending、offset=1 [判定] 重开 JobQueue 后全部恢复"
    )
    q1.close()

    q2 = JobQueue(settings)
    assert q2.get_offset("g").offset == 1
    assert q2.get_job(j2.id).state is JobState.PENDING
    assert q2.list_jobs() == [q2.get_job(j1.id), q2.get_job(j2.id)]
    assert q2.get_job(j1.id).state is JobState.INFLIGHT
    q2.close()


def test_inflight_redelivered_after_restart_with_short_timeout(settings) -> None:
    settings = replace(settings, visibility_timeout=0.03, base_backoff=0.0)
    q1 = JobQueue(settings)
    job = q1.enqueue({"n": 1})
    d1 = q1.claim("g", "old-consumer")
    q1.close()
    logger.info(
        "[输入] 消费者 old-consumer 占用 delivery=%s 后进程崩溃，等待占用超时后重启 [判定] 不丢失，被新消费者重新投递",
        d1.id,
    )
    time.sleep(0.05)
    q2 = JobQueue(settings)
    d2 = q2.claim("g", "new-consumer")
    assert d2 is not None
    assert d2.job_id == job.id and d2.attempt == 2 and d2.consumer == "new-consumer"
    q2.ack(d2.id, "new-consumer")
    assert q2.get_job(job.id).state is JobState.SUCCEEDED
    q2.close()


def test_succeeded_job_not_redelivered_after_restart(settings) -> None:
    q1 = JobQueue(settings)
    job = q1.enqueue({"n": 1})
    d = q1.claim("g", "c")
    q1.ack(d.id, "c")
    q1.close()
    q2 = JobQueue(settings)
    logger.info("[输入] succeeded 任务经历重启 [判定] 不会被重复投递")
    assert q2.claim("g", "c") is None
    assert q2.get_job(job.id).state is JobState.SUCCEEDED
    q2.close()


def test_retrying_survives_restart_and_delivers_after_backoff(settings) -> None:
    settings = replace(settings, base_backoff=0.03)
    q1 = JobQueue(settings)
    job = q1.enqueue({"n": 1})
    d = q1.claim("g", "c")
    q1.nack(d.id, "c", "temporary")
    q1.close()
    time.sleep(0.04)
    q2 = JobQueue(settings)
    logger.info("[输入] retrying 任务重启且退避已过 [判定] 被重新投递，attempt=2")
    d2 = q2.claim("g", "c")
    assert d2 is not None and d2.attempt == 2
    q2.close()
