"""失败重试、指数退避、重试上限终态与不可处理拒绝。"""

from __future__ import annotations

import logging
import time
from dataclasses import replace

import pytest

from jobqueue.config import Settings
from jobqueue.errors import ErrorCode, QueueError
from jobqueue.models import JobState
from jobqueue.queue import JobQueue

logger = logging.getLogger("tests.test_retry")


def _drain_until_terminal(queue, job_id: str, group: str = "g") -> object:
    """反复占用+nack 直到终态，返回最终 Job。"""
    while True:
        d = queue.claim(group, "c")
        if d is None:
            time.sleep(0.005)
            if queue.get_job(job_id).terminal:
                return queue.get_job(job_id)
            continue
        if queue.get_job(job_id).terminal:
            return queue.get_job(job_id)
        queue.nack(d.id, "c", "transient failure")
        job = queue.get_job(job_id)
        if job.terminal:
            return job


def test_nack_requires_reason(queue) -> None:
    queue.enqueue({"x": 1})
    d = queue.claim("g", "c")
    logger.info("[输入] nack 时不传失败原因 [判定] invalid_payload，禁止静默失败")
    with pytest.raises(QueueError) as exc:
        queue.nack(d.id, "c", "   ")
    assert exc.value.code is ErrorCode.INVALID_PAYLOAD
    assert queue.find_job(d.job_id).state is JobState.INFLIGHT


def test_retries_backoff_then_terminal_failed(settings, queue) -> None:
    settings = replace(settings, base_backoff=0.0)
    queue.close()
    queue = JobQueue(settings)
    job = queue.enqueue({"x": 1})
    final = _drain_until_terminal(queue, job.id)
    logger.info(
        "[输入] 连续 nack max_retries+1 次 [判定] attempts=4 后进入 failed 终态，"
        "原因可查询：%s",
        final.last_error,
    )
    assert final.state is JobState.FAILED
    assert final.attempts == settings.max_retries + 1
    assert "重试已耗尽" in (final.last_error or "")
    # 终态任务不会再被投递
    assert queue.claim("g", "c") is None


def test_backoff_delays_redelivery(settings, tmp_path) -> None:
    s = Settings(
        db_path=str(tmp_path / "b.db"),
        visibility_timeout=10.0,
        max_retries=3,
        base_backoff=0.05,
        backoff_multiplier=2.0,
        backoff_cap=1.0,
    )
    queue = JobQueue(s)
    job = queue.enqueue({"x": 1})
    d = queue.claim("g", "c")
    queue.nack(d.id, "c", "boom")
    logger.info("[输入] nack 后立即 claim（退避 0.05s）[判定] 退避窗口内不可投递")
    assert queue.claim("g", "c") is None
    assert queue.get_job(job.id).state is JobState.RETRYING
    time.sleep(0.06)
    d2 = queue.claim("g", "c")
    assert d2 is not None and d2.attempt == 2
    queue.close()


def test_unprocessable_nack_enters_rejected_immediately(queue) -> None:
    job = queue.enqueue({"x": 1})
    d = queue.claim("g", "c")
    queue.nack(d.id, "c", "不支持的任务类型", unprocessable=True)
    final = queue.get_job(job.id)
    logger.info(
        "[输入] nack(reject=true) [判定] 立即进入 rejected 终态且不再投递，原因=%s",
        final.last_error,
    )
    assert final.state is JobState.REJECTED
    assert final.attempts == 1
    assert "不可处理" in (final.last_error or "")
    assert queue.claim("g", "c") is None


def test_failed_and_rejected_have_distinct_reasons(settings, queue) -> None:
    queue.close()
    queue = JobQueue(replace(settings, base_backoff=0.0))
    j1 = queue.enqueue({"kind": "flaky"})
    d1 = queue.claim("g", "c")
    queue.nack(d1.id, "c", "bad format", unprocessable=True)
    j2 = queue.enqueue({"kind": "flaky"})
    d2 = queue.claim("g", "c")
    for _ in range(10):
        queue.nack(d2.id, "c", "network down")
        d2 = queue.claim("g", "c")
        if d2 is None:
            break
    rejected, failed = queue.get_job(j1.id), queue.get_job(j2.id)
    logger.info(
        "[输入] reject 与重试耗尽各一条 [判定] rejected.last_error=%r；failed.last_error=%r",
        rejected.last_error,
        failed.last_error,
    )
    assert rejected.state is JobState.REJECTED
    assert failed.state is JobState.FAILED
    assert "不可处理" in rejected.last_error
    assert "重试已耗尽" in failed.last_error
