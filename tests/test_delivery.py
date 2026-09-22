"""投递占用、确认、重复确认/越权确认/过期确认。"""

from __future__ import annotations

import logging
import time

import pytest

from jobqueue.errors import ErrorCode, QueueError
from jobqueue.models import JobState
from jobqueue.queue import JobQueue

logger = logging.getLogger("tests.test_delivery")


def test_claim_returns_none_when_empty(queue) -> None:
    logger.info("[输入] 空队列 claim [判定] 返回 None（API 层映射 empty_queue）")
    assert queue.claim("g", "c1") is None


def test_claim_marks_inflight_and_second_claim_blocks(queue) -> None:
    job = queue.enqueue({"x": 1})
    d1 = queue.claim("g", "c1")
    logger.info(
        "[输入] c1 占用 job=%s 后 c2 再次 claim [判定] 同一时刻仅一个消费者占用，第二次为 None",
        job.id,
    )
    assert d1 is not None and d1.attempt == 1 and d1.consumer == "c1"
    assert queue.get_job(job.id).state is JobState.INFLIGHT
    assert queue.claim("g", "c2") is None


def test_ack_succeeds_and_duplicate_ack_rejected(queue) -> None:
    job = queue.enqueue({"x": 1})
    d = queue.claim("g", "c1")
    queue.ack(d.id, "c1")
    logger.info("[输入] 对同一 delivery 重复 ack [判定] 首次成功 succeeded，重复确认 delivery_terminal")
    assert queue.get_job(job.id).state is JobState.SUCCEEDED
    with pytest.raises(QueueError) as exc:
        queue.ack(d.id, "c1")
    assert exc.value.code is ErrorCode.DELIVERY_TERMINAL


def test_ack_by_wrong_consumer_rejected(queue) -> None:
    queue.enqueue({"x": 1})
    d = queue.claim("g", "owner")
    logger.info("[输入] 由 consumer=other 确认 owner 的投递 [判定] delivery_owner_mismatch")
    with pytest.raises(QueueError) as exc:
        queue.ack(d.id, "other")
    assert exc.value.code is ErrorCode.DELIVERY_OWNER_MISMATCH
    assert queue.find_job(d.job_id).state is JobState.INFLIGHT


def test_unknown_delivery_ack_is_not_found(queue) -> None:
    with pytest.raises(QueueError) as exc:
        queue.ack(9999, "c1")
    assert exc.value.code is ErrorCode.DELIVERY_NOT_FOUND


def test_ack_after_visibility_timeout_rejected(settings, queue) -> None:
    # 关闭退避，使超时回收后立即可重新投递
    from dataclasses import replace

    queue.close()
    queue = JobQueue(replace(settings, base_backoff=0.0))
    job = queue.enqueue({"x": 1})
    d = queue.claim("g", "slow", visibility_timeout=0.05)
    time.sleep(0.07)
    logger.info(
        "[输入] 占用超时后旧消费者仍 ack delivery=%s [判定] delivery_expired，任务将被重新投递",
        d.id,
    )
    with pytest.raises(QueueError) as exc:
        queue.ack(d.id, "slow")
    assert exc.value.code is ErrorCode.DELIVERY_EXPIRED
    # 下一次 claim 触发回收并重投
    d2 = queue.claim("g", "fast")
    assert d2 is not None and d2.attempt == 2
    queue.ack(d2.id, "fast")
    assert queue.get_job(job.id).state is JobState.SUCCEEDED
    deliveries = queue.list_deliveries(job.id)
    assert [x.state for x in deliveries] == ["timed_out", "acked"]
