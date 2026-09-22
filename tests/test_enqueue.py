"""入队、去重与不可静默丢弃的校验。"""

from __future__ import annotations

import logging

import pytest

from jobqueue.errors import ErrorCode, QueueError
from jobqueue.models import JobState

logger = logging.getLogger("tests.test_enqueue")


def test_enqueue_assigns_pending_and_id(queue) -> None:
    job = queue.enqueue({"task": "send_email", "to": "a@example.com"})
    logger.info("[输入] payload={task=send_email} [判定] 入队后状态应为 pending 且生成 id")
    assert job.state is JobState.PENDING
    assert job.attempts == 0
    assert job.id
    assert queue.list_jobs() == [job]


def test_duplicate_dedupe_key_is_rejected_not_duplicated(queue) -> None:
    first = queue.enqueue({"n": 1}, dedupe_key="idem-1")
    logger.info("[输入] 重复提交 dedupe_key=idem-1 [判定] 第二次必须返回 duplicate_job，且任务仍只有一条")
    with pytest.raises(QueueError) as exc:
        queue.enqueue({"n": 2}, dedupe_key="idem-1")
    assert exc.value.code is ErrorCode.DUPLICATE_JOB
    assert first.id in exc.value.message
    jobs = queue.list_jobs()
    assert len(jobs) == 1
    assert jobs[0].payload == {"n": 1}
    assert queue.get_existing_by_dedupe_key("idem-1").id == first.id


def test_same_payload_without_dedupe_key_creates_two_jobs(queue) -> None:
    queue.enqueue({"n": 1})
    queue.enqueue({"n": 1})
    logger.info("[输入] 两次相同 payload 但无 dedupe_key [判定] 应视为两条独立任务")
    assert len(queue.list_jobs()) == 2


def test_invalid_payload_rejected_with_distinct_code(queue) -> None:
    logger.info("[输入] 直接向存储写入非 dict payload [判定] 必须返回 invalid_payload 而非静默丢弃")
    with pytest.raises(QueueError) as exc:
        queue.store.enqueue(["not", "a", "mapping"], None)  # type: ignore[arg-type]
    assert exc.value.code is ErrorCode.INVALID_PAYLOAD
    assert queue.list_jobs() == []
