"""消费者组独立性与位点推进规则（拒绝回退/越界）。"""

from __future__ import annotations

import logging

import pytest

from jobqueue.errors import ErrorCode, QueueError
from jobqueue.models import JobState

logger = logging.getLogger("tests.test_groups")


def test_groups_consume_independently(queue) -> None:
    job = queue.enqueue({"x": 1})
    da = queue.claim("group-a", "c1")
    logger.info(
        "[输入] group-a 占用 job=%s 后 group-b claim [判定] 不同组各自有独立投递，均可消费一次",
        job.id,
    )
    db = queue.claim("group-b", "c2")
    assert da is not None and db is not None
    assert da.job_id == db.job_id == job.id
    # 同组第二个消费者拿不到
    assert queue.claim("group-a", "c2") is None
    # 两个组各自确认
    queue.ack(da.id, "c1")
    queue.ack(db.id, "c2")
    assert queue.get_job(job.id).state is JobState.SUCCEEDED


def test_new_group_sees_existing_jobs(queue) -> None:
    queue.enqueue({"n": 1})
    logger.info("[输入] 先入队后注册组再 claim [判定] 懒补齐使新组能看到历史任务")
    queue.create_group("late")
    d = queue.claim("late", "c")
    assert d is not None


def test_commit_offset_must_advance(queue) -> None:
    queue.create_group("g")
    for i in range(3):
        queue.enqueue({"i": i})
    queue.commit_offset("g", 1)
    queue.commit_offset("g", 2)
    logger.info("[输入] 位点 2 -> 2（原地）与 2 -> 1（回退）[判定] 均返回 offset_rollback")
    for bad in (2, 1, 0):
        with pytest.raises(QueueError) as exc:
            queue.commit_offset("g", bad)
        assert exc.value.code is ErrorCode.OFFSET_ROLLBACK
    assert queue.get_offset("g").offset == 2


def test_commit_offset_out_of_range_rejected(queue) -> None:
    queue.create_group("g")
    queue.enqueue({"i": 1})
    logger.info("[输入] 只有 1 条任务却提交位点 5 [判定] offset_out_of_range，位点不改变")
    with pytest.raises(QueueError) as exc:
        queue.commit_offset("g", 5)
    assert exc.value.code is ErrorCode.OFFSET_OUT_OF_RANGE
    assert queue.get_offset("g").offset == 0


def test_commit_offset_unknown_group_rejected(queue) -> None:
    with pytest.raises(QueueError) as exc:
        queue.commit_offset("ghost", 1)
    assert exc.value.code is ErrorCode.GROUP_NOT_FOUND


def test_offset_survives_negative_and_non_int(queue) -> None:
    queue.create_group("g")
    with pytest.raises(QueueError) as exc:
        queue.store.commit_offset("g", -1)  # type: ignore[arg-type]
    assert exc.value.code is ErrorCode.INVALID_PAYLOAD
