"""入队校验与重复提交幂等：重复提交绝不导致重复执行；非法输入必须被拒绝并给出原因。"""

import logging

import pytest

from taskqueue.exceptions import EnqueueRejected
from taskqueue.models import RejectReason, TaskStatus

logger = logging.getLogger(__name__)


def test_enqueue_assigns_monotonic_ids_and_pending_state(queue):
    logger.info("输入: 连续入队 echo#1 echo#2；判定: id 严格递增且状态为 pending")
    t1 = queue.enqueue("echo", {"i": 1})
    t2 = queue.enqueue("echo", {"i": 2})
    logger.info("结果: t1.id=%d t2.id=%d 状态=%s/%s", t1.id, t2.id, t1.status.value, t2.status.value)
    assert t2.id == t1.id + 1
    assert t1.status is TaskStatus.PENDING
    assert t1.attempts == 0


def test_duplicate_dedup_key_returns_same_task(queue):
    logger.info("输入: 同 dedup_key=k 提交两次(payload 不同)；判定: 第二次必须返回同一任务，只入队一条")
    t1 = queue.enqueue("echo", {"v": "original"}, dedup_key="k")
    t2 = queue.enqueue("echo", {"v": "changed"}, dedup_key="k")
    logger.info("结果: id 相同=%s, 保留原 payload=%s", t1.id == t2.id, t2.payload)
    counts = queue.status_counts()
    assert t1.id == t2.id
    assert '"original"' in t2.payload
    assert counts.get(TaskStatus.PENDING.value, 0) == 1


def test_duplicate_submit_never_executed_twice(queue):
    """重复投递/重复提交在投递层也只会产生一次执行（只被 poll 到一次）。"""
    logger.info("输入: dedup_key=k 提交 3 次，然后 poll；判定: 只投递 1 条")
    ids = {queue.enqueue("echo", {"n": n}, dedup_key="k").id for n in range(3)}
    delivered = queue.poll("g", "c1")
    logger.info("结果: 唯一 id 集合=%s, 投递数量=%d", ids, len(delivered))
    assert ids == {1}
    assert len(delivered) == 1
    assert queue.poll("g", "c2") == []


@pytest.mark.parametrize(
    "payload",
    ['', '   ', 'not-json', '123', 'true', '[1,2,3]', 'null'],
)
def test_malformed_payload_is_rejected_with_reason(queue, payload):
    logger.info("输入: 非法 payload=%r；判定: 抛 EnqueueRejected(MALFORMED_PAYLOAD)", payload)
    with pytest.raises(EnqueueRejected) as exc:
        queue.enqueue("echo", payload)
    logger.info("结果: reason=%s detail=%s", exc.value.reason.value, exc.value.detail)
    assert exc.value.reason is RejectReason.MALFORMED_PAYLOAD
    # 任务未被静默接受
    assert queue.status_counts().get("pending", 0) == 0


def test_empty_task_type_rejected(queue):
    logger.info("输入: task_type='  '；判定: MALFORMED_PAYLOAD")
    with pytest.raises(EnqueueRejected) as exc:
        queue.enqueue("   ", {"x": 1})
    assert exc.value.reason is RejectReason.MALFORMED_PAYLOAD


def test_empty_dedup_key_rejected(queue):
    with pytest.raises(EnqueueRejected) as exc:
        queue.enqueue("echo", {}, dedup_key="  ")
    assert exc.value.reason is RejectReason.MALFORMED_PAYLOAD
