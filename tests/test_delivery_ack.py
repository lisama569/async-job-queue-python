"""投递与确认：独占租约、fence token 防重复完成、重复 ack 幂等、终态不可变更。"""

import logging

import pytest

from taskqueue.exceptions import AcknowledgeRejected
from taskqueue.models import RejectReason, TaskStatus

logger = logging.getLogger(__name__)


def test_poll_grants_exclusive_lease(queue):
    logger.info("输入: 1 个任务，g/c1 与 g/c2 同时 poll；判定: 只有一个消费者拿到")
    t = queue.enqueue("echo", {})
    d1 = queue.poll("g", "c1")
    d2 = queue.poll("g", "c2")
    logger.info("结果: c1=%s c2=%s owner=%s",
                [x.task.id for x in d1], [x.task.id for x in d2],
                queue.get_task(t.id).lease_owner)
    assert [x.task.id for x in d1] == [t.id]
    assert d2 == []
    assert queue.get_task(t.id).status is TaskStatus.INFLIGHT
    assert queue.get_task(t.id).lease_owner == "g/c1"


def test_stale_epoch_ack_is_rejected(queue):
    logger.info("输入: 用错误 epoch ack；判定: LEASE_EXPIRED（fence 拒绝陈旧持有者）")
    t = queue.enqueue("echo", {})
    d = queue.poll("g", "c1")[0]
    with pytest.raises(AcknowledgeRejected) as exc:
        queue.acknowledge(t.id, d.epoch + 999)
    logger.info("结果: reason=%s", exc.value.reason.value)
    assert exc.value.reason is RejectReason.LEASE_EXPIRED
    # 任务仍在原租约中，没有被完成
    assert queue.get_task(t.id).status is TaskStatus.INFLIGHT


def test_ack_success_is_idempotent_on_repeat(queue):
    logger.info("输入: 成功 ack 后用同一 epoch 重复 ack；判定: 幂等返回 succeeded，不重复执行")
    t = queue.enqueue("echo", {})
    d = queue.poll("g", "c1")[0]
    a1 = queue.acknowledge(t.id, d.epoch)
    a2 = queue.acknowledge(t.id, d.epoch)
    logger.info("结果: 两次状态=%s/%s, 任务总数守恒", a1.status.value, a2.status.value)
    assert a1.status is TaskStatus.SUCCEEDED
    assert a2.status is TaskStatus.SUCCEEDED
    counts = queue.status_counts()
    assert counts.get("succeeded", 0) == 1


def test_nack_after_success_rejected_not_dropped(queue):
    logger.info("输入: 已成功的任务再 nack；判定: NOT_INFLIGHT 被明确拒绝")
    t = queue.enqueue("echo", {})
    d = queue.poll("g", "c1")[0]
    queue.acknowledge(t.id, d.epoch)
    with pytest.raises(AcknowledgeRejected) as exc:
        queue.fail(t.id, d.epoch, "late failure")
    logger.info("结果: reason=%s", exc.value.reason.value)
    assert exc.value.reason is RejectReason.NOT_INFLIGHT
    assert queue.get_task(t.id).status is TaskStatus.SUCCEEDED


def test_ack_unknown_task_is_distinguishable(queue):
    with pytest.raises(AcknowledgeRejected) as exc:
        queue.acknowledge(424242, epoch=1)
    assert exc.value.reason is RejectReason.UNKNOWN_TASK


def test_fail_requires_a_reason(queue):
    logger.info("输入: 空失败原因 nack；判定: 拒绝，失败原因不可为空（不可静默）")
    t = queue.enqueue("echo", {})
    d = queue.poll("g", "c1")[0]
    with pytest.raises(AcknowledgeRejected) as exc:
        queue.fail(t.id, d.epoch, "   ")
    assert exc.value.reason is RejectReason.MALFORMED_PAYLOAD


def test_terminal_failed_task_keeps_reason_and_is_queryable(queue):
    logger.info("输入: max_attempts=1 下失败一次；判定: 进入 failed 终态且原因可查询")
    queue.config = __import__("dataclasses").replace(queue.config, max_attempts=1)
    t = queue.enqueue("echo", {})
    d = queue.poll("g", "c1")[0]
    failed = queue.fail(t.id, d.epoch, "DOWNSTREAM_UNAVAILABLE", retry=True)
    logger.info("结果: status=%s reason=%s attempts=%d",
                failed.status.value, failed.terminal_reason, failed.attempts)
    assert failed.status is TaskStatus.FAILED
    assert failed.terminal_reason == "DOWNSTREAM_UNAVAILABLE"
    # 终态任务不会再被投递
    assert queue.poll("g", "c1") == []


def test_ack_after_redelivery_uses_new_epoch_and_old_ack_loses(queue, clock):
    logger.info("输入: 租约超时后重投，旧持有者迟到 ack；判定: 旧 epoch 被拒，新 epoch 成功")
    t = queue.enqueue("echo", {})
    first = queue.poll("g", "c1")[0]
    clock.bump(queue.config.lease_timeout_ms + 1)
    reclaimed = queue.reclaim_expired_leases()
    second = queue.poll("g", "c2")[0]
    logger.info("结果: 回收=%d, 第一次 epoch=%d 第二次 epoch=%d", reclaimed, first.epoch, second.epoch)
    assert reclaimed == 1
    assert second.epoch == first.epoch + 1
    assert second.task.lease_owner == "g/c2"
    with pytest.raises(AcknowledgeRejected) as exc:
        queue.acknowledge(t.id, first.epoch)
    assert exc.value.reason is RejectReason.LEASE_EXPIRED
    done = queue.acknowledge(t.id, second.epoch)
    assert done.status is TaskStatus.SUCCEEDED
    # 重复的“迟到完成”不可能把任务翻回 inflight
    assert queue.status_counts().get("inflight", 0) == 0


def test_poll_ordering_is_fifo_by_id(queue):
    logger.info("输入: 入队 a,b,c 后 poll 批量；判定: 按 id FIFO")
    ids = [queue.enqueue("echo", {"n": n}).id for n in range(3)]
    got = [d.task.id for d in queue.poll("g", "c1", max_messages=10)]
    assert got == ids
