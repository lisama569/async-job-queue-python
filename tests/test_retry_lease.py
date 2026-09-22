"""失败重试：指数退避、尝试次数上限终态、租约超时自动回收重投。"""

import logging

from taskqueue.models import TaskStatus

logger = logging.getLogger(__name__)


def test_retry_respects_exponential_backoff(queue, clock):
    logger.info("输入: 失败后分别在退避点前/后 poll；判定: 退避期内不可见，到期可重投")
    t = queue.enqueue("echo", {})
    first = queue.poll("g", "c1")[0]
    failed = queue.fail(t.id, first.epoch, "TRANSIENT")
    base = queue.config.backoff_base_ms
    logger.info("结果: available_at 推迟=%dms, 状态=%s",
                failed.available_at_ms - clock.now_ms, failed.status.value)
    assert failed.status is TaskStatus.PENDING
    assert failed.available_at_ms == clock.now_ms + base

    clock.bump(base - 1)
    assert queue.poll("g", "c1") == []
    clock.bump(2)
    second = queue.poll("g", "c1")[0]
    assert second.task.id == t.id
    assert second.epoch == 2 and second.task.attempts == 2


def test_backoff_grows_per_attempt(queue, clock):
    logger.info("输入: 连续失败；判定: 退避 100->200->400(封顶 10000)")
    t = queue.enqueue("echo", {})
    expected = [100, 200, 400]
    for i, delay in enumerate(expected, start=1):
        d = queue.poll("g", "c1")[0]
        assert d.epoch == i
        f = queue.fail(t.id, d.epoch, f"err{i}")
        assert f.available_at_ms == clock.now_ms + delay
        clock.bump(delay + 1)
    assert queue.get_task(t.id).attempts == 3


def test_exceeding_max_attempts_enters_failed_terminal(queue, clock):
    logger.info("输入: max_attempts=4，持续失败；判定: 第 4 次失败后进入 failed 终态且不再投递")
    t = queue.enqueue("echo", {})
    statuses = []
    for i in range(1, 5):
        d = queue.poll("g", "c1")[0]
        f = queue.fail(t.id, d.epoch, "BUSY")
        statuses.append(f.status.value)
        clock.bump(queue.config.backoff_max_ms + 1)
    final = queue.get_task(t.id)
    logger.info("结果: 历次状态=%s 最终=%s 原因=%s 尝试=%d/%d",
                statuses, final.status.value, final.terminal_reason,
                final.attempts, final.max_attempts)
    assert statuses == ["pending", "pending", "pending", "failed"]
    assert final.is_terminal()
    assert final.terminal_reason == "BUSY"
    # 时间再怎么推进也不会再出来
    clock.bump(10_000_000)
    assert queue.poll("g", "c1") == []
    counts = queue.status_counts()
    assert counts.get("failed", 0) == 1


def test_explicit_no_retry_fails_immediately(queue, clock):
    logger.info("输入: fail(retry=False)；判定: 不消费重试预算直接终态，原因保留")
    t = queue.enqueue("echo", {})
    d = queue.poll("g", "c1")[0]
    f = queue.fail(t.id, d.epoch, "INVALID_ARGUMENT", retry=False)
    assert f.status is TaskStatus.FAILED
    assert f.attempts == 1
    assert f.terminal_reason == "INVALID_ARGUMENT"


def test_expired_lease_is_reclaimed_and_redelivered(queue, clock):
    logger.info("输入: c1 拿到任务后崩溃，超过可见超时；判定: 回收计数=1，c2 可重新拿到")
    t = queue.enqueue("echo", {})
    d = queue.poll("g", "c1")[0]
    clock.bump(queue.config.lease_timeout_ms - 1)
    assert queue.reclaim_expired_leases() == 0  # 还差 1ms，未到期
    clock.bump(1)
    assert queue.reclaim_expired_leases() == 1  # 到达到期点立即可回收
    again = queue.poll("g", "c2")[0]
    logger.info("结果: 重投任务 id=%s epoch %d->%d owner=%s",
                again.task.id, d.epoch, again.epoch, again.task.lease_owner)
    assert again.task.id == t.id
    assert again.epoch == d.epoch + 1
    assert again.task.lease_owner == "g/c2"


def test_non_expired_lease_is_not_reclaimed(queue, clock):
    queue.enqueue("echo", {})
    queue.poll("g", "c1")
    clock.bump(queue.config.lease_timeout_ms - 1)
    assert queue.reclaim_expired_leases() == 0
    assert queue.poll("g", "c2") == []


def test_poll_itself_reclaims_expired_leases(queue, clock):
    """poll 前会顺带回收，所以即使没有显式调用 reaper，超时任务也能重投。"""
    t = queue.enqueue("echo", {})
    queue.poll("g", "c1")
    clock.bump(queue.config.lease_timeout_ms + 1)
    redelivered = queue.poll("g", "c2")
    assert [d.task.id for d in redelivered] == [t.id]
