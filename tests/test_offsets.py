"""消费者组与投递位点：只进不退、逐槽推进、不得越界、不得越过未终态任务。"""

import logging

import pytest

from taskqueue.exceptions import OffsetRejected
from taskqueue.models import RejectReason, TaskStatus

logger = logging.getLogger(__name__)


def _make_terminal_tasks(queue, n: int) -> list[int]:
    """入队 n 个任务并全部 ack 成功，返回 id 列表（即日志位点 1..n）。"""
    ids = [queue.enqueue("echo", {"i": i}).id for i in range(n)]
    deliveries = queue.poll("g-worker", "c")
    for d in deliveries:
        queue.acknowledge(d.task.id, d.epoch)
    return ids


def test_commit_advances_exactly_one_slot(queue):
    logger.info("输入: 3 个已完成任务，从位点 0 逐槽提交；判定: 每次只能 +1")
    _make_terminal_tasks(queue, 3)
    queue.create_group("grp")
    g = queue.commit_offset("grp", 1)
    assert g.committed_offset == 1
    g = queue.commit_offset("grp", 2)
    assert g.committed_offset == 2
    g = queue.commit_offset("grp", 3)
    assert g.committed_offset == 3


def test_rollback_is_rejected(queue):
    logger.info("输入: 已提交 2 后请求回退到 1；判定: OFFSET_ROLLBACK")
    _make_terminal_tasks(queue, 3)
    queue.create_group("grp")
    queue.commit_offset("grp", 1)
    queue.commit_offset("grp", 2)
    for requested in (0, 1, 2):
        with pytest.raises(OffsetRejected) as exc:
            queue.commit_offset("grp", requested)
        assert exc.value.reason is RejectReason.OFFSET_ROLLBACK, requested
        logger.info("回退到 %d -> %s（已拒绝）", requested, exc.value.reason.value)
    assert queue.status_counts  # 状态守恒不受失败提交影响


def test_skipping_is_rejected(queue):
    logger.info("输入: 从 0 直接提交 3；判定: OFFSET_SKIP（不能从中间继续推进）")
    _make_terminal_tasks(queue, 3)
    queue.create_group("grp")
    with pytest.raises(OffsetRejected) as exc:
        queue.commit_offset("grp", 3)
    logger.info("结果: %s %s", exc.value.reason.value, exc.value.detail)
    assert exc.value.reason is RejectReason.OFFSET_SKIP
    # 被拒后位点仍停在 0
    assert queue.storage.get_group("grp").committed_offset == 0


def test_overrun_is_rejected(queue):
    logger.info("输入: 日志只有 2 条却提交 5；判定: OFFSET_OVERRUN")
    _make_terminal_tasks(queue, 2)
    queue.create_group("grp")
    with pytest.raises(OffsetRejected) as exc:
        queue.commit_offset("grp", 5)
    assert exc.value.reason is RejectReason.OFFSET_OVERRUN
    assert queue.commit_offset("grp", 1).committed_offset == 1  # 合法推进
    assert queue.commit_offset("grp", 2).committed_offset == 2  # 合法推进
    with pytest.raises(OffsetRejected) as exc:
        queue.commit_offset("grp", 3)
    assert exc.value.reason is RejectReason.OFFSET_OVERRUN


def test_cannot_commit_past_non_terminal_task(queue):
    logger.info("输入: 位点 1 已成功，位点 2 仍 inflight，提交 2；判定: OFFSET_NOT_TERMINAL")
    ids = [queue.enqueue("echo", {"i": i}).id for i in range(2)]
    deliveries = queue.poll("g-worker", "c")
    queue.acknowledge(ids[0], deliveries[0].epoch)
    queue.create_group("grp")
    queue.commit_offset("grp", 1)
    with pytest.raises(OffsetRejected) as exc:
        queue.commit_offset("grp", 2)
    logger.info("结果: %s %s", exc.value.reason.value, exc.value.detail)
    assert exc.value.reason is RejectReason.OFFSET_NOT_TERMINAL
    # 等任务 2 完成后即可推进
    queue.acknowledge(ids[1], deliveries[1].epoch)
    assert queue.commit_offset("grp", 2).committed_offset == 2


def test_failed_terminal_task_can_be_committed(queue):
    """failed 也是终态；位点必须能越过失败任务，否则队列会被永久卡死。"""
    logger.info("输入: 位点 1=成功, 位点 2=失败终态；判定: 可顺序提交 1、2")
    queue.enqueue("echo", {})
    queue.enqueue("echo", {})
    ds = queue.poll("g-worker", "c")
    queue.acknowledge(ds[0].task.id, ds[0].epoch)
    f = queue.fail(ds[1].task.id, ds[1].epoch, "POISON", retry=False)
    assert f.status is TaskStatus.FAILED
    queue.create_group("grp")
    assert queue.commit_offset("grp", 1).committed_offset == 1
    assert queue.commit_offset("grp", 2).committed_offset == 2


def test_unknown_group_rejected(queue):
    with pytest.raises(OffsetRejected) as exc:
        queue.commit_offset("ghost", 1)
    assert exc.value.reason is RejectReason.UNKNOWN_GROUP


def test_groups_are_independent(queue):
    logger.info("输入: 两个组各自提交位点；判定: 互不影响，重启后仍在")
    _make_terminal_tasks(queue, 2)
    queue.create_group("a")
    queue.create_group("b")
    queue.commit_offset("a", 1)
    assert queue.storage.get_group("a").committed_offset == 1
    assert queue.storage.get_group("b").committed_offset == 0
