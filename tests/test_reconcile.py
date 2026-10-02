"""进度对账导出与位点推进的拒绝规则（跳过未定论任务）。"""

from __future__ import annotations

import logging

import pytest

from jobqueue.errors import ErrorCode, QueueError

logger = logging.getLogger("tests.test_reconcile")


def _make_group_with_mixed_states(queue):
    """构造组 g：j1 succeeded、j2 rejected、j3 inflight、j4 pending。"""
    queue.create_group("g")
    jobs = [queue.enqueue({"n": i}) for i in range(4)]
    d = queue.claim("g", "c")
    queue.ack(d.id, "c")  # j1 -> succeeded
    d = queue.claim("g", "c")
    queue.nack(d.id, "c", "无法处理", unprocessable=True)  # j2 -> rejected
    queue.claim("g", "c")  # j3 -> inflight（不确认）
    return jobs


def test_group_progress_reconciliation(queue) -> None:
    jobs = _make_group_with_mixed_states(queue)
    queue.commit_offset("g", 2)  # 前两条已定论，位点可推进到 2
    p = queue.group_progress("g")
    logger.info(
        "[输入] 组 g：j1 ack、j2 reject、j3 inflight、j4 未投递，位点=2 "
        "[判定] 对账分桶与任务 id 一一对应"
    )
    assert p.group == "g" and p.offset == 2 and p.total_jobs == 4
    assert p.buckets["succeeded"] == [jobs[0].id]
    assert p.buckets["rejected"] == [jobs[1].id]
    assert p.buckets["inflight"] == [jobs[2].id]
    assert p.buckets["pending"] == [jobs[3].id]
    assert p.buckets["failed"] == [] and p.buckets["retrying"] == []
    assert p.counts == {
        "pending": 1, "inflight": 1, "retrying": 0,
        "succeeded": 1, "failed": 0, "rejected": 1,
    }


def test_group_progress_counts_failed_and_retrying(settings) -> None:
    from dataclasses import replace

    from jobqueue.queue import JobQueue

    queue = JobQueue(replace(settings, base_backoff=0.0))  # 退避置零，便于快速耗尽重试
    queue.create_group("g")
    j1 = queue.enqueue({"n": 1})
    j2 = queue.enqueue({"n": 2})
    # j1：重试耗尽 -> failed（settings.max_retries=3，第 4 次 nack 终态）
    for _ in range(4):
        d = queue.claim("g", "c")
        queue.nack(d.id, "c", "故障")
    # j2：失败一次 -> retrying（退避中）
    d = queue.claim("g", "c")
    queue.nack(d.id, "c", "故障")
    p = queue.group_progress("g")
    logger.info(
        "[输入] j1 重试耗尽、j2 退避中 [判定] 对账含 failed=1、retrying=1，id 正确"
    )
    assert p.buckets["failed"] == [j1.id]
    assert p.buckets["retrying"] == [j2.id]


def test_group_progress_unknown_group_rejected(queue) -> None:
    with pytest.raises(QueueError) as exc:
        queue.group_progress("ghost")
    assert exc.value.code is ErrorCode.GROUP_NOT_FOUND


def test_commit_offset_skipping_inflight_rejected(queue) -> None:
    jobs = _make_group_with_mixed_states(queue)
    logger.info(
        "[输入] j3 仍在途、j4 未投递时提交位点 3 与 4 "
        "[判定] 均返回 offset_gap，位点保持 0"
    )
    for bad in (3, 4):
        with pytest.raises(QueueError) as exc:
            queue.commit_offset("g", bad)
        assert exc.value.code is ErrorCode.OFFSET_GAP
        assert jobs[2].id in exc.value.message  # 错误信息指认未定论任务
    assert queue.get_offset("g").offset == 0


def test_commit_offset_skipping_retrying_rejected(queue) -> None:
    queue.create_group("g")
    queue.enqueue({"n": 1})
    d = queue.claim("g", "c")
    queue.nack(d.id, "c", "故障")  # retrying（退避中，未定论）
    logger.info("[输入] 唯一任务退避中提交位点 1 [判定] offset_gap，退避不算定论")
    with pytest.raises(QueueError) as exc:
        queue.commit_offset("g", 1)
    assert exc.value.code is ErrorCode.OFFSET_GAP
    assert queue.get_offset("g").offset == 0


def test_commit_offset_gap_then_advance_after_decision(queue) -> None:
    queue.create_group("g")
    queue.enqueue({"n": 1})
    queue.enqueue({"n": 2})
    d1 = queue.claim("g", "c")
    queue.ack(d1.id, "c")
    queue.claim("g", "c")  # j2 inflight
    logger.info(
        "[输入] j1 已 ack、j2 在途：提交 2 被拒；j2 ack 后再提交 "
        "[判定] 先 offset_gap 后成功，位点=2"
    )
    with pytest.raises(QueueError) as exc:
        queue.commit_offset("g", 2)
    assert exc.value.code is ErrorCode.OFFSET_GAP
    # j2 确认后即可推进
    deliveries = queue.list_deliveries(queue.list_jobs()[1].id)
    queue.ack(deliveries[-1].id, "c")
    queue.commit_offset("g", 2)
    assert queue.get_offset("g").offset == 2


def test_offset_gap_is_per_group(queue) -> None:
    """同一任务在 A 组已定论、B 组未投递：B 组提交位点仍被拒。"""
    job = queue.enqueue({"n": 1})
    da = queue.claim("A", "c1")
    queue.ack(da.id, "c1")
    queue.create_group("B")
    logger.info(
        "[输入] 任务在 A 组 succeeded、B 组未投递，B 组提交位点 1 "
        "[判定] offset_gap（位点是组级进度，不共享结论）"
    )
    queue.commit_offset("A", 1)
    with pytest.raises(QueueError) as exc:
        queue.commit_offset("B", 1)
    assert exc.value.code is ErrorCode.OFFSET_GAP
    assert queue.get_offset("A").offset == 1
    assert queue.get_offset("B").offset == 0
