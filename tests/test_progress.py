"""组进度对账与位点推进规则。

覆盖：
- 对账结果五类任务桶（成功/失败/拒绝/在途/待处理，retrying 单列）与
  位点、安全位点 safe_offset 的一致性；
- 位点提交的拒绝：原地/回退、跳过未定论（在途/未投递/retrying）、越界；
- 只有连续定论前缀才允许推进；rejected/failed 同样算定论。
"""

from __future__ import annotations

import logging
import time

import pytest

from jobqueue.errors import ErrorCode, QueueError

logger = logging.getLogger("tests.test_progress")


def _ack_first(queue, group: str, consumer: str = "c1"):
    d = queue.claim(group, consumer)
    assert d is not None
    queue.ack(d.id, consumer)
    return d


def test_progress_buckets_and_safe_offset(queue) -> None:
    queue.create_group("g")
    j_ok = queue.enqueue({"k": "ok"})        # -> succeeded
    j_inf = queue.enqueue({"k": "inflight"})  # -> inflight（卡住位点）
    j3 = queue.enqueue({"k": "p3"})          # 未投递
    j4 = queue.enqueue({"k": "p4"})          # 未投递
    j5 = queue.enqueue({"k": "p5"})          # 未投递

    _ack_first(queue, "g")       # j_ok 成功
    d2 = queue.claim("g", "c1")  # j_inf 在途，不确认
    assert d2.job_id == j_inf.id

    p = queue.group_progress("g")
    logger.info(
        "[输入] 5 条：j1 ack、j2 inflight、其余未投递 [判定] offset=%s safe_offset=%s "
        "ok=%d inflight=%d pending=%d",
        p.offset, p.safe_offset, len(p.succeeded), len(p.inflight), len(p.pending),
    )
    assert p.total == 5
    assert p.offset == 0
    assert p.succeeded == [j_ok.id]
    assert p.inflight == [j_inf.id]
    assert set(p.pending) == {j3.id, j4.id, j5.id}
    assert p.safe_offset == 1
    queue.commit_offset("g", 1)
    assert queue.get_offset("g").offset == 1


def test_offset_cannot_skip_inflight(queue) -> None:
    queue.create_group("g")
    j1 = queue.enqueue({"i": 1})
    j2 = queue.enqueue({"i": 2})
    _ack_first(queue, "g")               # j1 定论
    d2 = queue.claim("g", "c1")          # j2 在途
    assert d2.job_id == j2.id
    queue.commit_offset("g", 1)
    logger.info("[输入] j1 定论、j2 在途，提交位点 2 [判定] offset_gap，位点保持 1")
    with pytest.raises(QueueError) as exc:
        queue.commit_offset("g", 2)
    assert exc.value.code is ErrorCode.OFFSET_GAP
    assert queue.get_offset("g").offset == 1


def test_offset_cannot_skip_undelivered(queue) -> None:
    queue.create_group("g")
    queue.enqueue({"i": 1})
    queue.enqueue({"i": 2})
    _ack_first(queue, "g")  # 第 1 条定论，第 2 条从未投递
    logger.info("[输入] 第 1 条定论、第 2 条未投递，提交位点 2 [判定] offset_gap")
    with pytest.raises(QueueError) as exc:
        queue.commit_offset("g", 2)
    assert exc.value.code is ErrorCode.OFFSET_GAP


def test_retrying_blocks_offset_and_is_own_bucket(queue) -> None:
    queue.create_group("g")
    queue.enqueue({"i": 1})
    queue.enqueue({"i": 2})
    _ack_first(queue, "g")  # j1 succeeded
    d2 = queue.claim("g", "c1")
    queue.nack(d2.id, "c1", "transient")  # j2 retrying，退避中
    p = queue.group_progress("g")
    logger.info(
        "[输入] j2 nack 进入 retrying [判定] retrying=[%s] pending=0 safe_offset=1，位点 2 被 gap 拒绝",
        d2.job_id,
    )
    assert p.retrying == [d2.job_id]
    assert p.pending == []
    assert p.safe_offset == 1
    with pytest.raises(QueueError) as exc:
        queue.commit_offset("g", 2)
    assert exc.value.code is ErrorCode.OFFSET_GAP


def test_failed_and_rejected_count_as_decided(queue) -> None:
    queue.create_group("g")
    j1 = queue.enqueue({"i": 1})
    j2 = queue.enqueue({"i": 2})
    d1 = queue.claim("g", "c1")
    queue.nack(d1.id, "c1", "nope", unprocessable=True)  # j1 rejected
    # j2 重试耗尽：max_retries=3，共投递 4 次 -> failed（每次退避后等待到期）
    failed_id = None
    for _ in range(4):
        d = queue.claim("g", "c1")
        while d is None:
            time.sleep(0.02)
            d = queue.claim("g", "c1")
        failed_id = d.job_id
        queue.nack(d.id, "c1", "always fail")
    p = queue.group_progress("g")
    logger.info(
        "[输入] j1 rejected、j2 重试耗尽 failed [判定] rejected/failed 都计入定论，safe_offset=2"
    )
    assert p.rejected == [j1.id]
    assert p.failed == [j2.id]
    assert failed_id == j2.id
    assert p.pending == []
    assert p.safe_offset == 2
    queue.commit_offset("g", 2)  # rejected/failed 都是定论，位点可推进
    assert queue.get_offset("g").offset == 2


def test_rollback_in_place_and_out_of_range_distinct(queue) -> None:
    queue.create_group("g")
    for i in range(3):
        queue.enqueue({"i": i})
    for _ in range(3):
        d = queue.claim("g", "c1")
        queue.ack(d.id, "c1")
    queue.commit_offset("g", 2)
    logger.info("[输入] 3 条全定论、位点=2 [判定] 原地/回退=rollback；4=out_of_range")
    for bad in (2, 1, 0):
        with pytest.raises(QueueError) as exc:
            queue.commit_offset("g", bad)
        assert exc.value.code is ErrorCode.OFFSET_ROLLBACK
    with pytest.raises(QueueError) as exc:
        queue.commit_offset("g", 4)
    assert exc.value.code is ErrorCode.OFFSET_OUT_OF_RANGE
    queue.commit_offset("g", 3)
    assert queue.get_offset("g").offset == 3


def test_gap_rejected_before_out_of_range_order(queue) -> None:
    """缺口 + 越界同时存在时，优先按回退/越界语义；缺口是最贴近进度的拒绝。"""
    queue.create_group("g")
    queue.enqueue({"i": 1})  # 仅 1 条且未定论
    with pytest.raises(QueueError) as exc:
        queue.commit_offset("g", 5)
    # 总数 1 < 5，先命中越界
    assert exc.value.code is ErrorCode.OFFSET_OUT_OF_RANGE


def test_progress_unknown_group_404(queue) -> None:
    with pytest.raises(QueueError) as exc:
        queue.group_progress("ghost")
    assert exc.value.code is ErrorCode.GROUP_NOT_FOUND
