"""消费者组之间的隔离性。

覆盖：
- 一个任务在某组已有最终结论后，别的组再投递 / 确认 / 失败，
  都不能把它的组内结论或全局状态改回在途 / 待处理（终态粘滞）；
- 某组失败进入退避等待时，别的组照常取件、按自己节奏推进；
- 查询单个任务能分别看到它在各组的结论。
"""

from __future__ import annotations

import logging
import time

from jobqueue.models import GroupStatus, JobState

logger = logging.getLogger("tests.test_group_isolation")


def test_terminal_not_reverted_by_other_group_claim(queue) -> None:
    job = queue.enqueue({"x": 1})
    da = queue.claim("a", "c1")
    queue.ack(da.id, "c1")  # a 组成功定论
    logger.info(
        "[输入] job 在 a 组 ack 成功后，b 组再 claim [判定] b 组可独立投递，但全局状态粘滞 succeeded、a 组结论不变"
    )
    db = queue.claim("b", "c2")
    assert db is not None and db.job_id == job.id
    # 全局状态仍是 succeeded，不被 b 组的 inflight 拉回
    assert queue.get_job(job.id).state is JobState.SUCCEEDED
    views = {v.group: v for v in queue.get_job_group_views(job.id)}
    assert views["a"].status is GroupStatus.SUCCEEDED
    assert views["b"].status is GroupStatus.INFLIGHT
    # 即使 b 组最终失败，也不影响 a 组成功结论与全局 succeeded
    queue.nack(db.id, "c2", "boom", unprocessable=True)
    assert queue.get_job(job.id).state is JobState.SUCCEEDED
    views = {v.group: v for v in queue.get_job_group_views(job.id)}
    assert views["a"].status is GroupStatus.SUCCEEDED
    assert views["b"].status is GroupStatus.REJECTED


def test_failed_in_one_group_then_succeeded_in_another(queue) -> None:
    job = queue.enqueue({"x": 1})
    # a 组重试耗尽 -> failed
    for _ in range(4):
        d = queue.claim("a", "c1")
        while d is None:
            time.sleep(0.02)
            d = queue.claim("a", "c1")
        queue.nack(d.id, "c1", "bad")
    logger.info("[输入] a 组 failed 后 b 组再 claim 并 ack [判定] 全局定格 failed，b 独立 succeeded")
    assert queue.get_job(job.id).state is JobState.FAILED
    db = queue.claim("b", "c2")
    assert db is not None
    queue.ack(db.id, "c2")
    views = {v.group: v for v in queue.get_job_group_views(job.id)}
    assert views["a"].status is GroupStatus.FAILED
    assert views["b"].status is GroupStatus.SUCCEEDED
    # 终态优先级：出现 succeeded 后全局以 succeeded 为准（终态之间按既定汇总）
    assert queue.get_job(job.id).state is JobState.SUCCEEDED


def test_backoff_in_one_group_does_not_block_other(queue) -> None:
    j1 = queue.enqueue({"i": 1})
    j2 = queue.enqueue({"i": 2})
    queue.create_group("a")
    queue.create_group("b")
    # a 组把 j1 nack 进入退避
    da = queue.claim("a", "c1")
    queue.nack(da.id, "c1", "transient")
    pa = queue.group_progress("a")
    logger.info(
        "[输入] a 组 j1 retrying [判定] a 取不到 j1（退避未到期）但能取 j2；b 组立刻可取 j1"
    )
    assert pa.retrying == [j1.id]
    # a 组退避窗口内取不到 j1，但可以越过它继续取 j2（本组后续任务不被拖累）
    da2 = queue.claim("a", "c1")
    assert da2 is not None and da2.job_id == j2.id
    # b 组有自己的节奏：立刻取到 j1，不受 a 组退避影响
    db = queue.claim("b", "c2")
    assert db is not None and db.job_id == j1.id
    queue.ack(db.id, "c2")
    # a 组退避到期后仍能重投 j1
    time.sleep(0.02)
    da1_retry = queue.claim("a", "c1")
    assert da1_retry is not None and da1_retry.job_id == j1.id


def test_groups_advance_at_own_pace(queue) -> None:
    for i in range(3):
        queue.enqueue({"i": i})
    queue.create_group("fast")
    queue.create_group("slow")
    # fast 组立刻全部消费完
    for _ in range(3):
        d = queue.claim("fast", "cf")
        queue.ack(d.id, "cf")
    logger.info("[输入] fast 组全部 ack、slow 组尚未开始 [判定] fast safe_offset=3，slow safe_offset=0")
    assert queue.group_progress("fast").safe_offset == 3
    slow = queue.group_progress("slow")
    assert slow.safe_offset == 0 and slow.total == 3
    # slow 随后按自己节奏消费，不影响 fast 位点
    d = queue.claim("slow", "cs")
    queue.ack(d.id, "cs")
    assert queue.group_progress("slow").safe_offset == 1
    assert queue.get_offset("fast").offset == 0  # 位点是游标记账，不自动推进


def test_group_terminal_row_never_flips_back(queue) -> None:
    """同组终态不可逆；且其它组活动不改变本组终态行。"""
    job = queue.enqueue({"x": 1})
    da = queue.claim("a", "c1")
    queue.ack(da.id, "c1")
    # b 组反复投递/超时重投
    for _ in range(2):
        db = queue.claim("b", "c2")
        assert db is not None
        queue.nack(db.id, "c2", "again")  # retrying
        time.sleep(0.02)
    views = {v.group: v.status for v in queue.get_job_group_views(job.id)}
    logger.info("[输入] a=succeeded，b 反复失败重试 [判定] a 始终 succeeded，b 在 retrying/failed")
    assert views["a"] is GroupStatus.SUCCEEDED
    assert views["b"] in (GroupStatus.RETRYING, GroupStatus.FAILED)
