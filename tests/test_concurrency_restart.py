"""并发消费与进程重启边界：

* 多线程同时 poll：每个任务同一时刻只被一个消费者占用；
* 多进程并发争抢同一个 SQLite 库：不重复投递；
* 进程重启：inflight 租约状态、已提交位点、终态任务全部可恢复。
"""

import logging
import multiprocessing as mp
import os
import tempfile

import pytest

from taskqueue.config import QueueConfig
from taskqueue.models import TaskStatus
from taskqueue.queue import TaskQueue
from taskqueue.storage import Storage

logger = logging.getLogger(__name__)


def test_concurrent_threads_never_share_a_task(queue):
    logger.info("输入: 50 个任务，8 个线程各持独立消费者并发 poll/ack；"
                "判定: 每个任务只被成功 ack 一次，succeeded==50")
    n = 50
    for i in range(n):
        queue.enqueue("echo", {"i": i})

    acked: list[int] = []
    import threading
    lock = threading.Lock()

    def worker(cid: int) -> None:
        while True:
            deliveries = queue.poll("grp", f"c{cid}", max_messages=5)
            if not deliveries:
                return
            for d in deliveries:
                queue.acknowledge(d.task.id, d.epoch)
                with lock:
                    acked.append(d.task.id)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    logger.info("结果: ack 总数=%d 去重后=%d 期望=%d", len(acked), len(set(acked)), n)
    assert len(acked) == n
    assert len(set(acked)) == n
    counts = queue.status_counts()
    assert counts.get("succeeded", 0) == n
    assert counts.get("inflight", 0) == 0


def test_concurrent_poll_with_contended_leases(queue):
    """部分线程在任务处于租约期内 poll：只能拿到空，不能偷走别人的任务。"""
    logger.info("输入: 1 个任务先被 c1 占用，7 个线程并发抢；判定: 全部空返回")
    queue.enqueue("echo", {})
    first = queue.poll("grp", "c1")
    assert len(first) == 1

    results = []

    def grab(cid: int) -> None:
        results.append(queue.poll("grp", f"x{cid}"))

    import threading
    ts = [threading.Thread(target=grab, args=(i,)) for i in range(7)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert all(r == [] for r in results)
    assert queue.get_task(1).lease_owner == "grp/c1"


def _child_lease_tasks(db_path: str, consumer: str, count: int, outq) -> None:
    """子进程入口：独立 queue 实例争抢租约并 ack。"""
    cfg = QueueConfig(db_path=db_path, lease_timeout_ms=30000)
    q = TaskQueue(cfg, Storage(db_path))
    got = []
    while len(got) < count:
        for d in q.poll("grp", consumer, max_messages=5):
            q.acknowledge(d.task.id, d.epoch)
            got.append(d.task.id)
    outq.put((consumer, got))
    q.close()


@pytest.mark.skipif(os.name == "nt", reason="multiprocessing semantics on CI")
def test_multiple_processes_do_not_double_deliver(tmp_path):
    logger.info("输入: 两个独立进程各持 Storage 连接消费同一 DB，共 40 任务；"
                "判定: 两进程拿到的任务集合不相交且并集=全部")
    db_path = str(tmp_path / "multi.db")
    cfg = QueueConfig(db_path=db_path)
    q = TaskQueue(cfg, Storage(db_path))
    n = 40
    for i in range(n):
        q.enqueue("echo", {"i": i})
    q.close()

    ctx = mp.get_context("spawn")
    outq = ctx.Queue()
    procs = [
        ctx.Process(target=_child_lease_tasks, args=(db_path, "p1", 20, outq)),
        ctx.Process(target=_child_lease_tasks, args=(db_path, "p2", 20, outq)),
    ]
    for p in procs:
        p.start()
    p1_ids, p2_ids = [], []
    for _ in range(2):
        name, ids = outq.get(timeout=60)
        (p1_ids if name == "p1" else p2_ids).extend(ids)
    for p in procs:
        p.join(timeout=10)
        assert p.exitcode == 0

    logger.info("结果: p1=%d 条 p2=%d 条 交集=%d", len(p1_ids), len(p2_ids),
                len(set(p1_ids) & set(p2_ids)))
    assert set(p1_ids).isdisjoint(p2_ids)
    assert set(p1_ids) | set(p2_ids) == set(range(1, n + 1))

    # 重新开库校验终态
    check = TaskQueue(QueueConfig(db_path=db_path), Storage(db_path))
    assert check.status_counts().get("succeeded", 0) == n
    check.close()


def test_restart_recovers_inflight_pending_and_offsets(tmp_path):
    logger.info("输入: 模拟进程崩溃（t1 成功 / t2 inflight / t3 未投递 / 组位点=1）"
                "后重新打开同一 DB；判定: 所有状态原样恢复，超时后 t2 可重投")
    db_path = str(tmp_path / "restart.db")

    # 用“假时钟 + 极短租约”的确定性方式：崩溃时把租约写为 30s 后到期，
    # 重启瞬间尚未过期；随后时钟越过到期点即可观察回收与重投。
    class Clock:
        def __init__(self):
            self.t = 1_000_000

        def __call__(self):
            return self.t / 1000.0

        def bump(self, ms):
            self.t += ms

    clock = Clock()
    cfg = QueueConfig(db_path=db_path, lease_timeout_ms=30_000, max_attempts=5)

    q = TaskQueue(cfg, Storage(db_path), clock=clock)
    q.create_group("grp")
    t1 = q.enqueue("echo", {"n": 1}).id
    t2 = q.enqueue("echo", {"n": 2}).id
    t3 = q.enqueue("echo", {"n": 3}).id
    # 首批：t1、t2 被 c1 拿走并处于 inflight
    first = q.poll("grp", "c1", max_messages=2)
    assert [d.task.id for d in first] == [1, 2]
    q.acknowledge(1, epoch=1)
    q.commit_offset("grp", 1)
    q.close()  # 模拟进程崩溃退出：t2 仍 inflight（未 ack），t3 从未投递

    # —— 重启：时钟不动，租约尚未过期 ——
    q2 = TaskQueue(cfg, Storage(db_path), clock=clock)
    assert q2.get_task(t1).status is TaskStatus.SUCCEEDED
    assert q2.get_task(t2).status is TaskStatus.INFLIGHT
    assert q2.get_task(t3).status is TaskStatus.PENDING
    assert q2.storage.get_group("grp").committed_offset == 1
    logger.info("重启后状态: t1=succeeded t2=inflight(owner=%s) t3=pending offset=1",
                q2.get_task(t2).lease_owner)

    # 越过可见超时：t2 被回收。先把时钟推进到“t2 仅剩 5ms、尚未过期”的位置
    # 让 c2 拿到 t3，随后再越过 t2 的到期点；此时 t3 仍在新的 30s 保护窗口内。
    clock.bump(30_000 - 5)
    early = q2.poll("grp", "c2", max_messages=10)
    assert [d.task.id for d in early] == [t3]
    clock.bump(10)  # 越过 t2 的到期点，但 t3 还有约 30s
    assert q2.reclaim_expired_leases() == 1
    red = q2.poll("grp", "c2", max_messages=10)
    red_ids = {d.task.id: d for d in red}
    assert set(red_ids) == {t2}
    assert red_ids[t2].epoch == 2 and red_ids[t2].task.lease_owner == "grp/c2"
    # 迟到的旧持有者 ack 必须被 fence 拒绝
    from taskqueue.exceptions import AcknowledgeRejected
    from taskqueue.models import RejectReason
    with pytest.raises(AcknowledgeRejected) as exc:
        q2.acknowledge(t2, epoch=1)
    assert exc.value.reason is RejectReason.LEASE_EXPIRED
    q2.close()
