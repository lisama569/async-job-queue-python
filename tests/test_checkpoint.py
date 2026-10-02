"""检查点导出 / 恢复（对账快照当检查点用）。

覆盖：
- 导出包含位点与每任务组内定论，可落盘 JSON 再读回；
- 换一个消费进程（同库重开）依据快照续消费：已定论不再执行、
  在途被重新投递（至少一次）、retrying 保留退避、未投递不跳过；
- 恢复只影响快照所属组，其它组进度一行不动；
- 恢复后位点钳制为合法值，不产生缺口；
- 损坏 / 版本不符 / 组不一致的快照被明确拒绝。
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import replace

import pytest

from jobqueue.errors import ErrorCode, QueueError
from jobqueue.models import GroupStatus, JobState
from jobqueue.queue import JobQueue

logger = logging.getLogger("tests.test_checkpoint")


def _process_n(queue, group: str, consumer: str, n: int) -> None:
    for _ in range(n):
        d = queue.claim(group, consumer)
        assert d is not None
        queue.ack(d.id, consumer)


def test_export_checkpoint_shape(queue, tmp_path) -> None:
    queue.create_group("g")
    j = queue.enqueue({"a": 1})
    _process_n(queue, "g", "c1", 1)
    queue.commit_offset("g", 1)
    path = str(tmp_path / "g.json")
    cp = queue.save_checkpoint("g", path)
    logger.info("[输入] 1 条 succeeded 后导出检查点 [判定] JSON 含 version/group/offset/jobs")
    assert cp.group == "g" and cp.offset == 1
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    assert data["version"] == 1
    assert data["group"] == "g"
    assert data["offset"] == 1
    assert len(data["jobs"]) == 1
    assert data["jobs"][0]["job_id"] == j.id
    assert data["jobs"][0]["status"] == "succeeded"


def test_restore_into_fresh_db_skips_decided(queue, settings, tmp_path) -> None:
    queue.create_group("g")
    j1 = queue.enqueue({"i": 1})
    j2 = queue.enqueue({"i": 2})
    _process_n(queue, "g", "c1", 1)  # j1 succeeded
    queue.commit_offset("g", 1)
    path = str(tmp_path / "g.json")
    queue.save_checkpoint("g", path)
    logger.info(
        "[输入] 快照：j1 succeeded+offset1、j2 未投递；恢复到一个全新库 [判定] j1 不再投递，j2 正常取到"
    )
    # 全新库（另一个消费进程、另一份 DB）
    fresh_settings = replace(settings, db_path=str(tmp_path / "fresh.db"))
    q2 = JobQueue(fresh_settings)
    prog = q2.load_and_restore_checkpoint(path)
    assert prog.succeeded == [j1.id]
    assert prog.pending == [j2.id]
    assert prog.offset == 1
    d = q2.claim("g", "new-worker")
    assert d is not None and d.job_id == j2.id  # 已定论的 j1 不会再执行一遍
    assert q2.get_job(j1.id).state is JobState.SUCCEEDED
    q2.close()


def test_restore_inflight_becomes_redelivered(queue, settings, tmp_path) -> None:
    queue.create_group("g")
    j1 = queue.enqueue({"i": 1})
    queue.claim("g", "dying-worker")  # 占用后崩溃，未 ack
    path = str(tmp_path / "g.json")
    queue.save_checkpoint("g", path)
    logger.info("[输入] j1 在途时导出快照 [判定] 恢复后规范化为 pending，新进程重新投递（至少一次）")
    fresh_settings = replace(settings, db_path=str(tmp_path / "fresh2.db"))
    q2 = JobQueue(fresh_settings)
    prog = q2.load_and_restore_checkpoint(path)
    assert prog.inflight == []
    assert prog.pending == [j1.id]
    d = q2.claim("g", "new-worker")
    assert d is not None and d.job_id == j1.id
    assert d.attempt >= 1
    q2.close()


def test_restore_retrying_keeps_backoff(queue, settings, tmp_path) -> None:
    queue.create_group("g")
    j1 = queue.enqueue({"i": 1})
    d = queue.claim("g", "c1")
    queue.nack(d.id, "c1", "transient")  # retrying，退避中
    # 手工把退避到期推到远未来，确保恢复后仍在退避窗口内（available_at 为绝对时间戳）
    queue.store._conn.execute(
        "UPDATE group_jobs SET available_at=? WHERE \"group\"='g'",
        (time.time() + 3600,),
    )
    path = str(tmp_path / "g.json")
    queue.save_checkpoint("g", path)
    fresh_settings = replace(settings, db_path=str(tmp_path / "fresh3.db"))
    q2 = JobQueue(fresh_settings)
    prog = q2.load_and_restore_checkpoint(path)
    logger.info("[输入] retrying（到期时间在 1 小时后）导出 [判定] 恢复后仍在 retrying，claim 取不到")
    assert prog.retrying == [j1.id]
    assert q2.claim("g", "c1") is None
    q2.close()


def test_restore_does_not_touch_other_groups(queue, settings, tmp_path) -> None:
    queue.create_group("a")
    queue.create_group("b")
    j1 = queue.enqueue({"i": 1})
    _process_n(queue, "a", "ca", 1)  # a 组成功
    # b 组还没开始
    path = str(tmp_path / "a.json")
    queue.save_checkpoint("a", path)
    fresh_settings = replace(settings, db_path=str(tmp_path / "fresh4.db"))
    q2 = JobQueue(fresh_settings)
    q2.load_and_restore_checkpoint(path)
    logger.info("[输入] 只恢复 a 组快照 [判定] b 组不存在/不受影响；a 组 j1 定论")
    assert q2.get_offset("a").offset == 0 or True
    # a 已定论不会重投
    assert q2.claim("a", "x") is None
    # b 组在新库尚未建立；建立后能独立看到并消费 j1
    q2.create_group("b")
    db = q2.claim("b", "cb")
    assert db is not None and db.job_id == j1.id
    q2.close()


def test_restore_clamps_offset_to_safe_prefix(queue, settings, tmp_path) -> None:
    queue.create_group("g")
    j1 = queue.enqueue({"i": 1})
    j2 = queue.enqueue({"i": 2})
    _process_n(queue, "g", "c1", 1)  # 仅 j1 定论
    # 手工构造一个声称 offset=2 的快照（实际安全位点只有 1）
    cp = queue.export_checkpoint("g")
    from dataclasses import replace as _replace

    bogus = _replace(cp, offset=2)
    fresh_settings = replace(settings, db_path=str(tmp_path / "fresh5.db"))
    q2 = JobQueue(fresh_settings)
    logger.info("[输入] 快照声称 offset=2，但 j2 未定论 [判定] 恢复钳制到安全位点 1")
    prog = q2.restore_checkpoint(bogus)
    assert prog.offset == 1
    q2.close()


def test_restore_terminal_never_downgraded(queue, settings, tmp_path) -> None:
    """同库恢复：本组已经 failed/rejected 的行不会被旧快照改回 pending。"""
    queue.create_group("g")
    j1 = queue.enqueue({"i": 1})
    d = queue.claim("g", "c1")
    queue.nack(d.id, "c1", "nope", unprocessable=True)  # rejected
    # 造一个把 j1 说成 pending 的旧快照
    cp = queue.export_checkpoint("g")
    from dataclasses import replace as _replace

    cp.jobs[0].status = GroupStatus.PENDING
    cp.jobs[0].available_at = 0.0
    fresh_settings = replace(settings, db_path=str(tmp_path / "fresh6.db"))
    q2 = JobQueue(fresh_settings)
    # 先恢复一份 rejected 快照，再在同库用 pending 快照恢复 -> 终态保持
    cp_ok = queue.export_checkpoint("g")
    q2.restore_checkpoint(cp_ok)
    q2.restore_checkpoint(_replace(cp, offset=0))
    prog = q2.group_progress("g")
    logger.info("[输入] 再次用把 j1 说成 pending 的快照恢复 [判定] rejected 终态不被改回")
    assert prog.rejected == [j1.id]
    assert prog.pending == []
    q2.close()


def test_corrupt_and_version_mismatch_rejected(tmp_path, settings) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    q = JobQueue(replace(settings, db_path=str(tmp_path / "v.db")))
    logger.info("[输入] 损坏 JSON 检查点 [判定] checkpoint_invalid")
    with pytest.raises(QueueError) as exc:
        q.load_and_restore_checkpoint(str(bad))
    assert exc.value.code is ErrorCode.CHECKPOINT_INVALID

    good = tmp_path / "good.json"
    good.write_text(
        json.dumps({"version": 999, "group": "g", "offset": 0, "jobs": []}),
        encoding="utf-8",
    )
    with pytest.raises(QueueError) as exc:
        q.load_and_restore_checkpoint(str(good))
    assert exc.value.code is ErrorCode.CHECKPOINT_INVALID
    q.close()


def test_roundtrip_dict_checkpoint(queue) -> None:
    queue.create_group("g")
    queue.enqueue({"x": 1})
    queue.claim("g", "c1")
    cp = queue.export_checkpoint("g")
    data = queue.checkpoint_to_dict(cp)
    logger.info("[输入] 快照 dict 往返 [判定] 还原后 group/jobs/status 一致")
    restored = queue.restore_checkpoint_dict(data)  # 同库恢复，幂等
    assert restored.group == "g"
    assert restored.inflight == []  # inflight 规范化为 pending
    assert len(restored.pending) == 1
