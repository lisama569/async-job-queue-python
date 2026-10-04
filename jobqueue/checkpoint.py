"""检查点（组进度快照）的 JSON 序列化与文件读写。

快照是一份自描述的 JSON 文档，可跨进程、跨机器搬运：
重启后或另一个消费进程将其恢复进队列，即可让对应组从
已定论的进度接着消费，而不影响其它组。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jobqueue.errors import ErrorCode, QueueError
from jobqueue.models import TERMINAL_GROUP_STATUSES, Checkpoint, CheckpointJob

_FORMAT = "jobqueue-checkpoint"
_VERSION = 1


def checkpoint_to_dict(cp: Checkpoint) -> dict[str, Any]:
    return {
        "format": _FORMAT,
        "version": _VERSION,
        "group": cp.group,
        "offset": cp.offset,
        "created_at": cp.created_at,
        "conclusions": dict(cp.conclusions),
        "jobs": [
            {
                "id": j.id,
                "payload": j.payload,
                "dedupe_key": j.dedupe_key,
                "seq": j.seq,
                "created_at": j.created_at,
                "max_retries": j.max_retries,
            }
            for j in cp.jobs
        ],
    }


def _invalid(msg: str) -> QueueError:
    return QueueError(ErrorCode.CHECKPOINT_INVALID, msg)


def checkpoint_from_dict(data: Any) -> Checkpoint:
    """解析并校验快照字典；结构非法抛 checkpoint_invalid。"""
    if not isinstance(data, dict) or data.get("format") != _FORMAT:
        raise _invalid("不是有效的 jobqueue 检查点文档")
    if data.get("version") != _VERSION:
        raise _invalid(f"不支持的检查点版本：{data.get('version')!r}")
    group = data.get("group")
    offset = data.get("offset")
    conclusions = data.get("conclusions")
    if (
        not isinstance(group, str)
        or not group
        or not isinstance(offset, int)
        or isinstance(offset, bool)
        or offset < 0
        or not isinstance(conclusions, dict)
        or not all(
            isinstance(k, str) and v in TERMINAL_GROUP_STATUSES
            for k, v in conclusions.items()
        )
    ):
        raise _invalid("检查点字段缺失或非法（group/offset/conclusions）")

    raw_jobs = data.get("jobs", [])
    jobs: list[CheckpointJob] = []
    seen_ids: set[str] = set()
    seen_seqs: set[int] = set()
    # 旧版快照没有 jobs 字段：仍可解析，但只能恢复进包含这些任务的队列
    if "jobs" in data and not isinstance(raw_jobs, list):
        raise _invalid("检查点 jobs 字段必须是数组")
    if raw_jobs:
        for item in raw_jobs:
            if not isinstance(item, dict):
                raise _invalid("检查点 jobs 中的任务记录必须是对象")
            job_id = item.get("id")
            payload = item.get("payload")
            dedupe_key = item.get("dedupe_key")
            seq = item.get("seq")
            created_at = item.get("created_at", 0.0)
            max_retries = item.get("max_retries", 0)
            if (
                not isinstance(job_id, str)
                or not job_id
                or not isinstance(payload, dict)
                or not (dedupe_key is None or isinstance(dedupe_key, str))
                or not isinstance(seq, int)
                or isinstance(seq, bool)
                or seq <= 0
                or not isinstance(created_at, (int, float))
                or isinstance(created_at, bool)
                or not isinstance(max_retries, int)
                or isinstance(max_retries, bool)
                or max_retries < 0
            ):
                raise _invalid(
                    "检查点 jobs 中存在字段缺失或非法的任务记录"
                    f"（job_id={job_id!r}）"
                )
            if job_id in seen_ids:
                raise _invalid(f"检查点 jobs 中任务 id 重复：{job_id}")
            if seq in seen_seqs:
                raise _invalid(f"检查点 jobs 中入队序号 seq 重复：{seq}")
            seen_ids.add(job_id)
            seen_seqs.add(seq)
            jobs.append(
                CheckpointJob(
                    id=job_id,
                    payload=payload,
                    dedupe_key=dedupe_key,
                    seq=seq,
                    created_at=float(created_at),
                    max_retries=max_retries,
                )
            )
        missing = [jid for jid in conclusions if jid not in seen_ids]
        if missing:
            raise _invalid(
                f"检查点终态结论引用了 jobs 中不存在的任务：{missing[:3]}"
                f"（共 {len(missing)} 条）"
            )
    return Checkpoint(
        group=group,
        offset=offset,
        created_at=float(data.get("created_at", 0.0)),
        conclusions=dict(conclusions),
        jobs=jobs,
    )


def save_checkpoint_file(cp: Checkpoint, path: str | Path) -> None:
    """原子写入：先写临时文件再改名，避免崩溃留下半个快照。"""
    target = Path(path)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(
        json.dumps(checkpoint_to_dict(cp), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(target)


def load_checkpoint_file(path: str | Path) -> Checkpoint:
    try:
        raw = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise QueueError(
            ErrorCode.CHECKPOINT_INVALID, f"无法读取检查点文件 {path}: {exc}"
        ) from exc
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise QueueError(
            ErrorCode.CHECKPOINT_INVALID, f"检查点文件不是合法 JSON: {exc}"
        ) from exc
    return checkpoint_from_dict(data)
