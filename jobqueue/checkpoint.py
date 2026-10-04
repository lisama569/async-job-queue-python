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
# v1：仅含 group/offset/conclusions，只能恢复进已有所引任务的队列；
# v2：额外收录导出时的全部任务（jobs），快照自足，可恢复进空队列数据。
_VERSION = 2
_SUPPORTED_VERSIONS = (1, _VERSION)


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
                "dedupe_key": j.dedupe_key,
                "payload": j.payload,
                "max_retries": j.max_retries,
                "created_at": j.created_at,
            }
            for j in cp.jobs
        ],
    }


def checkpoint_from_dict(data: Any) -> Checkpoint:
    """解析并校验快照字典；结构非法抛 checkpoint_invalid。"""
    if not isinstance(data, dict) or data.get("format") != _FORMAT:
        raise QueueError(
            ErrorCode.CHECKPOINT_INVALID, "不是有效的 jobqueue 检查点文档"
        )
    version = data.get("version")
    if version not in _SUPPORTED_VERSIONS:
        raise QueueError(
            ErrorCode.CHECKPOINT_INVALID,
            f"不支持的检查点版本：{version!r}",
        )
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
        raise QueueError(
            ErrorCode.CHECKPOINT_INVALID,
            "检查点字段缺失或非法（group/offset/conclusions）",
        )
    raw_jobs = data.get("jobs", [])
    if not isinstance(raw_jobs, list) or not all(
        isinstance(j, dict)
        and isinstance(j.get("id"), str)
        and j["id"]
        and (j.get("dedupe_key") is None or isinstance(j.get("dedupe_key"), str))
        and isinstance(j.get("payload"), dict)
        and isinstance(j.get("max_retries"), int)
        and not isinstance(j.get("max_retries"), bool)
        and j.get("max_retries") >= 0
        and isinstance(j.get("created_at"), (int, float))
        for j in raw_jobs
    ):
        raise QueueError(
            ErrorCode.CHECKPOINT_INVALID,
            "检查点字段 jobs 非法：须为包含 id/payload/max_retries 的任务列表",
        )
    jobs = [
        CheckpointJob(
            id=j["id"],
            dedupe_key=j.get("dedupe_key"),
            payload=j["payload"],
            max_retries=j["max_retries"],
            created_at=float(j["created_at"]),
        )
        for j in raw_jobs
    ]
    ids = [j.id for j in jobs]
    if len(set(ids)) != len(ids):
        raise QueueError(
            ErrorCode.CHECKPOINT_INVALID, "检查点 jobs 中存在重复任务 id"
        )
    if version == 2:
        missing = [jid for jid in conclusions if jid not in set(ids)]
        if missing:
            raise QueueError(
                ErrorCode.CHECKPOINT_INVALID,
                f"v2 检查点自相矛盾：结论引用了 jobs 未收录的任务：{missing[:3]}",
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
