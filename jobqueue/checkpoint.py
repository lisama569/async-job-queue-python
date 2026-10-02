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
from jobqueue.models import TERMINAL_GROUP_STATUSES, Checkpoint

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
    }


def checkpoint_from_dict(data: Any) -> Checkpoint:
    """解析并校验快照字典；结构非法抛 checkpoint_invalid。"""
    if not isinstance(data, dict) or data.get("format") != _FORMAT:
        raise QueueError(
            ErrorCode.CHECKPOINT_INVALID, "不是有效的 jobqueue 检查点文档"
        )
    if data.get("version") != _VERSION:
        raise QueueError(
            ErrorCode.CHECKPOINT_INVALID,
            f"不支持的检查点版本：{data.get('version')!r}",
        )
    group = data.get("group")
    offset = data.get("offset")
    conclusions = data.get("conclusions")
    if (
        not isinstance(group, str)
        or not group
        or not isinstance(offset, int)
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
    return Checkpoint(
        group=group,
        offset=offset,
        created_at=float(data.get("created_at", 0.0)),
        conclusions=dict(conclusions),
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
