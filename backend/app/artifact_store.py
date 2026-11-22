"""分区产物存储：按输入版本落盘，快照与补片任务只通过产物路径读取数据。

真实项目里这里会接对象存储或数据湖；当前实现放在内存里，但保留了
“写入一次、按路径读取、行数与校验和可核对”的语义，用来保证补片任务
读到的是快照确认时的那个版本，而不是同日期后来写入的其它版本。
"""
from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime
from typing import Any


def _preview(payload: list[dict[str, Any]] | None, row_count: int,
             input_version: str, partition: str) -> list[dict[str, Any]]:
    """给出最多三行可读样例；没有真实载荷时按行号生成占位样例。"""
    if payload is not None:
        return copy.deepcopy(payload[:3])
    return [
        {
            "row_no": index,
            "partition": partition,
            "input_version": input_version,
        }
        for index in range(1, min(row_count, 3) + 1)
    ]


class ArtifactStore:
    """内存产物仓库：产物路径唯一标识一份可读数据，读取时返回副本。"""

    def __init__(self) -> None:
        self._items: dict[str, dict[str, Any]] = {}
        self.read_count = 0
        self.write_count = 0

    def write(
        self,
        path: str,
        rows: int | str | None = None,
        *,
        payload: list[dict[str, Any]] | None = None,
        columns: list[str] | None = None,
        input_version: str = "",
        definition: str = "",
        date: str = "",
        partition: str = "",
        task_id: int | None = None,
        receipt_id: str = "",
    ) -> dict[str, Any]:
        """写入一份分区产物；有 payload 时以载荷行数作为准。"""
        if payload is not None:
            row_count = len(payload)
        else:
            try:
                row_count = int(float(str(rows)))
            except (TypeError, ValueError):
                row_count = 0
        if row_count < 0:
            row_count = 0
        payload_copy = copy.deepcopy(payload) if payload is not None else None
        checksum_source = json.dumps(
            {
                "path": path,
                "rows": row_count,
                "version": input_version,
                "partition": partition,
                "payload": payload_copy,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        record = {
            "path": path,
            "rows": row_count,
            "columns": list(columns or []),
            "checksum": hashlib.sha256(checksum_source.encode("utf-8")).hexdigest()[:16],
            "readable": True,
            "preview": _preview(payload_copy, row_count, input_version, partition),
            "input_version": input_version,
            "definition": definition,
            "date": date,
            "partition": partition,
            "task_id": task_id,
            "receipt_id": receipt_id,
            "written_at": datetime.now().isoformat(timespec="seconds"),
            "_payload": payload_copy,
        }
        self._items[path] = record
        self.write_count += 1
        return self.describe(path) or {}

    def describe(self, path: str) -> dict[str, Any] | None:
        """只返回元数据，不读取载荷，不推进读取计数。"""
        item = self._items.get(path)
        if item is None:
            return None
        return {key: copy.deepcopy(value) for key, value in item.items() if key != "_payload"}

    def exists(self, path: str) -> bool:
        return path in self._items

    def read(self, path: str) -> dict[str, Any] | None:
        """真正读取产物：补片任务执行时才调用这里。"""
        item = self._items.get(path)
        if item is None:
            return None
        self.read_count += 1
        result = {
            key: copy.deepcopy(value) for key, value in item.items() if key != "_payload"
        }
        result["payload"] = copy.deepcopy(item.get("_payload"))
        result["read_at"] = datetime.now().isoformat(timespec="seconds")
        result["rows_read"] = item["rows"]
        return result

    def paths(self) -> list[str]:
        return sorted(self._items)

    def reset(self) -> None:
        self._items.clear()
        self.read_count = 0
        self.write_count = 0


artifact_store = ArtifactStore()


def default_artifact_path(definition: str, date: str, input_version: str,
                          partition: str, task_id: int | None) -> str:
    """产物路径带版本段：同日期不同输入版本不会互相覆盖。"""
    safe_definition = (definition or "compute").replace("/", "_")
    suffix = f"task_{task_id}" if task_id is not None else "task_unknown"
    return (
        f"outputs/{safe_definition}/{date or 'unknown-date'}/"
        f"{input_version or 'v1'}/{partition}/{suffix}.parquet"
    )
