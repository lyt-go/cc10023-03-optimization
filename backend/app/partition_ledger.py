"""分区台账：按「计算定义 + 输入版本 + 业务日期 + 分区」记录产物与完成回执。

现有重试只能看到整项任务，无法知道哪个日期/分区已经有数据。这里把
执行信息拆到分区粒度：没确认的分区才是缺口，已确认的分区在重试时直接复用。
完成回执按回执身份去重，晚到的回执仍归属到实际产生它的那个输入版本。
"""
from __future__ import annotations

import copy
from datetime import datetime
from typing import Any

from app.artifact_store import artifact_store, default_artifact_path


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


class PartitionLedger:
    def __init__(self) -> None:
        self.runs: dict[int, dict[str, Any]] = {}
        self.states: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        self.receipts: dict[str, dict[str, Any]] = {}
        self._seq = 0

    # ── 运行登记 ──
    def reset(self) -> None:
        self.runs.clear()
        self.states.clear()
        self.receipts.clear()
        self._seq = 0

    def _state_key(self, definition: str, date: str, version: str, partition: str):
        return (definition, date, version, partition)

    def ensure_state(self, definition: str, date: str, version: str,
                     partition: str) -> dict[str, Any]:
        key = self._state_key(definition, date, version, partition)
        state = self.states.get(key)
        if state is None:
            state = {
                "definition": definition,
                "date": date,
                "input_version": version,
                "partition": partition,
                "status": "未开始",
                "rows": None,
                "artifact_path": None,
                "receipts": [],
                "accepted_count": 0,
                "duplicate_count": 0,
                "confirmed_task_id": None,
                "confirmed_receipt_id": None,
                "confirmed_at": None,
                "failure_reason": None,
                "recompute_requested": False,
                "last_task_id": None,
                "last_sequence": 0,
            }
            self.states[key] = state
        return state

    def register_run(
        self,
        *,
        task_id: int,
        definition: str,
        input_version: str,
        date: str,
        partitions: list[str],
        completed: list[dict[str, Any]] | None = None,
        failed: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """登记一次实际运行；partitions 是这次运行的完整分区清单。"""
        self._seq += 1
        run = {
            "task_id": task_id,
            "definition": definition,
            "input_version": input_version,
            "date": date,
            "partitions": list(partitions),
            "sequence": self._seq,
            "created_at": _now(),
            "failed": [],
        }
        self.runs[task_id] = run
        for partition in partitions:
            state = self.ensure_state(definition, date, input_version, partition)
            state["last_task_id"] = task_id
            state["last_sequence"] = max(int(state.get("last_sequence") or 0), self._seq)
        for item in completed or []:
            partition = str(item.get("partition") or "")
            if partition not in partitions:
                partitions.append(partition)
                run["partitions"].append(partition)
            self.report_completion(
                task_id,
                partition,
                rows=item.get("rows"),
                artifact_path=item.get("artifact_path"),
                payload=item.get("payload"),
                columns=item.get("columns"),
                receipt_id=item.get("receipt_id") or f"seed-{task_id}-{partition}",
                stated_version=item.get("input_version"),
            )
        for item in failed or []:
            partition = str(item.get("partition") or "")
            if partition not in partitions:
                partitions.append(partition)
                run["partitions"].append(partition)
            self.report_failure(task_id, partition, item.get("reason"))
        return self.describe_run(task_id)

    def describe_run(self, task_id: int) -> dict[str, Any]:
        run = self.runs.get(task_id)
        if run is None:
            return {}
        rows = []
        for partition in run["partitions"]:
            state = self.ensure_state(
                run["definition"], run["date"], run["input_version"], partition)
            rows.append(self.describe_state(state))
        return {
            "task_id": task_id,
            **{key: run[key] for key in
               ("definition", "input_version", "date", "partitions", "sequence", "created_at")},
            "failed_partitions": [s["partition"] for s in rows if s["status"] == "已失败"],
            "confirmed_partitions": [s["partition"] for s in rows if s["status"] == "已完成"],
            "pending_partitions": [s["partition"] for s in rows if s["status"] != "已完成"],
            "partitions_detail": rows,
        }

    def describe_state(self, state: dict[str, Any]) -> dict[str, Any]:
        result = copy.deepcopy(state)
        result.pop("last_sequence", None)
        artifact = (
            artifact_store.describe(state["artifact_path"])
            if state.get("artifact_path")
            else None
        )
        result["artifact"] = artifact
        result["读产物"] = bool(artifact and artifact.get("readable"))
        return result

    # ── 失败与完成回执 ──
    def report_failure(self, task_id: int, partition: str, reason: str | None = None) -> dict[str, Any]:
        run = self.runs.get(task_id)
        if run is None:
            return {"ok": False, "message": f"运行任务 {task_id} 未登记"}
        state = self.ensure_state(
            run["definition"], run["date"], run["input_version"], partition)
        if state["status"] == "已完成":
            return {
                "ok": False,
                "message": f"分区 {partition} 已有确认产物，失败回执不改写已完成分区",
                "status": state["status"],
            }
        state["status"] = "已失败"
        state["failure_reason"] = reason or "计算失败"
        state["last_task_id"] = task_id
        state["last_sequence"] = max(int(state.get("last_sequence") or 0), int(run["sequence"]))
        if partition not in run["failed"]:
            run["failed"].append(partition)
        return {
            "ok": True,
            "task_id": task_id,
            "partition": partition,
            "input_version": run["input_version"],
            "date": run["date"],
            "status": state["status"],
            "reason": state["failure_reason"],
        }

    def report_completion(
        self,
        task_id: int,
        partition: str,
        *,
        rows: int | str | None = None,
        artifact_path: str | None = None,
        payload: list[dict[str, Any]] | None = None,
        columns: list[str] | None = None,
        receipt_id: str | None = None,
        stated_version: str | None = None,
    ) -> dict[str, Any]:
        """接收完成回执；重复回执只计一次，版本归属以实际运行登记为准。"""
        run = self.runs.get(task_id)
        if run is None:
            return {"ok": False, "message": f"运行任务 {task_id} 未登记"}
        actual_version = run["input_version"]
        state = self.ensure_state(run["definition"], run["date"], actual_version, partition)
        identity = receipt_id or f"{task_id}|{partition}|{rows}|{artifact_path or ''}"
        base = {
            "task_id": task_id,
            "partition": partition,
            "date": run["date"],
            "received_at": _now(),
            "receipt_id": identity,
        }

        # 回执身份已经出现过：直接按重复回执返回，不重复计数。
        # 回执归属它首次送达的那次运行，即使同一身份后来被另一个任务重发。
        previous = self.receipts.get(identity)
        if previous is not None:
            owner_task_id = int(previous.get("task_id") or task_id)
            owner_run = self.runs.get(owner_task_id) or run
            owner_state = self.ensure_state(
                owner_run["definition"], owner_run["date"],
                owner_run["input_version"], previous.get("partition") or partition)
            owner_state["duplicate_count"] = int(owner_state.get("duplicate_count") or 0) + 1
            previous["duplicate_deliveries"] = (
                int(previous.get("duplicate_deliveries") or 0) + 1
            )
            if stated_version and stated_version != previous["attributed_version"]:
                previous["version_mismatch_deliveries"] = (
                    int(previous.get("version_mismatch_deliveries") or 0) + 1
                )
            return {
                **base,
                "counted": False,
                "duplicate": True,
                "late": bool(previous.get("late")),
                "attributed_version": previous["attributed_version"],
                "actual_run_version": owner_run["input_version"],
                "reason": "重复送达的完成回执，只计一次",
                "status": owner_state["status"],
                "rows": owner_state["rows"],
                "artifact_path": owner_state["artifact_path"],
            }

        # 分区已经由另一次回执确认过：内容一致的视为重复，内容冲突的保留原产物。
        if state["status"] == "已完成" and state.get("artifact_path"):
            existing_rows = state.get("rows")
            incoming_rows = self._coerce_rows(rows, payload)
            same_content = (
                incoming_rows is None
                or existing_rows is None
                or int(incoming_rows) == int(existing_rows)
            )
            same_source = (
                artifact_path is None
                or state["artifact_path"] == artifact_path
            )
            if same_content and same_source:
                state["duplicate_count"] = int(state.get("duplicate_count") or 0) + 1
                self.receipts[identity] = {
                    **base,
                    "counted": False,
                    "duplicate": True,
                    "attributed_version": actual_version,
                    "late": self._is_late(run, partition, actual_version),
                }
                return {
                    **base,
                    "ok": True,
                    "counted": False,
                    "duplicate": True,
                    "late": bool(self.receipts[identity]["late"]),
                    "attributed_version": actual_version,
                    "actual_run_version": actual_version,
                    "reason": "分区已有确认产物，重复回执不再计数",
                    "status": state["status"],
                    "rows": state["rows"],
                    "artifact_path": state["artifact_path"],
                }
            return {
                **base,
                "ok": False,
                "counted": False,
                "duplicate": False,
                "conflict": True,
                "late": self._is_late(run, partition, actual_version),
                "attributed_version": actual_version,
                "actual_run_version": actual_version,
                "message": (
                    f"分区 {partition} 已有确认产物 {existing_rows} 行，"
                    f"冲突回执 {incoming_rows} 行未覆盖原产物"
                ),
                "status": state["status"],
                "rows": state["rows"],
                "artifact_path": state["artifact_path"],
            }

        # 首次确认：写入产物。路径按版本隔离，v1 与 v2 同日同分区不会互相覆盖。
        path = artifact_path or default_artifact_path(
            run["definition"], run["date"], actual_version, partition, task_id)
        incoming_rows = self._coerce_rows(rows, payload)
        if artifact_store.exists(path):
            artifact = artifact_store.describe(path) or {}
            if incoming_rows is not None and int(incoming_rows) != int(artifact.get("rows", 0)):
                return {
                    **base,
                    "ok": False,
                    "counted": False,
                    "duplicate": False,
                    "conflict": True,
                    "message": f"产物 {path} 已存在且行数不一致，未覆盖",
                    "rows": artifact.get("rows"),
                    "artifact_path": path,
                }
            effective_rows = artifact.get("rows", 0)
        else:
            artifact = artifact_store.write(
                path,
                incoming_rows if incoming_rows is not None else 0,
                payload=payload,
                columns=columns,
                input_version=actual_version,
                definition=run["definition"],
                date=run["date"],
                partition=partition,
                task_id=task_id,
                receipt_id=identity,
            )
            effective_rows = artifact["rows"]

        late = self._is_late(run, partition, actual_version)
        state.update({
            "status": "已完成",
            "rows": int(effective_rows),
            "artifact_path": path,
            "confirmed_task_id": task_id,
            "confirmed_receipt_id": identity,
            "confirmed_at": _now(),
            "failure_reason": None,
            "recompute_requested": False,
            "last_task_id": task_id,
            "last_sequence": max(int(state.get("last_sequence") or 0), int(run["sequence"]),
                                 int(run.get("sequence") or 0)),
        })
        state["accepted_count"] = int(state.get("accepted_count") or 0) + 1
        receipt = {
            **base,
            "counted": True,
            "duplicate": False,
            "late": late,
            "attributed_version": actual_version,
            "stated_version": stated_version,
            "actual_run_version": actual_version,
            "rows": int(effective_rows),
            "artifact_path": path,
        }
        state["receipts"].append(receipt)
        self.receipts[identity] = receipt
        if partition in run["failed"]:
            run["failed"].remove(partition)
        return {
            **base,
            "ok": True,
            "counted": True,
            "duplicate": False,
            "late": late,
            "attributed_version": actual_version,
            "actual_run_version": actual_version,
            "version_mismatch": bool(stated_version and stated_version != actual_version),
            "status": state["status"],
            "rows": int(effective_rows),
            "artifact_path": path,
        }

    @staticmethod
    def _coerce_rows(rows: int | str | None,
                     payload: list[dict[str, Any]] | None) -> int | None:
        if payload is not None:
            return len(payload)
        if rows is None or rows == "":
            return None
        try:
            return int(float(str(rows)))
        except (TypeError, ValueError):
            return None

    def _is_late(self, run: dict[str, Any], partition: str, version: str) -> bool:
        """同一逻辑分区已有其它输入版本的确认产物时，这条回执照算晚到。

        晚到只影响标记，不改变归属：回执仍写入它实际所属运行登记的版本。
        """
        for (d, dt, v, p), state in self.states.items():
            if d != run["definition"] or dt != run["date"] or p != partition:
                continue
            if v == version:
                continue
            if state["status"] == "已完成" and state.get("artifact_path"):
                return True
        return False

    # ── 缺口定位与快照清单 ──
    def gap_partitions(self, task_id: int) -> list[dict[str, Any]]:
        run = self.runs.get(task_id)
        if run is None:
            return []
        gaps = []
        for partition in run["partitions"]:
            state = self.ensure_state(
                run["definition"], run["date"], run["input_version"], partition)
            artifact = (
                artifact_store.describe(state["artifact_path"])
                if state.get("artifact_path")
                else None
            )
            if (
                state["status"] != "已完成"
                or not state.get("artifact_path")
                or not (artifact or {}).get("readable")
            ):
                gaps.append({
                    "task_id": task_id,
                    "partition": partition,
                    "definition": run["definition"],
                    "input_version": run["input_version"],
                    "date": run["date"],
                    "status": state["status"],
                    "reason": state.get("failure_reason")
                    or ("产物缺失" if state["status"] == "已完成" else "未完成"),
                })
        return gaps

    def reusable_partitions(self, definition: str, date: str, version: str) -> list[dict[str, Any]]:
        reusable = []
        for (d, dt, v, p), state in sorted(self.states.items()):
            if d != definition or dt != date or v != version:
                continue
            if state["status"] != "已完成":
                continue
            artifact = artifact_store.describe(state.get("artifact_path") or "")
            if not artifact or not artifact.get("readable"):
                continue
            reusable.append({
                "partition": p,
                "rows": state.get("rows"),
                "artifact_path": state.get("artifact_path"),
                "checksum": artifact.get("checksum"),
            })
        return reusable

    def manifest(self, definition: str, date: str, version: str) -> dict[str, Any]:
        """只汇总已确认且可读的产物；未完成分区单独列出，供继续补片。"""
        confirmed = self.reusable_partitions(definition, date, version)
        states = [
            state for (d, dt, v, p), state in self.states.items()
            if d == definition and dt == date and v == version
        ]
        confirmed_names = {item["partition"] for item in confirmed}
        pending = sorted(
            state["partition"] for state in states
            if state["status"] != "已完成"
            or not state.get("artifact_path")
            or state["partition"] not in confirmed_names
        )
        total_rows = sum(int(item.get("rows") or 0) for item in confirmed)
        artifacts = []
        for item in confirmed:
            artifact = artifact_store.describe(item["artifact_path"]) or {}
            state = self.ensure_state(definition, date, version, item["partition"])
            artifacts.append({
                "partition": item["partition"],
                "artifact_path": item["artifact_path"],
                "rows": int(artifact.get("rows", 0)),
                "columns": artifact.get("columns", []),
                "checksum": artifact.get("checksum"),
                "readable": bool(artifact.get("readable")),
                "input_version": version,
                "date": date,
                "receipt_id": state.get("confirmed_receipt_id"),
                "confirmed_at": state.get("confirmed_at"),
                "sample": artifact.get("preview", []),
            })
        return {
            "definition": definition,
            "input_version": version,
            "date": date,
            "partitions": artifacts,
            "confirmed_partitions": [item["partition"] for item in artifacts],
            "pending_partitions": pending,
            "partition_count": len(artifacts) + len(pending),
            "confirmed_count": len(artifacts),
            "total_rows": total_rows,
            "all_confirmed": bool(artifacts) and not pending,
            "receipts": {
                "accepted": sum(int(state.get("accepted_count") or 0) for state in states),
                "duplicates": sum(int(state.get("duplicate_count") or 0) for state in states),
                "delivered": sum(
                    int(state.get("accepted_count") or 0)
                    + int(state.get("duplicate_count") or 0)
                    for state in states
                ),
            },
        }

    def mark_recompute(self, task_id: int, partition: str) -> dict[str, Any]:
        run = self.runs.get(task_id)
        if run is None:
            return {}
        state = self.ensure_state(
            run["definition"], run["date"], run["input_version"], partition)
        if state["status"] == "已完成":
            return self.describe_state(state)
        state["recompute_requested"] = True
        state["last_task_id"] = task_id
        return self.describe_state(state)


partition_ledger = PartitionLedger()
