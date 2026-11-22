"""特征计算任务领域服务：批量计算的编排、分区管理与执行追踪。"""
from __future__ import annotations

from typing import Any

from app.artifact_store import artifact_store, default_artifact_path
from app.partition_ledger import partition_ledger
from app.services.base import BaseModuleService
from app.store import store


class FeatureComputeService(BaseModuleService):
    MODULE = "feature_compute"
    ENTITY = "计算任务"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["任务编号", "关联特征", "数据切片", "计算引擎", "输入分区", "输出分区", "并行度", "任务状态"]
    REQUIRED_FIELDS = ["任务编号", "关联特征", "数据切片"]
    STATUS_ORDER = ["草稿", "等待中", "计算中", "已完成", "已失败"]
    ACTION_RULES = {"提交执行": "草稿", "停止计算": "计算中", "确认完成": "已完成"}
    NEGATIVE_ACTIONS = []

    # ── 链路1: 特征计算 → 特征快照 ──
    # ── 链路5: 特征计算 → 相关性矩阵 → 特征漂移监控 ──
    def execute_and_snapshot(self, task_id: int) -> dict[str, Any]:
        """执行特征计算任务并在完成后自动生成特征快照存储到snapshots目录。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": f"计算任务 {task_id} 不存在"}
        task["status"] = "已完成"
        task["pending"] = False
        snapshot_rows = store.rows("feature_snapshot")
        next_id = max((int(r.get("id", 0)) for r in snapshot_rows), default=0) + 1
        feats = task.get("关联特征", "unknown")
        snapshot_rows.append({
            "id": next_id,
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "快照编号": f"SNAP-{next_id:04d}",
            "特征集": feats,
            "快照日期": "2026-11-18",
            "特征数量": 12,
            "数据行数": "100000",
            "存储路径": f"snapshots/compute_{task_id}_{next_id}.json",
            "保留天数": "30",
            "快照状态": "已完成",
        })
        return {
            "ok": True,
            "task_id": task_id,
            "snapshot_id": next_id,
            "message": f"计算任务 {task_id} 已完成，生成特征快照 {next_id}",
        }

    def compute_correlation_matrix(self, task_id: int) -> dict[str, Any]:
        """基于特征计算结果计算特征间相关性矩阵并写入存储。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        corr_rows = store.rows("feature_corr")
        next_id = max((int(r.get("id", 0)) for r in corr_rows), default=0) + 1
        feats = task.get("关联特征", "")
        corr_rows.append({
            "id": next_id,
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "矩阵编号": f"CORR-{next_id:04d}",
            "特征集": feats,
            "计算方法": "pearson",
            "阈值筛选": "0.7",
            "可视化配置": "heatmap_full",
            "计算时间": "2026-11-18T10:00:00",
            "矩阵版本": "v1.0.0",
            "结果存储": f"corr_matrix/compute_{task_id}_v1.json",
        })
        return {"ok": True, "corr_id": next_id, "task_id": task_id}

    def trigger_drift_monitoring(self, task_id: int) -> dict[str, Any]:
        """基于特征计算结果触发特征漂移监控，关联基准分布并启动PSI计算。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        drift_rows = store.rows("drift_monitor")
        next_id = max((int(r.get("id", 0)) for r in drift_rows), default=0) + 1
        feats = task.get("关联特征", "")
        drift_rows.append({
            "id": next_id,
            "status": "监控中",
            "pending": False,
            "abnormal": False,
            "监控编号": f"DRIFT-{next_id:04d}",
            "关联特征": feats,
            "基准分布": f"baseline_compute_{task_id}",
            "当前分布": f"current_{task_id}_2026_11_18",
            "PSI值": "0.015",
            "漂移阈值": "0.25",
            "采样窗口": "30d",
            "监控状态": "监控中",
        })
        return {"ok": True, "drift_id": next_id, "task_id": task_id, "psi_value": 0.015}

    # ── 分区级运行登记与回执 ──
    def register_partition_run(
        self,
        task_id: int,
        *,
        definition: str | None = None,
        input_version: str | None = None,
        date: str | None = None,
        partitions: list[str] | None = None,
        completed: list[dict[str, Any]] | None = None,
        failed: list[dict[str, Any]] | None = None,
        task_row: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """登记一次实际运行的分区清单，并把运行信息回写到任务记录。

        输入版本必须显式落到这次运行上：同一逻辑日期出现 v1/v2 两个任务时，
        晚到的完成回执也只会记到它实际所属的版本。
        """
        task = store.find(self.MODULE, task_id)
        if task is None and task_row is not None:
            task = self._attach_task_row(task_id, task_row)
        if task is None:
            return {"ok": False, "message": f"计算任务 {task_id} 不存在"}
        definition = definition or task.get("计算定义") or task.get("任务编号") or f"compute_{task_id}"
        input_version = input_version or task.get("输入版本") or "v1"
        date = date or task.get("业务日期") or "2026-11-18"
        partitions = list(partitions or [])
        if not partitions:
            partitions = self._task_partitions(task)
        if not partitions:
            return {"ok": False, "message": "分区清单为空，无法登记运行"}
        run = partition_ledger.register_run(
            task_id=task_id,
            definition=definition,
            input_version=input_version,
            date=date,
            partitions=partitions,
            completed=completed,
            failed=failed,
        )
        task.update({
            "计算定义": definition,
            "输入版本": input_version,
            "业务日期": date,
            "分区清单": ",".join(run["partitions"]),
            "已完成分区": ",".join(run["confirmed_partitions"]),
            "失败分区": ",".join(run["failed_partitions"]),
            "状态明细": run["partitions_detail"],
        })
        data_gaps = run["pending_partitions"]
        task["status"] = "已失败" if data_gaps else "已完成"
        task["pending"] = bool(data_gaps)
        task["abnormal"] = bool(data_gaps)
        task["任务状态"] = task["status"]
        return {"ok": True, **run}

    def _attach_task_row(self, task_id: int, values: dict[str, Any]) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        entry = {
            "id": task_id,
            "status": values.get("status") or "草稿",
            "pending": values.get("pending", True),
            "abnormal": False,
        }
        entry.update({field: values.get(field) for field in self.LIST_FIELDS})
        entry["任务状态"] = entry.get("任务状态") or entry["status"]
        rows.append(entry)
        return entry

    @staticmethod
    def _task_partitions(task: dict[str, Any]) -> list[str]:
        raw = task.get("分区清单")
        if isinstance(raw, list):
            return [str(item).strip() for item in raw if str(item).strip()]
        if raw:
            return [item.strip() for item in str(raw).split(",") if item.strip()]
        # 没登记过分区时，用输出分区/输入分区作为单分区兜底。
        for field in ("输出分区", "输入分区"):
            value = str(task.get(field) or "").strip()
            if value:
                return [value]
        return []

    def report_partition_failure(self, task_id: int, partition: str,
                                 reason: str | None = None) -> dict[str, Any]:
        result = partition_ledger.report_failure(task_id, partition, reason)
        self._refresh_task_state(task_id)
        return result

    def receive_partition_receipt(
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
        """接收单条完成回执：重复只计一次，版本以实际运行登记为准。"""
        result = partition_ledger.report_completion(
            task_id,
            partition,
            rows=rows,
            artifact_path=artifact_path,
            payload=payload,
            columns=columns,
            receipt_id=receipt_id,
            stated_version=stated_version,
        )
        self._refresh_task_state(task_id)
        return result

    def receive_completion_receipts(self, receipts: list[dict[str, Any]]) -> dict[str, Any]:
        """批量接收回执，逐条给出计数/重复/晚到与归属版本。"""
        results = []
        for item in receipts:
            result = self.receive_partition_receipt(
                int(item.get("task_id") or item.get("taskId") or 0),
                str(item.get("partition") or ""),
                rows=item.get("rows"),
                artifact_path=item.get("artifact_path") or item.get("artifactPath"),
                payload=item.get("payload"),
                columns=item.get("columns"),
                receipt_id=item.get("receipt_id") or item.get("receiptId"),
                stated_version=item.get("input_version") or item.get("version"),
            )
            results.append({"partition": item.get("partition"), **result})
        return {
            "ok": all(item.get("ok") for item in results) if results else False,
            "total": len(results),
            "counted": sum(1 for item in results if item.get("counted")),
            "duplicates": sum(1 for item in results if item.get("duplicate")),
            "late": sum(1 for item in results if item.get("late")),
            "results": results,
        }

    def _refresh_task_state(self, task_id: int) -> None:
        task = store.find(self.MODULE, task_id)
        run = partition_ledger.describe_run(task_id)
        if task is None or not run:
            return
        task["已完成分区"] = ",".join(run["confirmed_partitions"])
        task["失败分区"] = ",".join(run["failed_partitions"])
        task["状态明细"] = run["partitions_detail"]
        gaps = run["pending_partitions"]
        if gaps:
            task["status"] = "已失败"
            task["pending"] = True
            task["abnormal"] = True
        else:
            task["status"] = "已完成"
            task["pending"] = False
            task["abnormal"] = False
        task["任务状态"] = task["status"]

    def describe_partition_run(self, task_id: int) -> dict[str, Any]:
        run = partition_ledger.describe_run(task_id)
        if not run:
            return {"ok": False, "message": f"计算任务 {task_id} 未登记分区运行"}
        return {"ok": True, **run}

    # ── 优化后的批量重试：按缺口补算、按版本复用 ──
    def retry_failed_tasks(self, task_ids: list[int]) -> dict[str, Any]:
        """按分区缺口重试，不再把整项任务打回草稿。

        同一计算定义、输入版本、日期下已完成的分区直接复用；只对未完成分区
        生成新的运行任务。任务没有任何缺口时原任务保持不动。
        """
        items = [{"task_id": tid} for tid in task_ids]
        return self.batch_retry(items)

    def batch_retry(self, items: list[dict[str, Any]] | list[int]) -> dict[str, Any]:
        """受理勾选的失败分区，逐条列出补算或复用。

        items 可以是任务 id，也可以是 {task_id, partitions:[...]}；只指定分区时
        仅对勾选且确实缺数据的分区补算。
        """
        normalized: list[dict[str, Any]] = []
        for item in items:
            if isinstance(item, int):
                normalized.append({"task_id": item})
            elif isinstance(item, dict):
                normalized.append({
                    "task_id": int(item.get("task_id") or item.get("taskId") or 0),
                    "partitions": item.get("partitions") or item.get("failed_partitions"),
                })
        entries: list[dict[str, Any]] = []
        for item in normalized:
            entries.extend(self._retry_one(item["task_id"], item.get("partitions")))

        retried = [
            {"task_id": e["source_task_id"], "partition": e["partition"],
             "new_task_id": e.get("new_task_id")}
            for e in entries if e["decision"] == "补算"
        ]
        reused = [
            {"task_id": e["source_task_id"], "partition": e["partition"],
             "rows": e.get("rows"), "artifact_path": e.get("artifact_path")}
            for e in entries if e["decision"] == "复用"
        ]
        rejected = [e for e in entries if e["decision"] == "不受理"]
        retried_sources = sorted({e["source_task_id"] for e in entries
                                  if e["decision"] == "补算"})
        rejected_sources = sorted({e["source_task_id"] for e in entries
                                   if e["decision"] == "不受理"})
        return {
            "ok": any(e["decision"] in ("补算", "复用") for e in entries),
            "submitted": len(normalized),
            "receipt_id": f"RETRY-{len(entries):04d}-{len(retried)}",
            "entries": entries,
            "retried_partitions": retried,
            "reused_partitions": reused,
            "rejected": rejected,
            "补算": len(retried),
            "复用": len(reused),
            # 兼容旧调用方：整任务维度不再回退草稿，只列出实际补算的分区。
            "retried": retried_sources,
            "failed_skipped": rejected_sources,
        }

    def _retry_one(self, task_id: int, selected: list[str] | None) -> list[dict[str, Any]]:
        run = partition_ledger.describe_run(task_id)
        if not run:
            return [{
                "source_task_id": task_id,
                "partition": None,
                "decision": "不受理",
                "reason": "任务没有分区运行登记，无法定位缺口",
            }]
        gaps = partition_ledger.gap_partitions(task_id)
        gap_names = {gap["partition"] for gap in gaps}
        target_set = None
        if selected:
            target_set = {str(p).strip() for p in selected if str(p).strip()}

        entries: list[dict[str, Any]] = []
        for partition in run["partitions"]:
            if target_set is not None and partition not in target_set:
                continue
            state = next(
                (detail for detail in run["partitions_detail"]
                 if detail["partition"] == partition), None)
            if partition not in gap_names:
                entries.append({
                    "source_task_id": task_id,
                    "partition": partition,
                    "decision": "复用",
                    "input_version": run["input_version"],
                    "date": run["date"],
                    "rows": state.get("rows") if state else None,
                    "artifact_path": state.get("artifact_path") if state else None,
                    "artifact_readable": bool(state and state.get("读产物")),
                    "reason": "同一定义、输入版本与日期下分区已确认，直接复用",
                })
                continue
            entries.append({
                "source_task_id": task_id,
                "partition": partition,
                "decision": "补算",
                "input_version": run["input_version"],
                "date": run["date"],
                "reason": (state or {}).get("failure_reason") or "分区未完成",
            })
        if target_set is not None:
            for partition in sorted(target_set - set(run["partitions"])):
                entries.append({
                    "source_task_id": task_id,
                    "partition": partition,
                    "decision": "不受理",
                    "reason": "该分区不在这次运行登记的分区清单内",
                })

        patch = [e for e in entries if e["decision"] == "补算"]
        if patch:
            new_task_id = self._create_retry_task(run, [e["partition"] for e in patch])
            for entry in patch:
                entry["new_task_id"] = new_task_id
                partition_ledger.mark_recompute(task_id, entry["partition"])
            self._refresh_task_state(task_id)
        return entries

    def _create_retry_task(self, run: dict[str, Any], partitions: list[str]) -> int:
        rows = store.rows(self.MODULE)
        next_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        row = {
            "id": next_id,
            "status": "等待中",
            "pending": True,
            "abnormal": False,
            "任务编号": f"COMP-RTY-{next_id:04d}",
            "关联特征": run.get("definition", ""),
            "数据切片": run.get("definition", ""),
            "计算引擎": "spark",
            "输入分区": ",".join(partitions),
            "输出分区": ",".join(
                default_artifact_path(
                    run["definition"], run["date"], run["input_version"], p, next_id)
                for p in partitions
            ),
            "并行度": str(len(partitions)),
            "任务状态": "等待中",
            "计算定义": run["definition"],
            "输入版本": run["input_version"],
            "业务日期": run["date"],
            "分区清单": ",".join(partitions),
            "已完成分区": "",
            "失败分区": "",
            "重试来源": run["task_id"],
        }
        rows.append(row)
        partition_ledger.register_run(
            task_id=next_id,
            definition=run["definition"],
            input_version=run["input_version"],
            date=run["date"],
            partitions=partitions,
        )
        return next_id

    def query_compute_progress(self, task_id: int) -> dict[str, Any]:
        """查询特征计算任务的执行进度、引擎状态和资源消耗情况。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        return {
            "ok": True,
            "task_id": task_id,
            "status": task.get("status"),
            "engine": task.get("计算引擎"),
            "parallelism": task.get("并行度"),
            "input": task.get("输入分区"),
            "output": task.get("输出分区"),
        }
