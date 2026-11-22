"""特征快照管理领域服务：特征数据集的定时快照、版本追溯与回滚。"""
from __future__ import annotations

import copy
from datetime import datetime
from typing import Any

from app.artifact_store import artifact_store
from app.partition_ledger import partition_ledger
from app.services.base import BaseModuleService
from app.store import store


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


class FeatureSnapshotService(BaseModuleService):
    MODULE = "feature_snapshot"
    ENTITY = "特征快照"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["快照编号", "特征集", "快照日期", "特征数量", "数据行数", "存储路径", "保留天数", "快照状态"]
    REQUIRED_FIELDS = ["快照编号", "特征集", "快照日期"]
    STATUS_ORDER = ["草稿", "创建中", "已完成", "已过期"]
    ACTION_RULES = {"创建快照": "创建中", "确认完成": "已完成", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    # ── 链路1: 特征计算 → 特征快照 ──
    def archive_expired_snapshots(self) -> dict[str, Any]:
        """将超过保留天数的特征快照自动归档，并清理过期记录。"""
        rows = store.rows(self.MODULE)
        expired_ids = []
        from datetime import date, timedelta
        today = date.today()
        for r in rows:
            retain_days_str = r.get("保留天数", "0")
            try:
                retain_days = int(float(str(retain_days_str)))
            except (ValueError, TypeError):
                retain_days = 90
            snap_date_str = r.get("快照日期", "")
            try:
                snap_date = date.fromisoformat(str(snap_date_str)[:10])
            except (ValueError, TypeError):
                continue
            if (today - snap_date).days > retain_days:
                r["status"] = "已过期"
                r["abnormal"] = False
                expired_ids.append(r["id"])
        return {"ok": True, "archived_count": len(expired_ids), "expired_ids": expired_ids}

    def snapshot_feature_set(self, feature_set: str, compute_id: int) -> dict[str, Any]:
        """对指定特征集创建数据快照并存储到snapshots目录。"""
        rows = store.rows(self.MODULE)
        next_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        feats = feature_set.split(",") if feature_set else []
        rows.append({
            "id": next_id,
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "快照编号": f"SNAP-C{compute_id:04d}-{next_id:04d}",
            "特征集": feature_set,
            "快照日期": "2026-11-18",
            "特征数量": len(feats),
            "数据行数": str(len(feats) * 10000),
            "存储路径": f"snapshots/feature_set_{next_id}.json",
            "保留天数": "30",
            "快照状态": "已完成",
        })
        return {"ok": True, "snapshot_id": next_id, "feature_count": len(feats)}

    # ── 复算快照：只汇总已确认产物，未完成分区留给补片 ──
    def generate_recompute_snapshot(self, task_id: int) -> dict[str, Any]:
        """按一次实际运行生成复算快照，版本取自该运行登记的输入版本。"""
        run = partition_ledger.describe_run(task_id)
        if not run:
            return {"ok": False, "message": f"计算任务 {task_id} 未登记分区运行，无法生成快照"}
        return self._create_recompute_snapshot(
            definition=run["definition"],
            date=run["date"],
            version=run["input_version"],
            source_task_id=task_id,
        )

    def generate_recompute_snapshot_for(self, definition: str, date: str,
                                        input_version: str) -> dict[str, Any]:
        """按计算定义 + 日期 + 输入版本生成复算快照（用于同日期多版本核对）。"""
        return self._create_recompute_snapshot(
            definition=definition,
            date=date,
            version=input_version,
            source_task_id=None,
        )

    def _create_recompute_snapshot(self, *, definition: str, date: str,
                                   version: str, source_task_id: int | None) -> dict[str, Any]:
        manifest = partition_ledger.manifest(definition, date, version)
        if not manifest["partitions"] and not manifest["pending_partitions"]:
            return {
                "ok": False,
                "message": (
                    f"定义 {definition} 在 {date} 的输入版本 {version} 下没有可汇总的分区"
                ),
            }
        rows = store.rows(self.MODULE)
        next_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        storage_path = f"snapshots/recompute/{definition}/{date}/{version}/SNAP-RC-{next_id:04d}.json"
        snapshot = {
            "definition": definition,
            "input_version": version,
            "date": date,
            "source_task_id": source_task_id,
            "created_at": _now(),
            "storage_path": storage_path,
            **manifest,
        }
        snapshot["readable_artifacts"] = [
            item for item in snapshot["partitions"] if item["readable"]
        ]
        snapshot["rows"] = snapshot["total_rows"]
        row = {
            "id": next_id,
            "status": "已完成" if manifest["all_confirmed"] else "创建中",
            "pending": not manifest["all_confirmed"],
            "abnormal": False,
            "快照编号": f"SNAP-RC-{next_id:04d}",
            "特征集": definition,
            "快照日期": date,
            "特征数量": manifest["confirmed_count"],
            "数据行数": str(manifest["total_rows"]),
            "存储路径": storage_path,
            "保留天数": "30",
            "快照状态": "已完成" if manifest["all_confirmed"] else "创建中",
            "快照类型": "复算快照",
            "计算定义": definition,
            "输入版本": version,
            "来源任务": source_task_id,
            "已确认分区": ",".join(manifest["confirmed_partitions"]),
            "未完成分区": ",".join(manifest["pending_partitions"]),
            "产物明细": copy.deepcopy(manifest["partitions"]),
            "回执统计": copy.deepcopy(manifest["receipts"]),
            "生成时间": snapshot["created_at"],
        }
        rows.append(row)
        return {
            "ok": True,
            "snapshot_id": next_id,
            "snapshot_code": row["快照编号"],
            "storage_path": storage_path,
            "definition": definition,
            "input_version": version,
            "date": date,
            "rows": manifest["total_rows"],
            "confirmed_count": manifest["confirmed_count"],
            "pending_partitions": manifest["pending_partitions"],
            "partitions": manifest["partitions"],
            "message": (
                f"复算快照 {row['快照编号']} 已生成：{manifest['confirmed_count']} 个已确认分区，"
                f"共 {manifest['total_rows']} 行；未完成分区可继续补片"
            ),
        }

    def describe_recompute_snapshot(self, snapshot_id: int) -> dict[str, Any]:
        """给恢复用的快照版本：确切行数、日期与逐分区可读产物。"""
        row = store.find(self.MODULE, snapshot_id)
        if row is None:
            return {"ok": False, "message": f"快照 {snapshot_id} 不存在"}
        is_recompute = row.get("快照类型") == "复算快照"
        if not is_recompute:
            return {
                "ok": False,
                "message": f"快照 {snapshot_id} 不是复算快照，没有分区级复算明细",
                "snapshot_code": row.get("快照编号"),
            }
        artifacts = []
        for item in row.get("产物明细") or []:
            path = item.get("artifact_path")
            live = artifact_store.describe(path) if path else None
            artifacts.append({
                "partition": item.get("partition"),
                "artifact_path": path,
                "snapshot_rows": int(item.get("rows") or 0),
                "live_rows": int((live or {}).get("rows", 0)),
                "readable": bool(live and live.get("readable")),
                "checksum": item.get("checksum"),
                "live_checksum": (live or {}).get("checksum"),
                "date": item.get("date") or row.get("快照日期"),
                "input_version": item.get("input_version") or row.get("输入版本"),
                "receipt_id": item.get("receipt_id"),
                "sample": (live or {}).get("preview", []),
            })
        return {
            "ok": True,
            "snapshot_id": snapshot_id,
            "snapshot_code": row.get("快照编号"),
            "snapshot_type": row.get("快照类型", "复算快照"),
            "definition": row.get("计算定义"),
            "input_version": row.get("输入版本"),
            "date": row.get("快照日期"),
            "rows": int(str(row.get("数据行数") or "0") or 0),
            "confirmed_partitions": [
                item for item in str(row.get("已确认分区") or "").split(",") if item
            ],
            "pending_partitions": [
                item for item in str(row.get("未完成分区") or "").split(",") if item
            ],
            "receipts": row.get("回执统计") or {},
            "artifacts": artifacts,
            "readable_artifacts": [item for item in artifacts if item["readable"]],
            "status": row.get("status"),
            "storage_path": row.get("存储路径"),
        }

    def _snapshot_row(self, snapshot_id: int) -> dict[str, Any] | None:
        return store.find(self.MODULE, snapshot_id)

    # ── 从快照生成补片任务 ──
    def create_patch_task(self, snapshot_id: int,
                          partitions: list[str] | None = None) -> dict[str, Any]:
        """从复算快照生成补片任务；创建时不读取数据，执行时再读快照产物。"""
        snap = self._snapshot_row(snapshot_id)
        if snap is None:
            return {"ok": False, "message": f"快照 {snapshot_id} 不存在"}
        if snap.get("快照类型") != "复算快照":
            return {"ok": False, "message": f"快照 {snapshot_id} 不是复算快照，不能生成补片任务"}
        snapshot = self.describe_recompute_snapshot(snapshot_id)
        confirmed = set(snapshot["confirmed_partitions"])
        pending = set(snapshot["pending_partitions"])
        requested = [str(p).strip() for p in (partitions or []) if str(p).strip()]
        if requested:
            unknown = set(requested) - confirmed - pending
            if unknown:
                return {
                    "ok": False,
                    "message": f"分区 {sorted(unknown)} 不在快照 {snap.get('快照编号')} 范围内",
                }
            target_confirmed = [p for p in requested if p in confirmed]
            target_pending = [p for p in requested if p in pending]
        else:
            target_confirmed = sorted(confirmed)
            target_pending = sorted(pending)

        compute_rows = store.rows("feature_compute")
        next_id = max((int(r.get("id", 0)) for r in compute_rows), default=0) + 1
        task = {
            "id": next_id,
            "status": "等待中" if target_pending else "草稿",
            "pending": True,
            "abnormal": False,
            "任务编号": f"COMP-PATCH-{next_id:04d}",
            "关联特征": snap.get("计算定义", ""),
            "数据切片": snap.get("计算定义", ""),
            "计算引擎": "spark",
            "输入分区": ",".join(target_confirmed + target_pending),
            "输出分区": ",".join(target_pending) if target_pending else "读取快照产物",
            "并行度": str(max(len(target_confirmed) + len(target_pending), 1)),
            "任务状态": "等待中" if target_pending else "草稿",
            "计算定义": snap.get("计算定义"),
            "输入版本": snap.get("输入版本"),
            "业务日期": snap.get("快照日期"),
            "分区清单": ",".join(target_confirmed + target_pending),
            "补片来源快照": snapshot_id,
            "快照编号": snap.get("快照编号"),
            "复用分区清单": ",".join(target_confirmed),
            "待补算分区清单": ",".join(target_pending),
        }
        compute_rows.append(task)
        partition_ledger.register_run(
            task_id=next_id,
            definition=snap.get("计算定义", ""),
            input_version=snap.get("输入版本", "v1"),
            date=snap.get("快照日期", ""),
            partitions=target_confirmed + target_pending,
        )
        return {
            "ok": True,
            "patch_task_id": next_id,
            "compute_id": next_id,
            "snapshot_id": snapshot_id,
            "snapshot_code": snap.get("快照编号"),
            "input_version": snap.get("输入版本"),
            "date": snap.get("快照日期"),
            "read_partitions": target_confirmed,
            "pending_partitions": target_pending,
            "message": (
                f"补片任务 {task['任务编号']} 已创建，执行时读取快照 "
                f"{snap.get('快照编号')} 的已确认产物；待补算分区 {target_pending or '无'}"
            ),
        }

    def execute_patch_task(self, patch_task_id: int) -> dict[str, Any]:
        """执行补片任务：此刻才读快照里的已确认产物，并核对行数。"""
        task = store.find("feature_compute", patch_task_id)
        if task is None:
            return {"ok": False, "message": f"补片任务 {patch_task_id} 不存在"}
        snapshot_id = task.get("补片来源快照")
        if snapshot_id is None:
            return {"ok": False, "message": f"任务 {patch_task_id} 不是补片任务"}
        snapshot = self.describe_recompute_snapshot(int(snapshot_id))
        if not snapshot.get("ok"):
            return {"ok": False, "message": snapshot.get("message")}

        expected = {
            item["partition"]: item for item in snapshot["artifacts"]
        }
        if "复用分区清单" in task:
            read_partitions = [
                item for item in str(task.get("复用分区清单") or "").split(",") if item
            ]
        else:
            read_partitions = snapshot["confirmed_partitions"]
        reads = []
        mismatches = []
        for partition in read_partitions:
            item = expected.get(partition)
            if item is None:
                mismatches.append({"partition": partition, "reason": "快照中没有该分区产物"})
                continue
            artifact = artifact_store.read(item["artifact_path"])
            if artifact is None:
                mismatches.append({
                    "partition": partition,
                    "reason": f"产物 {item['artifact_path']} 不可读",
                })
                continue
            row_match = int(artifact["rows"]) == int(item["snapshot_rows"])
            checksum_match = artifact["checksum"] == item["checksum"]
            read = {
                "partition": partition,
                "artifact_path": item["artifact_path"],
                "input_version": artifact["input_version"],
                "date": artifact["date"],
                "rows": artifact["rows"],
                "checksum": artifact["checksum"],
                "snapshot_rows": item["snapshot_rows"],
                "row_match": row_match,
                "checksum_match": checksum_match,
                "columns": artifact.get("columns", []),
                "sample": artifact.get("preview", []),
                "read_at": artifact.get("read_at"),
            }
            reads.append(read)
            if not row_match or not checksum_match:
                mismatches.append({
                    "partition": partition,
                    "reason": "读取行数与快照记录不一致",
                    "rows": artifact["rows"],
                    "snapshot_rows": item["snapshot_rows"],
                })

        pending = [p for p in snapshot["pending_partitions"] if p]
        row_counts = {item["partition"]: item["rows"] for item in reads}
        task["状态明细"] = reads
        task["读取产物"] = ",".join(item["artifact_path"] for item in reads)
        if mismatches or pending:
            task["status"] = "已失败" if mismatches else "等待中"
            task["pending"] = True
            task["abnormal"] = bool(mismatches)
        else:
            task["status"] = "已完成"
            task["pending"] = False
            task["abnormal"] = False
        task["任务状态"] = task["status"]
        return {
            "ok": not mismatches,
            "patch_task_id": patch_task_id,
            "snapshot_id": int(snapshot_id),
            "snapshot_code": snapshot.get("snapshot_code"),
            "input_version": snapshot.get("input_version"),
            "date": snapshot.get("date"),
            "reads": reads,
            "partition_rows": row_counts,
            "total_rows": sum(int(item["rows"]) for item in reads),
            "mismatches": mismatches,
            "pending_partitions": pending,
            "task_status": task["status"],
            "message": (
                "补片任务已读取快照产物并核对通过"
                if not mismatches
                else f"补片任务读取核对失败：{mismatches}"
            ),
        }

    def restore_snapshot_to_compute(self, snapshot_id: int) -> dict[str, Any]:
        """从历史快照中恢复特征集版本，生成回填用的补片任务。"""
        snap = self._snapshot_row(snapshot_id)
        if snap is None:
            return {"ok": False, "message": f"快照 {snapshot_id} 不存在"}
        if snap.get("快照类型") == "复算快照":
            result = self.create_patch_task(snapshot_id)
            if not result.get("ok"):
                return result
            return {
                "ok": True,
                "restored_from": snapshot_id,
                "compute_id": result["patch_task_id"],
                "snapshot_id": snapshot_id,
                "input_version": result.get("input_version"),
                "message": "已从复算快照生成补片任务，执行时读取快照产物",
            }
        compute_rows = store.rows("feature_compute")
        next_id = max((int(r.get("id", 0)) for r in compute_rows), default=0) + 1
        compute_rows.append({
            "id": next_id,
            "status": "草稿",
            "pending": True,
            "abnormal": False,
            "任务编号": f"COMP-RST-{next_id:04d}",
            "关联特征": snap.get("特征集", ""),
            "数据切片": "restored_slice",
            "计算引擎": "spark",
            "输入分区": f"snapshot_{snapshot_id}",
            "输出分区": f"restored/output_{next_id}",
            "并行度": "4",
            "任务状态": "草稿",
        })
        return {"ok": True, "restored_from": snapshot_id, "compute_id": next_id, "message": "快照已恢复并创建回填计算任务"}

    def merge_snapshots(self, snapshot_ids: list[int]) -> dict[str, Any]:
        """将多个特征快照归并为一个综合快照，合并特征集并写入新版本。"""
        all_features = set()
        for sid in snapshot_ids:
            snap = store.find(self.MODULE, sid)
            if snap is None:
                continue
            feats = snap.get("特征集", "")
            for f in feats.split(","):
                if f.strip():
                    all_features.add(f.strip())
        rows = store.rows(self.MODULE)
        next_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        merged_feats = ",".join(sorted(all_features))
        rows.append({
            "id": next_id,
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "快照编号": f"SNAP-MRG-{next_id:04d}",
            "特征集": merged_feats,
            "快照日期": "2026-11-18",
            "特征数量": len(all_features),
            "数据行数": str(len(all_features) * 8000),
            "存储路径": f"snapshots/merged_{next_id}.json",
            "保留天数": "60",
            "快照状态": "已完成",
        })
        return {"ok": True, "merged_id": next_id, "total_features": len(all_features), "sources": snapshot_ids}
