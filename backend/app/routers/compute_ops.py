"""计算分区重试与复算快照动作接口：把领域服务里的分区级能力暴露给工作台。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services import service_for

router = APIRouter(prefix="/api", tags=["分区重试与补片"])


class RunRegisterPayload(BaseModel):
    task_id: int
    definition: str | None = None
    input_version: str | None = None
    date: str | None = None
    partitions: list[str] = Field(default_factory=list)
    completed: list[dict[str, Any]] = Field(default_factory=list)
    failed: list[dict[str, Any]] = Field(default_factory=list)
    task_row: dict[str, Any] | None = None


class ReceiptPayload(BaseModel):
    task_id: int
    partition: str
    rows: int | str | None = None
    artifact_path: str | None = None
    payload: list[dict[str, Any]] | None = None
    columns: list[str] | None = None
    receipt_id: str | None = None
    input_version: str | None = None


class ReceiptBatchPayload(BaseModel):
    receipts: list[ReceiptPayload] = Field(default_factory=list)


class FailurePayload(BaseModel):
    partition: str
    reason: str | None = None


class RetryBatchPayload(BaseModel):
    task_ids: list[int] | None = None
    items: list[dict[str, Any]] | None = None


class RetryOnePayload(BaseModel):
    partitions: list[str] | None = None


class RecomputePayload(BaseModel):
    task_id: int | None = None
    definition: str | None = None
    date: str | None = None
    input_version: str | None = None


class PatchPayload(BaseModel):
    partitions: list[str] | None = None


class ExecutePatchPayload(BaseModel):
    patch_task_id: int | None = None


def _compute():
    service = service_for("feature_compute")
    if service is None:
        raise HTTPException(status_code=404, detail="特征计算任务服务不可用")
    return service


def _snapshot():
    service = service_for("feature_snapshot")
    if service is None:
        raise HTTPException(status_code=404, detail="特征快照服务不可用")
    return service


@router.post("/feature_compute/runs")
def register_partition_run(payload: RunRegisterPayload) -> dict[str, Any]:
    """登记一次实际运行：分区清单、输入版本、日期与初始完成/失败状态。"""
    values = payload.model_dump()
    return _compute().register_partition_run(values.pop("task_id"), **values)


@router.get("/feature_compute/{task_id}/partitions")
def list_partition_run(task_id: int) -> dict[str, Any]:
    return _compute().describe_partition_run(task_id)


@router.post("/feature_compute/{task_id}/partitions/fail")
def report_partition_failure(task_id: int, payload: FailurePayload) -> dict[str, Any]:
    return _compute().report_partition_failure(task_id, payload.partition, payload.reason)


@router.post("/feature_compute/{task_id}/receipts")
def receive_receipt(task_id: int, payload: ReceiptPayload) -> dict[str, Any]:
    """完成回执按任务实际登记的版本归属；重复回执只计一次。"""
    values = payload.model_dump(exclude={"task_id"})
    # 回执里带的版本只作对照，真正归属仍取任务运行登记。
    values["stated_version"] = values.pop("input_version")
    return _compute().receive_partition_receipt(task_id, **values)


@router.post("/feature_compute/receipts")
def receive_receipts(payload: ReceiptBatchPayload) -> dict[str, Any]:
    return _compute().receive_completion_receipts(
        [item.model_dump() for item in payload.receipts])


@router.post("/feature_compute/retry")
def batch_retry(payload: RetryBatchPayload) -> dict[str, Any]:
    """值班员勾选失败分区一次提交：逐条返回补算或复用。"""
    items: list[Any] = payload.items or payload.task_ids or []
    if not items:
        raise HTTPException(status_code=400, detail="请至少勾选一个任务或失败分区")
    result = _compute().batch_retry(items)
    message = f"受理 {result['submitted']} 项：补算 {result['补算']} 个分区，复用 {result['复用']} 个分区"
    return {**result, "message": message}


@router.post("/feature_compute/{task_id}/retry")
def retry_one(task_id: int, payload: RetryOnePayload) -> dict[str, Any]:
    result = _compute().batch_retry([{"task_id": task_id, "partitions": payload.partitions}])
    message = f"受理：补算 {result['补算']} 个分区，复用 {result['复用']} 个分区"
    return {**result, "message": message}


@router.post("/feature_compute/{task_id}/patch-execute")
def execute_patch(task_id: int, payload: ExecutePatchPayload | None = None) -> dict[str, Any]:
    return _snapshot().execute_patch_task(task_id)


@router.post("/feature_snapshot/recompute")
def create_recompute_snapshot(payload: RecomputePayload) -> dict[str, Any]:
    service = _snapshot()
    if payload.task_id is not None:
        return service.generate_recompute_snapshot(payload.task_id)
    if not payload.definition or not payload.date or not payload.input_version:
        raise HTTPException(
            status_code=400, detail="按版本生成快照需要 definition、date、input_version")
    return service.generate_recompute_snapshot_for(
        payload.definition, payload.date, payload.input_version)


@router.get("/feature_snapshot/{snapshot_id}/recompute-detail")
def recompute_detail(snapshot_id: int) -> dict[str, Any]:
    """恢复用的快照版本：确切行数、日期与每个分区的可读产物。"""
    return _snapshot().describe_recompute_snapshot(snapshot_id)


@router.post("/feature_snapshot/{snapshot_id}/patch-task")
def create_patch_task(snapshot_id: int, payload: PatchPayload | None = None) -> dict[str, Any]:
    partitions = payload.partitions if payload else None
    return _snapshot().create_patch_task(snapshot_id, partitions)


@router.post("/feature_snapshot/{snapshot_id}/restore")
def restore_snapshot(snapshot_id: int) -> dict[str, Any]:
    return _snapshot().restore_snapshot_to_compute(snapshot_id)
