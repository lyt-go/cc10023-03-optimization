"""分区级重试、回执归版与复算快照补片的端到端场景测试。

对应值班场景：原输入 v1 的 A 片已产出 10 行、B 片失败；同日 v2 还有一份
B 在写；事后才收到 v2 的 B 回执与 v1 的 A 重复回执。补片任务必须读到
v1 快照确认过的产物，不能被后到的回执改写。
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.artifact_store import artifact_store
from app.partition_ledger import partition_ledger
from app.services import service_for

DEFINITION = "FEAT-PART-DEMO"
DATE = "2026-11-19"


def setup_function():
    artifact_store.reset()
    partition_ledger.reset()
    # 清掉本场景创建的任务行，避免重复执行时 id 冲突。
    rows = [row for row in __import__("app.store", fromlist=["store"]).store.rows(
        "feature_compute") if row.get("计算定义") == DEFINITION]
    from app.store import store
    for row in rows:
        store.rows("feature_compute").remove(row)
    snapshots = [row for row in store.rows("feature_snapshot")
                 if row.get("计算定义") == DEFINITION]
    for row in snapshots:
        store.rows("feature_snapshot").remove(row)


def _register(task_id, version, partitions=("A", "B"), completed=None, failed=None):
    compute = service_for("feature_compute")
    return compute.register_partition_run(
        task_id,
        definition=DEFINITION,
        input_version=version,
        date=DATE,
        partitions=list(partitions),
        completed=completed,
        failed=failed,
        task_row={
            "任务编号": f"DEMO-{task_id}",
            "关联特征": DEFINITION,
            "数据切片": DEFINITION,
            "计算引擎": "spark",
        },
    )


def test_retry_only_recomputes_gaps_and_reuses_confirmed_partitions():
    """A 片已完成 10 行，只补 B；受理回执逐条列出复用/补算。"""
    compute = service_for("feature_compute")
    _register(101, "v1", completed=[{"partition": "A", "rows": 10}], failed=[
        {"partition": "B", "reason": "executor 超时"}])

    retry = compute.batch_retry([{"task_id": 101, "partitions": ["A", "B"]}])
    entries = retry["entries"]
    assert len(entries) == 2, entries
    by_partition = {entry["partition"]: entry for entry in entries}
    assert by_partition["A"]["decision"] == "复用"
    assert by_partition["A"]["rows"] == 10
    assert by_partition["A"]["artifact_readable"] is True
    assert by_partition["B"]["decision"] == "补算"
    assert by_partition["B"]["new_task_id"] > 101
    assert retry["复用"] == 1 and retry["补算"] == 1

    # 原任务不再被打回草稿；A 的产物也没有被清空。
    task = compute.get_entry(101)
    assert task["输入版本"] == "v1"
    assert task["已完成分区"] == "A"
    assert task["失败分区"] == "B"
    artifact_path = by_partition["A"]["artifact_path"]
    assert artifact_store.describe(artifact_path)["rows"] == 10

    # 补算任务只包含 B，且沿用 v1。
    retry_task = compute.get_entry(by_partition["B"]["new_task_id"])
    assert retry_task["分区清单"] == "B"
    assert retry_task["输入版本"] == "v1"
    assert retry_task["重试来源"] == 101


def test_receipts_dedupe_and_late_receipts_stay_in_their_version():
    """v1 的 A 重复回执只计一次；v2 的 B 晚到回执仍归到 v2。"""
    compute = service_for("feature_compute")
    _register(101, "v1", completed=[{"partition": "A", "rows": 10}],
              failed=[{"partition": "B", "reason": "executor 超时"}])
    _register(102, "v2")

    retry = compute.batch_retry([{"task_id": 101}])
    patch_task_id = next(e["new_task_id"] for e in retry["entries"]
                         if e["partition"] == "B")

    # v1 的 B 在新补算任务上完成。
    compute.receive_partition_receipt(
        patch_task_id, "B", rows=7, receipt_id="R-v1-B")

    # v2 的 B 随后完成：晚到，但归属 v2。
    late = compute.receive_partition_receipt(
        102, "B", rows=9, receipt_id="R-v2-B", stated_version="v2")
    assert late["counted"] is True
    assert late["late"] is True
    assert late["attributed_version"] == "v2"
    assert late["artifact_path"] != artifact_store.paths()[0] or "v2" in late["artifact_path"]

    # v1 的 A 重复回执：不再计数，也不改 A 的行数。
    dup = compute.receive_partition_receipt(101, "A", rows=10, receipt_id="R-v1-A-dup")
    assert dup["duplicate"] is True and dup["counted"] is False
    repeat = compute.receive_partition_receipt(101, "A", rows=10, receipt_id="R-v1-A-dup")
    assert repeat["duplicate"] is True and repeat["counted"] is False

    # 同一回执身份重复送达也只计数一次。
    again = compute.receive_partition_receipt(
        patch_task_id, "B", rows=7, receipt_id="R-v1-B")
    assert again["duplicate"] is True and again["counted"] is False

    run = compute.describe_partition_run(101)
    a_state = next(p for p in run["partitions_detail"] if p["partition"] == "A")
    assert a_state["rows"] == 10
    assert a_state["accepted_count"] == 1
    assert a_state["duplicate_count"] == 2


def test_recompute_snapshot_reports_exact_rows_and_patch_reads_snapshot_artifacts():
    """快照只统计已确认产物；补片任务在回执到达后才读取，读到的仍是 v1。"""
    compute = service_for("feature_compute")
    snapshot = service_for("feature_snapshot")

    _register(101, "v1", completed=[{"partition": "A", "rows": 10}],
              failed=[{"partition": "B", "reason": "executor 超时"}])
    _register(102, "v2", completed=[{"partition": "A", "rows": 12}])

    # 先让 v1 的 B 完成。
    retry = compute.batch_retry([{"task_id": 101}])
    v1_b_task = next(e["new_task_id"] for e in retry["entries"] if e["partition"] == "B")
    compute.receive_partition_receipt(v1_b_task, "B", rows=7, receipt_id="R-v1-B")

    # 按 v1 的复算快照发起补片。
    snap = snapshot.generate_recompute_snapshot(101)
    assert snap["input_version"] == "v1"
    assert snap["rows"] == 17
    assert [p["partition"] for p in snap["partitions"]] == ["A", "B"]
    detail = snapshot.describe_recompute_snapshot(snap["snapshot_id"])
    assert detail["rows"] == 17
    assert detail["date"] == DATE
    assert {item["partition"]: item["live_rows"] for item in detail["artifacts"]} == {
        "A": 10, "B": 7}
    assert all(item["readable"] for item in detail["artifacts"])

    patch = snapshot.create_patch_task(snap["snapshot_id"])
    assert patch["read_partitions"] == ["A", "B"]
    # 创建补片任务时还没有读取任何产物。
    assert artifact_store.read_count == 0

    # 回执这时才到：v2 的 B，以及 v1 的 A 重复回执。
    late = compute.receive_partition_receipt(
        102, "B", rows=9, receipt_id="R-v2-B", stated_version="v2")
    assert late["attributed_version"] == "v2" and late["late"] is True
    compute.receive_partition_receipt(101, "A", rows=10, receipt_id="R-v1-A-dup")

    # 补片任务此刻才真正读取快照产物。
    execution = snapshot.execute_patch_task(patch["patch_task_id"])
    assert execution["ok"], execution
    assert execution["partition_rows"] == {"A": 10, "B": 7}
    assert execution["total_rows"] == 17
    assert all(item["input_version"] == "v1" for item in execution["reads"])
    assert all(item["row_match"] and item["checksum_match"] for item in execution["reads"])
    assert artifact_store.read_count == 2

    # v1 的 A 仍只有 10 行，没有被晚到回执或 v2 运行改写。
    a_state = next(p for p in compute.describe_partition_run(101)["partitions_detail"]
                   if p["partition"] == "A")
    assert a_state["rows"] == 10
    assert a_state["读产物"] is True

    # 从 v2 快照再跑一次恢复，其 B 必须来自 v2。
    v2_snap = snapshot.generate_recompute_snapshot(102)
    assert v2_snap["input_version"] == "v2"
    v2_detail = snapshot.describe_recompute_snapshot(v2_snap["snapshot_id"])
    v2_b = next(item for item in v2_detail["artifacts"] if item["partition"] == "B")
    assert v2_b["live_rows"] == 9
    assert v2_b["input_version"] == "v2"
    v2_patch = snapshot.create_patch_task(v2_snap["snapshot_id"])
    v2_execution = snapshot.execute_patch_task(v2_patch["patch_task_id"])
    v2_b_read = next(item for item in v2_execution["reads"] if item["partition"] == "B")
    assert v2_b_read["rows"] == 9
    assert v2_b_read["input_version"] == "v2"
    assert v2_b_read["artifact_path"] != execution["reads"][1]["artifact_path"]


def test_snapshot_with_unfinished_date_can_continue_patching():
    """未完成的日期仍在快照里列出缺口，可以继续补片。"""
    compute = service_for("feature_compute")
    snapshot = service_for("feature_snapshot")
    _register(101, "v1", completed=[{"partition": "A", "rows": 10}],
              failed=[{"partition": "B", "reason": "executor 超时"}])

    snap = snapshot.generate_recompute_snapshot(101)
    assert snap["rows"] == 10
    assert snap["pending_partitions"] == ["B"]

    patch = snapshot.create_patch_task(snap["snapshot_id"], partitions=["A"])
    assert patch["read_partitions"] == ["A"]
    assert patch["pending_partitions"] == []
    execution = snapshot.execute_patch_task(patch["patch_task_id"])
    assert execution["partition_rows"] == {"A": 10}

    # B 补齐后重新生成快照，新快照只统计已确认产物，再补 B 时读到 6 行。
    retry = compute.batch_retry([{"task_id": 101, "partitions": ["B"]}])
    compute.receive_partition_receipt(
        retry["entries"][0]["new_task_id"], "B", rows=6, receipt_id="R-v1-B-2")
    refreshed = snapshot.generate_recompute_snapshot(101)
    assert refreshed["rows"] == 16
    assert refreshed["confirmed_count"] == 2
    assert refreshed["pending_partitions"] == []
    refill = snapshot.create_patch_task(refreshed["snapshot_id"], partitions=["B"])
    refill_execution = snapshot.execute_patch_task(refill["patch_task_id"])
    assert refill_execution["partition_rows"] == {"B": 6}
    assert refill_execution["pending_partitions"] == []


def test_duplicate_receipt_only_counts_on_its_own_run():
    """同一回执身份只计到首次确认它的那次运行，重发不改变归属。"""
    compute = service_for("feature_compute")
    _register(101, "v1", completed=[{"partition": "A", "rows": 10}],
              failed=[{"partition": "B", "reason": "x"}])
    _register(102, "v2")

    before = compute.describe_partition_run(101)
    a_before = next(p for p in before["partitions_detail"] if p["partition"] == "A")
    assert a_before["accepted_count"] == 1

    # v2 任务把 v1 的 A 回执重发一遍：不计数，归属仍是 v1。
    again = compute.receive_partition_receipt(
        102, "A", rows=10, receipt_id="seed-101-A", stated_version="v1")
    assert again["duplicate"] is True and again["counted"] is False
    assert again["attributed_version"] == "v1"

    run_101 = compute.describe_partition_run(101)
    a_after = next(p for p in run_101["partitions_detail"] if p["partition"] == "A")
    assert a_after["accepted_count"] == 1
    assert a_after["duplicate_count"] == 1
    run_102 = compute.describe_partition_run(102)
    a_v2 = next(p for p in run_102["partitions_detail"] if p["partition"] == "A")
    assert a_v2["accepted_count"] == 0
    assert a_v2["duplicate_count"] == 0


if __name__ == "__main__":
    tests = [value for name, value in sorted(globals().items())
             if name.startswith("test_") and callable(value)]
    failed = 0
    for test in tests:
        setup_function()
        try:
            test()
            print(f"[PASS] {test.__name__}")
        except Exception as error:  # noqa: BLE001
            failed += 1
            print(f"[FAIL] {test.__name__}: {error}")
    print(f"\n总测试: {len(tests)}, 通过: {len(tests) - failed}, 失败: {failed}")
    sys.exit(1 if failed else 0)
