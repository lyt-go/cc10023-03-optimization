/** 分区级重试、回执与复算快照补片的接口封装。 */
import { request } from './client'

export type PartitionState = {
  partition: string
  status: string
  rows: number | null
  input_version: string
  date: string
  artifact_path: string | null
  读产物: boolean
  accepted_count: number
  duplicate_count: number
  reason?: string
}

export type PartitionRun = {
  ok: boolean
  task_id: number
  definition: string
  input_version: string
  date: string
  partitions: string[]
  failed_partitions: string[]
  confirmed_partitions: string[]
  pending_partitions: string[]
  partitions_detail: PartitionState[]
}

export type RetryEntry = {
  source_task_id: number
  partition: string | null
  decision: '补算' | '复用' | '不受理'
  input_version?: string
  date?: string
  rows?: number | null
  artifact_path?: string | null
  artifact_readable?: boolean
  new_task_id?: number
  reason?: string
}

export type RetryResult = {
  ok: boolean
  submitted: number
  receipt_id: string
  entries: RetryEntry[]
  补算: number
  复用: number
  message?: string
}

export type SnapshotArtifact = {
  partition: string
  artifact_path: string
  snapshot_rows: number
  live_rows: number
  readable: boolean
  input_version: string
  date: string
  checksum?: string
  receipt_id?: string
  sample?: Record<string, unknown>[]
}

export type RecomputeDetail = {
  ok: boolean
  snapshot_id: number
  snapshot_code: string
  definition: string
  input_version: string
  date: string
  rows: number
  confirmed_partitions: string[]
  pending_partitions: string[]
  artifacts: SnapshotArtifact[]
  readable_artifacts: SnapshotArtifact[]
  status: string
  storage_path: string
  receipts?: { accepted: number; duplicates: number; delivered: number }
}

export type PatchTask = {
  ok: boolean
  patch_task_id: number
  snapshot_id: number
  snapshot_code: string
  input_version: string
  date: string
  read_partitions: string[]
  pending_partitions: string[]
  message?: string
}

export type PatchExecution = {
  ok: boolean
  patch_task_id: number
  snapshot_id: number
  input_version: string
  date: string
  reads: {
    partition: string
    artifact_path: string
    input_version: string
    rows: number
    snapshot_rows: number
    row_match: boolean
    checksum_match: boolean
    sample?: Record<string, unknown>[]
  }[]
  partition_rows: Record<string, number>
  total_rows: number
  mismatches: Record<string, unknown>[]
  pending_partitions: string[]
  task_status: string
  message?: string
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  const payload = (await response.json()) as T & { detail?: string }
  if (!response.ok) {
    throw new Error(payload.detail ?? `接口返回 ${response.status}`)
  }
  return payload
}

export async function fetchPartitionRun(taskId: number): Promise<PartitionRun> {
  const response = await request(`/api/feature_compute/${taskId}/partitions`)
  if (!response.ok) throw new Error('分区台账读取失败')
  return (await response.json()) as PartitionRun
}

export async function submitRetry(
  items: { task_id: number; partitions: string[] }[],
): Promise<RetryResult> {
  return await postJson<RetryResult>('/api/feature_compute/retry', { items })
}

export async function fetchRecomputeDetail(snapshotId: number): Promise<RecomputeDetail> {
  const response = await request(`/api/feature_snapshot/${snapshotId}/recompute-detail`)
  if (!response.ok) throw new Error('复算快照明细读取失败')
  return (await response.json()) as RecomputeDetail
}

export async function createRecomputeSnapshot(taskId: number): Promise<RecomputeDetail> {
  return await postJson<RecomputeDetail>('/api/feature_snapshot/recompute', { task_id: taskId })
}

export async function createRecomputeSnapshotFor(
  definition: string,
  date: string,
  inputVersion: string,
): Promise<RecomputeDetail> {
  return await postJson<RecomputeDetail>('/api/feature_snapshot/recompute', {
    definition,
    date,
    input_version: inputVersion,
  })
}

export type ReceiptOutcome = {
  ok: boolean
  receipt_id: string
  counted: boolean
  duplicate: boolean
  late: boolean
  attributed_version: string
  actual_run_version: string
  status: string
  rows: number | null
  artifact_path: string | null
  reason?: string
}

export async function deliverReceipt(input: {
  taskId: number
  partition: string
  rows: number
  receiptId: string
  inputVersion?: string
}): Promise<ReceiptOutcome> {
  return await postJson<ReceiptOutcome>(`/api/feature_compute/${input.taskId}/receipts`, {
    task_id: input.taskId,
    partition: input.partition,
    rows: input.rows,
    receipt_id: input.receiptId,
    input_version: input.inputVersion,
  })
}

export async function createPatchTask(snapshotId: number): Promise<PatchTask> {
  return await postJson<PatchTask>(`/api/feature_snapshot/${snapshotId}/patch-task`, {})
}

export async function executePatchTask(patchTaskId: number): Promise<PatchExecution> {
  return await postJson<PatchExecution>(
    `/api/feature_compute/${patchTaskId}/patch-execute`,
    {},
  )
}

export type DemoScenario = {
  v1TaskId: number
  v2TaskId: number
  definition: string
  date: string
}

/** 面板之间共享当前场景，让快照面板能直接按 v1/v2 生成复算快照。 */
export const campaign: {
  definition: string
  v1TaskId: number | null
  v2TaskId: number | null
} = { definition: 'FEAT-CHURN', v1TaskId: null, v2TaskId: null }

/** 登记值班场景：v1 的 A 已产出 10 行、B 失败；同日 v2 的 A/B 都在写。 */
export async function registerDemoScenario(): Promise<DemoScenario> {
  const listResponse = await request('/api/feature_compute?page=1&size=200')
  if (!listResponse.ok) throw new Error('计算任务列表读取失败')
  const listing = (await listResponse.json()) as { items: { id: number }[] }
  const baseId = listing.items.reduce((max, item) => Math.max(max, Number(item.id)), 0)
  const v1TaskId = baseId + 1
  const v2TaskId = baseId + 2
  const definition = 'FEAT-CHURN'
  const date = '2026-11-19'
  await postJson('/api/feature_compute/runs', {
    task_id: v1TaskId,
    definition,
    input_version: 'v1',
    date,
    partitions: ['A', 'B'],
    completed: [{ partition: 'A', rows: 10, receipt_id: `R-v1-A-${v1TaskId}` }],
    failed: [{ partition: 'B', reason: 'executor 超时' }],
    task_row: {
      任务编号: `COMP-DEMO-${v1TaskId}`,
      关联特征: definition,
      数据切片: definition,
      计算引擎: 'spark',
    },
  })
  await postJson('/api/feature_compute/runs', {
    task_id: v2TaskId,
    definition,
    input_version: 'v2',
    date,
    partitions: ['A', 'B'],
    task_row: {
      任务编号: `COMP-DEMO-${v2TaskId}`,
      关联特征: definition,
      数据切片: definition,
      计算引擎: 'spark',
    },
  })
  return { v1TaskId, v2TaskId, definition, date }
}
