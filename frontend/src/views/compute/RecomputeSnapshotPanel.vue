<!-- 复算快照视图：按版本展示确切行数、日期与可读产物，并从快照生成补片任务。 -->
<template>
  <article class="partition-panel" data-testid="recompute-panel">
    <header class="partition-head">
      <div>
        <h3>复算快照与补片</h3>
        <p class="page-desc">
          快照只统计已确认产物；未完成日期可继续补片。补片任务在创建后才读取快照产物。
        </p>
      </div>
      <div class="page-actions">
        <input
          v-model="taskIdText"
          class="task-input"
          data-testid="recompute-task-id"
          placeholder="来源计算任务 ID"
        />
        <button class="btn" data-testid="create-recompute" type="button" @click="generate">
          按任务生成复算快照
        </button>
        <select v-model="versionText" class="task-input" data-testid="recompute-version">
          <option value="v1">v1</option>
          <option value="v2">v2</option>
        </select>
        <button class="btn" data-testid="create-recompute-by-version" type="button"
                @click="generateByVersion">
          按版本生成复算快照
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="recompute-error">{{ errorMessage }}</p>

    <table v-if="snapshots.length" class="data-table">
      <thead>
        <tr><th>快照</th><th>计算定义</th><th>输入版本</th><th>日期</th><th>行数</th><th>已确认/未完成分区</th><th>操作</th></tr>
      </thead>
      <tbody>
        <tr v-for="item in snapshots" :key="item.snapshot_id"
            :data-testid="`snapshot-row-${item.snapshot_id}`">
          <td>{{ item.snapshot_code }}</td>
          <td>{{ item.definition }}</td>
          <td>{{ item.input_version }}</td>
          <td>{{ item.date }}</td>
          <td data-testid="snapshot-rows">{{ item.rows }}</td>
          <td>
            {{ item.confirmed_partitions.join('、') || '无' }}
            / {{ item.pending_partitions.join('、') || '无' }}
          </td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`select-snapshot-${item.snapshot_id}`"
                    @click="select(item.snapshot_id)">查看</button>
          </td>
        </tr>
      </tbody>
    </table>

    <section v-if="detail" class="snapshot-detail" data-testid="snapshot-detail">
      <h4>
        {{ detail.snapshot_code }} · {{ detail.definition }} · {{ detail.date }} ·
        输入版本 {{ detail.input_version }} · {{ detail.rows }} 行
      </h4>
      <table class="data-table">
        <thead>
          <tr><th>分区</th><th>快照行数</th><th>当前可读行数</th><th>日期</th><th>版本</th><th>产物路径</th><th>可读</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in detail.artifacts" :key="item.partition"
              :data-testid="`snapshot-artifact-${item.partition}`">
            <td>{{ item.partition }}</td>
            <td>{{ item.snapshot_rows }}</td>
            <td>{{ item.live_rows }}</td>
            <td>{{ item.date }}</td>
            <td>{{ item.input_version }}</td>
            <td class="path-cell">{{ item.artifact_path }}</td>
            <td>{{ item.readable ? '是' : '否' }}</td>
          </tr>
          <tr v-if="!detail.artifacts.length">
            <td colspan="7" class="empty-state">该快照版本还没有已确认产物，可继续补片</td>
          </tr>
        </tbody>
      </table>
      <p class="page-desc">
        未完成分区：{{ detail.pending_partitions.join('、') || '无' }} ·
        回执计数：{{ detail.receipts?.accepted ?? 0 }} 条，重复 {{ detail.receipts?.duplicates ?? 0 }} 条
      </p>

      <div class="page-actions">
        <button class="btn" data-testid="create-patch-task" type="button" @click="createPatch">
          生成补片任务
        </button>
      </div>

      <p v-if="patch" data-testid="patch-task-message">{{ patch.message }}</p>
      <button v-if="patch" class="btn primary" data-testid="execute-patch-task" type="button"
              @click="executePatch">
        执行补片并读取产物
      </button>

      <div v-if="!patch && campaign.v1TaskId" class="page-actions">
        <button class="btn" data-testid="late-v2-receipt" type="button" @click="lateReceipt">
          送达 v2 的 B 回执（晚到）
        </button>
        <button class="btn" data-testid="duplicate-v1-receipt" type="button" @click="duplicateReceipt">
          重送 v1 的 A 回执
        </button>
      </div>
      <p v-if="receiptNote" data-testid="receipt-note">{{ receiptNote }}</p>

      <div v-if="execution" class="retry-receipt" data-testid="patch-execution">
        <p>{{ execution.message }}</p>
        <table class="data-table">
          <thead>
            <tr><th>分区</th><th>读取行数</th><th>快照行数</th><th>版本</th><th>行数一致</th><th>校验和一致</th><th>产物路径</th></tr>
          </thead>
          <tbody>
            <tr v-for="read in execution.reads" :key="read.partition"
                :data-testid="`patch-read-${read.partition}`">
              <td>{{ read.partition }}</td>
              <td>{{ read.rows }}</td>
              <td>{{ read.snapshot_rows }}</td>
              <td>{{ read.input_version }}</td>
              <td>{{ read.row_match ? '是' : '否' }}</td>
              <td>{{ read.checksum_match ? '是' : '否' }}</td>
              <td class="path-cell">{{ read.artifact_path }}</td>
            </tr>
          </tbody>
        </table>
        <p v-if="execution.mismatches.length" class="error-text">
          核对未通过：{{ execution.mismatches }}
        </p>
      </div>
    </section>
  </article>
</template>

<script setup lang="ts">
import { ref } from 'vue'

import {
  campaign,
  createPatchTask,
  createRecomputeSnapshot,
  createRecomputeSnapshotFor,
  deliverReceipt,
  executePatchTask,
  fetchRecomputeDetail,
} from '@/api/compute-ops'
import type { PatchExecution, PatchTask, RecomputeDetail } from '@/api/compute-ops'

const taskIdText = ref('')
const versionText = ref('v2')
const snapshots = ref<RecomputeDetail[]>([])
const detail = ref<RecomputeDetail | null>(null)
const patch = ref<PatchTask | null>(null)
const execution = ref<PatchExecution | null>(null)
const receiptNote = ref('')
const errorMessage = ref('')

function describeError(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback
}

async function generate() {
  errorMessage.value = ''
  const taskId = Number(taskIdText.value)
  if (!taskId) {
    errorMessage.value = '请输入来源计算任务 ID'
    return
  }
  try {
    const created = await createRecomputeSnapshot(taskId)
    if (!created.ok) {
      errorMessage.value = (created as unknown as { message?: string }).message ?? '生成失败'
      return
    }
    snapshots.value = [created, ...snapshots.value]
    await select(created.snapshot_id)
  } catch (error) {
    errorMessage.value = describeError(error, '复算快照生成失败')
  }
}

async function generateByVersion() {
  errorMessage.value = ''
  if (!campaign.definition) {
    errorMessage.value = '请先在计算任务页载入值班场景'
    return
  }
  try {
    const created = await createRecomputeSnapshotFor(
      campaign.definition, '2026-11-19', versionText.value)
    snapshots.value = [created, ...snapshots.value]
    await select(created.snapshot_id)
  } catch (error) {
    errorMessage.value = describeError(error, '按版本生成快照失败')
  }
}

async function lateReceipt() {
  if (!campaign.v2TaskId) return
  receiptNote.value = ''
  try {
    const outcome = await deliverReceipt({
      taskId: campaign.v2TaskId,
      partition: 'B',
      rows: 9,
      receiptId: `R-v2-B-${campaign.v2TaskId}`,
      inputVersion: 'v2',
    })
    receiptNote.value = (
      `v2 的 B 回执：计入=${outcome.counted}，晚到=${outcome.late}，归属=${outcome.attributed_version}`
    )
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执送达失败'
  }
}

async function duplicateReceipt() {
  if (!campaign.v1TaskId) return
  receiptNote.value = ''
  try {
    const outcome = await deliverReceipt({
      taskId: campaign.v1TaskId,
      partition: 'A',
      rows: 10,
      receiptId: `R-v1-A-${campaign.v1TaskId}`,
    })
    receiptNote.value = (
      `v1 的 A 重复回执：计入=${outcome.counted}，重复=${outcome.duplicate}，行数=${outcome.rows}`
    )
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执重送失败'
  }
}

async function select(snapshotId: number) {
  errorMessage.value = ''
  receiptNote.value = ''
  patch.value = null
  execution.value = null
  try {
    detail.value = await fetchRecomputeDetail(snapshotId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '快照明细读取失败'
  }
}

async function createPatch() {
  if (!detail.value) return
  errorMessage.value = ''
  receiptNote.value = ''
  try {
    patch.value = await createPatchTask(detail.value.snapshot_id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补片任务创建失败'
  }
}

async function executePatch() {
  if (!patch.value) return
  errorMessage.value = ''
  try {
    execution.value = await executePatchTask(patch.value.patch_task_id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补片任务执行失败'
  }
}
</script>
