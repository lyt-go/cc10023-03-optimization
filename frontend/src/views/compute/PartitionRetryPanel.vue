<!-- 特征计算任务的分区视图：勾选失败分区一次提交，看补算/复用回执。 -->
<template>
  <article class="partition-panel" data-testid="partition-panel">
    <header class="partition-head">
      <div>
        <h3>分区重试与复算</h3>
        <p class="page-desc">
          按失败分区和原输入版本定位缺口；已确认分区复用，未完成分区补算。
        </p>
      </div>
      <div class="page-actions">
        <input
          v-model="taskIdText"
          class="task-input"
          data-testid="partition-task-id"
          placeholder="计算任务 ID"
        />
        <button class="btn" data-testid="load-demo-scenario" type="button" @click="loadDemo">
          载入值班场景
        </button>
        <button class="btn" data-testid="load-partition-run" type="button" @click="loadRun">
          载入分区台账
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="partition-error">{{ errorMessage }}</p>

    <div v-if="run" class="partition-meta" data-testid="partition-meta">
      <span>计算定义：{{ run.definition }}</span>
      <span>输入版本：{{ run.input_version }}</span>
      <span>业务日期：{{ run.date }}</span>
      <span>已确认：{{ run.confirmed_partitions.join('、') || '无' }}</span>
      <span>未完成：{{ run.pending_partitions.join('、') || '无' }}</span>
    </div>

    <table v-if="run" class="data-table">
      <thead>
        <tr>
          <th>勾选</th>
          <th>分区</th>
          <th>分区状态</th>
          <th>行数</th>
          <th>输入版本</th>
          <th>产物路径</th>
          <th>可读</th>
          <th>回执（计/重）</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in run.partitions_detail" :key="item.partition"
            :data-testid="`partition-row-${item.partition}`">
          <td>
            <input
              type="checkbox"
              :data-testid="`partition-select-${item.partition}`"
              :checked="selected.includes(item.partition)"
              @change="toggle(item.partition)"
            />
          </td>
          <td>{{ item.partition }}</td>
          <td>{{ item.status }}</td>
          <td>{{ item.rows ?? '—' }}</td>
          <td>{{ item.input_version }}</td>
          <td class="path-cell">{{ item.artifact_path ?? '—' }}</td>
          <td>{{ item.读产物 ? '是' : '否' }}</td>
          <td>{{ item.accepted_count }} / {{ item.duplicate_count }}</td>
        </tr>
      </tbody>
    </table>

    <div v-if="run" class="page-actions">
      <button class="btn primary" data-testid="submit-retry" type="button" :disabled="retrying"
              @click="submit">
        {{ retrying ? '提交中…' : `批量重试已选 ${selected.length} 个分区` }}
      </button>
    </div>

    <div v-if="retry" class="retry-receipt" data-testid="retry-receipt">
      <h4>受理回执 <small>{{ retry.receipt_id }}</small></h4>
      <p>{{ retry.message }}</p>
      <table class="data-table">
        <thead>
          <tr><th>来源任务</th><th>分区</th><th>受理</th><th>新任务</th><th>行数</th><th>输入版本</th><th>说明</th></tr>
        </thead>
        <tbody>
          <tr v-for="(entry, index) in retry.entries" :key="`${entry.partition}-${index}`"
              :data-testid="`retry-entry-${entry.partition}`" :data-decision="entry.decision">
            <td>{{ entry.source_task_id }}</td>
            <td>{{ entry.partition ?? '—' }}</td>
            <td>{{ entry.decision }}</td>
            <td>{{ entry.new_task_id ?? '—' }}</td>
            <td>{{ entry.rows ?? '—' }}</td>
            <td>{{ entry.input_version ?? '—' }}</td>
            <td>{{ entry.reason ?? '' }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </article>
</template>

<script setup lang="ts">
import { ref } from 'vue'

import { campaign, fetchPartitionRun, registerDemoScenario, submitRetry } from '@/api/compute-ops'
import type { PartitionRun, RetryResult } from '@/api/compute-ops'

const taskIdText = ref('')
const run = ref<PartitionRun | null>(null)
const selected = ref<string[]>([])
const retry = ref<RetryResult | null>(null)
const retrying = ref(false)
const errorMessage = ref('')

function toggle(partition: string) {
  const current = new Set(selected.value)
  if (current.has(partition)) current.delete(partition)
  else current.add(partition)
  selected.value = [...current]
}

async function loadDemo() {
  errorMessage.value = ''
  try {
    const scenario = await registerDemoScenario()
    taskIdText.value = String(scenario.v1TaskId)
    campaign.definition = scenario.definition
    campaign.v1TaskId = scenario.v1TaskId
    campaign.v2TaskId = scenario.v2TaskId
    await loadRun()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '值班场景登记失败'
  }
}

async function loadRun() {
  errorMessage.value = ''
  retry.value = null
  await refreshRun()
}

async function refreshRun() {
  const taskId = Number(taskIdText.value)
  if (!taskId) {
    errorMessage.value = '请输入计算任务 ID'
    return
  }
  try {
    const payload = await fetchPartitionRun(taskId)
    if (!payload.ok) {
      errorMessage.value = `任务 ${taskId} 没有分区运行登记`
      run.value = null
      return
    }
    run.value = payload
    selected.value = payload.failed_partitions.length
      ? [...payload.failed_partitions]
      : [...payload.pending_partitions]
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分区台账读取失败'
  }
}

async function submit() {
  if (!run.value) return
  retrying.value = true
  errorMessage.value = ''
  try {
    retry.value = await submitRetry([
      { task_id: run.value.task_id, partitions: selected.value },
    ])
    // 受理回执要留在页面上，只刷新分区台账，不重置回执。
    await refreshRun()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重试提交失败'
  } finally {
    retrying.value = false
  }
}
</script>
