<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <!-- batch_slice：上游流式源可用状态 / 空态原因 -->
    <div
      v-if="meta.key === 'batch_slice'"
      class="source-panel"
      data-testid="batch-source-panel"
    >
      <template v-if="sourcePanel && !sourcePanel.empty">
        <span class="source-panel-title">可用于分片的已确认流式源：</span>
        <span
          v-for="source in sourcePanel.available"
          :key="Number(source.id)"
          class="source-chip"
          :data-testid="`available-source-${source.id}`"
        >
          {{ String(source['源标识'] ?? source.id) }}（定义 v{{ Number(source.version ?? source.definition_version ?? 1) }}）
        </span>
      </template>
      <p
        v-else-if="sourcePanel?.empty"
        class="source-empty"
        data-testid="source-empty-state"
      >
        {{ sourcePanel.empty_reason || sourcePanel.reason || '当前没有已校验确认的可用流式源' }}
      </p>
      <p v-else class="source-panel-hint">正在读取上游已确认定义…</p>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ formatCell(row[column]) }}</td>
          <td class="state-cell">
            <div>{{ row.status }}</div>

            <!-- stream_source：确认定义 / 异常字段 / 检测结果同步展示 -->
            <div v-if="meta.key === 'stream_source'" class="row-hints">
              <span
                class="badge"
                :class="confirmationState(row).cls"
                :data-testid="`confirm-badge-${row.id}`"
              >
                {{ confirmationState(row).label }}
              </span>
              <span
                v-if="abnormalFields(row).length"
                class="field-tags"
                :data-testid="`abnormal-fields-${row.id}`"
              >
                异常字段：
                <em v-for="field in abnormalFields(row)" :key="field" class="field-tag">{{ field }}</em>
              </span>
              <small
                v-if="typeof row['检测结果'] === 'string'"
                class="detect-result"
                :data-testid="`detect-result-${row.id}`"
              >
                {{ row['检测结果'] }}
              </small>
            </div>

            <!-- batch_slice：已打开的定义版本与暂缓裁决 -->
            <div v-else-if="meta.key === 'batch_slice' && selectionOf(row)" class="row-hints">
              <span class="badge badge-ok" :data-testid="`selection-version-${row.id}`">
                已绑定定义 v{{ Number(selectionOf(row)?.definition_version ?? 1) }}
                {{ selectionOf(row)?.confirmed ? '·已确认' : '·待确认' }}
              </span>
              <small
                v-if="resolutionOf(row)?.reason"
                class="detect-result"
                :data-testid="`selection-resolution-${row.id}`"
              >
                {{ String(resolutionOf(row)?.reason) }}
              </small>
            </div>
          </td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              v-for="extra in extraActions"
              :key="extra"
              class="link domain-action"
              type="button"
              :data-testid="`action-${row.id}-${extra}`"
              @click="onExtraAction(extra, row)"
            >
              {{ extra }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state" data-testid="table-empty-state">
            {{ emptyTableText }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" :data-testid="editingId ? 'edit-modal' : 'create-modal'">
      <form class="modal" @submit.prevent="submitForm">
        <h3>{{ editingId ? `更正${meta.entity} #${editingId}` : `登记${meta.entity}` }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}<em v-if="field === '并发度'" class="field-hint">（1-10 的整数，缺省 1）</em></span>
          <input
            v-model="createValues[field]"
            :data-testid="`create-${field}`"
            :placeholder="field === '并发度' ? '缺省 1，允许 1-10' : ''"
            required
          />
          <p
            v-if="createFieldErrors[field]"
            class="field-error"
            :data-testid="`field-error-${field}`"
          >
            {{ createFieldErrors[field] }}
          </p>
        </label>
        <p v-if="createFormError" class="field-error" data-testid="create-form-error">{{ createFormError }}</p>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button
            class="btn primary"
            :data-testid="editingId ? 'submit-edit' : 'submit-create'"
            type="submit"
            :disabled="creating"
          >
            {{ creating ? '提交中…' : editingId ? '确认更正' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="infoMessage" class="info-text" data-testid="action-info">{{ infoMessage }}</span>
      <span v-if="errorMessage" class="error-text" data-testid="action-error">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  availableBatchSources,
  createEntry,
  downloadEntries,
  listEntries,
  moduleMeta,
  runAction as applyAction,
  updateEntry,
  validateConcurrency,
} from '@/api/backend-service'
import type { AvailableSourcesResult, EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const infoMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const editingId = ref<number | null>(null)
const createValues = ref<Record<string, string>>({})
const createFieldErrors = ref<Record<string, string>>({})
const createFormError = ref('')
const sourcePanel = ref<AvailableSourcesResult | null>(null)

const extraActions = computed<string[]>(() => {
  if (props.moduleKey === 'stream_source') return ['连通性检测', '更正配置']
  if (props.moduleKey === 'batch_slice') return ['打开选源', '确认选源']
  return []
})

function onExtraAction(action: string, row: EntryRow) {
  if (action === '更正配置') {
    openEdit(row)
    return
  }
  void runAction(action, row)
}

function openEdit(row: EntryRow) {
  editingId.value = Number(row.id)
  createValues.value = Object.fromEntries(
    meta.fields.map((field) => [field, row[field] === null || row[field] === undefined ? '' : String(row[field])]),
  )
  createFieldErrors.value = {}
  createFormError.value = ''
  errorMessage.value = ''
  infoMessage.value = ''
  showCreate.value = true
}

const emptyTableText = computed(() => {
  if (props.moduleKey === 'batch_slice' && sourcePanel.value?.empty_reason) {
    return sourcePanel.value.empty_reason
  }
  return `暂无${meta.name}数据，可先登记${meta.entity}`
})

const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined || value === '') return '—'
  if (typeof value === 'object') return '—'
  return String(value)
}

function abnormalFields(row: EntryRow): string[] {
  const brief = asRecord(row.eligibility_brief)
  const raw = row.abnormal_fields ?? row['异常字段'] ?? brief?.abnormal_fields
  return Array.isArray(raw) ? raw.map((item) => String(item)) : []
}

function confirmationState(row: EntryRow): { label: string; cls: string } {
  const brief = asRecord(row.eligibility_brief)
  if (brief?.stale_enabled || (row.status === '已启用' && !row.verified_fingerprint)) {
    return { label: '旧启用标记·定义未确认', cls: 'badge-warn' }
  }
  if (row.verified_fingerprint && row.verified_fingerprint === row.definition_fingerprint) {
    return { label: `已校验确认 v${String(row.verified_version ?? row.definition_version ?? '')}`, cls: 'badge-ok' }
  }
  if (brief?.eligible) return { label: '已校验确认', cls: 'badge-ok' }
  return { label: '未完成校验', cls: 'badge-muted' }
}

function selectionOf(row: EntryRow): Record<string, unknown> | null {
  return asRecord(row.source_selection)
}

function resolutionOf(row: EntryRow): Record<string, unknown> | null {
  const selection = selectionOf(row)
  return asRecord(selection?.last_resolution) ?? asRecord(selection?.confirm_resolution)
}

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  infoMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  editingId.value = null
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  if (props.moduleKey === 'stream_source') {
    createValues.value['并发度'] = '1'
  }
  createFieldErrors.value = {}
  createFormError.value = ''
  errorMessage.value = ''
  infoMessage.value = ''
  showCreate.value = true
}

async function submitForm() {
  creating.value = true
  errorMessage.value = ''
  createFormError.value = ''
  createFieldErrors.value = {}
  try {
    // 流式源并发度先做字段级校验：非法值直接定位到字段并阻止提交/启用。
    // 若校验请求本身不可达，则放行给后端强校验（fail-open on transport）。
    if (props.moduleKey === 'stream_source') {
      const concurrency = (createValues.value['并发度'] ?? '').trim()
      try {
        const validation = await validateConcurrency(concurrency)
        if (!validation.ok) {
          createFieldErrors.value['并发度'] = validation.message
          creating.value = false
          return
        }
      } catch {
        // 交给后端校验
      }
    }

    if (editingId.value !== null) {
      const result = await updateEntry(meta.key, editingId.value, createValues.value)
      if (!result.ok) {
        const fieldErrors = asRecord(result.field_errors)
        if (fieldErrors) {
          createFieldErrors.value = Object.fromEntries(
            Object.entries(fieldErrors).map(([key, value]) => [key, String(value)]),
          )
        }
        createFormError.value = result.message
        return
      }
      showCreate.value = false
      infoMessage.value = result.message || '配置已更正，请重新完成校验'
      await reload()
      return
    }

    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) {
      const fieldErrors = asRecord(result.field_errors)
      if (fieldErrors) {
        createFieldErrors.value = Object.fromEntries(
          Object.entries(fieldErrors).map(([key, value]) => [key, String(value)]),
        )
      }
      // 草稿已落库（带 entry）：关闭弹窗，在清单里定位字段并允许更正；
      // 没有 entry 说明是必填缺失等未落地错误，保留弹窗直接修改后重试。
      if (result.entry?.id) {
        showCreate.value = false
        errorMessage.value = result.message
        await reload()
      } else {
        createFormError.value = result.message
      }
      return
    }
    showCreate.value = false
    infoMessage.value = result.message
    await reload()
  } catch (error) {
    createFormError.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  infoMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (result.ok) {
      infoMessage.value = result.message
    } else {
      errorMessage.value = result.message
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function loadSourcePanel() {
  if (props.moduleKey !== 'batch_slice') return
  try {
    sourcePanel.value = await availableBatchSources()
  } catch {
    sourcePanel.value = null
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
    await loadSourcePanel()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.source-panel {
  margin: 8px 0 12px;
  padding: 10px 14px;
  border: 1px solid var(--border-color, #e3e8ef);
  border-radius: 8px;
  background: #f7f9fc;
  font-size: 13px;
  line-height: 1.7;
}
.source-panel-title {
  color: #445;
  margin-right: 6px;
}
.source-chip {
  display: inline-block;
  margin: 2px 6px 2px 0;
  padding: 2px 10px;
  border-radius: 999px;
  background: #e6f4ea;
  color: #1e7e34;
  border: 1px solid #b7dfc1;
}
.source-empty {
  margin: 0;
  color: #b42318;
  background: #fef3f2;
  border: 1px solid #fecdca;
  border-radius: 6px;
  padding: 8px 10px;
}
.source-panel-hint {
  margin: 0;
  color: #778;
}
.state-cell {
  min-width: 180px;
}
.row-hints {
  display: flex;
  flex-direction: column;
  gap: 4px;
  margin-top: 4px;
}
.badge {
  display: inline-block;
  width: fit-content;
  padding: 1px 8px;
  border-radius: 999px;
  font-size: 12px;
  border: 1px solid transparent;
}
.badge-ok {
  background: #e6f4ea;
  color: #1e7e34;
  border-color: #b7dfc1;
}
.badge-warn {
  background: #fffaeb;
  color: #b54708;
  border-color: #fedf89;
}
.badge-muted {
  background: #f2f4f7;
  color: #667085;
  border-color: #e4e7ec;
}
.field-tags {
  font-size: 12px;
  color: #b42318;
}
.field-tag {
  font-style: normal;
  display: inline-block;
  margin: 0 4px;
  padding: 0 6px;
  border-radius: 4px;
  background: #fef3f2;
  border: 1px solid #fecdca;
}
.detect-result {
  color: #667085;
  font-size: 12px;
}
.domain-action {
  color: #175cd3;
}
.field-hint {
  color: #98a2b3;
  font-style: normal;
  font-size: 12px;
}
.field-error {
  margin: 4px 0 0;
  color: #b42318;
  font-size: 12px;
}
.info-text {
  color: #1e7e34;
  margin-right: 12px;
}
</style>
