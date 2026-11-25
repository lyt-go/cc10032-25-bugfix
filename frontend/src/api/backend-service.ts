import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  AvailableSourcesResult,
  EntryRow,
  OverviewResult,
  PageResult,
} from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function updateEntry(
  key: string,
  id: number,
  values: Record<string, unknown>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}`, {
    method: 'PATCH',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
  extra: Record<string, unknown> = {},
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action, ...extra }),
  })
  return await response.json()
}

export async function checkConnectivity(id: number): Promise<ActionResult & Record<string, unknown>> {
  const response = await request(`/api/stream_source/${id}/connectivity`, { method: 'POST' })
  return await response.json()
}

export async function validateConcurrency(value: string): Promise<ActionResult> {
  const response = await request('/api/stream_source/validate', {
    method: 'POST',
    body: JSON.stringify({ value }),
  })
  return await response.json()
}

export async function availableBatchSources(): Promise<AvailableSourcesResult> {
  const response = await request('/api/batch_slice/sources/available')
  if (!response.ok) throw new Error('可用流式源读取失败')
  return await response.json()
}

export async function openBatchSelection(
  batchId: number,
  sourceId?: number,
): Promise<ActionResult & Record<string, unknown>> {
  const response = await request(`/api/batch_slice/${batchId}/selection`, {
    method: 'POST',
    body: JSON.stringify(sourceId ? { source_id: sourceId } : {}),
  })
  return await response.json()
}

export async function confirmBatchSelection(
  batchId: number,
  options: { token?: string; execute?: boolean; conflictPolicy?: 'snapshot' | 'abort' } = {},
): Promise<ActionResult & Record<string, unknown>> {
  const response = await request(`/api/batch_slice/${batchId}/confirm`, {
    method: 'POST',
    body: JSON.stringify({
      token: options.token,
      execute: options.execute ?? false,
      conflict_policy: options.conflictPolicy ?? 'snapshot',
    }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}
