/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  // 后端会在行内同步 connectivity / eligibility / source_selection 等嵌套回执
  [field: string]: unknown
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
  entry?: EntryRow | null
  detail?: Record<string, unknown> | null
  field_errors?: Record<string, string> | string[] | null
}

export type SourceEligibility = {
  eligible: boolean
  confirmed?: boolean
  stale_enabled?: boolean
  reasons?: string[]
  abnormal_fields?: string[]
  field_errors?: Record<string, string>
  next_eligibility?: { eligible: boolean; reason?: string; required_action?: string }
}

export type AvailableSourcesResult = {
  available: EntryRow[]
  sources?: EntryRow[]
  unavailable: Array<{
    id: number
    源标识?: string
    status?: string
    reason: string
    abnormal_fields?: string[]
    stale_enabled?: boolean
  }>
  empty: boolean
  empty_reason: string | null
  空态原因?: string | null
  reason?: string | null
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}
