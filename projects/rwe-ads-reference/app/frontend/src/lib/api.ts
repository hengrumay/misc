// Shared API client for the RWE ADS Studio backend.
// Every response carries a `source` field ("lakebase" | "warehouse" | "synthetic")
// so pages can render the SourcePill and never crash on a degraded read.

export type Source = 'lakebase' | 'warehouse' | 'synthetic'

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!res.ok) {
    let detail = res.statusText
    try {
      detail = (await res.json()).detail ?? detail
    } catch {
      /* ignore */
    }
    throw new Error(`${res.status}: ${detail}`)
  }
  return res.json() as Promise<T>
}

const get = <T>(path: string) => req<T>(path)
const post = <T>(path: string, body?: unknown) =>
  req<T>(path, { method: 'POST', body: body ? JSON.stringify(body) : undefined })

// ---- Protocols --------------------------------------------------------------
export interface ProtocolListItem {
  study_id: string
  title: string | null
  complexity: string | null
  review_status: string | null
  source_protocol: string | null
  // extraction-eval signal (present once run_protocol_eval has run; null otherwise)
  eval_ok?: boolean | null
  review_priority?: number | null
  n_flags?: number | null
  n_hard_fails?: number | null
  min_confidence?: number | null
}

// Worst-first analyst review queue (raw.protocol_review_queue view).
export interface ProtocolReviewQueueItem {
  study_id: string
  title: string | null
  complexity: string | null
  review_status: string | null
  source_protocol: string | null
  eval_ok: boolean | null
  n_hard_fails: number | null
  hard_fail_reasons: string | null
  review_priority: number | null
  n_flags: number | null
  min_confidence: number | null
  completeness_ok: string | null
  eval_ts: string | null
}
export interface ProtocolSpec {
  source: Source
  study_id: string
  spec: Record<string, unknown>
}

export const api = {
  health: () => get<{ status: string }>('/api/health'),
  configSummary: () => get<any>('/api/config/summary'),

  listProtocols: () =>
    get<{ source: Source; protocols: ProtocolListItem[] }>('/api/protocols/list'),
  protocolReviewQueue: () =>
    get<{ source: Source; queue: ProtocolReviewQueueItem[] }>('/api/protocols/review_queue'),
  getProtocolSpec: (studyId: string) =>
    get<ProtocolSpec>(`/api/protocols/spec?study_id=${encodeURIComponent(studyId)}`),
  approveProtocol: (body: {
    study_id: string
    reviewer_name: string
    reviewer_email: string
    signature: string
    decision?: string
    comments?: string
    corrected_fields?: Record<string, unknown>
  }) => post<any>('/api/protocols/approve', body),
  triggerExtract: () => post<{ run_id?: number; run_url?: string }>('/api/protocols/extract'),

  // ---- Build ----------------------------------------------------------------
  triggerBuild: (studyId: string) =>
    post<{ run_id?: number; run_url?: string }>(`/api/ads/build?study_id=${encodeURIComponent(studyId)}`),

  // ---- Review ---------------------------------------------------------------
  reviewQueue: () => get<{ source: Source; queue: any[] }>('/api/review/queue'),
  reviewDetails: (reviewId: string) =>
    get<any>(`/api/review/details?review_id=${encodeURIComponent(reviewId)}`),
  approveAds: (body: {
    review_id: string
    ads_id: string
    study_id: string
    reviewer_name: string
    reviewer_email: string
    decision: string
    signature: string
    comments?: string
  }) => post<any>('/api/review/approve', body),

  // ---- Served ---------------------------------------------------------------
  servedAdsOutput: (studyId?: string, limit = 100) =>
    get<{ source: Source; rows: any[]; count: number }>(
      `/api/served/ads_output?limit=${limit}${studyId ? `&study_id=${encodeURIComponent(studyId)}` : ''}`,
    ),
  cohortSummary: () =>
    get<{ source: Source; rows: any[] }>('/api/served/cohort_summary'),

  // ---- Audit ----------------------------------------------------------------
  reproManifests: (limit = 20) =>
    get<{ source: Source; manifests: any[] }>(`/api/audit/reproducibility?limit=${limit}`),
  gxpAudit: (limit = 50) =>
    get<{ source: Source; events: any[]; chain_valid?: boolean }>(`/api/audit/gxp?limit=${limit}`),
}
