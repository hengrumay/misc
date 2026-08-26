import { useEffect, useState } from 'react'
import { motion } from 'framer-motion'
import { CheckCircle2, Loader2, ShieldCheck } from 'lucide-react'
import { api, Source } from '../lib/api'
import SourcePill from '../components/SourcePill'

export default function ReviewSignOff() {
  const [source, setSource] = useState<Source>('warehouse')
  const [queue, setQueue] = useState<any[]>([])
  const [sel, setSel] = useState<any | null>(null)
  const [sql, setSql] = useState<Record<string, string> | null>(null)
  const [reviewer, setReviewer] = useState('')
  const [signature, setSignature] = useState('')
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState<string | null>(null)

  const load = async () => {
    const r = await api.reviewQueue(); setSource(r.source); setQueue(r.queue || [])
  }
  useEffect(() => { load().catch((e) => setMsg(String(e))) }, [])

  const open = async (row: any) => {
    setSel(row); setSql(null)
    try { const d = await api.reviewDetails(row.review_id); { let g:any = d.review?.generated_sql; if (typeof g === 'string') { try { g = JSON.parse(g) } catch { g = { sql: g } } } setSql(g || null) } }
    catch { /* manifest may be absent */ }
  }

  const approve = async (decision: string) => {
    if (!sel) return
    if (decision === 'approve' && signature.trim().length < 2) { setMsg('E-signature required to approve.'); return }
    setBusy(true); setMsg(null)
    try {
      await api.approveAds({ review_id: sel.review_id, ads_id: sel.ads_id, study_id: sel.study_id,
        reviewer_name: reviewer || 'Analyst', reviewer_email: reviewer || 'analyst', decision,
        signature: signature || 'n/a' })
      setMsg(`ADS ${sel.ads_id} ${decision}d & recorded in the audit log.`)
      setSel(null); await load()
    } catch (e) { setMsg(`Failed: ${e}`) } finally { setBusy(false) }
  }

  return (
    <div className="max-w-6xl mx-auto space-y-6">
      <div className="flex items-start justify-between">
        <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
          <h1 className="text-3xl font-bold text-foreground mb-2 flex items-center gap-2"><CheckCircle2 className="w-7 h-7 text-primary" /> Review &amp; Sign-off</h1>
          <p className="text-muted-foreground">No ADS is approved without an analyst review of the generated SQL and an e-signature.</p>
        </motion.div>
        <SourcePill source={source} />
      </div>

      {msg && <div className="p-3 rounded-lg border border-border bg-card text-sm text-foreground">{msg}</div>}

      <div className="grid md:grid-cols-2 gap-6">
        <div className="space-y-3">
          <h2 className="text-xl font-semibold text-foreground">Pending builds</h2>
          {queue.length === 0 && <p className="text-sm text-muted-foreground">Nothing pending. Build an ADS from an approved protocol.</p>}
          {queue.map((row) => (
            <div key={row.review_id} onClick={() => open(row)}
              className={`p-4 rounded-lg border bg-card/50 hover:bg-card cursor-pointer transition-colors ${sel?.review_id === row.review_id ? 'border-primary' : 'border-border'}`}>
              <div className="flex justify-between"><span className="font-medium text-foreground">{row.study_id}</span>
                <span className="text-xs px-2 py-0.5 rounded bg-warning text-warning-foreground">{row.status}</span></div>
              <p className="text-xs text-muted-foreground font-mono truncate">{row.ads_id}</p>
              <p className="text-xs text-muted-foreground">{row.n_patients} patients · {row.complexity} · KB {row.kb_snippets_hash}</p>
            </div>
          ))}
        </div>

        <div className="space-y-3">
          <h2 className="text-xl font-semibold text-foreground">Generated SQL {sel ? `— ${sel.study_id}` : ''}</h2>
          {!sel && <p className="text-sm text-muted-foreground">Select a build to review its generated SQL and e-sign.</p>}
          {sel && (
            <div className="rounded-lg border border-border bg-card p-4 space-y-3">
              {sql ? Object.entries(sql).map(([step, s]) => (
                <details key={step} className="text-sm">
                  <summary className="cursor-pointer text-foreground font-medium">{step}</summary>
                  <pre className="mt-2 p-2 bg-muted rounded text-xs overflow-x-auto whitespace-pre-wrap">{s}</pre>
                </details>
              )) : <p className="text-xs text-muted-foreground">No manifest SQL found for this build.</p>}

              <div className="border-t border-border pt-3 space-y-2">
                <div className="flex items-center gap-2 text-sm font-medium text-foreground"><ShieldCheck className="w-4 h-4 text-primary" /> E-signature</div>
                <input value={reviewer} onChange={(e) => setReviewer(e.target.value)} placeholder="Reviewer email"
                  className="w-full px-3 py-2 rounded bg-muted text-foreground text-sm border border-border" />
                <input value={signature} onChange={(e) => setSignature(e.target.value)} placeholder="Type your e-signature"
                  className="w-full px-3 py-2 rounded bg-muted text-foreground text-sm border border-border" />
                <div className="flex gap-2">
                  <button onClick={() => approve('approve')} disabled={busy}
                    className="flex-1 px-3 py-2 bg-success text-success-foreground rounded-lg font-medium flex items-center justify-center gap-2 disabled:opacity-50">
                    {busy ? <Loader2 className="w-4 h-4 animate-spin" /> : <CheckCircle2 className="w-4 h-4" />} Approve
                  </button>
                  <button onClick={() => approve('reject')} disabled={busy}
                    className="px-3 py-2 bg-destructive text-destructive-foreground rounded-lg font-medium disabled:opacity-50">Reject</button>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
