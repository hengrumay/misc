import { useEffect, useRef, useState } from 'react'
import { motion } from 'framer-motion'
import { Upload, FileText, CheckCircle2, Loader2, PlayCircle, ShieldCheck } from 'lucide-react'
import { api, ProtocolListItem } from '../lib/api'
import SourcePill from '../components/SourcePill'

const statusBadge = (s: string | null) => {
  const m: Record<string, string> = {
    approved: 'bg-success text-success-foreground',
    extracted: 'bg-warning text-warning-foreground',
    rejected: 'bg-destructive text-destructive-foreground',
  }
  return m[s || ''] || 'bg-muted text-muted-foreground'
}

const CODED_FIELDS = [
  ['complexity', 'Complexity'], ['dx_codes', 'Dx codes'], ['ndc_codes', 'NDC codes'],
  ['exclude_dx', 'Exclusion dx'], ['outcome_codes', 'Outcome codes'],
  ['min_age', 'Min age'], ['max_age', 'Max age'], ['study_start', 'Study start'],
  ['study_end', 'Study end'], ['pre_days', 'Enroll pre (d)'], ['post_days', 'Enroll post (d)'],
  ['washout_days', 'Washout (d)'], ['baseline_days', 'Baseline (d)'], ['followup_days', 'Follow-up (d)'],
]

export default function Protocols() {
  const [source, setSource] = useState<'lakebase' | 'warehouse' | 'synthetic'>('warehouse')
  const [protocols, setProtocols] = useState<ProtocolListItem[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [spec, setSpec] = useState<Record<string, unknown> | null>(null)
  const [busy, setBusy] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)
  const [reviewer, setReviewer] = useState('')
  const [signature, setSignature] = useState('')
  const fileRef = useRef<HTMLInputElement>(null)

  const load = async () => {
    const r = await api.listProtocols()
    setSource(r.source)
    setProtocols(r.protocols)
  }
  useEffect(() => { load().catch((e) => setMsg(String(e))) }, [])

  const openSpec = async (studyId: string) => {
    setSelected(studyId); setSpec(null)
    try { setSpec((await api.getProtocolSpec(studyId)).spec) }
    catch (e) { setMsg(String(e)) }
  }

  const doUpload = async (f: File) => {
    setBusy('upload'); setMsg(null)
    try {
      const fd = new FormData(); fd.append('file', f)
      const res = await fetch('/api/protocols/upload', { method: 'POST', body: fd })
      if (!res.ok) throw new Error((await res.json()).detail || res.statusText)
      setMsg(`Uploaded ${f.name}. Click "Run extraction" to parse it.`)
    } catch (e) { setMsg(`Upload failed: ${e}`) } finally { setBusy(null) }
  }

  const runExtract = async () => {
    setBusy('extract'); setMsg(null)
    try { const r = await api.triggerExtract(); setMsg(`Extraction job started (run ${r.run_id}). Refresh in ~1–2 min.`) }
    catch (e) { setMsg(`Extract failed: ${e}`) } finally { setBusy(null) }
  }

  const approve = async () => {
    if (!selected) return
    if (signature.trim().length < 2) { setMsg('Enter an e-signature to approve.'); return }
    setBusy('approve'); setMsg(null)
    try {
      await api.approveProtocol({ study_id: selected, reviewer_name: reviewer || 'Analyst',
        reviewer_email: reviewer || 'analyst', signature, decision: 'approve' })
      setMsg(`Protocol ${selected} approved & e-signed.`)
      await load(); await openSpec(selected)
    } catch (e) { setMsg(`Approve failed: ${e}`) } finally { setBusy(null) }
  }

  const build = async () => {
    if (!selected) return
    setBusy('build'); setMsg(null)
    try { const r = await api.triggerBuild(selected); setMsg(`ADS build job started for ${selected} (run ${r.run_id}).`) }
    catch (e) { setMsg(`Build failed: ${e}`) } finally { setBusy(null) }
  }

  const fmt = (v: unknown): string => Array.isArray(v) ? (v.length ? v.join(', ') : '—') : String(v ?? '—')

  return (
    <div className="max-w-6xl mx-auto space-y-6">
      <div className="flex items-start justify-between">
        <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
          <h1 className="text-3xl font-bold text-foreground mb-2">Protocols</h1>
          <p className="text-muted-foreground">Upload a study protocol (PDF/DOCX), extract the coded spec, review &amp; e-sign, then build the ADS.</p>
        </motion.div>
        <SourcePill source={source} />
      </div>

      {msg && (
        <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }}
          className="p-3 rounded-lg border border-border bg-card text-sm text-foreground">{msg}</motion.div>
      )}

      {/* Upload + extract */}
      <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4, delay: 0.1 }}
        className="border-2 border-dashed border-border rounded-lg p-6 flex flex-col items-center gap-3">
        <Upload className="w-10 h-10 text-muted-foreground" />
        <p className="text-sm text-muted-foreground">PDF or DOCX → <code>protocols volume</code></p>
        <input ref={fileRef} type="file" accept=".pdf,.docx" className="hidden"
          onChange={(e) => e.target.files?.[0] && doUpload(e.target.files[0])} />
        <div className="flex gap-2">
          <button onClick={() => fileRef.current?.click()} disabled={busy === 'upload'}
            className="px-4 py-2 bg-primary text-primary-foreground rounded-lg hover:opacity-90 font-medium flex items-center gap-2">
            {busy === 'upload' ? <Loader2 className="w-4 h-4 animate-spin" /> : <Upload className="w-4 h-4" />} Choose File
          </button>
          <button onClick={runExtract} disabled={busy === 'extract'}
            className="px-4 py-2 bg-muted text-foreground rounded-lg hover:bg-border font-medium flex items-center gap-2">
            {busy === 'extract' ? <Loader2 className="w-4 h-4 animate-spin" /> : <PlayCircle className="w-4 h-4" />} Run extraction
          </button>
          <button onClick={() => load()} className="px-4 py-2 text-muted-foreground hover:text-foreground">Refresh</button>
        </div>
      </motion.div>

      <div className="grid md:grid-cols-2 gap-6">
        {/* List */}
        <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4, delay: 0.2 }}
          className="space-y-3">
          <h2 className="text-xl font-semibold text-foreground">Protocols</h2>
          {protocols.length === 0 && <p className="text-sm text-muted-foreground">No protocols yet.</p>}
          {protocols.map((p) => (
            <div key={p.study_id} onClick={() => openSpec(p.study_id)}
              className={`p-4 rounded-lg border bg-card/50 hover:bg-card cursor-pointer flex items-center gap-3 transition-colors ${selected === p.study_id ? 'border-primary' : 'border-border'}`}>
              <FileText className="w-5 h-5 text-primary flex-shrink-0" />
              <div className="flex-1 min-w-0">
                <h3 className="font-medium text-foreground truncate">{p.title || p.study_id}</h3>
                <p className="text-xs text-muted-foreground truncate">{p.study_id} · {p.source_protocol || 'seed'}</p>
              </div>
              <span className={`text-xs px-2 py-1 rounded ${statusBadge(p.review_status)}`}>{p.review_status || '—'}</span>
            </div>
          ))}
        </motion.div>

        {/* Spec detail + review */}
        <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4, delay: 0.25 }}
          className="space-y-3">
          <h2 className="text-xl font-semibold text-foreground">Extracted spec {selected ? `— ${selected}` : ''}</h2>
          {!selected && <p className="text-sm text-muted-foreground">Select a protocol to review its extracted coded fields.</p>}
          {selected && !spec && <Loader2 className="w-5 h-5 animate-spin text-muted-foreground" />}
          {spec && (
            <div className="rounded-lg border border-border bg-card p-4 space-y-3">
              <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
                {CODED_FIELDS.map(([k, label]) => (
                  <div key={k} className="flex justify-between gap-2">
                    <span className="text-muted-foreground">{label}</span>
                    <span className="text-foreground font-mono text-right break-all">{fmt(spec[k])}</span>
                  </div>
                ))}
              </div>
              <div className="text-xs text-muted-foreground">
                covariates: <span className="font-mono">{String(spec['covariates_coded'] ?? '—')}</span>
              </div>
              <div className="text-xs text-muted-foreground">extraction model: {String(spec['extraction_model'] ?? '—')}</div>

              <div className="border-t border-border pt-3 space-y-2">
                <div className="flex items-center gap-2 text-sm font-medium text-foreground">
                  <ShieldCheck className="w-4 h-4 text-primary" /> Analyst review &amp; e-signature
                </div>
                <input value={reviewer} onChange={(e) => setReviewer(e.target.value)} placeholder="Reviewer email"
                  className="w-full px-3 py-2 rounded bg-muted text-foreground text-sm border border-border" />
                <input value={signature} onChange={(e) => setSignature(e.target.value)} placeholder="Type your e-signature to approve"
                  className="w-full px-3 py-2 rounded bg-muted text-foreground text-sm border border-border" />
                <div className="flex gap-2">
                  <button onClick={approve} disabled={busy === 'approve' || spec['review_status'] === 'approved'}
                    className="flex-1 px-3 py-2 bg-success text-success-foreground rounded-lg font-medium flex items-center justify-center gap-2 disabled:opacity-50">
                    {busy === 'approve' ? <Loader2 className="w-4 h-4 animate-spin" /> : <CheckCircle2 className="w-4 h-4" />}
                    {spec['review_status'] === 'approved' ? 'Approved' : 'Approve & e-sign'}
                  </button>
                  <button onClick={build} disabled={busy === 'build' || spec['review_status'] !== 'approved'}
                    className="flex-1 px-3 py-2 bg-primary text-primary-foreground rounded-lg font-medium flex items-center justify-center gap-2 disabled:opacity-50"
                    title={spec['review_status'] !== 'approved' ? 'Approve first' : 'Build the ADS'}>
                    {busy === 'build' ? <Loader2 className="w-4 h-4 animate-spin" /> : <PlayCircle className="w-4 h-4" />} Build ADS
                  </button>
                </div>
              </div>
            </div>
          )}
        </motion.div>
      </div>
    </div>
  )
}
