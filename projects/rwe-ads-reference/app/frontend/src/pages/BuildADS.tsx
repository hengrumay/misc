import { useEffect, useState } from 'react'
import { motion } from 'framer-motion'
import { Zap, Loader2, PlayCircle, Lock } from 'lucide-react'
import { api, Source, ProtocolListItem } from '../lib/api'
import SourcePill from '../components/SourcePill'

export default function BuildADS() {
  const [source, setSource] = useState<Source>('warehouse')
  const [protocols, setProtocols] = useState<ProtocolListItem[]>([])
  const [busy, setBusy] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)

  const load = async () => { const r = await api.listProtocols(); setSource(r.source); setProtocols(r.protocols) }
  useEffect(() => { load().catch((e) => setMsg(String(e))) }, [])

  const build = async (studyId: string) => {
    setBusy(studyId); setMsg(null)
    try { const r = await api.triggerBuild(studyId); setMsg(`Build job started for ${studyId} (run ${r.run_id}). Track it under Review & Sign-off.`) }
    catch (e) { setMsg(`Build failed: ${e}`) } finally { setBusy(null) }
  }

  return (
    <div className="max-w-5xl mx-auto space-y-6">
      <div className="flex items-start justify-between">
        <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
          <h1 className="text-3xl font-bold text-foreground mb-2 flex items-center gap-2"><Zap className="w-7 h-7 text-primary" /> Build ADS</h1>
          <p className="text-muted-foreground">Compose an analysis-ready dataset from an <b>approved</b> protocol using only approved KB snippets. Every step is EXPLAIN-validated before anything runs, and execution only ever targets synthetic gold.</p>
        </motion.div>
        <SourcePill source={source} />
      </div>

      {msg && <div className="p-3 rounded-lg border border-border bg-card text-sm text-foreground">{msg}</div>}

      <div className="space-y-3">
        {protocols.map((p) => {
          const approved = p.review_status === 'approved'
          return (
            <div key={p.study_id} className="p-4 rounded-lg border border-border bg-card/50 flex items-center gap-3">
              <div className="flex-1 min-w-0">
                <h3 className="font-medium text-foreground truncate">{p.title || p.study_id}</h3>
                <p className="text-xs text-muted-foreground">{p.study_id} · {p.complexity || '—'} · {p.review_status || '—'}</p>
              </div>
              <button onClick={() => build(p.study_id)} disabled={!approved || busy === p.study_id}
                className={`px-4 py-2 rounded-lg font-medium flex items-center gap-2 ${approved ? 'bg-primary text-primary-foreground hover:opacity-90' : 'bg-muted text-muted-foreground cursor-not-allowed'}`}
                title={approved ? 'Build the ADS' : 'Approve the protocol first (Protocols page)'}>
                {busy === p.study_id ? <Loader2 className="w-4 h-4 animate-spin" /> : approved ? <PlayCircle className="w-4 h-4" /> : <Lock className="w-4 h-4" />}
                {approved ? 'Build ADS' : 'Locked'}
              </button>
            </div>
          )
        })}
        {protocols.length === 0 && <p className="text-sm text-muted-foreground">No protocols. Upload &amp; extract one first.</p>}
      </div>
    </div>
  )
}
