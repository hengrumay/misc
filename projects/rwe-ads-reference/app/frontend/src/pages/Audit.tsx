import { useEffect, useState } from 'react'
import { motion } from 'framer-motion'
import { ClipboardList, Link2 } from 'lucide-react'
import { api, Source } from '../lib/api'
import SourcePill from '../components/SourcePill'

export default function Audit() {
  const [source, setSource] = useState<Source>('warehouse')
  const [manifests, setManifests] = useState<any[]>([])
  const [events, setEvents] = useState<any[]>([])
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    api.reproManifests().then((r) => { setSource(r.source); setManifests(r.manifests || []) }).catch((e) => setErr(String(e)))
    api.gxpAudit().then((r) => setEvents(r.events || [])).catch(() => {})
  }, [])

  return (
    <div className="max-w-6xl mx-auto space-y-6">
      <div className="flex items-start justify-between">
        <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
          <h1 className="text-3xl font-bold text-foreground mb-2 flex items-center gap-2"><ClipboardList className="w-7 h-7 text-primary" /> Audit &amp; Reproducibility</h1>
          <p className="text-muted-foreground">Reproducibility manifests and the tamper-evident, hash-chained GxP event log.</p>
        </motion.div>
        <SourcePill source={source} />
      </div>

      {err && <div className="p-3 rounded-lg border border-border bg-card text-sm text-foreground">{err}</div>}

      <div className="rounded-lg border border-border bg-card p-4 overflow-x-auto">
        <h3 className="text-sm font-semibold text-foreground mb-3">Reproducibility manifests</h3>
        <table className="w-full text-sm">
          <thead><tr className="text-left text-muted-foreground border-b border-border">
            <th className="py-2 pr-4">ads_id</th><th className="pr-4">study</th><th className="pr-4">protocol</th>
            <th className="pr-4">model</th><th className="pr-4">decision</th><th className="pr-4">reviewer</th><th className="pr-4">created</th></tr></thead>
          <tbody>
            {manifests.map((m, i) => (
              <tr key={i} className="border-b border-border/50">
                <td className="py-1.5 pr-4 font-mono text-xs">{m.ads_id}</td>
                <td className="pr-4">{m.study_id}</td><td className="pr-4">{m.protocol_version}</td>
                <td className="pr-4 text-xs">{m.model}</td><td className="pr-4">{m.decision}</td>
                <td className="pr-4">{m.reviewer || '—'}</td><td className="pr-4 text-xs">{m.created_ts}</td>
              </tr>
            ))}
            {manifests.length === 0 && <tr><td colSpan={7} className="py-3 text-muted-foreground">No manifests yet.</td></tr>}
          </tbody>
        </table>
      </div>

      <div className="rounded-lg border border-border bg-card p-4 overflow-x-auto">
        <h3 className="text-sm font-semibold text-foreground mb-3 flex items-center gap-2"><Link2 className="w-4 h-4 text-primary" /> GxP event log (hash-chained)</h3>
        <table className="w-full text-sm">
          <thead><tr className="text-left text-muted-foreground border-b border-border">
            <th className="py-2 pr-4">event_type</th><th className="pr-4">actor</th><th className="pr-4">subject</th>
            <th className="pr-4">ts</th><th className="pr-4">row_hash</th></tr></thead>
          <tbody>
            {events.map((e, i) => (
              <tr key={i} className="border-b border-border/50">
                <td className="py-1.5 pr-4">
                  <span className={`text-xs px-2 py-0.5 rounded ${e.event_type?.includes('approval') ? 'bg-success text-success-foreground' : 'bg-muted text-muted-foreground'}`}>{e.event_type}</span>
                </td>
                <td className="pr-4 text-xs">{e.actor}</td><td className="pr-4 font-mono text-xs">{e.subject_id}</td>
                <td className="pr-4 text-xs">{e.ts}</td><td className="pr-4 font-mono text-xs">{e.row_hash}…</td>
              </tr>
            ))}
            {events.length === 0 && <tr><td colSpan={5} className="py-3 text-muted-foreground">No events yet.</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  )
}
