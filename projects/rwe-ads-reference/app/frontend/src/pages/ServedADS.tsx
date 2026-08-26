import { useEffect, useState } from 'react'
import { motion } from 'framer-motion'
import { Database } from 'lucide-react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import { api, Source } from '../lib/api'
import SourcePill from '../components/SourcePill'

export default function ServedADS() {
  const [source, setSource] = useState<Source>('warehouse')
  const [summary, setSummary] = useState<any[]>([])
  const [study, setStudy] = useState<string>('')
  const [rows, setRows] = useState<any[]>([])
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    api.cohortSummary().then((r) => {
      setSource(r.source); setSummary(r.rows || [])
      if (r.rows?.length && !study) setStudy(r.rows[0].study_id)
    }).catch((e) => setErr(String(e)))
  }, [])

  useEffect(() => {
    if (!study) return
    api.servedAdsOutput(study, 50).then((r) => setRows(r.rows || [])).catch((e) => setErr(String(e)))
  }, [study])

  const cur = summary.find((s) => s.study_id === study)
  const num = (v: any) => (v == null ? 0 : Number(v))
  const chartData = summary.map((s) => ({ study: s.study_id, patients: num(s.n_patients), outcomes: num(s.n_outcomes) }))

  return (
    <div className="max-w-6xl mx-auto space-y-6">
      <div className="flex items-start justify-between">
        <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}>
          <h1 className="text-3xl font-bold text-foreground mb-2 flex items-center gap-2"><Database className="w-7 h-7 text-primary" /> Served ADS</h1>
          <p className="text-muted-foreground">Analysis-ready datasets built from approved protocols, served for low-latency access.</p>
        </motion.div>
        <SourcePill source={source} />
      </div>

      {err && <div className="p-3 rounded-lg border border-border bg-card text-sm text-foreground">{err}</div>}

      {/* study selector */}
      <div className="flex gap-2 flex-wrap">
        {summary.map((s) => (
          <button key={s.study_id} onClick={() => setStudy(s.study_id)}
            className={`px-3 py-1.5 rounded-lg text-sm font-medium border ${study === s.study_id ? 'bg-primary text-primary-foreground border-primary' : 'bg-card text-muted-foreground border-border hover:text-foreground'}`}>
            {s.study_id}
          </button>
        ))}
      </div>

      {/* KPI tiles for the selected study */}
      {cur && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {[
            ['Patients', num(cur.n_patients)],
            ['Outcomes', num(cur.n_outcomes)],
            ['Outcome rate', `${(num(cur.outcome_rate) * 100).toFixed(1)}%`],
            ['Avg time-to-event (d)', num(cur.avg_time_to_event_days)],
          ].map(([label, val]) => (
            <motion.div key={label as string} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }}
              className="rounded-lg border border-border bg-card p-4">
              <div className="text-xs text-muted-foreground">{label}</div>
              <div className="text-2xl font-bold text-foreground">{val as any}</div>
            </motion.div>
          ))}
        </div>
      )}

      {/* cross-study chart */}
      {chartData.length > 0 && (
        <div className="rounded-lg border border-border bg-card p-4">
          <h3 className="text-sm font-semibold text-foreground mb-3">Cohort size by study</h3>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis dataKey="study" stroke="hsl(var(--muted-foreground))" fontSize={12} />
              <YAxis stroke="hsl(var(--muted-foreground))" fontSize={12} />
              <Tooltip contentStyle={{ background: 'hsl(var(--card))', border: '1px solid hsl(var(--border))' }} />
              <Bar dataKey="patients" fill="hsl(var(--primary))" radius={[4, 4, 0, 0]} />
              <Bar dataKey="outcomes" fill="hsl(var(--warning))" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* ADS output rows */}
      <div className="rounded-lg border border-border bg-card p-4 overflow-x-auto">
        <h3 className="text-sm font-semibold text-foreground mb-3">ADS output — {study} (first {rows.length})</h3>
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-muted-foreground border-b border-border">
              <th className="py-2 pr-4">patient_id</th><th className="pr-4">index_date</th>
              <th className="pr-4">outcome_flag</th><th className="pr-4">time_to_event</th>
              <th className="pr-4">covariates</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} className="border-b border-border/50">
                <td className="py-1.5 pr-4 font-mono text-xs">{r.patient_id}</td>
                <td className="pr-4">{r.index_date}</td>
                <td className="pr-4">{r.outcome_flag}</td>
                <td className="pr-4">{r.time_to_event}</td>
                <td className="pr-4 font-mono text-xs break-all">{typeof r.covariates === 'string' ? r.covariates : JSON.stringify(r.covariates)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
