import { motion } from 'framer-motion'
import {
  Upload,
  FileJson,
  Brain,
  CheckCircle2,
  Eye,
  Zap,
  Shield,
  Database,
  Lock,
  Gauge,
} from 'lucide-react'
import FlowDiagram from '../components/FlowDiagram'

export default function HowItWorks() {
  const nodes = [
    {
      id: 'protocol',
      x: 20,
      y: 120,
      step: 1,
      title: 'Protocol Upload',
      subtitle: 'Study definition',
      icon: <Upload className="w-4 h-4" />,
      accent: 'hsl(5, 100%, 50%)',
    },
    {
      id: 'parse',
      x: 240,
      y: 120,
      step: 2,
      title: 'Doc Intelligence',
      subtitle: 'Parse + extract',
      icon: <FileJson className="w-4 h-4" />,
      accent: 'hsl(5, 100%, 50%)',
    },
    {
      id: 'retrieve',
      x: 460,
      y: 120,
      step: 3,
      title: 'KB Retrieval',
      subtitle: 'Schema resolution',
      icon: <Brain className="w-4 h-4" />,
      accent: 'hsl(270, 84%, 60%)',
    },
    {
      id: 'eval',
      x: 680,
      y: 120,
      step: 4,
      title: 'Model-based Eval',
      subtitle: 'Confidence + judges',
      icon: <Gauge className="w-4 h-4" />,
      accent: 'hsl(160, 84%, 45%)',
    },
    {
      id: 'review',
      x: 900,
      y: 120,
      step: 5,
      title: 'Analyst Review',
      subtitle: 'Review spec + flags',
      icon: <Eye className="w-4 h-4" />,
      accent: 'hsl(270, 84%, 60%)',
    },
    {
      id: 'esign',
      x: 1120,
      y: 120,
      step: 6,
      title: 'E-Signature',
      subtitle: 'Compliance gate',
      icon: <CheckCircle2 className="w-4 h-4" />,
      accent: 'hsl(160, 84%, 45%)',
    },
    {
      id: 'build',
      x: 1340,
      y: 120,
      step: 7,
      title: 'ADS Builder',
      subtitle: 'Assemble SQL',
      icon: <Zap className="w-4 h-4" />,
      accent: 'hsl(5, 100%, 50%)',
    },
    {
      id: 'serve',
      x: 1560,
      y: 120,
      step: 8,
      title: 'Lakebase Serve',
      subtitle: 'Serve + audit',
      icon: <Database className="w-4 h-4" />,
      accent: 'hsl(192, 71%, 60%)',
    },

    // Guardrails lane
    {
      id: 'phi',
      x: 20,
      y: 350,
      w: 200,
      h: 80,
      title: 'PHI Containment',
      subtitle: 'In-process PII masking,\nno external egress',
      icon: <Shield className="w-4 h-4" />,
      accent: 'hsl(5, 100%, 50%)',
    },
    {
      id: 'audit',
      x: 240,
      y: 350,
      w: 200,
      h: 80,
      title: 'Reproducibility',
      subtitle: 'Manifests +\nversion hashes',
      icon: <Lock className="w-4 h-4" />,
      accent: 'hsl(160, 84%, 45%)',
    },
    {
      id: 'validate',
      x: 460,
      y: 350,
      w: 200,
      h: 80,
      title: 'Validation',
      subtitle: 'SQL syntax +\nEXPLAIN',
      icon: <CheckCircle2 className="w-4 h-4" />,
      accent: 'hsl(270, 84%, 60%)',
    },
    {
      id: 'synthetic',
      x: 680,
      y: 350,
      w: 200,
      h: 80,
      title: 'Synthetic RWD',
      subtitle: 'Never real\npatient data',
      icon: <Brain className="w-4 h-4" />,
      accent: 'hsl(192, 71%, 60%)',
    },
  ]

  const edges = [
    { from: 'protocol', to: 'parse', dots: 2 },
    { from: 'parse', to: 'retrieve', dots: 2 },
    { from: 'retrieve', to: 'eval', dots: 2 },
    { from: 'eval', to: 'review', dots: 2 },
    { from: 'review', to: 'esign', dots: 2 },
    { from: 'esign', to: 'build', dots: 2 },
    { from: 'build', to: 'serve', dots: 2 },
  ]

  return (
    <div className="max-w-7xl mx-auto space-y-12">
      {/* Header */}
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4 }}
        className="space-y-2"
      >
        <h1 className="text-3xl font-bold text-foreground">How It Works</h1>
        <p className="text-muted-foreground max-w-2xl">
          From protocol upload to analysis-ready dataset in Lakebase, with mandatory analyst
          review, full reproducibility tracking, and PHI-safe controls throughout.
        </p>
      </motion.div>

      {/* Flow diagram */}
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ duration: 0.5, delay: 0.1 }}
        className="rounded-lg border border-border p-6 bg-card glass"
      >
        <FlowDiagram
          nodes={nodes}
          edges={edges}
          width={1800}
          height={500}
        />
      </motion.div>

      {/* Step legend */}
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4, delay: 0.2 }}
        className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4"
      >
        {[
          {
            step: 1,
            title: 'Protocol Upload',
            desc: 'Upload study protocol (PDF/DOCX) defining cohort, exposures, outcomes, and follow-up.',
          },
          {
            step: 2,
            title: 'Doc Intelligence',
            desc: 'ai_parse_document + ai_extract pull a structured spec: population, inclusion/exclusion, covariates, endpoints.',
          },
          {
            step: 3,
            title: 'KB / Schema Resolution',
            desc: 'Vector Search retrieves validated SQL patterns from the approved-SQL knowledge base and resolves the target schema.',
          },
          {
            step: 4,
            title: 'Model-based Eval',
            desc: 'Deterministic validators + confidence + LLM judges flag or block a weak extraction and sort a worst-first review queue.',
          },
          {
            step: 5,
            title: 'Analyst Review',
            desc: 'Analyst reviews the extracted spec (with eval flags) and e-signs; only an approved spec is built.',
          },
          {
            step: 6,
            title: 'E-Signature',
            desc: 'Analyst types name + signature; approval gated on human validation (21 CFR Part 11).',
          },
          {
            step: 7,
            title: 'ADS Builder',
            desc: 'ADS Builder assembles SQL from approved KB snippets (template substitution), then validates syntax + EXPLAIN against synthetic gold.',
          },
          {
            step: 8,
            title: 'Serve + Audit',
            desc: 'ADS synced from Delta to Lakebase low-latency Postgres; manifest logs protocol version, KB versions, SQL, eval scores, and reviewer.',
          },
        ].map((item, i) => (
          <motion.div
            key={item.step}
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.3, delay: 0.3 + i * 0.05 }}
            className="p-4 rounded-lg border border-border bg-card/50 hover:bg-card transition-colors"
          >
            <div className="flex items-start gap-3">
              <div className="h-6 w-6 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-xs font-bold flex-shrink-0">
                {item.step}
              </div>
              <div>
                <h3 className="font-semibold text-sm text-foreground">{item.title}</h3>
                <p className="text-xs text-muted-foreground mt-1">{item.desc}</p>
              </div>
            </div>
          </motion.div>
        ))}
      </motion.div>

      {/* Key principles */}
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4, delay: 0.3 }}
        className="grid grid-cols-1 md:grid-cols-3 gap-6"
      >
        <div className="p-6 rounded-lg border border-border bg-card/50">
          <div className="flex items-center gap-2 mb-3">
            <Shield className="w-5 h-5 text-primary" />
            <h3 className="font-semibold text-foreground">PHI-Safe by Design</h3>
          </div>
          <p className="text-sm text-muted-foreground">
            Model calls run in-platform with in-process PII masking, audit logging, and
            external-egress denial (no data leaves the workspace). Data is synthetic (PHI-safe), but
            controls transfer to your production workspace with real RWD.
          </p>
        </div>

        <div className="p-6 rounded-lg border border-border bg-card/50">
          <div className="flex items-center gap-2 mb-3">
            <Lock className="w-5 h-5 text-primary" />
            <h3 className="font-semibold text-foreground">Reproducible by Construction</h3>
          </div>
          <p className="text-sm text-muted-foreground">
            Every ADS build emits a reproducibility manifest: protocol version, KB snippet hashes,
            generated SQL, source Delta table versions, model, eval scores, and e-signature. Ensures
            full audit trail for compliance.
          </p>
        </div>

        <div className="p-6 rounded-lg border border-border bg-card/50">
          <div className="flex items-center gap-2 mb-3">
            <Eye className="w-5 h-5 text-primary" />
            <h3 className="font-semibold text-foreground">Human-in-the-Loop Mandatory</h3>
          </div>
          <p className="text-sm text-muted-foreground">
            No ADS is "approved" without analyst review + e-sign. Validation-not-execution: the ADS builder
            assembles SQL from approved templates but executes only against synthetic gold. No real patient DB access possible by
            construction.
          </p>
        </div>
      </motion.div>
    </div>
  )
}
