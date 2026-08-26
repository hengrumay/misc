import { motion } from 'framer-motion'

interface SourcePillProps {
  source: 'lakebase' | 'warehouse' | 'synthetic'
}

export default function SourcePill({ source }: SourcePillProps) {
  const sourceConfig = {
    lakebase: {
      label: 'Lakebase',
      color: 'bg-success text-success-foreground',
      description: 'Low-latency serving DB',
    },
    warehouse: {
      label: 'Warehouse',
      color: 'bg-primary text-primary-foreground',
      description: 'SQL Warehouse fallback',
    },
    synthetic: {
      label: 'Synthetic',
      color: 'bg-warning text-warning-foreground',
      description: 'Demonstration data',
    },
  }

  const config = sourceConfig[source]

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.9 }}
      animate={{ opacity: 1, scale: 1 }}
      className={`px-3 py-2 rounded-lg text-sm font-medium flex items-center gap-2 ${config.color} shadow-lg`}
      title={config.description}
    >
      <div className="w-2 h-2 rounded-full bg-current opacity-70 animate-pulse" />
      {config.label}
    </motion.div>
  )
}
