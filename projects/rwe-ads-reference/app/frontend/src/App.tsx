import { useState, useEffect, ReactNode } from 'react'
import { BrowserRouter as Router, Routes, Route, useLocation, Link } from 'react-router-dom'
import { motion } from 'framer-motion'
import {
  Moon,
  Sun,
  LayoutGrid,
  FileText,
  Zap,
  CheckCircle2,
  Database,
  ClipboardList,
  AlertCircle,
} from 'lucide-react'

// Pages (lazy-loaded)
import HowItWorks from './pages/HowItWorks'
import Protocols from './pages/Protocols'
import BuildADS from './pages/BuildADS'
import ReviewSignOff from './pages/ReviewSignOff'
import ServedADS from './pages/ServedADS'
import Audit from './pages/Audit'

// Components
import ErrorBoundary from './components/ErrorBoundary'

const pages = [
  { path: '/', label: 'How It Works', icon: LayoutGrid, section: 'guides' },
  { path: '/protocols', label: 'Protocols', icon: FileText, section: 'build' },
  { path: '/build', label: 'Build ADS', icon: Zap, section: 'build' },
  { path: '/review', label: 'Review & Sign-off', icon: CheckCircle2, section: 'review' },
  { path: '/served', label: 'Served ADS', icon: Database, section: 'output' },
  { path: '/audit', label: 'Audit & Reproducibility', icon: ClipboardList, section: 'audit' },
]

// ============================================================================
// Navbar
// ============================================================================

function Navbar({ theme, onThemeToggle }: any) {
  return (
    <header className="shrink-0 h-16 bg-card border-b border-border z-40 flex items-center px-4 gap-4">
      {/* Logo / Title */}
      <div className="flex-1">
        <h1 className="text-lg font-bold text-foreground">
          RWE ADS Studio
        </h1>
        <p className="text-xs text-muted-foreground">Analysis-Ready Dataset Automation</p>
      </div>

      {/* Theme toggle */}
      <button
        onClick={onThemeToggle}
        className="p-2 hover:bg-muted rounded-lg transition-colors text-muted-foreground hover:text-foreground"
        aria-label="Toggle dark/light theme"
      >
        {theme === 'dark' ? <Sun className="w-5 h-5" /> : <Moon className="w-5 h-5" />}
      </button>
    </header>
  )
}

// ============================================================================
// Sidebar
// ============================================================================

function Sidebar() {
  const location = useLocation()
  const [collapsed, setCollapsed] = useState(false)

  // Persist collapse state
  useEffect(() => {
    const saved = localStorage.getItem('sidebar-collapsed')
    if (saved) setCollapsed(JSON.parse(saved))
  }, [])

  useEffect(() => {
    localStorage.setItem('sidebar-collapsed', JSON.stringify(collapsed))
  }, [collapsed])

  return (
      <aside
        className="w-64 shrink-0 h-full bg-sidebar-background border-r border-sidebar-border overflow-y-auto flex flex-col p-4 gap-4"
      >
        {/* Collapse toggle (desktop) */}
        <div className="hidden md:block pt-2">
          <button
            onClick={() => setCollapsed(!collapsed)}
            className="text-xs text-muted-foreground hover:text-foreground px-2 py-1"
          >
            {collapsed ? 'Expand' : 'Collapse'}
          </button>
        </div>

        {/* Nav sections */}
        <nav className="flex-1 space-y-6">
          {['guides', 'build', 'review', 'output', 'audit'].map((section) => (
            <div key={section}>
              <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider px-2 mb-2">
                {section === 'guides' && 'Getting Started'}
                {section === 'build' && 'Build Pipeline'}
                {section === 'review' && 'Review'}
                {section === 'output' && 'Analytics'}
                {section === 'audit' && 'Compliance'}
              </h3>
              <ul className="space-y-1">
                {pages
                  .filter((p) => p.section === section)
                  .map((page) => {
                    const Icon = page.icon
                    const isActive = location.pathname === page.path
                    return (
                      <motion.li key={page.path} whileHover={{ x: 2 }}>
                        <Link
                          to={page.path}
                          className={`flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-all ${
                            isActive
                              ? 'bg-primary text-primary-foreground'
                              : 'text-muted-foreground hover:text-foreground hover:bg-muted'
                          }`}
                        >
                          <Icon className="w-4 h-4" />
                          {!collapsed && page.label}
                        </Link>
                      </motion.li>
                    )
                  })}
              </ul>
            </div>
          ))}
        </nav>

        {/* Footer */}
        <div className="text-[10px] text-muted-foreground space-y-1 border-t border-border pt-4">
          <p>© 2026 RWE ADS</p>
          <p>Serverless deployment</p>
        </div>
      </aside>
  )
}

// ============================================================================
// Main App
// ============================================================================

export default function App() {
  const [theme, setTheme] = useState<'dark' | 'light'>('dark')

  // Theme persistence
  useEffect(() => {
    const saved = localStorage.getItem('theme')
    if (saved) setTheme(saved as 'dark' | 'light')
  }, [])

  useEffect(() => {
    localStorage.setItem('theme', theme)
    const html = document.documentElement
    if (theme === 'dark') {
      html.classList.add('dark')
      html.classList.remove('light')
    } else {
      html.classList.add('light')
      html.classList.remove('dark')
    }
  }, [theme])

  const toggleTheme = () => setTheme((t) => (t === 'dark' ? 'light' : 'dark'))

  return (
    <ErrorBoundary>
      <Router>
        <div className="h-screen flex flex-col overflow-hidden">
          {/* Navbar */}
          <Navbar
            theme={theme}
            onThemeToggle={toggleTheme}
          />

          <div className="flex flex-1 overflow-hidden">
            {/* Sidebar */}
            <Sidebar />

            {/* Main content */}
            <main className="flex-1 overflow-y-auto pt-4 px-6 pb-6">
              <Routes>
                <Route path="/" element={<HowItWorks />} />
                <Route path="/protocols" element={<Protocols />} />
                <Route path="/build" element={<BuildADS />} />
                <Route path="/review" element={<ReviewSignOff />} />
                <Route path="/served" element={<ServedADS />} />
                <Route path="/audit" element={<Audit />} />
              </Routes>

            </main>
          </div>
        </div>
      </Router>
    </ErrorBoundary>
  )
}
