import { useEffect, useState } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
)

// ---------------------------------------------------------------------------
// API response types
// ---------------------------------------------------------------------------

interface LabItem {
  id: number
  type: string
  title: string
  created_at: string
}

interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface PassRate {
  task: string
  avg_score: number | null
  attempts: number
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function apiFetch<T>(url: string, token: string): Promise<T> {
  return fetch(url, { headers: { Authorization: `Bearer ${token}` } }).then(
    (res) => {
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      return res.json() as Promise<T>
    },
  )
}

/** "Lab 04 — Testing" → "lab-04" */
function titleToLabId(title: string): string {
  const match = /Lab (\d+)/i.exec(title)
  return match ? `lab-${match[1]}` : ''
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

interface DashboardProps {
  token: string
}

export default function Dashboard({ token }: DashboardProps) {
  const [labs, setLabs] = useState<LabItem[]>([])
  const [selectedLab, setSelectedLab] = useState<string>('')

  const [scores, setScores] = useState<ScoreBucket[] | null>(null)
  const [timeline, setTimeline] = useState<TimelineEntry[] | null>(null)
  const [passRates, setPassRates] = useState<PassRate[] | null>(null)

  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Fetch labs list once on mount (or when token changes)
  useEffect(() => {
    if (!token) return
    apiFetch<LabItem[]>('/items/', token)
      .then((items) => {
        const labItems = items.filter((i) => i.type === 'lab')
        setLabs(labItems)
        if (labItems.length > 0) {
          setSelectedLab(titleToLabId(labItems[0].title))
        }
      })
      .catch((err: Error) => setError(err.message))
  }, [token])

  // Fetch analytics data whenever the selected lab changes
  useEffect(() => {
    if (!token || !selectedLab) return

    setLoading(true)
    setError(null)

    const q = `?lab=${selectedLab}`
    Promise.all([
      apiFetch<ScoreBucket[]>(`/analytics/scores${q}`, token),
      apiFetch<TimelineEntry[]>(`/analytics/timeline${q}`, token),
      apiFetch<PassRate[]>(`/analytics/pass-rates${q}`, token),
    ])
      .then(([s, t, p]) => {
        setScores(s)
        setTimeline(t)
        setPassRates(p)
        setLoading(false)
      })
      .catch((err: Error) => {
        setError(err.message)
        setLoading(false)
      })
  }, [token, selectedLab])

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  return (
    <div className="dashboard">
      {/* Lab selector */}
      <div className="dashboard-toolbar">
        <label htmlFor="lab-select">Lab:</label>
        <select
          id="lab-select"
          value={selectedLab}
          onChange={(e) => setSelectedLab(e.target.value)}
        >
          {labs.map((lab) => {
            const labId = titleToLabId(lab.title)
            return (
              <option key={lab.id} value={labId}>
                {lab.title}
              </option>
            )
          })}
        </select>
      </div>

      {loading && <p>Loading…</p>}
      {error && <p className="error">Error: {error}</p>}

      {!loading && !error && scores && timeline && passRates && (
        <div className="dashboard-grid">
          {/* Score distribution bar chart */}
          <section className="chart-card">
            <h2>Score Distribution</h2>
            <Bar
              data={{
                labels: scores.map((b) => b.bucket),
                datasets: [
                  {
                    label: 'Submissions',
                    data: scores.map((b) => b.count),
                    backgroundColor: 'rgba(59, 130, 246, 0.65)',
                    borderColor: 'rgba(59, 130, 246, 1)',
                    borderWidth: 1,
                  },
                ],
              }}
              options={{
                responsive: true,
                plugins: {
                  legend: { display: false },
                  title: { display: false },
                },
                scales: {
                  y: { beginAtZero: true, ticks: { stepSize: 1 } },
                },
              }}
            />
          </section>

          {/* Submissions per day line chart */}
          <section className="chart-card">
            <h2>Submissions per Day</h2>
            <Line
              data={{
                labels: timeline.map((t) => t.date),
                datasets: [
                  {
                    label: 'Submissions',
                    data: timeline.map((t) => t.submissions),
                    borderColor: 'rgb(16, 185, 129)',
                    backgroundColor: 'rgba(16, 185, 129, 0.1)',
                    tension: 0.3,
                    fill: true,
                    pointRadius: 3,
                  },
                ],
              }}
              options={{
                responsive: true,
                plugins: {
                  legend: { display: false },
                  title: { display: false },
                },
                scales: {
                  y: { beginAtZero: true, ticks: { stepSize: 1 } },
                },
              }}
            />
          </section>

          {/* Pass rates table */}
          <section className="table-card">
            <h2>Pass Rates by Task</h2>
            <table>
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Avg Score</th>
                  <th>Attempts</th>
                </tr>
              </thead>
              <tbody>
                {passRates.map((row) => (
                  <tr key={row.task}>
                    <td>{row.task}</td>
                    <td>
                      {row.avg_score !== null ? `${row.avg_score}%` : '—'}
                    </td>
                    <td>{row.attempts}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
        </div>
      )}
    </div>
  )
}
