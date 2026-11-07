/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { Box, ButtonBase, Divider } from '@mui/material'
import { alpha } from '@mui/material/styles'
import { useLocation, useNavigate } from 'react-router-dom'
import { useClusterStatsStore } from '../store'
import { formatDataSizeBytesWhole } from '../utils/utils.ts'

// Below this width the full labels would not fit on one line, so they shrink to their first letter. Full labels
// measure ~395px with small counts; the margin covers larger counts and memory values.
const FULL_LABELS_MIN_WIDTH = 440

const SummaryItem = ({
    label,
    short = label.charAt(0),
    title,
    value,
}: {
    label: string
    short?: string
    title?: string
    value: string | number
}) => (
    <Box
        component="span"
        title={title}
        sx={{ display: 'inline-flex', alignItems: 'baseline', gap: 0.75, whiteSpace: 'nowrap' }}
    >
        <Box component="span" sx={(theme) => ({ color: alpha(theme.palette.common.white, 0.8), fontSize: 12 })}>
            <span className="label-full">{label}</span>
            <span className="label-short">{short}</span>
        </Box>
        <Box component="span" sx={{ fontWeight: 500, fontSize: 14 }}>
            {value}
        </Box>
    </Box>
)

// Live cluster numbers in the app bar. Clicking opens the dashboard metric cards (navigating there first if needed)
export const ClusterSummary = () => {
    const { history, panelsOpen, setPanelsOpen, workerPoolBytes } = useClusterStatsStore()
    const location = useLocation()
    const navigate = useNavigate()
    const onDashboard = location.pathname === '/dashboard'
    // Histories are pre-filled, so the last entry is always the latest sample
    const latest = (values: number[]) => values[values.length - 1]
    // CPU is a rate: shown once the samples cover a few seconds, as a shorter window magnifies counter noise
    const cpuSamples = history.cpuSamples
    const lastRefreshKnown =
        cpuSamples.length > 1 && cpuSamples[cpuSamples.length - 1].time - cpuSamples[0].time >= 5000
    // 100% RAM: active workers (coordinator excluded unless it schedules work) times one worker's query memory pool
    const workers = latest(history.activeWorkers)
    const clusterQueryMemory = workerPoolBytes !== null && workers > 0 ? workers * workerPoolBytes : null

    const onClick = () => {
        if (onDashboard) {
            setPanelsOpen(!panelsOpen)
        } else {
            setPanelsOpen(true)
            navigate('/dashboard')
        }
    }

    return (
        <Box
            sx={{
                flex: '1 1 auto',
                minWidth: 0,
                mx: { xs: 0.5, sm: 1.5 },
                overflow: 'hidden',
                containerType: 'inline-size',
            }}
        >
            <ButtonBase
                onClick={onClick}
                aria-expanded={onDashboard && panelsOpen}
                aria-controls="dashboard-metric-panels"
                sx={{
                    gap: 1.5,
                    px: { xs: 0.5, sm: 1 },
                    py: 0.5,
                    borderRadius: 1,
                    color: 'inherit',
                    '&:hover': (theme) => ({ bgcolor: alpha(theme.palette.common.white, 0.08) }),
                    '& .label-short': { display: 'none' },
                    [`@container (max-width: ${FULL_LABELS_MIN_WIDTH}px)`]: {
                        gap: 0.75,
                        // Label and value closer together, so CPU and RAM still fit beside the query counts
                        '& > span': { gap: 0.4 },
                        '& .label-full': { display: 'none' },
                        '& .label-short': { display: 'inline' },
                    },
                }}
            >
                <SummaryItem label="Workers" value={latest(history.activeWorkers)} />
                <SummaryItem
                    label="CPU"
                    short="C"
                    title="Query CPU time per second, of all cluster cores"
                    value={lastRefreshKnown ? `${Math.round(latest(history.clusterCpuPercent))}%` : '—'}
                />
                <SummaryItem
                    label="RAM"
                    // R is taken by Running
                    short="M"
                    title={
                        clusterQueryMemory === null
                            ? 'Memory reserved by queries'
                            : `Memory reserved by queries: ${formatDataSizeBytesWhole(history.lastReservedMemory)} of ${formatDataSizeBytesWhole(clusterQueryMemory)} (workers × query memory pool)`
                    }
                    value={
                        clusterQueryMemory === null
                            ? '—'
                            : `${Math.round((history.lastReservedMemory * 100) / clusterQueryMemory)}%`
                    }
                />
                <Divider
                    orientation="vertical"
                    flexItem
                    sx={(theme) => ({ borderColor: alpha(theme.palette.common.white, 0.4) })}
                />
                <SummaryItem label="Running" value={latest(history.runningQueries)} />
                <SummaryItem label="Queued" value={latest(history.queuedQueries)} />
                <SummaryItem label="Blocked" value={latest(history.blockedQueries)} />
            </ButtonBase>
        </Box>
    )
}
