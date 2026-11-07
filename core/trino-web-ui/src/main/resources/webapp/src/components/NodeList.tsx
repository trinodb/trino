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
import { Fragment, ReactNode, useEffect, useState } from 'react'
import { BreakdownRow, BreakdownSegment } from './BreakdownRow.tsx'
import { WorkerThreadSnapshot } from './WorkerThreadSnapshot.tsx'
import { CPU_AVERAGE_WINDOW_MS } from '../utils/queryCpuShare.ts'
import { Link as RouterLink } from 'react-router-dom'
import { useClusterStatsStore } from '../store'
import { Box, ButtonBase, Collapse, Table, TableBody, TableCell, TableHead, TableRow, Typography } from '@mui/material'
import { styled } from '@mui/material/styles'
import { blue, grey, orange, red, teal } from '@mui/material/colors'
import { MemoryUsagePool, Worker, WorkerStatusInfo, workerApi, workerStatusApi } from '../api/webapp/api.ts'
import { Texts } from '../constant.ts'
import { useSnackbar } from './SnackbarContext.ts'
import { formatDataSize, formatDurationClock, parseDuration } from '../utils/utils.ts'

// One status request per node per refresh; slower than the query list since a cluster can have many nodes
const REFRESH_INTERVAL_MS = 3000
// The table shows this many nodes; the rest scroll
const VISIBLE_NODES = 6
const HEADER_HEIGHT = 37
const ROW_HEIGHT = 32

// A worker from the list, with its latest status when the node answered
interface WorkerRow {
    worker: Worker
    status: WorkerStatusInfo | null
}

const StyledLink = styled(RouterLink)(({ theme }) => ({
    textDecoration: 'none',
    color: theme.palette.info.main,
    '&:hover': {
        textDecoration: 'underline',
    },
}))

const formatPercent = (value: number) => `${Math.round(value)}%`

// Shared by the table bars and the breakdown bars in the details. Real colours, not palette paths: breakdown
// segments pick their label colour from them, and the dark theme's info colour is grey (CPU must stay blue).
const BAR_COLORS = {
    trinoCpu: blue[500],
    processCpu: blue[200],
    otherCpu: grey[500],
    reserved: orange[700],
    revocable: red[400],
    heap: teal[500],
    freeForQueries: grey[50],
    free: grey[300],
    track: 'action.hover',
} as const

// Load of one resource as a stacked bar over an empty track, with the main share as text
const UsageBar = (props: { segments: { label: string; percent: number; color: string }[]; text: string }) => (
    <Box
        component="span"
        title={props.segments.map((segment) => `${segment.label} ${formatPercent(segment.percent)}`).join(' · ')}
        sx={{ display: 'inline-flex', alignItems: 'center', gap: 1 }}
    >
        <Box
            component="span"
            sx={{
                display: 'flex',
                width: 96,
                height: 8,
                borderRadius: 0.5,
                overflow: 'hidden',
                bgcolor: BAR_COLORS.track,
            }}
        >
            {props.segments.map((segment) => (
                <Box
                    key={segment.label}
                    component="span"
                    sx={{ width: `${Math.min(100, segment.percent)}%`, bgcolor: segment.color }}
                />
            ))}
        </Box>
        <Box component="span" sx={{ width: 32, textAlign: 'right' }}>
            {props.text}
        </Box>
    </Box>
)

// The node reports its own process and the whole machine; the machine figure minus Trino is everything else
const otherCpuPercent = (status: WorkerStatusInfo) => Math.max(0, (status.systemCpuLoad - status.processCpuLoad) * 100)

// Heap in use as the memory bars draw it: what the heap reports, but never less than what queries account for
// (estimates and GC timing can put accounting ahead), and never more than the heap maximum
const usedHeapBytes = (status: WorkerStatusInfo) => {
    const pool = status.memoryInfo.pool
    return Math.min(status.heapAvailable, Math.max(status.heapUsed, pool.reservedBytes + pool.reservedRevocableBytes))
}
const memoryUsedPercent = (status: WorkerStatusInfo) => (usedHeapBytes(status) * 100) / status.heapAvailable

// One node's memory on the JVM heap maximum, as disjoint segments that sum to it (see WorkerDetails for how the
// numbers relate). Used by the table's Memory column and the expanded node's Memory breakdown.
const memorySegments = (status: WorkerStatusInfo): BreakdownSegment[] => {
    const pool = status.memoryInfo.pool
    const queryBytes = pool.reservedBytes + pool.reservedRevocableBytes
    const usedHeap = usedHeapBytes(status)
    const freeHeap = status.heapAvailable - usedHeap
    // The pool's free budget may be larger than the heap actually free right now; queries can only use the smaller
    const poolFree = Math.max(0, pool.maxBytes - queryBytes)
    const freeForQueries = Math.min(freeHeap, poolFree)
    return [
        {
            label: 'Queries',
            short: 'Q',
            description: 'Reserved by running queries (Trino’s accounting of what they hold)',
            color: BAR_COLORS.reserved,
            value: pool.reservedBytes,
        },
        {
            label: 'Queries, revocable',
            short: 'Rev',
            description: 'Held by queries but released by spilling to disk when memory is short',
            color: BAR_COLORS.revocable,
            value: pool.reservedRevocableBytes,
        },
        {
            label: 'Other heap',
            short: 'H',
            description: 'Heap in use outside query accounting: untracked allocations and garbage not yet collected',
            color: BAR_COLORS.heap,
            value: usedHeap - queryBytes,
        },
        {
            label: 'Free for queries',
            short: 'FQ',
            description: `Free heap that queries may still reserve (the pool reports ${formatDataSize(poolFree)} left of its ${formatDataSize(pool.maxBytes)})`,
            color: BAR_COLORS.freeForQueries,
            value: freeForQueries,
        },
        {
            label: 'Free heap, headroom',
            short: 'F',
            description: 'Free heap kept back from queries for untracked allocations (memory.heap-headroom-per-node)',
            color: BAR_COLORS.free,
            value: freeHeap - freeForQueries,
        },
    ]
}

// Column definitions: label, cell, and the value it sorts by. Status columns sort nodes that did not answer last.
const COLUMNS: {
    key: string
    label: string
    title?: string
    align?: 'right'
    value: (row: WorkerRow) => string | number | null
    cell: (row: WorkerRow) => ReactNode
}[] = [
    { key: 'NODE', label: 'Node', value: (r) => r.worker.nodeId, cell: (r) => r.worker.nodeId },
    { key: 'ADDRESS', label: 'Address', value: (r) => r.worker.nodeIp, cell: (r) => r.worker.nodeIp },
    {
        key: 'ROLE',
        label: 'Role',
        // Coordinator first when ascending
        value: (r) => (r.worker.coordinator ? 0 : 1),
        cell: (r) => (r.worker.coordinator ? 'Coordinator' : 'Worker'),
    },
    { key: 'STATE', label: 'State', value: (r) => r.worker.state, cell: (r) => r.worker.state },
    {
        key: 'CPU',
        label: 'CPU',
        title: 'Share of all cores: Trino (blue) and other processes on the machine (grey). Sorts by Trino.',
        value: (r) => r.status && r.status.processCpuLoad * 100,
        cell: (r) =>
            r.status ? (
                <UsageBar
                    text={formatPercent(r.status.processCpuLoad * 100)}
                    segments={[
                        { label: 'Trino', percent: r.status.processCpuLoad * 100, color: BAR_COLORS.trinoCpu },
                        { label: 'Other', percent: otherCpuPercent(r.status), color: BAR_COLORS.otherCpu },
                    ]}
                />
            ) : (
                '—'
            ),
    },
    {
        key: 'MEMORY',
        label: 'Memory',
        title: 'JVM heap: queries (orange), revocable query memory (red), other heap (teal), free for queries (white), headroom (grey). Sorts by heap in use.',
        value: (r) => r.status && memoryUsedPercent(r.status),
        cell: (r) =>
            r.status ? (
                <UsageBar
                    text={formatPercent(memoryUsedPercent(r.status))}
                    segments={memorySegments(r.status).map((segment) => ({
                        label: segment.label,
                        percent: (segment.value * 100) / r.status!.heapAvailable,
                        color: segment.color,
                    }))}
                />
            ) : (
                '—'
            ),
    },
    {
        key: 'UPTIME',
        label: 'Uptime',
        align: 'right',
        value: (r) => r.status && parseDuration(r.status.uptime),
        cell: (r) => (r.status ? formatDurationClock(parseDuration(r.status.uptime)) : '—'),
    },
]

const compareRows = (key: string, ascending: boolean) => {
    const column = COLUMNS.find((c) => c.key === key) ?? COLUMNS[0]
    return (a: WorkerRow, b: WorkerRow) => {
        const va = column.value(a)
        const vb = column.value(b)
        if (va === null || vb === null) {
            return va === vb ? a.worker.nodeId.localeCompare(b.worker.nodeId) : va === null ? 1 : -1
        }
        const order = typeof va === 'number' && typeof vb === 'number' ? va - vb : String(va).localeCompare(String(vb))
        return order === 0 ? a.worker.nodeId.localeCompare(b.worker.nodeId) : ascending ? order : -order
    }
}

// Compact rows like the query page tables; the label column keeps its width
const DETAILS_TABLE_SX = { '& td': { py: 0.75, px: 1 }, '& td:first-of-type': { width: 130 } }

// Queries holding pool memory on the node. Revocable memory is only reported per task (task IDs start with the
// query ID: "<queryId>.<stage>.<partition>.<attempt>"), so it is summed per query here.
const poolQueries = (pool: MemoryUsagePool) => {
    const revocable: Record<string, number> = {}
    for (const [taskId, bytes] of Object.entries(pool.taskMemoryRevocableReservations ?? {})) {
        const queryId = taskId.split('.')[0]
        revocable[queryId] = (revocable[queryId] ?? 0) + bytes
    }
    return [...new Set([...Object.keys(pool.queryMemoryReservations ?? {}), ...Object.keys(revocable)])]
        .map((queryId) => ({
            queryId,
            reserved: pool.queryMemoryReservations?.[queryId] ?? 0,
            revocable: revocable[queryId] ?? 0,
        }))
        .filter((query) => query.reserved + query.revocable > 0)
        .sort((a, b) => b.reserved + b.revocable - (a.reserved + a.revocable))
}

// Expanded part of a row: CPU and memory breakdowns, and the queries holding memory on the node.
//
// How the node's numbers relate (Trino StatusResource, LocalMemoryManager, MemoryPool):
// - heapAvailable is the JVM heap maximum and heapUsed what the heap holds now, live objects and garbage not yet
//   collected alike.
// - The query memory pool is not separate memory: it is a budget carved out of the heap, its size being the heap
//   maximum minus memory.heap-headroom-per-node (30% of the heap by default). Reserved and revocable bytes are
//   Trino's own accounting of what operators hold, and those objects live on the heap, so they are part of
//   heapUsed. Free query memory is budget, not a region of the heap, so the bar splits the free heap instead: the
//   part queries may still reserve, and the rest (held back as headroom). Every segment is disjoint and they sum to
//   the heap maximum.
// - Non-heap (class metadata, compiled code) is outside the heap maximum and not on this scale.
const WorkerDetails = (props: { status: WorkerStatusInfo | null }) => {
    const { status } = props
    // Cluster query CPU share from the app bar's stats poll, once its 10 s window has enough samples
    const clusterQueryCpuPercent = useClusterStatsStore(({ history }) => {
        const samples = history.cpuSamples
        const ready = samples.length > 1 && samples[samples.length - 1].time - samples[0].time >= 5000
        return ready ? history.clusterCpuPercent[history.clusterCpuPercent.length - 1] : null
    })
    if (!status) {
        return (
            <Typography variant="body2" sx={{ py: 1.5, color: 'text.secondary' }}>
                {Texts.Error.NodeInformationNotLoaded}
            </Typography>
        )
    }
    const pool = status.memoryInfo.pool
    const queries = poolQueries(pool)
    const trinoCores = status.processCpuLoad * status.processors
    const otherCores = (otherCpuPercent(status) / 100) * status.processors
    // No request reports query CPU per node; estimate it from the cluster's query CPU share, capped at Trino's use
    const queryCores =
        clusterQueryCpuPercent === null
            ? null
            : Math.min(trinoCores, (clusterQueryCpuPercent / 100) * status.processors)
    const formatCores = (cores: number) => `${cores.toFixed(1)} cores`

    return (
        <Box sx={{ py: 1.5, display: 'flex', flexDirection: 'column', gap: 1.5 }}>
            <Typography variant="body2" sx={{ color: 'text.secondary' }}>
                External {status.externalAddress} · Internal {status.internalAddress} · {status.processors} cores ·{' '}
                {formatDataSize(status.heapAvailable)} heap · {formatDataSize(status.nonHeapUsed)} non-heap ·{' '}
                {queries.length} {queries.length === 1 ? 'query' : 'queries'} holding memory · version{' '}
                {status.nodeVersion.version}
            </Typography>
            <Table size="small" sx={DETAILS_TABLE_SX}>
                <TableBody>
                    <BreakdownRow
                        keepOrder
                        label="CPU"
                        formatValue={formatCores}
                        segments={[
                            ...(queryCores === null
                                ? [
                                      {
                                          label: 'Trino',
                                          description: 'This node’s Trino process',
                                          color: BAR_COLORS.trinoCpu,
                                          value: trinoCores,
                                      },
                                  ]
                                : [
                                      {
                                          label: 'Query CPU',
                                          short: 'Q',
                                          description:
                                              'Estimate: the cluster-wide query CPU share (app bar CPU, last 10 s) applied to this node’s cores. Assumes queries spread evenly; a skewed query makes this node busier than this.',
                                          color: BAR_COLORS.trinoCpu,
                                          value: queryCores,
                                      },
                                      {
                                          label: 'Process CPU',
                                          short: 'P',
                                          description:
                                              'The rest of the Trino process: garbage collection, JIT compilation, data exchange and compression, and any query work above the estimate',
                                          color: BAR_COLORS.processCpu,
                                          value: trinoCores - queryCores,
                                      },
                                  ]),
                            {
                                label: 'Other processes',
                                description: 'Everything else on the machine (system load minus Trino)',
                                color: BAR_COLORS.otherCpu,
                                value: otherCores,
                            },
                            {
                                label: 'Idle',
                                description: 'Unused cores',
                                color: BAR_COLORS.free,
                                value: Math.max(0, status.processors - trinoCores - otherCores),
                            },
                        ]}
                    />
                    <BreakdownRow
                        keepOrder
                        label="Memory"
                        formatValue={formatDataSize}
                        segments={memorySegments(status)}
                    />
                </TableBody>
            </Table>
            {queries.length === 0 ? (
                <Typography variant="body2" sx={{ color: 'text.secondary' }}>
                    No queries hold memory on this node
                </Typography>
            ) : (
                <Box sx={{ overflowX: 'auto' }}>
                    <Table size="small" sx={{ '& td, & th': { px: 1, whiteSpace: 'nowrap' } }}>
                        <TableHead>
                            <TableRow>
                                <TableCell sx={{ color: 'text.secondary' }}>Query</TableCell>
                                <TableCell align="right" sx={{ color: 'text.secondary' }}>
                                    Of query memory
                                </TableCell>
                                <TableCell align="right" sx={{ color: 'text.secondary' }}>
                                    Reserved
                                </TableCell>
                                <TableCell align="right" sx={{ color: 'text.secondary' }}>
                                    Revocable
                                </TableCell>
                            </TableRow>
                        </TableHead>
                        <TableBody>
                            {queries.map((query) => (
                                <TableRow key={query.queryId}>
                                    <TableCell>
                                        <StyledLink to={`/queries/${query.queryId}`}>{query.queryId}</StyledLink>
                                    </TableCell>
                                    <TableCell align="right">
                                        {formatPercent(((query.reserved + query.revocable) * 100) / pool.maxBytes)}
                                    </TableCell>
                                    <TableCell align="right">{formatDataSize(query.reserved)}</TableCell>
                                    <TableCell align="right">{formatDataSize(query.revocable)}</TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </Box>
            )}
            {/* Captured on demand only (a button), so it adds no polling */}
            <WorkerThreadSnapshot nodeId={status.nodeId} />
        </Box>
    )
}

// All nodes in one table, sortable by any column; clicking a node expands its details in place. Shown on the
// dashboard from the server button in the app bar; the expanded node lives in the cluster store so links to a
// node (e.g. a task's host on the query page) can open the table with it expanded. Mounted only while shown, so
// node statuses are fetched only then.
export const NodeList = () => {
    const { showSnackbar } = useSnackbar()
    const { expandedNodeId, setExpandedNodeId } = useClusterStatsStore()
    const [rows, setRows] = useState<WorkerRow[]>([])
    // False until the first list and status requests finish, so an empty table reads as loading, not as no nodes
    const [loaded, setLoaded] = useState(false)
    const [sortKey, setSortKey] = useState('ROLE')
    const [ascending, setAscending] = useState(true)

    useEffect(() => {
        // Per node: raw CPU loads with their times, for the 10 s average (lives as long as the panel is open)
        const cpuSamples = new Map<string, { time: number; process: number; system: number }[]>()
        let cancelled = false
        let timeoutId: number
        const refresh = async () => {
            // One status request per node is the heaviest poll in the UI: skip it while the tab is hidden
            if (document.hidden) {
                timeoutId = window.setTimeout(refresh, REFRESH_INTERVAL_MS)
                return
            }
            const listResponse = await workerApi()
            if (cancelled) {
                return
            }
            if (listResponse.status !== 200 || !listResponse.data) {
                showSnackbar(`${Texts.Error.Communication} ${listResponse.status}: ${listResponse.message}`, 'error')
            } else {
                const workers = listResponse.data
                const statuses = await Promise.all(workers.map((worker) => workerStatusApi(worker.nodeId)))
                if (cancelled) {
                    return
                }
                const now = Date.now()
                const nextRows = workers.map((worker, index) => {
                    const status = statuses[index].status === 200 && statuses[index].data ? statuses[index].data : null
                    if (status) {
                        // Report CPU as the average of this node's samples over the last 10 s, like the query and
                        // cluster CPU figures, instead of the node's own short "recent load"
                        const samples = [
                            ...(cpuSamples.get(worker.nodeId) ?? []),
                            { time: now, process: status.processCpuLoad, system: status.systemCpuLoad },
                        ].filter((sample) => now - sample.time <= CPU_AVERAGE_WINDOW_MS)
                        cpuSamples.set(worker.nodeId, samples)
                        const average = (pick: (sample: (typeof samples)[number]) => number) =>
                            samples.reduce((sum, sample) => sum + pick(sample), 0) / samples.length
                        status.processCpuLoad = average((sample) => sample.process)
                        status.systemCpuLoad = average((sample) => sample.system)
                    }
                    return { worker, status }
                })
                setRows(nextRows)
            }
            setLoaded(true)
            timeoutId = window.setTimeout(refresh, REFRESH_INTERVAL_MS)
        }
        refresh()
        return () => {
            cancelled = true
            window.clearTimeout(timeoutId)
        }
    }, [showSnackbar])

    const sortBy = (key: string) => {
        if (key === sortKey) {
            setAscending(!ascending)
        } else {
            setSortKey(key)
            setAscending(true)
        }
    }

    const toggle = (nodeId: string) => setExpandedNodeId(nodeId === expandedNodeId ? null : nodeId)

    // Height of the open node's details, so the scroll window can grow by exactly that much
    const [detailsElement, setDetailsElement] = useState<HTMLElement | null>(null)
    const [detailsHeight, setDetailsHeight] = useState(0)
    useEffect(() => {
        if (!detailsElement) {
            return
        }
        const observer = new ResizeObserver(() => setDetailsHeight(detailsElement.offsetHeight))
        observer.observe(detailsElement)
        return () => {
            observer.disconnect()
            setDetailsHeight(0)
        }
    }, [detailsElement])

    const sortedRows = [...rows].sort(compareRows(sortKey, ascending))

    return (
        // Scrolls sideways on narrow screens; the page itself stays at the screen width. The container size lets
        // expanded details match the visible width (100cqw).
        <Box
            sx={{
                overflowX: 'auto',
                // Six nodes visible, the rest scroll under the sticky header; an open node's details add their own
                // height so they are never squeezed into the six-row window
                overflowY: 'auto',
                maxHeight: HEADER_HEIGHT + VISIBLE_NODES * ROW_HEIGHT + detailsHeight,
                containerType: 'inline-size',
                mb: 1,
                borderBottom: 1,
                borderColor: 'divider',
            }}
        >
            <Table
                size="small"
                stickyHeader
                sx={{
                    minWidth: 960,
                    '& td, & th': { px: 1, fontSize: 13, whiteSpace: 'nowrap' },
                }}
            >
                <TableHead>
                    <TableRow sx={{ height: HEADER_HEIGHT }}>
                        {COLUMNS.map((column) => (
                            <TableCell
                                key={column.key}
                                align={column.align}
                                aria-sort={
                                    sortKey === column.key ? (ascending ? 'ascending' : 'descending') : undefined
                                }
                            >
                                <ButtonBase
                                    disableRipple
                                    title={column.title}
                                    onClick={() => sortBy(column.key)}
                                    sx={{
                                        font: 'inherit',
                                        fontWeight: 500,
                                        color: sortKey === column.key ? 'text.primary' : 'text.secondary',
                                        '&:hover': { color: 'text.primary' },
                                    }}
                                >
                                    {column.label}
                                    {/* ↕ marks every sortable column; the active one shows its direction */}
                                    <Box component="span" sx={{ ml: 0.5, opacity: sortKey === column.key ? 1 : 0.5 }}>
                                        {sortKey === column.key ? (ascending ? '▲' : '▼') : '↕'}
                                    </Box>
                                </ButtonBase>
                            </TableCell>
                        ))}
                    </TableRow>
                </TableHead>
                <TableBody>
                    {sortedRows.map((row) => {
                        const expanded = row.worker.nodeId === expandedNodeId
                        return (
                            <Fragment key={row.worker.nodeId}>
                                <TableRow
                                    hover
                                    onClick={() => toggle(row.worker.nodeId)}
                                    aria-expanded={expanded}
                                    selected={expanded}
                                    sx={{
                                        height: ROW_HEIGHT,
                                        cursor: 'pointer',
                                        '& td': { borderBottom: expanded ? 0 : undefined },
                                        color: row.status ? undefined : 'text.disabled',
                                    }}
                                >
                                    {COLUMNS.map((column, index) => (
                                        <TableCell
                                            key={column.key}
                                            align={column.align}
                                            sx={{
                                                color: 'inherit',
                                                fontWeight: index === 0 ? 500 : undefined,
                                            }}
                                        >
                                            {index === 0 && (
                                                <Box component="span" sx={{ mr: 0.75, color: 'text.secondary' }}>
                                                    {expanded ? '▾' : '▸'}
                                                </Box>
                                            )}
                                            {column.cell(row)}
                                        </TableCell>
                                    ))}
                                </TableRow>
                                <TableRow>
                                    <TableCell
                                        colSpan={COLUMNS.length}
                                        sx={{ py: 0, ...(expanded ? {} : { p: 0, height: 0, borderBottom: 0 }) }}
                                    >
                                        <Collapse in={expanded} unmountOnExit>
                                            {/* Pinned to the visible width so details stay readable while the
                                                table is scrolled sideways */}
                                            <Box
                                                // Only the open node reports height; a closing one is still
                                                // mounted while it animates out
                                                ref={expanded ? setDetailsElement : undefined}
                                                sx={{
                                                    position: 'sticky',
                                                    left: 8,
                                                    width: 'calc(100cqw - 16px)',
                                                    whiteSpace: 'normal',
                                                }}
                                            >
                                                <WorkerDetails status={row.status} />
                                            </Box>
                                        </Collapse>
                                    </TableCell>
                                </TableRow>
                            </Fragment>
                        )
                    })}
                </TableBody>
            </Table>
            {rows.length === 0 && (
                <Typography variant="body2" sx={{ py: 2, color: 'text.secondary' }}>
                    {loaded ? 'No workers' : 'Loading nodes…'}
                </Typography>
            )}
        </Box>
    )
}
