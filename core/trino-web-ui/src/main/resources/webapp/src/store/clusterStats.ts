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
import { create } from 'zustand'
import { ApiResponse } from '../api/base'
import { statsApi, Stats, workerApi, workerStatusApi } from '../api/webapp/api'
import { Texts } from '../constant'
import { MAX_HISTORY, addExponentiallyWeightedToHistory, addToHistory } from '../utils/utils'
import { CPU_AVERAGE_WINDOW_MS } from '../utils/queryCpuShare'
import { pollCoordinator } from '../utils/poll'

// Cluster stats: one request at a time, at most this often
const STATS_INTERVAL_MS = 1_500

export interface ClusterStatsHistory {
    runningQueries: number[]
    queuedQueries: number[]
    blockedQueries: number[]
    activeWorkers: number[]
    runningDrivers: number[]
    reservedMemory: number[]
    rowInputRate: number[]
    byteInputRate: number[]
    perWorkerCpuTimeRate: number[]
    clusterCpuPercent: number[]

    lastRefresh: number | null

    lastInputRows: number
    lastInputBytes: number
    lastCpuTime: number
    // Latest reported reservation, unsmoothed (the reservedMemory history is a moving average that starts at 0)
    lastReservedMemory: number
    // Cores of every node the coordinator tracks (coordinator included); the base of the per-query CPU share
    lastAvailableProcessors: number
    // Cumulative query CPU seconds counted only above the counter's high-water mark (cpuPeak), with sample times,
    // over the last CPU_WINDOW_MS; cpuBelowPeakSince is when the raw counter last fell below the mark
    cpuSamples: { time: number; cpuSecs: number }[]
    cpuPeak: number
    cpuBelowPeakSince: number | null
}

const initialFilledHistory: number[] = Array(MAX_HISTORY).fill(0)

// Window over which the cluster CPU rate is measured (same as the query and node CPU averages)
const CPU_WINDOW_MS = CPU_AVERAGE_WINDOW_MS
// Until a worker reports its query memory pool size, ask again at most this often
const POOL_RETRY_MS = 10_000

const initialHistory: ClusterStatsHistory = {
    runningQueries: initialFilledHistory,
    queuedQueries: initialFilledHistory,
    blockedQueries: initialFilledHistory,
    activeWorkers: initialFilledHistory,
    runningDrivers: initialFilledHistory,
    reservedMemory: initialFilledHistory,
    rowInputRate: initialFilledHistory,
    byteInputRate: initialFilledHistory,
    perWorkerCpuTimeRate: initialFilledHistory,
    clusterCpuPercent: initialFilledHistory,

    lastRefresh: null,

    lastInputRows: 0,
    lastInputBytes: 0,
    lastCpuTime: 0,
    lastReservedMemory: 0,
    lastAvailableProcessors: 0,
    cpuSamples: [],
    cpuPeak: 0,
    cpuBelowPeakSince: null,
}

const nextHistory = (prev: ClusterStatsHistory, stats: Stats): ClusterStatsHistory => {
    let rowInputRate = initialFilledHistory
    let byteInputRate = initialFilledHistory
    let perWorkerCpuTimeRate = initialFilledHistory
    if (prev.lastRefresh !== null) {
        const secsSinceRefresh = (Date.now() - prev.lastRefresh) / 1000.0 || 1
        rowInputRate = addExponentiallyWeightedToHistory(
            (stats.totalInputRows - prev.lastInputRows) / secsSinceRefresh,
            prev.rowInputRate
        )
        byteInputRate = addExponentiallyWeightedToHistory(
            (stats.totalInputBytes - prev.lastInputBytes) / secsSinceRefresh,
            prev.byteInputRate
        )
        perWorkerCpuTimeRate = addExponentiallyWeightedToHistory(
            (stats.totalCpuTimeSecs - prev.lastCpuTime) / (stats.activeWorkers || 1) / secsSinceRefresh,
            prev.perWorkerCpuTimeRate
        )
    }

    // Cluster CPU %: query CPU seconds per second over all cores the coordinator knows of (every node, coordinator
    // included). totalCpuTimeSecs is not monotonic: it is the CPU of finished queries (added by a state listener
    // after a query ends, and not at all when the final query info is missing) plus that of running ones, so it
    // dips whenever a query finishes. A per-second delta of it, smoothed with addExponentiallyWeightedToHistory
    // (which resets to 0 on any value below 1), dropped to 0% at every dip. Instead CPU is counted only above the
    // highest value seen, so a dip and its recovery add nothing; if the counter stays below that mark for a whole
    // window (a finished query whose CPU was never added), the mark moves down to it. The rate is taken over the
    // last CPU_WINDOW_MS.
    const now = Date.now()
    const lastSample = prev.cpuSamples[prev.cpuSamples.length - 1]
    const raw = stats.totalCpuTimeSecs
    let cpuTotal = lastSample ? lastSample.cpuSecs : 0
    let cpuPeak = lastSample ? prev.cpuPeak : raw
    let cpuBelowPeakSince: number | null = null
    if (raw >= cpuPeak) {
        cpuTotal += raw - cpuPeak
        cpuPeak = raw
    } else {
        cpuBelowPeakSince = prev.cpuBelowPeakSince ?? now
        if (now - cpuBelowPeakSince > CPU_WINDOW_MS) {
            cpuPeak = raw
            cpuBelowPeakSince = null
        }
    }
    const cpuSamples = [...prev.cpuSamples, { time: now, cpuSecs: cpuTotal }].filter(
        (sample) => now - sample.time <= CPU_WINDOW_MS
    )
    const windowStart = cpuSamples.length > 1 ? cpuSamples[0] : null

    return {
        // instantaneous stats
        runningQueries: addToHistory(stats.runningQueries, prev.runningQueries),
        queuedQueries: addToHistory(stats.queuedQueries, prev.queuedQueries),
        blockedQueries: addToHistory(stats.blockedQueries, prev.blockedQueries),
        activeWorkers: addToHistory(stats.activeWorkers, prev.activeWorkers),

        // moving averages
        runningDrivers: addExponentiallyWeightedToHistory(stats.runningDrivers, prev.runningDrivers),
        reservedMemory: addExponentiallyWeightedToHistory(stats.reservedMemory, prev.reservedMemory),

        // moving averages for diffs
        rowInputRate,
        byteInputRate,
        perWorkerCpuTimeRate,
        clusterCpuPercent:
            windowStart && now > windowStart.time
                ? addToHistory(
                      ((cpuTotal - windowStart.cpuSecs) /
                          ((now - windowStart.time) / 1000) /
                          (stats.totalAvailableProcessors || 1)) *
                          100,
                      prev.clusterCpuPercent
                  )
                : prev.clusterCpuPercent,

        lastInputRows: stats.totalInputRows,
        lastInputBytes: stats.totalInputBytes,
        lastCpuTime: stats.totalCpuTimeSecs,
        lastReservedMemory: stats.reservedMemory,
        lastAvailableProcessors: stats.totalAvailableProcessors,
        cpuSamples,
        cpuPeak,
        cpuBelowPeakSince,

        lastRefresh: now,
    }
}

export interface ClusterStatsStore {
    history: ClusterStatsHistory
    error: string | null
    // Whether the dashboard shows its metric cards; toggled from the summary in the app bar
    panelsOpen: boolean
    setPanelsOpen: (open: boolean) => void
    // Whether the dashboard shows the nodes table (server button in the app bar), and which node is expanded
    nodesOpen: boolean
    setNodesOpen: (open: boolean) => void
    expandedNodeId: string | null
    setExpandedNodeId: (nodeId: string | null) => void
    // Query memory pool of one worker (bytes), loaded once per page load; null until known or if no worker answered
    workerPoolBytes: number | null
    startPolling: () => void
}

// One poller for the whole app: the app bar summary and the dashboard cards read the same history
let polling = false

// Query memory pool size of one worker, read once per page load (kept in memory only, so a reload refreshes it).
// Workers in a cluster share one config, so cluster query memory is this times the active worker count. Tries a
// few workers in case the first does not answer.
const loadWorkerPoolBytes = async (): Promise<number | null> => {
    const listResponse = await workerApi()
    const workers = (listResponse.status === 200 && listResponse.data) || []
    // WorkerResource reports the state lowercase ("active" / "inactive")
    const active = workers.filter((w) => !w.coordinator && w.state.toLowerCase() === 'active')
    for (const worker of active.slice(0, 3)) {
        const statusResponse = await workerStatusApi(worker.nodeId)
        if (statusResponse.status === 200 && statusResponse.data) {
            return statusResponse.data.memoryInfo.pool.maxBytes
        }
    }
    return null
}

export const useClusterStatsStore = create<ClusterStatsStore>()((set, get) => ({
    history: initialHistory,
    error: null,
    panelsOpen: false,
    setPanelsOpen: (panelsOpen) => set({ panelsOpen }),
    nodesOpen: false,
    setNodesOpen: (nodesOpen) => set({ nodesOpen }),
    expandedNodeId: null,
    setExpandedNodeId: (expandedNodeId) => set({ expandedNodeId }),
    workerPoolBytes: null,
    startPolling: () => {
        if (polling) {
            return
        }
        polling = true
        // The pool size is read once known to be reachable: after a successful stats call (so not before sign-in),
        // retried at most every POOL_RETRY_MS until a worker answers
        let poolLoading = false
        let poolLastTry = 0
        const ensureWorkerPoolBytes = () => {
            if (get().workerPoolBytes !== null || poolLoading || Date.now() - poolLastTry < POOL_RETRY_MS) {
                return
            }
            poolLoading = true
            poolLastTry = Date.now()
            loadWorkerPoolBytes().then((workerPoolBytes) => {
                poolLoading = false
                set({ workerPoolBytes })
            })
        }
        pollCoordinator(
            () =>
                statsApi().then((apiResponse: ApiResponse<Stats>) => {
                    if (apiResponse.status === 200 && apiResponse.data) {
                        set({ history: nextHistory(get().history, apiResponse.data), error: null })
                        ensureWorkerPoolBytes()
                    } else {
                        set({ error: `${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}` })
                    }
                }),
            STATS_INTERVAL_MS
        )
    },
}))
