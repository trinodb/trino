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
import { QueryInfo } from '../api/webapp/api.ts'
import { parseDuration } from './utils.ts'

// Window over which a query's CPU rate is measured; the same 10 s as the app bar's cluster CPU. The list reports
// totalCpuTime as a rounded duration string ("1.25h", "3.40d": two decimals of the largest
// unit), so long-running queries move in coarse steps that the window averages out.
export const CPU_AVERAGE_WINDOW_MS = 10_000
const WINDOW_MS = CPU_AVERAGE_WINDOW_MS
// Shorter spans than this give too noisy a rate to show
const MIN_SPAN_MS = 3_000

// Per query: (time, cumulative CPU seconds) samples within the window. Kept for the page's lifetime, so the
// window survives leaving and returning to the dashboard; a reload starts over.
const samplesByQuery = new Map<string, { time: number; cpuSecs: number }[]>()

// Records this poll's CPU time of every listed query and sets queryInfo.cpuShare: the share of all cluster cores
// (0-100) it used over the window. A query on 40 nodes using 4% of each is 4% of a 40-node cluster.
export const updateQueryCpuShares = (queries: QueryInfo[], clusterCores: number) => {
    const now = Date.now()
    const listed = new Set<string>()
    for (const query of queries) {
        listed.add(query.queryId)
        const cpuSecs = (parseDuration(query.queryStats.totalCpuTime) ?? 0) / 1000
        const samples = [...(samplesByQuery.get(query.queryId) ?? []), { time: now, cpuSecs }].filter(
            (sample) => now - sample.time <= WINDOW_MS
        )
        samplesByQuery.set(query.queryId, samples)
        const first = samples[0]
        const span = now - first.time
        const done = query.state === 'FINISHED' || query.state === 'FAILED'
        query.cpuShare =
            done || span < MIN_SPAN_MS || clusterCores <= 0
                ? undefined
                : (Math.max(0, cpuSecs - first.cpuSecs) / (span / 1000) / clusterCores) * 100
    }
    for (const queryId of samplesByQuery.keys()) {
        if (!listed.has(queryId)) {
            samplesByQuery.delete(queryId)
        }
    }
}
