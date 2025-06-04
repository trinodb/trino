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
import { useEffect, useState } from 'react'
import Typography from '@mui/material/Typography'
import { Box, Divider, Grid2 as Grid } from '@mui/material'
import { MetricCard } from './MetricCard.tsx'
import { QueryList } from './QueryList.tsx'
import { useSnackbar } from './SnackbarContext.ts'
import { ApiResponse } from '../api/base.ts'
import { statsApi, Stats } from '../api/webapp/api.ts'
import { Texts } from '../constant.ts'
import {
    MAX_HISTORY,
    addExponentiallyWeightedToHistory,
    addToHistory,
    formatCount,
    formatDataSizeBytes,
    precisionRound,
} from '../utils/utils.ts'

interface ClusterStats {
    runningQueries: number[]
    queuedQueries: number[]
    blockedQueries: number[]
    activeWorkers: number[]
    runningDrivers: number[]
    reservedMemory: number[]
    rowInputRate: number[]
    byteInputRate: number[]
    perWorkerCpuTimeRate: number[]

    lastRefresh: number | null

    lastInputRows: number
    lastInputBytes: number
    lastCpuTime: number
}

export const Dashboard = () => {
    const { showSnackbar } = useSnackbar()
    const initialFilledHistory = Array(MAX_HISTORY).fill(0)
    const [clusterStats, setClusterStats] = useState<ClusterStats>({
        runningQueries: initialFilledHistory,
        queuedQueries: initialFilledHistory,
        blockedQueries: initialFilledHistory,
        activeWorkers: initialFilledHistory,
        runningDrivers: initialFilledHistory,
        reservedMemory: initialFilledHistory,
        rowInputRate: initialFilledHistory,
        byteInputRate: initialFilledHistory,
        perWorkerCpuTimeRate: initialFilledHistory,

        lastRefresh: null,

        lastInputRows: 0,
        lastInputBytes: 0,
        lastCpuTime: 0,
    })
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        const runLoop = () => {
            getClusterStats()
            setTimeout(runLoop, 1000)
        }
        runLoop()
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [])

    useEffect(() => {
        if (error) {
            showSnackbar(error, 'error')
        }
    }, [error, showSnackbar])

    const getClusterStats = () => {
        setError(null)
        statsApi().then((apiResponse: ApiResponse<Stats>) => {
            if (apiResponse.status === 200 && apiResponse.data) {
                const newClusterStats: Stats = apiResponse.data
                setClusterStats((prevClusterStats) => {
                    let newRowInputRate: number[] = initialFilledHistory
                    let newByteInputRate: number[] = initialFilledHistory
                    let newPerWorkerCpuTimeRate: number[] = initialFilledHistory
                    if (prevClusterStats.lastRefresh !== null) {
                        const rowsInputSinceRefresh = newClusterStats.totalInputRows - prevClusterStats.lastInputRows
                        const bytesInputSinceRefresh = newClusterStats.totalInputBytes - prevClusterStats.lastInputBytes
                        const cpuTimeSinceRefresh = newClusterStats.totalCpuTimeSecs - prevClusterStats.lastCpuTime
                        const secsSinceRefresh = (Date.now() - prevClusterStats.lastRefresh) / 1000.0

                        newRowInputRate = addExponentiallyWeightedToHistory(
                            rowsInputSinceRefresh / (secsSinceRefresh || 1),
                            prevClusterStats.rowInputRate
                        )
                        newByteInputRate = addExponentiallyWeightedToHistory(
                            bytesInputSinceRefresh / (secsSinceRefresh || 1),
                            prevClusterStats.byteInputRate
                        )
                        newPerWorkerCpuTimeRate = addExponentiallyWeightedToHistory(
                            cpuTimeSinceRefresh / (newClusterStats.activeWorkers || 1) / (secsSinceRefresh || 1),
                            prevClusterStats.perWorkerCpuTimeRate
                        )
                    }

                    return {
                        // instantaneous stats
                        runningQueries: addToHistory(newClusterStats.runningQueries, prevClusterStats.runningQueries),
                        queuedQueries: addToHistory(newClusterStats.queuedQueries, prevClusterStats.queuedQueries),
                        blockedQueries: addToHistory(newClusterStats.blockedQueries, prevClusterStats.blockedQueries),
                        activeWorkers: addToHistory(newClusterStats.activeWorkers, prevClusterStats.activeWorkers),

                        // moving averages
                        runningDrivers: addExponentiallyWeightedToHistory(
                            newClusterStats.runningDrivers,
                            prevClusterStats.runningDrivers
                        ),
                        reservedMemory: addExponentiallyWeightedToHistory(
                            newClusterStats.reservedMemory,
                            prevClusterStats.reservedMemory
                        ),

                        // moving averages for diffs
                        rowInputRate: newRowInputRate,
                        byteInputRate: newByteInputRate,
                        perWorkerCpuTimeRate: newPerWorkerCpuTimeRate,

                        lastInputRows: newClusterStats.totalInputRows,
                        lastInputBytes: newClusterStats.totalInputBytes,
                        lastCpuTime: newClusterStats.totalCpuTimeSecs,

                        lastRefresh: Date.now(),
                    }
                })
                setError(null)
            } else {
                setError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`)
            }
        })
    }

    return (
        <>
            <Box sx={{ pb: 2 }}>
                <Typography variant="h4">Cluster overview</Typography>
            </Box>
            <>
                <Grid container spacing={3}>
                    <Grid size={{ xs: 12, sm: 12, md: 6, lg: 4 }}>
                        <MetricCard
                            title="Running queries"
                            values={clusterStats.runningQueries}
                            tooltip="Total number of queries currently running"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, sm: 12, md: 6, lg: 4 }}>
                        <MetricCard
                            title="Active workers"
                            values={clusterStats.activeWorkers}
                            link="/workers"
                            tooltip="Total number of active worker nodes"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, sm: 12, md: 6, lg: 4 }}>
                        <MetricCard
                            title="Input rows/s"
                            values={clusterStats.rowInputRate}
                            numberFormatter={formatCount}
                            tooltip="Moving average of input rows processed per second"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, sm: 12, md: 6, lg: 4 }}>
                        <MetricCard
                            title="Queued queries"
                            values={clusterStats.queuedQueries}
                            tooltip="Total number of queries currently queued and awaiting execution"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, sm: 12, md: 6, lg: 4 }}>
                        <MetricCard
                            title="Runnable drivers"
                            values={clusterStats.runningDrivers}
                            numberFormatter={precisionRound}
                            tooltip="Moving average of total running drivers"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, sm: 12, md: 6, lg: 4 }}>
                        <MetricCard
                            title="Input bytes/s"
                            values={clusterStats.byteInputRate}
                            numberFormatter={formatDataSizeBytes}
                            tooltip="Moving average of input bytes processed per second"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, sm: 12, md: 6, lg: 4 }}>
                        <MetricCard
                            title="Blocked queries"
                            values={clusterStats.blockedQueries}
                            tooltip="Total number of queries currently blocked and unable to make progress"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, sm: 12, md: 6, lg: 4 }}>
                        <MetricCard
                            title="Reserved memory"
                            values={clusterStats.reservedMemory}
                            numberFormatter={formatDataSizeBytes}
                            tooltip="Total amount of memory reserved by all running queries"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, sm: 12, md: 6, lg: 4 }}>
                        <MetricCard
                            title="Worker parallelism"
                            values={clusterStats.perWorkerCpuTimeRate}
                            numberFormatter={precisionRound}
                            tooltip="Moving average of CPU time utilized per second per worker"
                        />
                    </Grid>
                </Grid>

                <Box sx={{ pt: 2 }}>
                    <Typography variant="h6">Query details</Typography>
                    <Divider />
                </Box>
                <QueryList />
            </>
        </>
    )
}
