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
import { useEffect } from 'react'
import { Collapse, Grid } from '@mui/material'
import { MetricCard } from './MetricCard.tsx'
import { QueryList } from './QueryList.tsx'
import { useSnackbar } from './SnackbarContext.ts'
import { useClusterStatsStore } from '../store'
import { formatCount, formatDataSizeBytes, precisionRound } from '../utils/utils.ts'

export const Dashboard = () => {
    const { showSnackbar } = useSnackbar()
    const { history: clusterStats, error, panelsOpen } = useClusterStatsStore()

    useEffect(() => {
        if (error) {
            showSnackbar(error, 'error')
        }
    }, [error, showSnackbar])

    return (
        <>
            {/* Opened from the cluster summary in the app bar */}
            <Collapse in={panelsOpen} id="dashboard-metric-panels">
                {/* Three tiles per row from the md breakpoint (900px); one per row below, where three would not fit */}
                <Grid container spacing={1} sx={{ pb: 1 }}>
                    <Grid size={{ xs: 12, md: 4 }}>
                        <MetricCard
                            title="Running queries"
                            values={clusterStats.runningQueries}
                            tooltip="Total number of queries currently running"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, md: 4 }}>
                        <MetricCard
                            title="Active workers"
                            values={clusterStats.activeWorkers}
                            link="/workers"
                            tooltip="Total number of active worker nodes"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, md: 4 }}>
                        <MetricCard
                            title="Input rows/s"
                            values={clusterStats.rowInputRate}
                            numberFormatter={formatCount}
                            tooltip="Moving average of input rows processed per second"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, md: 4 }}>
                        <MetricCard
                            title="Queued queries"
                            values={clusterStats.queuedQueries}
                            tooltip="Total number of queries currently queued and awaiting execution"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, md: 4 }}>
                        <MetricCard
                            title="Runnable drivers"
                            values={clusterStats.runningDrivers}
                            numberFormatter={precisionRound}
                            tooltip="Moving average of total running drivers"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, md: 4 }}>
                        <MetricCard
                            title="Input bytes/s"
                            values={clusterStats.byteInputRate}
                            numberFormatter={formatDataSizeBytes}
                            tooltip="Moving average of input bytes processed per second"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, md: 4 }}>
                        <MetricCard
                            title="Blocked queries"
                            values={clusterStats.blockedQueries}
                            tooltip="Total number of queries currently blocked and unable to make progress"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, md: 4 }}>
                        <MetricCard
                            title="Reserved memory"
                            values={clusterStats.reservedMemory}
                            numberFormatter={formatDataSizeBytes}
                            tooltip="Total amount of memory reserved by all running queries"
                        />
                    </Grid>
                    <Grid size={{ xs: 12, md: 4 }}>
                        <MetricCard
                            title="Worker parallelism"
                            values={clusterStats.perWorkerCpuTimeRate}
                            numberFormatter={precisionRound}
                            tooltip="Moving average of CPU time utilized per second per worker"
                        />
                    </Grid>
                </Grid>
            </Collapse>
            <QueryList />
        </>
    )
}
