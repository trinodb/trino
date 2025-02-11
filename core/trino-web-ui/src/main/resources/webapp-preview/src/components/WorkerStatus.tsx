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
import { useParams } from 'react-router-dom'
import { useTheme } from '@mui/material/styles'
import {
    Alert,
    Box,
    CircularProgress,
    Divider,
    Grid2 as Grid,
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableHead,
    TableRow,
    Typography,
} from '@mui/material'
import { PieChart } from '@mui/x-charts/PieChart'
import { SparkLineChart } from '@mui/x-charts/SparkLineChart'
import { MemoryUsagePool, WorkerStatusInfo, workerStatusApi } from '../api/webapp/api.ts'
import { ApiResponse } from '../api/base.ts'
import { Texts } from '../constant.ts'
import { useSnackbar } from './SnackbarContext.ts'
import { useEffect, useState } from 'react'
import { addToHistory, formatCount, formatDataSize, MAX_HISTORY, precisionRound } from '../utils/utils.ts'

interface IWorkerStatus {
    info: WorkerStatusInfo | null
    processCpuLoad: number[]
    systemCpuLoad: number[]
    heapPercentUsed: number[]
    nonHeapUsed: number[]

    lastRefresh: number | null
}

export const WorkerStatus = () => {
    const { showSnackbar } = useSnackbar()
    const { nodeId } = useParams()
    const theme = useTheme()
    const initialFilledHistory = Array(MAX_HISTORY).fill(0)
    const [workerStatus, setWorkerStatus] = useState<IWorkerStatus>({
        info: null,
        processCpuLoad: initialFilledHistory,
        systemCpuLoad: initialFilledHistory,
        heapPercentUsed: initialFilledHistory,
        nonHeapUsed: initialFilledHistory,
        lastRefresh: null,
    })
    const [loading, setLoading] = useState<boolean>(true)
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        const runLoop = () => {
            getWorkerStatus()
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

    const getWorkerStatus = () => {
        setError(null)
        if (nodeId) {
            workerStatusApi(nodeId).then((apiResponse: ApiResponse<WorkerStatusInfo>) => {
                setLoading(false)
                if (apiResponse.status === 200 && apiResponse.data) {
                    const newWorkerStatusInfo: WorkerStatusInfo = apiResponse.data
                    setWorkerStatus((prevWorkerStatus) => {
                        return {
                            info: newWorkerStatusInfo,
                            processCpuLoad: addToHistory(
                                newWorkerStatusInfo.processCpuLoad * 100.0,
                                prevWorkerStatus.processCpuLoad
                            ),
                            systemCpuLoad: addToHistory(
                                newWorkerStatusInfo.systemCpuLoad * 100.0,
                                prevWorkerStatus.systemCpuLoad
                            ),
                            heapPercentUsed: addToHistory(
                                (newWorkerStatusInfo.heapUsed * 100.0) / newWorkerStatusInfo.heapAvailable,
                                prevWorkerStatus.heapPercentUsed
                            ),
                            nonHeapUsed: addToHistory(newWorkerStatusInfo.nonHeapUsed, prevWorkerStatus.nonHeapUsed),

                            lastRefresh: Date.now(),
                        }
                    })
                } else {
                    setError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`)
                }
            })
        }
    }

    const renderPoolChart = (name: string, pool: MemoryUsagePool) => {
        if (!pool) {
            return <></>
        }

        const size = pool.maxBytes
        const reserved = pool.reservedBytes
        const revocable = pool.reservedRevocableBytes
        const available = size - reserved - revocable

        return (
            <>
                <Grid container spacing={3}>
                    <Grid size={{ sm: 8 }}>
                        <Typography sx={{ fontWeight: 'medium', m: 1 }}>{name} Pool</Typography>
                    </Grid>
                    <Grid size={{ sm: 4 }}>
                        <Typography sx={{ fontWeight: 'regular', m: 1 }}>{formatDataSize(size)} total</Typography>
                    </Grid>
                </Grid>
                <Grid container spacing={3}>
                    <Grid size={{ sm: 12 }}>
                        <PieChart
                            series={[
                                {
                                    data: [
                                        {
                                            id: 0,
                                            value: available,
                                            label: `Available (${formatDataSize(available)})`,
                                            color: theme.palette.success.light,
                                        },
                                        {
                                            id: 1,
                                            value: reserved,
                                            label: `Reserved (${formatDataSize(reserved)})`,
                                            color: theme.palette.warning.light,
                                        },
                                        {
                                            id: 2,
                                            value: revocable,
                                            label: `Revocable (${formatDataSize(revocable)})`,
                                            color: theme.palette.error.light,
                                        },
                                    ],
                                    valueFormatter: (value) => formatDataSize(value.value),
                                },
                            ]}
                            height={150}
                            skipAnimation
                        />
                    </Grid>
                </Grid>
            </>
        )
    }

    const renderPoolQueries = (pool: MemoryUsagePool) => {
        if (!pool) {
            return <></>
        }

        const queries: { [key: string]: number[] } = {}
        const reservations = pool.queryMemoryReservations
        const revocableReservations = pool.queryMemoryRevocableReservations

        for (let query in reservations) {
            queries[query] = [reservations[query], 0]
        }

        for (let query in revocableReservations) {
            if (queries.hasOwnProperty.call(queries, query)) {
                queries[query][1] = revocableReservations[query]
            } else {
                queries[query] = [0, revocableReservations[query]]
            }
        }

        const size = pool.maxBytes

        if (Object.keys(queries).length === 0) {
            return <Typography>No queries using pool</Typography>
        }

        return (
            <Grid container spacing={3}>
                <Grid size={{ sm: 12 }}>
                    <TableContainer>
                        <Table aria-label="simple table">
                            <TableHead>
                                <TableRow>
                                    <TableCell>Query Id</TableCell>
                                    <TableCell align="right">Reserved %</TableCell>
                                    <TableCell align="right">Reserved</TableCell>
                                    <TableCell align="right">Revocable</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {Object.entries(queries)
                                    .sort(([, reservationsA], [, reservationsB]) => reservationsB[0] - reservationsA[0])
                                    .map(([query, reservations]) => (
                                        <TableRow key={query}>
                                            <TableCell align="left">{query}</TableCell>
                                            <TableCell align="right">
                                                {Math.round((reservations[0] * 100.0) / size)}%
                                            </TableCell>
                                            <TableCell align="right">{formatDataSize(reservations[0])}</TableCell>
                                            <TableCell align="right">{formatDataSize(reservations[1])}</TableCell>
                                        </TableRow>
                                    ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                </Grid>
            </Grid>
        )
    }

    return (
        <>
            <Box sx={{ pb: 2 }}>
                <Typography variant="h4">Worker status</Typography>
            </Box>

            {loading && <CircularProgress />}
            {error && <Alert severity="error">{Texts.Error.NodeInformationNotLoaded}</Alert>}

            {!loading && !error && workerStatus.info && (
                <>
                    <Box sx={{ pt: 2 }}>
                        <Typography variant="h6">Overview</Typography>
                        <Divider />
                    </Box>
                    <Grid container spacing={3}>
                        <Grid size={{ sm: 12, md: 6 }}>
                            <TableContainer>
                                <Table aria-label="simple table">
                                    <TableBody>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Node ID</TableCell>
                                            <TableCell>{workerStatus.info.nodeId}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Heap memory</TableCell>
                                            <TableCell>{formatDataSize(workerStatus.info.heapAvailable)}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Processors</TableCell>
                                            <TableCell>{workerStatus.info.processors}</TableCell>
                                        </TableRow>
                                    </TableBody>
                                </Table>
                            </TableContainer>
                        </Grid>
                        <Grid size={{ sm: 12, md: 6 }}>
                            <TableContainer>
                                <Table aria-label="simple table">
                                    <TableBody>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Uptime</TableCell>
                                            <TableCell>{workerStatus.info.uptime}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>External address</TableCell>
                                            <TableCell>{workerStatus.info.externalAddress}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Internal address</TableCell>
                                            <TableCell>{workerStatus.info.internalAddress}</TableCell>
                                        </TableRow>
                                    </TableBody>
                                </Table>
                            </TableContainer>
                        </Grid>
                    </Grid>

                    <Box sx={{ pt: 2 }}>
                        <Typography variant="h6">Resource utilization</Typography>
                        <Divider />
                    </Box>
                    <Grid container spacing={3}>
                        <Grid size={{ sm: 12, md: 6 }}>
                            <TableContainer>
                                <Table aria-label="simple table">
                                    <TableBody>
                                        <TableRow>
                                            <TableCell sx={{ border: 'none', fontWeight: 'bold' }}>
                                                Process CPU utilization
                                            </TableCell>
                                            <TableCell rowSpan={2}>
                                                <SparkLineChart
                                                    data={workerStatus.processCpuLoad}
                                                    valueFormatter={precisionRound}
                                                    height={50}
                                                    area
                                                    showHighlight
                                                    showTooltip
                                                />
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell>
                                                {formatCount(
                                                    workerStatus.processCpuLoad[workerStatus.processCpuLoad.length - 1]
                                                )}
                                                %
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ border: 'none', fontWeight: 'bold' }}>
                                                System CPU utilization
                                            </TableCell>
                                            <TableCell rowSpan={2}>
                                                <SparkLineChart
                                                    data={workerStatus.systemCpuLoad}
                                                    valueFormatter={precisionRound}
                                                    height={50}
                                                    area
                                                    showHighlight
                                                    showTooltip
                                                />
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell>
                                                {formatCount(
                                                    workerStatus.systemCpuLoad[workerStatus.systemCpuLoad.length - 1]
                                                )}
                                                %
                                            </TableCell>
                                        </TableRow>
                                    </TableBody>
                                </Table>
                            </TableContainer>
                        </Grid>
                        <Grid size={{ sm: 12, md: 6 }}>
                            <TableContainer>
                                <Table aria-label="simple table">
                                    <TableBody>
                                        <TableRow>
                                            <TableCell sx={{ border: 'none', fontWeight: 'bold' }}>
                                                Heap utilization
                                            </TableCell>
                                            <TableCell rowSpan={2}>
                                                <SparkLineChart
                                                    data={workerStatus.heapPercentUsed}
                                                    valueFormatter={precisionRound}
                                                    height={50}
                                                    area
                                                    showHighlight
                                                    showTooltip
                                                />
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell>
                                                {formatCount(
                                                    (workerStatus.info.heapUsed * 100) / workerStatus.info.heapAvailable
                                                )}
                                                %
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ border: 'none', fontWeight: 'bold' }}>
                                                Non-Heap memory used
                                            </TableCell>
                                            <TableCell rowSpan={2}>
                                                <SparkLineChart
                                                    data={workerStatus.nonHeapUsed}
                                                    valueFormatter={precisionRound}
                                                    height={50}
                                                    area
                                                    showHighlight
                                                    showTooltip
                                                />
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell>{formatDataSize(workerStatus.info.nonHeapUsed)}</TableCell>
                                        </TableRow>
                                    </TableBody>
                                </Table>
                            </TableContainer>
                        </Grid>
                    </Grid>

                    <Box sx={{ pt: 2 }}>
                        <Typography variant="h6">Memory</Typography>
                        <Divider />
                    </Box>
                    <Grid container spacing={3}>
                        <Grid size={{ sm: 12, md: 6 }}>
                            {renderPoolChart('Memory usage', workerStatus.info.memoryInfo.pool)}
                        </Grid>
                        <Grid size={{ sm: 12 }}>{renderPoolQueries(workerStatus.info.memoryInfo.pool)}</Grid>
                    </Grid>
                </>
            )}
        </>
    )
}
