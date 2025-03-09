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
import { useEffect, useRef, useState } from 'react'
import { QueryProgressBar } from './QueryProgressBar'
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
    TableRow,
    Typography,
} from '@mui/material'
import { SparkLineChart } from '@mui/x-charts/SparkLineChart'
import { Texts } from '../constant.ts'
import { StackInfo, queryStatusApi, QueryStatusInfo, Session } from '../api/webapp/api.ts'
import { ApiResponse } from '../api/base.ts'
import {
    addToHistory,
    formatCount,
    formatDataSize,
    formatShortDateTime,
    parseAndFormatDataSize,
    parseDataSize,
    parseDuration,
} from '../utils/utils.ts'
import { CodeBlock } from './CodeBlock.tsx'

interface IQueryStatus {
    info: QueryStatusInfo | null

    lastScheduledTime: number
    lastCpuTime: number
    lastPhysicalInputReadTime: number
    lastProcessedInputPositions: number
    lastProcessedInputDataSize: number
    lastPhysicalInputDataSize: number

    scheduledTimeRate: number[]
    cpuTimeRate: number[]
    rowInputRate: number[]
    byteInputRate: number[]
    reservedMemory: number[]
    physicalInputRate: number[]

    lastRefresh: number | null
}

export const QueryOverview = () => {
    const { queryId } = useParams()
    const initialQueryStatus: IQueryStatus = {
        info: null,

        lastScheduledTime: 0,
        lastCpuTime: 0,
        lastPhysicalInputReadTime: 0,
        lastProcessedInputPositions: 0,
        lastProcessedInputDataSize: 0,
        lastPhysicalInputDataSize: 0,

        scheduledTimeRate: [],
        cpuTimeRate: [],
        rowInputRate: [],
        byteInputRate: [],
        reservedMemory: [],
        physicalInputRate: [],

        lastRefresh: null,
    }
    const [queryStatus, setQueryStatus] = useState<IQueryStatus>(initialQueryStatus)
    const [loading, setLoading] = useState<boolean>(true)
    const [error, setError] = useState<string | null>(null)
    const queryStatusRef = useRef(queryStatus)

    useEffect(() => {
        queryStatusRef.current = queryStatus
    }, [queryStatus])

    useEffect(() => {
        const runLoop = () => {
            const queryEnded = !!queryStatusRef.current.info?.finalQueryInfo
            if (!queryEnded) {
                getQueryStatus()
                setTimeout(runLoop, 3000)
            }
        }

        if (queryId) {
            queryStatusRef.current = initialQueryStatus
        }

        runLoop()
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [queryId])

    const getQueryStatus = () => {
        if (queryId) {
            queryStatusApi(queryId).then((apiResponse: ApiResponse<QueryStatusInfo>) => {
                setLoading(false)
                if (apiResponse.status === 200 && apiResponse.data) {
                    const newQueryStatusInfo: QueryStatusInfo = apiResponse.data
                    setQueryStatus((prevQueryStatus) => {
                        let lastRefresh = prevQueryStatus.lastRefresh
                        const lastScheduledTime = prevQueryStatus.lastScheduledTime
                        const lastCpuTime = prevQueryStatus.lastCpuTime
                        const lastPhysicalInputReadTime = prevQueryStatus.lastPhysicalInputReadTime
                        const lastProcessedInputPositions = prevQueryStatus.lastProcessedInputPositions
                        const lastProcessedInputDataSize = prevQueryStatus.lastProcessedInputDataSize
                        const lastPhysicalInputDataSize = prevQueryStatus.lastPhysicalInputDataSize
                        const nowMillis = Date.now()

                        const elapsedTime = parseDuration(newQueryStatusInfo.queryStats.elapsedTime || '') || 0

                        let newScheduledTimeRate = prevQueryStatus.scheduledTimeRate
                        let newCpuTimeRate = prevQueryStatus.cpuTimeRate
                        let newRowInputRate = prevQueryStatus.rowInputRate
                        let newByteInputRate = prevQueryStatus.byteInputRate
                        let newReservedMemory = prevQueryStatus.reservedMemory
                        let newPhysicalInputRate = prevQueryStatus.physicalInputRate

                        const newQueryStats = newQueryStatusInfo.queryStats

                        if (lastRefresh === null) {
                            lastRefresh = nowMillis - elapsedTime
                        }

                        const elapsedSecsSinceLastRefresh = (nowMillis - lastRefresh) / 1000.0
                        if (elapsedSecsSinceLastRefresh >= 0) {
                            const calcSafeRate = (newVal: number, lastVal: number, elapsedMs: number): number =>
                                elapsedMs > 0 && Number.isFinite(newVal - lastVal) ? (newVal - lastVal) / elapsedMs : 0
                            const safeAddToHistory = (val: number, arr: number[]): number[] =>
                                arr.length === 0 || val > 0 ? addToHistory(val, arr) : arr

                            const newScheduledTime = parseDuration(newQueryStats.totalScheduledTime) || 0
                            const newCpuTime = parseDuration(newQueryStats.totalCpuTime) || 0
                            const newPhysicalInputReadTime = parseDuration(newQueryStats.physicalInputReadTime) || 0
                            const newProcessedInputPositions = newQueryStats.processedInputPositions
                            const newProcessedInputDataSize = parseDataSize(newQueryStats.processedInputDataSize) || 0
                            const newPhysicalInputDataSize = parseDataSize(newQueryStats.physicalInputDataSize) || 0
                            const currentReservedMemory = parseDataSize(newQueryStats.totalMemoryReservation) || 0

                            const currentScheduledTimeRate = calcSafeRate(
                                newScheduledTime,
                                lastScheduledTime,
                                elapsedSecsSinceLastRefresh * 1000
                            )
                            const currentCpuTimeRate = calcSafeRate(
                                newCpuTime,
                                lastCpuTime,
                                elapsedSecsSinceLastRefresh * 1000
                            )
                            const currentPhysicalReadTime = calcSafeRate(
                                newPhysicalInputReadTime,
                                lastPhysicalInputReadTime,
                                1000
                            )
                            const currentRowInputRate = calcSafeRate(
                                newProcessedInputPositions,
                                lastProcessedInputPositions,
                                elapsedSecsSinceLastRefresh
                            )
                            const currentByteInputRate = calcSafeRate(
                                newProcessedInputDataSize,
                                lastProcessedInputDataSize,
                                elapsedSecsSinceLastRefresh
                            )
                            const currentPhysicalInputRate =
                                currentPhysicalReadTime > 0
                                    ? calcSafeRate(
                                          newPhysicalInputDataSize,
                                          lastPhysicalInputDataSize,
                                          currentPhysicalReadTime
                                      )
                                    : 0

                            newScheduledTimeRate = safeAddToHistory(
                                currentScheduledTimeRate,
                                prevQueryStatus.scheduledTimeRate
                            )
                            newCpuTimeRate = safeAddToHistory(currentCpuTimeRate, prevQueryStatus.cpuTimeRate)
                            newRowInputRate = safeAddToHistory(currentRowInputRate, prevQueryStatus.rowInputRate)
                            newByteInputRate = safeAddToHistory(currentByteInputRate, prevQueryStatus.byteInputRate)
                            newReservedMemory = safeAddToHistory(currentReservedMemory, prevQueryStatus.reservedMemory)
                            newPhysicalInputRate = safeAddToHistory(
                                currentPhysicalInputRate,
                                prevQueryStatus.physicalInputRate
                            )
                        }

                        return {
                            info: newQueryStatusInfo,

                            lastScheduledTime: parseDuration(newQueryStats.totalScheduledTime) || 0,
                            lastCpuTime: parseDuration(newQueryStats.totalCpuTime) || 0,
                            lastPhysicalInputReadTime: parseDuration(newQueryStats.physicalInputReadTime) || 0,
                            lastProcessedInputPositions: newQueryStats.processedInputPositions,
                            lastProcessedInputDataSize: parseDataSize(newQueryStats.processedInputDataSize) || 0,
                            lastPhysicalInputDataSize: parseDataSize(newQueryStats.physicalInputDataSize) || 0,

                            scheduledTimeRate: newScheduledTimeRate,
                            cpuTimeRate: newCpuTimeRate,
                            rowInputRate: newRowInputRate,
                            byteInputRate: newByteInputRate,
                            reservedMemory: newReservedMemory,
                            physicalInputRate: newPhysicalInputRate,

                            lastRefresh: nowMillis,
                        }
                    })
                    setError(null)
                } else {
                    setError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`)
                }
            })
        }
    }

    const formatStackTrace = (info: StackInfo) => {
        return formatStackTraceHelper(info, [], '', '')
    }

    const formatStackTraceHelper = (info: StackInfo, parentStack: string[], prefix: string, linePrefix: string) => {
        let s = linePrefix + prefix + failureInfoToString(info) + '\n'

        if (info.stack) {
            let sharedStackFrames = 0
            if (parentStack !== null) {
                sharedStackFrames = countSharedStackFrames(info.stack, parentStack)
            }

            for (let i = 0; i < info.stack.length - sharedStackFrames; i++) {
                s += linePrefix + '\tat ' + info.stack[i] + '\n'
            }
            if (sharedStackFrames !== 0) {
                s += linePrefix + '\t... ' + sharedStackFrames + ' more' + '\n'
            }
        }

        if (info.suppressed) {
            for (let i = 0; i < info.suppressed.length; i++) {
                s += formatStackTraceHelper(info.suppressed[i], info.stack, 'Suppressed: ', linePrefix + '\t')
            }
        }

        if (info.cause) {
            s += formatStackTraceHelper(info.cause, info.stack, 'Caused by: ', linePrefix)
        }

        return s
    }

    const countSharedStackFrames = (stack: string[], parentStack: string[]) => {
        let n = 0
        const minStackLength = Math.min(stack.length, parentStack.length)
        while (n < minStackLength && stack[stack.length - 1 - n] === parentStack[parentStack.length - 1 - n]) {
            n++
        }
        return n
    }

    const failureInfoToString = (t: StackInfo) => {
        return t.message !== null ? t.type + ': ' + t.message : t.type
    }

    const renderSessionProperties = (session: Session) => {
        return (
            <>
                {Object.entries(session.systemProperties).map(([key, value]) => (
                    <div key={key}>
                        {key}={value}
                    </div>
                ))}
                {Object.entries(session.catalogProperties).map(([key, value]) => (
                    <div key={key}>
                        {key}={value}
                    </div>
                ))}
            </>
        )
    }

    const renderSparkLine = (data: number[], numberFormatter: (n: number | null) => string) => {
        return (
            <Grid container>
                <Grid size={12} sx={{ textAlign: 'right' }}>
                    {numberFormatter(data[data.length - 1])}
                </Grid>
                <Grid size={12}>
                    {data.length > 1 ? (
                        <SparkLineChart
                            data={data}
                            valueFormatter={numberFormatter}
                            height={53}
                            area
                            showHighlight
                            showTooltip
                        />
                    ) : (
                        <div />
                    )}
                </Grid>
            </Grid>
        )
    }

    const renderWarningInfo = () => {
        const queryStatusInfo = queryStatus.info

        if (queryStatusInfo?.warnings?.length) {
            return (
                <Grid size={{ xs: 12 }}>
                    <Box sx={{ pt: 2 }}>
                        <Typography variant="h6">Warnings</Typography>
                        <Divider />
                    </Box>

                    <TableContainer>
                        <Table aria-label="simple table">
                            <TableBody>
                                {queryStatusInfo.warnings.map((warning) => (
                                    <TableRow>
                                        <TableCell sx={{ fontWeight: 'bold' }}>{warning.warningCode.name}</TableCell>
                                        <TableCell>{warning.message}</TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </TableContainer>
                </Grid>
            )
        } else {
            return null
        }
    }

    const renderFailureInfo = () => {
        const queryStatusInfo = queryStatus.info

        if (queryStatusInfo?.failureInfo) {
            return (
                <Grid size={{ xs: 12 }}>
                    <Box sx={{ pt: 2 }}>
                        <Typography variant="h6">Error information</Typography>
                        <Divider />
                    </Box>

                    <TableContainer>
                        <Table aria-label="simple table">
                            <TableBody>
                                <TableRow>
                                    <TableCell sx={{ fontWeight: 'bold' }}>Error type</TableCell>
                                    <TableCell>{queryStatusInfo.errorType}</TableCell>
                                </TableRow>
                                <TableRow>
                                    <TableCell sx={{ fontWeight: 'bold' }}>Error code</TableCell>
                                    <TableCell>
                                        {queryStatusInfo.errorCode.name + ' (' + queryStatusInfo.errorCode.code + ')'}
                                    </TableCell>
                                </TableRow>
                                <TableRow>
                                    <TableCell sx={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>Stack trace</TableCell>
                                    <TableCell
                                        sx={{
                                            fontSize: '0.6rem',
                                        }}
                                    >
                                        <CodeBlock
                                            language="java"
                                            code={formatStackTrace(queryStatusInfo.failureInfo)}
                                        />
                                    </TableCell>
                                </TableRow>
                            </TableBody>
                        </Table>
                    </TableContainer>
                </Grid>
            )
        } else {
            return null
        }
    }

    const renderPreparedQuery = () => {
        const queryStatusInfo = queryStatus.info

        if (queryStatusInfo?.preparedQuery) {
            return (
                <Grid size={{ xs: 12 }}>
                    <Box sx={{ pt: 2 }}>
                        <Typography variant="h6">Prepared query</Typography>
                        <Divider />
                    </Box>
                    <Box>
                        <CodeBlock language="sql" code={queryStatusInfo.preparedQuery} />
                    </Box>
                </Grid>
            )
        } else {
            return null
        }
    }

    const taskRetriesEnabled = queryStatus.info?.retryPolicy == 'TASK'
    return (
        <>
            {loading && <CircularProgress />}
            {error && <Alert severity="error">{Texts.Error.QueryNotFound}</Alert>}

            {!loading && !error && queryStatus.info && (
                <Grid container spacing={0}>
                    <Grid size={{ xs: 12 }}>
                        <Box sx={{ pt: 2 }}>
                            <QueryProgressBar queryInfoBase={queryStatus.info} />
                        </Box>
                    </Grid>

                    <Grid container spacing={3}>
                        <Grid size={{ xs: 12, md: 6 }}>
                            <Box sx={{ pt: 2 }}>
                                <Typography variant="h6">Session</Typography>
                                <Divider />
                            </Box>
                            <TableContainer>
                                <Table aria-label="simple table">
                                    <TableBody>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>User</TableCell>
                                            <TableCell>{queryStatus.info.session.user}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Principal</TableCell>
                                            <TableCell>{queryStatus.info.session.principal}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Source</TableCell>
                                            <TableCell>{queryStatus.info.session.source}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Catalog</TableCell>
                                            <TableCell>{queryStatus.info.session.catalog}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Schema</TableCell>
                                            <TableCell>{queryStatus.info.session.schema}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Time zone</TableCell>
                                            <TableCell>{queryStatus.info.session.timeZone}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Client address</TableCell>
                                            <TableCell>{queryStatus.info.session.remoteUserAddress}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Client tags</TableCell>
                                            <TableCell>{queryStatus.info.session.clientTags.join(', ')}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Protocol encoding</TableCell>
                                            <TableCell>
                                                {queryStatus.info.session.queryDataEncoding
                                                    ? 'spooled ' + queryStatus.info.session.queryDataEncoding
                                                    : 'non-spooled'}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Session properties</TableCell>
                                            <TableCell>{renderSessionProperties(queryStatus.info.session)}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Resource estimates</TableCell>
                                            <TableCell></TableCell>
                                        </TableRow>
                                    </TableBody>
                                </Table>
                            </TableContainer>
                        </Grid>

                        <Grid size={{ xs: 12, md: 6 }}>
                            <Box sx={{ pt: 2 }}>
                                <Typography variant="h6">Execution</Typography>
                                <Divider />
                            </Box>
                            <TableContainer>
                                <Table aria-label="simple table">
                                    <TableBody>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Resource group</TableCell>
                                            <TableCell>
                                                {queryStatus.info.resourceGroupId
                                                    ? queryStatus.info.resourceGroupId.join('.')
                                                    : 'n/a'}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Submission time</TableCell>
                                            <TableCell>
                                                {formatShortDateTime(new Date(queryStatus.info.queryStats.createTime))}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Completion time</TableCell>
                                            <TableCell>
                                                {queryStatus.info.queryStats.endTime
                                                    ? formatShortDateTime(new Date(queryStatus.info.queryStats.endTime))
                                                    : ''}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Elapsed time</TableCell>
                                            <TableCell>{queryStatus.info.queryStats.elapsedTime}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Queued time</TableCell>
                                            <TableCell>{queryStatus.info.queryStats.queuedTime}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Elapsed time</TableCell>
                                            <TableCell>{queryStatus.info.queryStats.elapsedTime}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Planned time</TableCell>
                                            <TableCell>{queryStatus.info.queryStats.planningTime}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Execution time</TableCell>
                                            <TableCell>{queryStatus.info.queryStats.executionTime}</TableCell>
                                        </TableRow>
                                    </TableBody>
                                </Table>
                            </TableContainer>
                        </Grid>

                        <Grid size={{ xs: 12, md: 6 }}>
                            <Box sx={{ pt: 2 }}>
                                <Typography variant="h6">Resource utilization</Typography>
                                <Divider />
                            </Box>
                            <TableContainer>
                                <Table aria-label="simple table">
                                    <TableBody>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>CPU time</TableCell>
                                            <TableCell>{queryStatus.info.queryStats.totalCpuTime}</TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>{queryStatus.info.queryStats.failedCpuTime}</TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Planning CPU time</TableCell>
                                            <TableCell>{queryStatus.info.queryStats.planningCpuTime}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Scheduled time</TableCell>
                                            <TableCell>{queryStatus.info.queryStats.totalScheduledTime}</TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>{queryStatus.info.queryStats.failedScheduledTime}</TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Input rows</TableCell>
                                            <TableCell>
                                                {formatCount(queryStatus.info.queryStats.processedInputPositions)}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {queryStatus.info.queryStats.failedProcessedInputPositions}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Input data</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(
                                                    queryStatus.info.queryStats.processedInputDataSize
                                                )}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {queryStatus.info.queryStats.failedProcessedInputDataSize}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Physical input rows</TableCell>
                                            <TableCell>
                                                {formatCount(queryStatus.info.queryStats.physicalInputPositions)}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {queryStatus.info.queryStats.failedPhysicalInputPositions}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Physical input data</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(
                                                    queryStatus.info.queryStats.physicalInputDataSize
                                                )}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {queryStatus.info.queryStats.failedPhysicalInputDataSize}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Physical input read time</TableCell>
                                            <TableCell>{queryStatus.info.queryStats.physicalInputReadTime}</TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {queryStatus.info.queryStats.failedPhysicalInputReadTime}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Internal network rows</TableCell>
                                            <TableCell>
                                                {formatCount(queryStatus.info.queryStats.internalNetworkInputPositions)}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {formatCount(
                                                        queryStatus.info.queryStats.failedInternalNetworkInputPositions
                                                    )}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Internal network data</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(
                                                    queryStatus.info.queryStats.internalNetworkInputDataSize
                                                )}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {parseAndFormatDataSize(
                                                        queryStatus.info.queryStats.failedInternalNetworkInputDataSize
                                                    )}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Peak user memory</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(
                                                    queryStatus.info.queryStats.peakUserMemoryReservation
                                                )}
                                            </TableCell>
                                        </TableRow>
                                        {queryStatus.info.queryStats.peakRevocableMemoryReservation && (
                                            <TableRow>
                                                <TableCell sx={{ fontWeight: 'bold' }}>Peak revocable memory</TableCell>
                                                <TableCell>
                                                    {parseAndFormatDataSize(
                                                        queryStatus.info.queryStats.peakRevocableMemoryReservation
                                                    )}
                                                </TableCell>
                                            </TableRow>
                                        )}
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Peak total memory</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(
                                                    queryStatus.info.queryStats.peakTotalMemoryReservation
                                                )}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Cumulative user memory</TableCell>
                                            <TableCell>
                                                {formatDataSize(
                                                    queryStatus.info.queryStats.cumulativeUserMemory / 1000.0
                                                ) + '*seconds'}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {formatDataSize(
                                                        queryStatus.info.queryStats.failedCumulativeUserMemory / 1000.0
                                                    ) + '*seconds'}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Output rows</TableCell>
                                            <TableCell>
                                                {formatCount(queryStatus.info.queryStats.outputPositions)}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {formatCount(queryStatus.info.queryStats.failedOutputPositions)}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Output data</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(queryStatus.info.queryStats.outputDataSize)}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {parseAndFormatDataSize(
                                                        queryStatus.info.queryStats.failedOutputDataSize
                                                    )}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Written rows</TableCell>
                                            <TableCell>
                                                {formatCount(queryStatus.info.queryStats.writtenPositions)}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Logical written data</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(
                                                    queryStatus.info.queryStats.logicalWrittenDataSize
                                                )}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Physical written data</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(
                                                    queryStatus.info.queryStats.physicalWrittenDataSize
                                                )}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {parseAndFormatDataSize(
                                                        queryStatus.info.queryStats.failedPhysicalWrittenDataSize
                                                    )}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        {queryStatus.info.queryStats.spilledDataSize && (
                                            <TableRow>
                                                <TableCell sx={{ fontWeight: 'bold' }}>Spilled data</TableCell>
                                                <TableCell>
                                                    {parseAndFormatDataSize(
                                                        queryStatus.info.queryStats.spilledDataSize
                                                    )}
                                                </TableCell>
                                            </TableRow>
                                        )}
                                    </TableBody>
                                </Table>
                            </TableContainer>
                        </Grid>

                        <Grid size={{ xs: 12, md: 6 }}>
                            <Box sx={{ pt: 2 }}>
                                <Typography variant="h6">Timeline</Typography>
                                <Divider />
                            </Box>
                            <TableContainer>
                                <Table aria-label="simple table">
                                    <TableBody>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>
                                                Parallelism
                                            </TableCell>
                                            <TableCell>
                                                {renderSparkLine(queryStatus.cpuTimeRate, formatCount)}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>
                                                Scheduled time/s
                                            </TableCell>
                                            <TableCell>
                                                {renderSparkLine(queryStatus.scheduledTimeRate, formatCount)}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>
                                                Input rows/s
                                            </TableCell>
                                            <TableCell>
                                                {renderSparkLine(queryStatus.rowInputRate, formatCount)}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>
                                                Input bytes/s
                                            </TableCell>
                                            <TableCell>
                                                {renderSparkLine(queryStatus.byteInputRate, formatDataSize)}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>
                                                Physical input bytes/s
                                            </TableCell>
                                            <TableCell>
                                                {renderSparkLine(queryStatus.physicalInputRate, formatDataSize)}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>
                                                Memory utilization
                                            </TableCell>
                                            <TableCell>
                                                {renderSparkLine(queryStatus.reservedMemory, formatDataSize)}
                                            </TableCell>
                                        </TableRow>
                                    </TableBody>
                                </Table>
                            </TableContainer>
                        </Grid>
                        {renderWarningInfo()}
                        {renderFailureInfo()}
                        <Grid size={{ xs: 12 }}>
                            <Box sx={{ pt: 2 }}>
                                <Typography variant="h6">Query</Typography>
                                <Divider />
                            </Box>
                            <Box>
                                <CodeBlock language="sql" code={queryStatus.info.query} />
                            </Box>
                        </Grid>
                        {renderPreparedQuery()}
                    </Grid>
                </Grid>
            )}
        </>
    )
}
