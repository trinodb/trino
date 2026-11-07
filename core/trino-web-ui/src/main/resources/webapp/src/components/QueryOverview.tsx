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
import { useEffect, useRef, useState } from 'react'
import { QueryProgressBar } from './QueryProgressBar'
import {
    Alert,
    Box,
    Button,
    CircularProgress,
    Divider,
    Grid,
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableRow,
    Typography,
} from '@mui/material'
import { amber, blue, deepOrange, green, indigo, orange, purple, teal } from '@mui/material/colors'
import { SparkLineChart } from '@mui/x-charts/SparkLineChart'
import { Texts } from '../constant.ts'
import { StackInfo, killQueryApi, QueryStage, QueryStages, QueryStatusInfo, Session } from '../api/webapp/api.ts'
import { ApiResponse } from '../api/base.ts'
import {
    addToHistory,
    formatCount,
    formatDataSize,
    formatDurationClock,
    formatShortDateTime,
    getStageNumber,
    parseAndFormatDataSize,
    parseDataSize,
    parseDuration,
} from '../utils/utils.ts'
import { QueryStageCard } from './QueryStageCard'
import { BreakdownRow } from './BreakdownRow.tsx'
import { CodeBlock } from './CodeBlock.tsx'
import { useQueryStatus } from './QueryStatusContext'

interface IDerivedQueryStatus {
    lastQueryStages: QueryStages | null

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

const initialDerivedStatus: IDerivedQueryStatus = {
    lastQueryStages: null,

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

// Halves the default vertical cell padding; long unbroken values (user names, IDs) wrap instead of widening the page
const compactTableSx = { '& .MuiTableCell-root': { py: 1, overflowWrap: 'anywhere' } }

export const QueryOverview = () => {
    const { queryStatusInfo, loading, error } = useQueryStatus()

    const [derivedStatus, setDerivedStatus] = useState<IDerivedQueryStatus>(initialDerivedStatus)
    const [queryActionError, setQueryActionError] = useState<string | null>(null)
    const [queryActionRunning, setQueryActionRunning] = useState<boolean>(false)
    const prevQueryStatusInfoRef = useRef<QueryStatusInfo | null>(null)

    useEffect(() => {
        if (!queryStatusInfo) {
            setDerivedStatus(initialDerivedStatus)
            setQueryActionError(null)
            return
        }

        if (queryStatusInfo === prevQueryStatusInfoRef.current) {
            return
        }
        prevQueryStatusInfoRef.current = queryStatusInfo

        setDerivedStatus((prevDerivedStatus) => {
            let lastRefresh = prevDerivedStatus.lastRefresh

            const lastScheduledTime = prevDerivedStatus.lastScheduledTime
            const lastCpuTime = prevDerivedStatus.lastCpuTime
            const lastPhysicalInputReadTime = prevDerivedStatus.lastPhysicalInputReadTime
            const lastProcessedInputPositions = prevDerivedStatus.lastProcessedInputPositions
            const lastProcessedInputDataSize = prevDerivedStatus.lastProcessedInputDataSize
            const lastPhysicalInputDataSize = prevDerivedStatus.lastPhysicalInputDataSize
            const nowMillis = Date.now()

            const elapsedTime = parseDuration(queryStatusInfo.queryStats.elapsedTime || '') || 0

            let newScheduledTimeRate = prevDerivedStatus.scheduledTimeRate
            let newCpuTimeRate = prevDerivedStatus.cpuTimeRate
            let newRowInputRate = prevDerivedStatus.rowInputRate
            let newByteInputRate = prevDerivedStatus.byteInputRate
            let newReservedMemory = prevDerivedStatus.reservedMemory
            let newPhysicalInputRate = prevDerivedStatus.physicalInputRate

            const newQueryStats = queryStatusInfo.queryStats

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
                const currentCpuTimeRate = calcSafeRate(newCpuTime, lastCpuTime, elapsedSecsSinceLastRefresh * 1000)
                const currentPhysicalReadTime = calcSafeRate(newPhysicalInputReadTime, lastPhysicalInputReadTime, 1000)
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
                        ? calcSafeRate(newPhysicalInputDataSize, lastPhysicalInputDataSize, currentPhysicalReadTime)
                        : 0

                newScheduledTimeRate = safeAddToHistory(currentScheduledTimeRate, prevDerivedStatus.scheduledTimeRate)
                newCpuTimeRate = safeAddToHistory(currentCpuTimeRate, prevDerivedStatus.cpuTimeRate)
                newRowInputRate = safeAddToHistory(currentRowInputRate, prevDerivedStatus.rowInputRate)
                newByteInputRate = safeAddToHistory(currentByteInputRate, prevDerivedStatus.byteInputRate)
                newReservedMemory = safeAddToHistory(currentReservedMemory, prevDerivedStatus.reservedMemory)
                newPhysicalInputRate = safeAddToHistory(currentPhysicalInputRate, prevDerivedStatus.physicalInputRate)
            }

            return {
                lastQueryStages: queryStatusInfo.stages,

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
    }, [queryStatusInfo])

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
                {Object.entries(session.catalogProperties).map(([catalog, catalogProperties]) => (
                    <div key={catalog}>
                        {Object.entries(catalogProperties).map(([key, value]) => (
                            <div key={catalog + key}>
                                {catalog}.{key}={value}
                            </div>
                        ))}
                    </div>
                ))}
            </>
        )
    }

    const renderSparkLine = (data: number[], numberFormatter: (n: number | null) => string) => {
        return (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                <Box sx={{ flexGrow: 1, minWidth: 0, height: 20 }}>
                    {data.length > 1 ? (
                        <SparkLineChart
                            data={data}
                            valueFormatter={numberFormatter}
                            height={20}
                            area
                            showHighlight
                            showTooltip
                        />
                    ) : null}
                </Box>
                <Box sx={{ minWidth: 72, textAlign: 'right', whiteSpace: 'nowrap' }}>
                    {numberFormatter(data[data.length - 1])}
                </Box>
            </Box>
        )
    }

    // Splits driver time into disjoint segments that sum to scheduled + blocked time. Scheduled time is CPU plus
    // off-CPU wall on a driver thread; blocked time is spent off-thread waiting on a future. Storage write and output
    // write come from per-operator stats (table writers and task/partitioned output operators), whose wall and CPU
    // timings are slices of the same driver timer. Sub-totals are clamped so overlapping sources (read time includes
    // some CPU, every blocked operator records the same blocked interval) never exceed their parent.
    const renderTimeBreakdown = (queryStatusInfo: QueryStatusInfo) => {
        const queryStats = queryStatusInfo.queryStats
        const operatorTime = (operatorTypes: string[]) => {
            const time = { cpu: 0, wall: 0, blocked: 0 }
            for (const stage of queryStatusInfo.stages?.stages ?? []) {
                for (const operator of stage.stageStats.operatorSummaries ?? []) {
                    if (operatorTypes.includes(operator.operatorType)) {
                        time.cpu +=
                            (parseDuration(operator.addInputCpu) || 0) +
                            (parseDuration(operator.getOutputCpu) || 0) +
                            (parseDuration(operator.finishCpu) || 0)
                        time.wall +=
                            (parseDuration(operator.addInputWall) || 0) +
                            (parseDuration(operator.getOutputWall) || 0) +
                            (parseDuration(operator.finishWall) || 0)
                        time.blocked += parseDuration(operator.blockedWall) || 0
                    }
                }
            }
            return time
        }
        const writer = operatorTime(['TableWriterOperator', 'MergeWriterOperator', 'TableFinishOperator'])
        const output = operatorTime(['TaskOutputOperator', 'PartitionedOutputOperator'])

        const cpu = parseDuration(queryStats.totalCpuTime) || 0
        const writeCpu = Math.min(cpu, writer.cpu)
        const outputCpu = Math.min(cpu - writeCpu, output.cpu)

        const offCpu = Math.max(0, (parseDuration(queryStats.totalScheduledTime) || 0) - cpu)
        const storageRead = Math.min(offCpu, parseDuration(queryStats.physicalInputReadTime) || 0)
        const writeOffCpu = Math.min(offCpu - storageRead, Math.max(0, writer.wall - writer.cpu))
        const outputOffCpu = Math.min(offCpu - storageRead - writeOffCpu, Math.max(0, output.wall - output.cpu))

        const blocked = parseDuration(queryStats.totalBlockedTime) || 0
        const inputBlocked = Math.min(blocked, parseDuration(queryStats.inputBlockedTime) || 0)
        const outputBlocked = Math.min(blocked - inputBlocked, parseDuration(queryStats.outputBlockedTime) || 0)
        const writeBlocked = Math.min(blocked - inputBlocked - outputBlocked, writer.blocked)

        const segments = [
            {
                label: 'CPU',
                short: 'CPU',
                description: 'On CPU, outside storage and output writes',
                color: blue[700],
                value: cpu - writeCpu - outputCpu,
            },
            {
                label: 'Storage write CPU',
                description: 'On CPU in table writers: encoding and compressing files, commit',
                color: blue[400],
                value: writeCpu,
            },
            {
                label: 'Output write CPU',
                description: 'On CPU in output operators: serializing pages into output buffers',
                color: blue[200],
                value: outputCpu,
            },
            {
                label: 'Storage read',
                description: 'Off-CPU wall time inside connector reads (storage I/O wait)',
                color: teal[800],
                value: storageRead,
            },
            {
                label: 'Storage write',
                description: 'Off-CPU wall time in table writers (storage I/O wait)',
                color: teal[600],
                value: writeOffCpu,
            },
            {
                label: 'Output write',
                description: 'Off-CPU wall time in output operators',
                color: teal[400],
                value: outputOffCpu,
            },
            {
                label: 'Other off-CPU',
                description: 'Scheduled on a thread but not on CPU, outside reads and writes',
                color: teal[200],
                value: offCpu - storageRead - writeOffCpu - outputOffCpu,
            },
            {
                label: 'Input wait',
                description: 'Blocked waiting for input: upstream stages (exchange), splits or async reads',
                color: deepOrange[600],
                value: inputBlocked,
            },
            {
                label: 'Output wait',
                description: 'Blocked on a full output buffer (downstream backpressure)',
                color: amber[800],
                value: outputBlocked,
            },
            {
                label: 'Storage write wait',
                description: 'Blocked on asynchronous table writes or file commits',
                color: orange[400],
                value: writeBlocked,
            },
            {
                label: 'Other blocked',
                description: 'Blocked inside the pipeline: memory, join build side, local exchange',
                color: amber[300],
                value: blocked - inputBlocked - outputBlocked - writeBlocked,
            },
        ]
        return <BreakdownRow label="Time breakdown" segments={segments} formatValue={formatDurationClock} />
    }

    // Bytes moved over the network, split by traffic type. Each counter is measured at a different operator, so
    // the segments do not overlap: connector reads at the source scan, exchange input at the receiving
    // ExchangeOperator, connector writes at the table writer, and client output at the output stage.
    const renderNetworkBreakdown = (queryStats: QueryStatusInfo['queryStats']) => (
        <BreakdownRow
            label="Network breakdown"
            segments={[
                {
                    label: 'Storage read',
                    description: 'Read by connectors (object storage for Iceberg/Hive)',
                    color: indigo[400],
                    value: parseDataSize(queryStats.physicalInputDataSize) || 0,
                },
                {
                    label: 'Exchange',
                    description: 'Shuffled between tasks of different stages',
                    color: teal[500],
                    value: parseDataSize(queryStats.internalNetworkInputDataSize) || 0,
                },
                {
                    label: 'Storage write',
                    description: 'Written by connectors',
                    color: purple[400],
                    value: parseDataSize(queryStats.physicalWrittenDataSize) || 0,
                },
                {
                    label: 'Client output',
                    description: 'Returned to the client by the output stage',
                    color: green[600],
                    value: parseDataSize(queryStats.outputDataSize) || 0,
                },
            ]}
            formatValue={formatDataSize}
        />
    )

    const renderWarningInfo = () => {
        if (queryStatusInfo?.warnings?.length) {
            return (
                <Grid size={{ xs: 12 }}>
                    <Box sx={{ pt: 2 }}>
                        <Typography variant="h6">Warnings</Typography>
                        <Divider />
                    </Box>

                    <TableContainer>
                        <Table aria-label="simple table" sx={compactTableSx}>
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
        if (queryStatusInfo?.failureInfo) {
            return (
                <Grid size={{ xs: 12 }}>
                    <Box sx={{ pt: 2 }}>
                        <Typography variant="h6">Error information</Typography>
                        <Divider />
                    </Box>

                    <TableContainer>
                        <Table aria-label="simple table" sx={compactTableSx}>
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
        if (queryStatusInfo?.preparedQuery) {
            return (
                <Grid size={{ xs: 12 }}>
                    <Box sx={{ pt: 2 }}>
                        <Typography variant="h6">Prepared query</Typography>
                        <Divider />
                    </Box>
                    <Box>
                        <CodeBlock
                            language="sql"
                            code={queryStatusInfo.preparedQuery}
                            monacoOptions={{ lineNumbers: 'on' }}
                        />
                    </Box>
                </Grid>
            )
        } else {
            return null
        }
    }

    const renderStages = (taskRetriesEnabled: boolean) => {
        const stages = derivedStatus.lastQueryStages?.stages

        return (
            <Grid size={{ xs: 12 }}>
                <Box sx={{ pt: 2 }}>
                    <Typography variant="h6">Stages</Typography>
                    <Divider />

                    {stages ? (
                        stages
                            .slice()
                            .sort((stageA, stageB) => getStageNumber(stageA.stageId) - getStageNumber(stageB.stageId))
                            .map((stage: QueryStage) => (
                                <QueryStageCard
                                    key={stage.stageId}
                                    stage={stage}
                                    taskRetriesEnabled={taskRetriesEnabled}
                                />
                            ))
                    ) : (
                        <>
                            <Box sx={{ width: '100%', mt: 1 }}>
                                <Alert severity="info">No stage information available.</Alert>
                            </Box>
                        </>
                    )}
                </Box>
            </Grid>
        )
    }

    const runQueryAction = (queryAction: (queryId: string) => Promise<ApiResponse<void>>, actionName: string) => {
        const queryId = queryStatusInfo?.queryId
        if (!queryId || queryActionRunning || queryStatusInfo?.finalQueryInfo) {
            return
        }

        setQueryActionError(null)
        setQueryActionRunning(true)
        queryAction(queryId)
            .then((apiResponse: ApiResponse<void>) => {
                if (apiResponse.status === 403) {
                    setQueryActionError(
                        `${Texts.Error.Forbidden}: You do not have permission to ${actionName} this query.`
                    )
                } else if (apiResponse.status !== 202 && apiResponse.status !== 409) {
                    setQueryActionError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`)
                }
            })
            .finally(() => {
                setQueryActionRunning(false)
            })
    }

    const handleCancel = () => {
        runQueryAction(killQueryApi, 'cancel')
    }

    const taskRetriesEnabled = queryStatusInfo?.retryPolicy == 'TASK'
    const queryActionDisabled = queryActionRunning || !!queryStatusInfo?.finalQueryInfo
    return (
        <>
            {loading && <CircularProgress />}
            {error && <Alert severity="error">{error}</Alert>}

            {!loading && !error && queryStatusInfo && (
                <Grid container spacing={0}>
                    <Grid size={{ xs: 12 }}>
                        <Box sx={{ pt: 2, display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 1 }}>
                            <Box sx={{ flexGrow: 1, minWidth: 0 }}>
                                <QueryProgressBar queryInfoBase={queryStatusInfo} />
                            </Box>
                            <Box sx={{ display: 'flex', flexShrink: 0, gap: 1 }}>
                                <Button
                                    variant="outlined"
                                    color="error"
                                    size="small"
                                    disabled={queryActionDisabled || !!queryActionError}
                                    onClick={handleCancel}
                                >
                                    Cancel query
                                </Button>
                            </Box>
                        </Box>
                        {queryActionError && (
                            <Alert severity="error" sx={{ mt: 1 }}>
                                {queryActionError}
                            </Alert>
                        )}
                    </Grid>

                    <Grid container spacing={3}>
                        <Grid size={{ xs: 12, md: 6 }}>
                            <Box sx={{ pt: 2 }}>
                                <Typography variant="h6">Session</Typography>
                                <Divider />
                            </Box>
                            <TableContainer>
                                <Table aria-label="simple table" sx={compactTableSx}>
                                    <TableBody>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>User</TableCell>
                                            <TableCell>{queryStatusInfo.session.user}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Principal</TableCell>
                                            <TableCell>{queryStatusInfo.session.principal}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Source</TableCell>
                                            <TableCell>{queryStatusInfo.session.source}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Catalog</TableCell>
                                            <TableCell>{queryStatusInfo.session.catalog}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Schema</TableCell>
                                            <TableCell>{queryStatusInfo.session.schema}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Time zone</TableCell>
                                            <TableCell>{queryStatusInfo.session.timeZone}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Client address</TableCell>
                                            <TableCell>{queryStatusInfo.session.remoteUserAddress}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Client tags</TableCell>
                                            <TableCell>{queryStatusInfo.session.clientTags.join(', ')}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Protocol encoding</TableCell>
                                            <TableCell>
                                                {queryStatusInfo.session.queryDataEncoding
                                                    ? 'spooled ' + queryStatusInfo.session.queryDataEncoding
                                                    : 'non-spooled'}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Session properties</TableCell>
                                            <TableCell>{renderSessionProperties(queryStatusInfo.session)}</TableCell>
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
                                <Table aria-label="simple table" sx={compactTableSx}>
                                    <TableBody>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Resource group</TableCell>
                                            <TableCell>
                                                {queryStatusInfo.resourceGroupId
                                                    ? queryStatusInfo.resourceGroupId.join('.')
                                                    : 'n/a'}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Submission time</TableCell>
                                            <TableCell>
                                                {formatShortDateTime(new Date(queryStatusInfo.queryStats.createTime))}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Completion time</TableCell>
                                            <TableCell>
                                                {queryStatusInfo.queryStats.endTime
                                                    ? formatShortDateTime(new Date(queryStatusInfo.queryStats.endTime))
                                                    : ''}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Elapsed time</TableCell>
                                            <TableCell>{queryStatusInfo.queryStats.elapsedTime}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Queued time</TableCell>
                                            <TableCell>{queryStatusInfo.queryStats.queuedTime}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Elapsed time</TableCell>
                                            <TableCell>{queryStatusInfo.queryStats.elapsedTime}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Planned time</TableCell>
                                            <TableCell>{queryStatusInfo.queryStats.planningTime}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Execution time</TableCell>
                                            <TableCell>{queryStatusInfo.queryStats.executionTime}</TableCell>
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
                                <Table aria-label="simple table" sx={compactTableSx}>
                                    <TableBody>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>CPU time</TableCell>
                                            <TableCell>{queryStatusInfo.queryStats.totalCpuTime}</TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>{queryStatusInfo.queryStats.failedCpuTime}</TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Planning CPU time</TableCell>
                                            <TableCell>{queryStatusInfo.queryStats.planningCpuTime}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Scheduled time</TableCell>
                                            <TableCell>{queryStatusInfo.queryStats.totalScheduledTime}</TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>{queryStatusInfo.queryStats.failedScheduledTime}</TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Input rows</TableCell>
                                            <TableCell>
                                                {formatCount(queryStatusInfo.queryStats.processedInputPositions)}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {queryStatusInfo.queryStats.failedProcessedInputPositions}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Input data</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(
                                                    queryStatusInfo.queryStats.processedInputDataSize
                                                )}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {queryStatusInfo.queryStats.failedProcessedInputDataSize}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Physical input rows</TableCell>
                                            <TableCell>
                                                {formatCount(queryStatusInfo.queryStats.physicalInputPositions)}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {queryStatusInfo.queryStats.failedPhysicalInputPositions}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Physical input data</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(
                                                    queryStatusInfo.queryStats.physicalInputDataSize
                                                )}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {queryStatusInfo.queryStats.failedPhysicalInputDataSize}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Physical input read time</TableCell>
                                            <TableCell>{queryStatusInfo.queryStats.physicalInputReadTime}</TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {queryStatusInfo.queryStats.failedPhysicalInputReadTime}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Internal network rows</TableCell>
                                            <TableCell>
                                                {formatCount(queryStatusInfo.queryStats.internalNetworkInputPositions)}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {formatCount(
                                                        queryStatusInfo.queryStats.failedInternalNetworkInputPositions
                                                    )}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Internal network data</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(
                                                    queryStatusInfo.queryStats.internalNetworkInputDataSize
                                                )}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {parseAndFormatDataSize(
                                                        queryStatusInfo.queryStats.failedInternalNetworkInputDataSize
                                                    )}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Peak user memory</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(
                                                    queryStatusInfo.queryStats.peakUserMemoryReservation
                                                )}
                                            </TableCell>
                                        </TableRow>
                                        {queryStatusInfo.queryStats.peakRevocableMemoryReservation && (
                                            <TableRow>
                                                <TableCell sx={{ fontWeight: 'bold' }}>Peak revocable memory</TableCell>
                                                <TableCell>
                                                    {parseAndFormatDataSize(
                                                        queryStatusInfo.queryStats.peakRevocableMemoryReservation
                                                    )}
                                                </TableCell>
                                            </TableRow>
                                        )}
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Peak total memory</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(
                                                    queryStatusInfo.queryStats.peakTotalMemoryReservation
                                                )}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Cumulative user memory</TableCell>
                                            <TableCell>
                                                {formatDataSize(
                                                    queryStatusInfo.queryStats.cumulativeUserMemory / 1000.0
                                                ) + '*seconds'}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {formatDataSize(
                                                        queryStatusInfo.queryStats.failedCumulativeUserMemory / 1000.0
                                                    ) + '*seconds'}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Output rows</TableCell>
                                            <TableCell>
                                                {formatCount(queryStatusInfo.queryStats.outputPositions)}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {formatCount(queryStatusInfo.queryStats.failedOutputPositions)}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Output data</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(queryStatusInfo.queryStats.outputDataSize)}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {parseAndFormatDataSize(
                                                        queryStatusInfo.queryStats.failedOutputDataSize
                                                    )}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Written rows</TableCell>
                                            <TableCell>
                                                {formatCount(queryStatusInfo.queryStats.writtenPositions)}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Logical written data</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(
                                                    queryStatusInfo.queryStats.logicalWrittenDataSize
                                                )}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold' }}>Physical written data</TableCell>
                                            <TableCell>
                                                {parseAndFormatDataSize(
                                                    queryStatusInfo.queryStats.physicalWrittenDataSize
                                                )}
                                            </TableCell>
                                            {taskRetriesEnabled && (
                                                <TableCell>
                                                    {parseAndFormatDataSize(
                                                        queryStatusInfo.queryStats.failedPhysicalWrittenDataSize
                                                    )}
                                                </TableCell>
                                            )}
                                        </TableRow>
                                        {queryStatusInfo.queryStats.spilledDataSize && (
                                            <TableRow>
                                                <TableCell sx={{ fontWeight: 'bold' }}>Spilled data</TableCell>
                                                <TableCell>
                                                    {parseAndFormatDataSize(queryStatusInfo.queryStats.spilledDataSize)}
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
                                <Table aria-label="simple table" sx={compactTableSx}>
                                    <TableBody>
                                        {renderTimeBreakdown(queryStatusInfo)}
                                        {renderNetworkBreakdown(queryStatusInfo.queryStats)}
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>
                                                Parallelism
                                            </TableCell>
                                            <TableCell>
                                                {renderSparkLine(derivedStatus.cpuTimeRate, formatCount)}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>
                                                Scheduled time/s
                                            </TableCell>
                                            <TableCell>
                                                {renderSparkLine(derivedStatus.scheduledTimeRate, formatCount)}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>
                                                Input rows/s
                                            </TableCell>
                                            <TableCell>
                                                {renderSparkLine(derivedStatus.rowInputRate, formatCount)}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>
                                                Input bytes/s
                                            </TableCell>
                                            <TableCell>
                                                {renderSparkLine(derivedStatus.byteInputRate, formatDataSize)}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>
                                                Physical input bytes/s
                                            </TableCell>
                                            <TableCell>
                                                {renderSparkLine(derivedStatus.physicalInputRate, formatDataSize)}
                                            </TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell sx={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>
                                                Memory utilization
                                            </TableCell>
                                            <TableCell>
                                                {renderSparkLine(derivedStatus.reservedMemory, formatDataSize)}
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
                                <CodeBlock
                                    language="sql"
                                    code={queryStatusInfo.query}
                                    monacoOptions={{ lineNumbers: 'on' }}
                                />
                            </Box>
                        </Grid>
                        {renderPreparedQuery()}
                        {renderStages(taskRetriesEnabled)}
                    </Grid>
                </Grid>
            )}
        </>
    )
}
