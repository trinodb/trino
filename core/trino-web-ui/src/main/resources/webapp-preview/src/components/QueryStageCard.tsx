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
import { Link as RouterLink } from 'react-router'
import {
    Box,
    Card,
    Grid2 as Grid,
    Typography,
    Accordion,
    AccordionSummary,
    AccordionDetails,
    Button,
    Dialog,
    DialogActions,
    DialogContent,
    DialogTitle,
    Divider,
    Link,
    Table,
    TableBody,
    TableContainer,
    TableHead,
    TableRow,
    TableCell,
    Tooltip,
    CircularProgress,
    Alert,
    FormControl,
    InputLabel,
    Select,
    MenuItem,
    Checkbox,
    ListItemText,
    TableSortLabel,
} from '@mui/material'
import Chip, { ChipProps } from '@mui/material/Chip'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import PauseCircleIcon from '@mui/icons-material/PauseCircle'
import PlayCircleIcon from '@mui/icons-material/PlayCircle'
import RemoveCircleRoundedIcon from '@mui/icons-material/RemoveCircleRounded'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import { BarChart } from '@mui/x-charts/BarChart'
import { Texts } from '../constant'
import { QueryStage, QueryTask, WorkerTaskInfo, workerTaskApi } from '../api/webapp/api'
import { ApiResponse } from '../api/base'
import {
    computeRate,
    formatCount,
    formatDataSize,
    formatDataSizeBytes,
    formatDuration,
    getHostname,
    getStageNumber,
    getTaskIdSuffix,
    parseAndFormatDataSize,
    parseDataSize,
    parseDuration,
} from '../utils/utils'
import { CodeBlock } from './CodeBlock'
import { styled } from '@mui/material/styles'

interface IQueryStageCard {
    key: string
    stage: QueryStage
    taskRetriesEnabled: boolean
}

interface WorkerTaskPath {
    nodeId: string
    taskId: string
}

const StyledLink = styled(RouterLink)(({ theme }) => ({
    textDecoration: 'none',
    color: theme.palette.info.main,
    '&:hover': {
        textDecoration: 'underline',
    },
}))

const TASK_STATE_TYPE = {
    Planned: 'Planned',
    Running: 'Running',
    Finished: 'Finished',
    Aborted_Canceled_Failed: 'Aborted/Canceled/Failed',
} as const

const KpiRow = ({ label, value, isLast = false }: { label: string; value: string | number; isLast?: boolean }) => (
    <Box sx={{ display: 'flex', mb: isLast ? 0 : 0.5 }}>
        <Typography variant="body2" color="text.secondary" sx={{ flex: 1, textAlign: 'right', pr: 2 }}>
            {label}
        </Typography>
        <Typography variant="body2" sx={{ flex: 1, textAlign: 'left' }}>
            {value}
        </Typography>
    </Box>
)

export const QueryStageCard = ({ stage, taskRetriesEnabled }: IQueryStageCard) => {
    const { stageStats } = stage

    const [workerTaskPath, setWorkerTaskPath] = useState<WorkerTaskPath | null>(null)
    const [workerTaskJson, setWorkerTaskJson] = useState<string | null>(null)
    const [showModal, setShowModal] = useState<boolean>(false)
    const [loading, setLoading] = useState<boolean>(true)
    const [error, setError] = useState<string | null>(null)
    const [taskStateFilters, setTaskStateFilters] = useState<string[]>(Object.values(TASK_STATE_TYPE))
    const [tasksOrderBy, setTasksOrderBy] = useState<string>('taskId')
    const [tasksOrder, setTasksOrder] = useState<'asc' | 'desc'>('asc')

    useEffect(() => {
        if (workerTaskPath) {
            setShowModal(true)
            getWorkerTaskJson(workerTaskPath.nodeId, workerTaskPath.taskId)
        }
    }, [workerTaskPath])

    const getWorkerTaskJson = (nodeId: string, taskId: string) => {
        if (nodeId && taskId) {
            setLoading(true)
            workerTaskApi(nodeId, taskId).then((apiResponse: ApiResponse<WorkerTaskInfo>) => {
                setLoading(false)
                if (apiResponse.status === 200 && apiResponse.data) {
                    setWorkerTaskJson(JSON.stringify(apiResponse.data, null, 2))
                    setError(null)
                } else {
                    setError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`)
                }
            })
        }
    }

    const STATE_COLOR_MAP: Record<string, ChipProps['color']> = {
        QUEUED: 'default',
        RUNNING: 'info',
        PLANNING: 'info',
        FINISHED: 'success',
        FAILED: 'error',
    }

    const getStateColor = (state: string): ChipProps['color'] => {
        switch (state) {
            case 'QUEUED':
                return STATE_COLOR_MAP.QUEUED
            case 'PLANNING':
                return STATE_COLOR_MAP.PLANNING
            case 'STARTING':
            case 'FINISHING':
            case 'RUNNING':
                return STATE_COLOR_MAP.RUNNING
            case 'FAILED':
                return STATE_COLOR_MAP.FAILED
            case 'FINISHED':
                return STATE_COLOR_MAP.FINISHED
            default:
                return STATE_COLOR_MAP.QUEUED
        }
    }

    const totalBufferedBytes: number = stage.tasks
        .map((task) => task.outputBuffers.totalBufferedBytes)
        .reduce((a, b) => a + b, 0)

    const handleTaskIdClick = (nodeId: string, taskId: string) => {
        setWorkerTaskPath({ nodeId, taskId })
    }

    const handleModalClose = () => {
        setShowModal(false)
    }

    const handleRequestSort = (property: string) => {
        const isAsc = tasksOrderBy === property && tasksOrder === 'asc'
        setTasksOrder(isAsc ? 'desc' : 'asc')
        setTasksOrderBy(property)
    }

    const createSortHandler = (property: string) => () => {
        handleRequestSort(property)
    }

    const renderSortableTableCell = (
        property: string,
        label: string,
        icon?: React.ReactNode,
        tooltipTitle?: string
    ) => (
        <TableCell align="right">
            {icon && tooltipTitle ? (
                <Tooltip placement="top-start" title={tooltipTitle}>
                    <Box display="flex" alignItems="center" justifyContent="flex-end">
                        <TableSortLabel
                            active={tasksOrderBy === property}
                            direction={tasksOrderBy === property ? tasksOrder : 'asc'}
                            onClick={createSortHandler(property)}
                            hideSortIcon={tasksOrderBy !== property}
                            sx={{
                                '& .MuiTableSortLabel-icon': {
                                    opacity: tasksOrderBy === property ? 1 : 0,
                                },
                            }}
                        >
                            {icon}
                        </TableSortLabel>
                    </Box>
                </Tooltip>
            ) : (
                <TableSortLabel
                    active={tasksOrderBy === property}
                    direction={tasksOrderBy === property ? tasksOrder : 'asc'}
                    onClick={createSortHandler(property)}
                    hideSortIcon={tasksOrderBy !== property}
                    sx={{
                        '& .MuiTableSortLabel-icon': {
                            opacity: tasksOrderBy === property ? 1 : 0,
                        },
                    }}
                >
                    {label}
                </TableSortLabel>
            )}
        </TableCell>
    )

    const renderHistogram = (tasks: QueryTask[], timeField: 'totalScheduledTime' | 'totalCpuTime') => {
        const inputData = tasks.map((task) => parseDuration(task.stats[timeField]) || 0)

        const numBuckets = Math.min(175, Math.floor(Math.sqrt(inputData.length)))
        const dataMin = Math.min(...inputData)
        const dataMax = Math.max(...inputData)
        const bucketSize = (dataMax - dataMin) / numBuckets

        const histogramData = bucketSize === 0 ? [inputData.length] : Array.from({ length: numBuckets + 1 }, () => 0)

        if (bucketSize !== 0) {
            for (const value of inputData) {
                const bucket = Math.floor((value - dataMin) / bucketSize)
                histogramData[bucket] += 1
            }
        }

        const tooltipValueLookups = histogramData.map(
            (_, i) => `${formatDuration(dataMin + i * bucketSize)} - ${formatDuration(dataMin + (i + 1) * bucketSize)}`
        )

        return (
            <BarChart
                xAxis={[
                    {
                        scaleType: 'band',
                        data: tooltipValueLookups,
                        disableTicks: true,
                    },
                ]}
                yAxis={[{ disableTicks: true }]}
                series={[{ data: histogramData, valueFormatter: (value) => `${value} tasks` }]}
                margin={{ left: 6, right: 0, top: 10, bottom: 6 }}
                height={112}
            />
        )
    }

    const renderTasksTimeBarChart = (tasks: QueryTask[], timeField: 'totalScheduledTime' | 'totalCpuTime') => {
        const orderedTasks = tasks.slice().sort((a, b) => a.taskStatus.taskId.localeCompare(b.taskStatus.taskId))

        const xAxisData: string[] = orderedTasks.map((task) => `Task ${getTaskIdSuffix(task.taskStatus.taskId)}`)
        const seriesData: number[] = orderedTasks.map((task) => parseDuration(task.stats[timeField]) || 0)

        return (
            <BarChart
                xAxis={[
                    {
                        scaleType: 'band',
                        data: xAxisData,
                        disableTicks: true,
                    },
                ]}
                yAxis={[{ disableTicks: true }]}
                series={[
                    {
                        data: seriesData,
                        valueFormatter: (value) => formatDuration(value || 0),
                    },
                ]}
                margin={{ left: 6, right: 0, top: 10, bottom: 6 }}
                height={100}
            />
        )
    }

    const renderTaskFilterSelectItem = (newTaskStateType: (typeof TASK_STATE_TYPE)[keyof typeof TASK_STATE_TYPE]) => {
        const handleClick = () => {
            const newTaskStateFilters = taskStateFilters.slice()
            if (taskStateFilters.includes(newTaskStateType)) {
                newTaskStateFilters.splice(newTaskStateFilters.indexOf(newTaskStateType), 1)
            } else {
                newTaskStateFilters.push(newTaskStateType)
            }
            setTaskStateFilters(newTaskStateFilters)
        }

        return (
            <MenuItem key={newTaskStateType} value={newTaskStateType}>
                <Checkbox
                    checked={taskStateFilters.includes(newTaskStateType)}
                    size="small"
                    sx={{ padding: 0 }}
                    onClick={handleClick}
                />
                <ListItemText primary={<Typography sx={smallFormControlSx}>{newTaskStateType}</Typography>} />
            </MenuItem>
        )
    }

    const sortTasks = (tasks: QueryTask[]) => {
        const getComparableValue = (task: QueryTask, property: string) => {
            switch (property) {
                case 'taskId':
                    return task.taskStatus.taskId
                case 'host':
                    return getHostname(task.taskStatus.self)
                case 'state':
                    return task.stats.fullyBlocked && task.taskStatus.state === 'RUNNING'
                        ? 'BLOCKED'
                        : task.taskStatus.state
                case 'pendingSplits':
                    return task.stats.queuedDrivers || 0
                case 'runningSplits':
                    return task.stats.runningDrivers || 0
                case 'blockedSplits':
                    return task.stats.blockedDrivers || 0
                case 'completedSplits':
                    return task.stats.completedDrivers || 0
                case 'rows':
                    return task.stats.processedInputPositions || 0
                case 'rowsPerSec': {
                    const elapsedTime =
                        parseDuration(task.stats.elapsedTime) || Date.now() - Date.parse(task.stats.createTime)
                    return computeRate(task.stats.processedInputPositions, elapsedTime)
                }
                case 'bytes':
                    return parseDataSize(task.stats.processedInputDataSize) || 0
                case 'bytesPerSec': {
                    const elapsedTimeBytes =
                        parseDuration(task.stats.elapsedTime) || Date.now() - Date.parse(task.stats.createTime)
                    return computeRate(parseDataSize(task.stats.processedInputDataSize) || 0, elapsedTimeBytes)
                }
                case 'elapsed':
                    return parseDuration(task.stats.elapsedTime) || 0
                case 'cpuTime':
                    return parseDuration(task.stats.totalCpuTime) || 0
                case 'memory':
                    return parseDataSize(task.stats.userMemoryReservation) || 0
                case 'peakMemory':
                    return parseDataSize(task.stats.peakUserMemoryReservation) || 0
                case 'estMemory':
                    return parseDataSize(task.estimatedMemory) || 0
                default:
                    return 0
            }
        }

        return tasks.slice().sort((a, b) => {
            const aValue = getComparableValue(a, tasksOrderBy)
            const bValue = getComparableValue(b, tasksOrderBy)

            if (typeof aValue === 'string' && typeof bValue === 'string') {
                return tasksOrder === 'asc' ? aValue.localeCompare(bValue) : bValue.localeCompare(aValue)
            }

            const numA = typeof aValue === 'number' ? aValue : 0
            const numB = typeof bValue === 'number' ? bValue : 0
            return tasksOrder === 'asc' ? numA - numB : numB - numA
        })
    }

    const getFilteredTasks = () => {
        return stage.tasks.filter((task) => {
            const taskState = task.taskStatus.state
            return taskStateFilters.some((filter) => {
                if (filter === TASK_STATE_TYPE.Planned) return taskState === 'PLANNED'
                if (filter === TASK_STATE_TYPE.Running) return taskState === 'RUNNING'
                if (filter === TASK_STATE_TYPE.Finished) return taskState === 'FINISHED'
                if (filter === TASK_STATE_TYPE.Aborted_Canceled_Failed)
                    return ['ABORTED', 'CANCELED', 'FAILED'].includes(taskState)
                return false
            })
        })
    }

    const smallFormControlSx = {
        fontSize: '0.8rem',
    }

    const smallDropdownMenuPropsSx = {
        PaperProps: {
            sx: {
                '& .MuiMenuItem-root': smallFormControlSx,
            },
        },
    }

    return (
        <Card
            sx={{
                borderLeft: '4px solid',
                borderColor: `${getStateColor(stage.state)}.main`,
                mb: 2,
            }}
        >
            <Accordion>
                <AccordionSummary
                    expandIcon={<ExpandMoreIcon />}
                    sx={{
                        px: 2,
                        py: 1,
                        alignItems: 'flex-start',
                        '&:hover': {
                            backgroundColor: 'action.hover',
                        },
                        '&:hover .MuiAccordionSummary-expandIconWrapper': {
                            color: 'primary.main',
                        },
                        '&.Mui-expanded .MuiAccordionSummary-expandIconWrapper': {
                            color: 'primary.dark',
                        },
                        '& .MuiAccordionSummary-expandIconWrapper': {
                            alignSelf: 'flex-end',
                            marginBottom: '0.5rem',
                        },
                    }}
                >
                    <Grid container spacing={2} alignItems="center" sx={{ width: '100%' }}>
                        <Grid size={{ xs: 2 }}>
                            <Box
                                style={{
                                    display: 'flex',
                                    flexDirection: 'column',
                                    alignItems: 'center',
                                    width: 'fit-content',
                                }}
                            >
                                <Typography variant="h3" sx={{ fontWeight: 'bold' }} color="textSecondary">
                                    {getStageNumber(stage.stageId)}
                                </Typography>
                                <Chip size="small" label={stage.state} color={getStateColor(stage.state)} />
                            </Box>
                        </Grid>

                        <Grid size={{ xs: 2 }}>
                            <Box sx={{ display: 'flex', mb: 0.5 }}>
                                <Typography variant="subtitle2" sx={{ flex: 1, textAlign: 'right', pr: 2 }}>
                                    Time
                                </Typography>
                                <Typography sx={{ flex: 1 }} />
                            </Box>

                            <KpiRow label="Scheduled" value={stageStats.totalScheduledTime} />
                            <KpiRow label="Blocked" value={stageStats.totalBlockedTime} />
                            <KpiRow label="CPU" value={stageStats.totalCpuTime} />
                            <KpiRow label="Failed" value={stageStats.failedScheduledTime} />
                            <KpiRow label="CPU Failed" value={stageStats.failedCpuTime} isLast />
                        </Grid>

                        <Grid size={{ xs: 2 }}>
                            <Box sx={{ display: 'flex', mb: 0.5 }}>
                                <Typography variant="subtitle2" sx={{ flex: 1, textAlign: 'right', pr: 2 }}>
                                    Memory
                                </Typography>
                                <Typography sx={{ flex: 1 }} />
                            </Box>
                            <KpiRow
                                label="Cumulative"
                                value={formatDataSizeBytes(stageStats.cumulativeUserMemory / 1000)}
                            />
                            <KpiRow label="Current" value={parseAndFormatDataSize(stageStats.userMemoryReservation)} />
                            <KpiRow label="Buffers" value={formatDataSize(totalBufferedBytes)} />
                            <KpiRow label="Peak" value={parseAndFormatDataSize(stageStats.peakUserMemoryReservation)} />
                            <KpiRow
                                label="Failed"
                                value={formatDataSizeBytes(stageStats.failedCumulativeUserMemory / 1000)}
                                isLast
                            />
                        </Grid>

                        <Grid size={{ xs: 2 }}>
                            <Box sx={{ display: 'flex', mb: 0.5 }}>
                                <Typography variant="subtitle2" sx={{ flex: 1, textAlign: 'right', pr: 2 }}>
                                    Tasks
                                </Typography>
                                <Typography sx={{ flex: 1 }} />
                            </Box>
                            <KpiRow
                                label="Pending"
                                value={stage.tasks.filter((task) => task.taskStatus.state === 'PLANNED').length}
                            />
                            <KpiRow
                                label="Running"
                                value={stage.tasks.filter((task) => task.taskStatus.state === 'RUNNING').length}
                            />
                            <KpiRow
                                label="Blocked"
                                value={stage.tasks.filter((task) => task.stats.fullyBlocked).length}
                            />
                            <KpiRow
                                label="Failed"
                                value={stage.tasks.filter((task) => task.taskStatus.state === 'FAILED').length}
                            />
                            <KpiRow label="Total" value={stage.tasks.length} isLast />
                        </Grid>

                        <Grid size={{ xs: 2 }}>
                            <Box sx={{ mb: 0.5 }}>
                                <Typography variant="subtitle2" sx={{ textAlign: 'right', pr: 2 }}>
                                    Scheduled time skew
                                </Typography>
                                <Typography sx={{ flex: 1 }} />
                            </Box>
                            <Box sx={{ mb: 0.5 }}>{renderHistogram(stage.tasks, 'totalScheduledTime')}</Box>
                        </Grid>

                        <Grid size={{ xs: 2 }}>
                            <Box sx={{ mb: 0.5 }}>
                                <Typography variant="subtitle2" sx={{ textAlign: 'center', pr: 2 }}>
                                    CPU time skew
                                </Typography>
                                <Typography sx={{ flex: 1 }} />
                            </Box>
                            <Box sx={{ mb: 0.5 }}>{renderHistogram(stage.tasks, 'totalCpuTime')}</Box>
                        </Grid>
                    </Grid>
                </AccordionSummary>
                <AccordionDetails>
                    <Box>
                        <Divider />
                        <Grid container spacing={2} alignItems="center" sx={{ width: '100%', mb: 0.5 }}>
                            <Grid size={{ xs: 2 }} />
                            <Grid size={{ xs: 2 }}>
                                <Typography
                                    variant="body2"
                                    color="text.secondary"
                                    sx={{ flex: 1, textAlign: 'right', pr: 2 }}
                                >
                                    Task scheduled time
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 8 }}>{renderTasksTimeBarChart(stage.tasks, 'totalScheduledTime')}</Grid>

                            <Grid size={{ xs: 2 }} />
                            <Grid size={{ xs: 2 }}>
                                <Typography
                                    variant="body2"
                                    color="text.secondary"
                                    sx={{ flex: 1, textAlign: 'right', pr: 2 }}
                                >
                                    Task CPU time
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 8 }}>{renderTasksTimeBarChart(stage.tasks, 'totalCpuTime')}</Grid>
                        </Grid>
                        <Divider />
                        <Box
                            sx={{
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'space-between',
                                mt: 1,
                                mb: 1,
                            }}
                        >
                            <Typography variant="h6">Tasks</Typography>
                            <FormControl size="small" sx={{ minWidth: 200 }}>
                                <InputLabel sx={smallFormControlSx}>States</InputLabel>
                                <Select
                                    label="States"
                                    sx={smallFormControlSx}
                                    MenuProps={smallDropdownMenuPropsSx}
                                    value={taskStateFilters}
                                    renderValue={(selected: string[]) => selected.join(', ')}
                                    multiple
                                >
                                    {renderTaskFilterSelectItem(TASK_STATE_TYPE.Planned)}
                                    {renderTaskFilterSelectItem(TASK_STATE_TYPE.Running)}
                                    {renderTaskFilterSelectItem(TASK_STATE_TYPE.Finished)}
                                    {renderTaskFilterSelectItem(TASK_STATE_TYPE.Aborted_Canceled_Failed)}
                                </Select>
                            </FormControl>
                        </Box>
                        <Divider />

                        <TableContainer>
                            <Table aria-label="simple table">
                                <TableHead>
                                    <TableRow>
                                        {renderSortableTableCell('taskId', 'ID')}
                                        {renderSortableTableCell('host', 'Host')}
                                        {renderSortableTableCell('state', 'State')}
                                        {renderSortableTableCell(
                                            'pendingSplits',
                                            '',
                                            <PauseCircleIcon sx={{ color: 'text.secondary' }} />,
                                            'Pending splits'
                                        )}
                                        {renderSortableTableCell(
                                            'runningSplits',
                                            '',
                                            <PlayCircleIcon sx={{ color: 'text.secondary' }} />,
                                            'Running splits'
                                        )}
                                        {renderSortableTableCell(
                                            'blockedSplits',
                                            '',
                                            <RemoveCircleRoundedIcon sx={{ color: 'text.secondary' }} />,
                                            'Blocked splits'
                                        )}
                                        {renderSortableTableCell(
                                            'completedSplits',
                                            '',
                                            <CheckCircleIcon sx={{ color: 'text.secondary' }} />,
                                            'Completed splits'
                                        )}
                                        {renderSortableTableCell('rows', 'Rows')}
                                        {renderSortableTableCell('rowsPerSec', 'Rows/s')}
                                        {renderSortableTableCell('bytes', 'Bytes')}
                                        {renderSortableTableCell('bytesPerSec', 'Bytes/s')}
                                        {renderSortableTableCell('elapsed', 'Elapsed')}
                                        {renderSortableTableCell('cpuTime', 'CPU time')}
                                        {renderSortableTableCell('memory', 'Mem')}
                                        {renderSortableTableCell('peakMemory', 'Peak mem')}
                                        {taskRetriesEnabled && renderSortableTableCell('estMemory', 'Est mem')}
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {sortTasks(getFilteredTasks()).map((task) => {
                                        let elapsedTime = parseDuration(task.stats.elapsedTime) || 0
                                        if (!elapsedTime) {
                                            elapsedTime = Date.now() - Date.parse(task.stats.createTime)
                                        }

                                        return (
                                            <TableRow key={task.taskStatus.taskId}>
                                                <TableCell component="th" scope="row">
                                                    <Link
                                                        sx={{
                                                            textDecoration: 'none',
                                                            color: 'info.main',
                                                            '&:hover': {
                                                                textDecoration: 'underline',
                                                            },
                                                        }}
                                                        component="button"
                                                        onClick={() =>
                                                            handleTaskIdClick(
                                                                task.taskStatus.nodeId,
                                                                task.taskStatus.taskId
                                                            )
                                                        }
                                                    >
                                                        {getTaskIdSuffix(task.taskStatus.taskId)}
                                                    </Link>
                                                </TableCell>
                                                <TableCell component="th" scope="row">
                                                    <StyledLink to={`/workers/${task.taskStatus.nodeId}`}>
                                                        {getHostname(task.taskStatus.self)}
                                                    </StyledLink>
                                                </TableCell>
                                                <TableCell align="right">
                                                    {task.stats.fullyBlocked && task.taskStatus.state === 'RUNNING'
                                                        ? 'BLOCKED'
                                                        : task.taskStatus.state}
                                                </TableCell>
                                                <TableCell align="right">{task.stats.queuedDrivers}</TableCell>
                                                <TableCell align="right">{task.stats.runningDrivers}</TableCell>
                                                <TableCell align="right">{task.stats.blockedDrivers}</TableCell>
                                                <TableCell align="right">{task.stats.completedDrivers}</TableCell>
                                                <TableCell align="right">
                                                    {formatCount(task.stats.processedInputPositions)}
                                                </TableCell>
                                                <TableCell align="right">
                                                    {formatCount(
                                                        computeRate(task.stats.processedInputPositions, elapsedTime)
                                                    )}
                                                </TableCell>
                                                <TableCell align="right">
                                                    {formatDataSizeBytes(
                                                        parseDataSize(task.stats.processedInputDataSize)
                                                    )}
                                                </TableCell>
                                                <TableCell align="right">
                                                    {formatDataSizeBytes(
                                                        computeRate(
                                                            parseDataSize(task.stats.processedInputDataSize) || 0,
                                                            elapsedTime
                                                        )
                                                    )}
                                                </TableCell>
                                                <TableCell align="right">{task.stats.elapsedTime}</TableCell>
                                                <TableCell align="right">{task.stats.totalCpuTime}</TableCell>
                                                <TableCell align="right">
                                                    {parseAndFormatDataSize(task.stats.userMemoryReservation)}
                                                </TableCell>
                                                <TableCell align="right">
                                                    {parseAndFormatDataSize(task.stats.peakUserMemoryReservation)}
                                                </TableCell>
                                                {taskRetriesEnabled && (
                                                    <TableCell align="right">
                                                        {parseAndFormatDataSize(task.estimatedMemory)}
                                                    </TableCell>
                                                )}
                                            </TableRow>
                                        )
                                    })}
                                </TableBody>
                            </Table>
                        </TableContainer>
                    </Box>
                </AccordionDetails>
            </Accordion>
            <Dialog open={showModal} maxWidth="md" fullWidth>
                <DialogTitle>Task JSON</DialogTitle>
                <DialogContent>
                    <Grid container spacing={0}>
                        <Grid size={{ xs: 12 }}>
                            <Box sx={{ pt: 2 }}>
                                {loading && <CircularProgress />}
                                {error && <Alert severity="error">{Texts.Error.QueryNotFound}</Alert>}

                                {!loading && !error && workerTaskJson && (
                                    <CodeBlock language="json" code={workerTaskJson} />
                                )}
                            </Box>
                        </Grid>
                    </Grid>
                </DialogContent>
                <DialogActions>
                    <Button onClick={handleModalClose}>Close</Button>
                </DialogActions>
            </Dialog>
        </Card>
    )
}
