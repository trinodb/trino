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
} from '@mui/material'
import Chip, { ChipProps } from '@mui/material/Chip'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import PauseCircleIcon from '@mui/icons-material/PauseCircle'
import PlayCircleIcon from '@mui/icons-material/PlayCircle'
import RemoveCircleRoundedIcon from '@mui/icons-material/RemoveCircleRounded'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import { Texts } from '../constant'
import { QueryStage, WorkerTaskInfo, workerTaskApi } from '../api/webapp/api'
import { ApiResponse } from '../api/base'
import {
    computeRate,
    formatCount,
    formatDataSize,
    formatDataSizeBytes,
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

                        <Grid size={{ xs: 4 }}>
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

                        <Grid size={{ xs: 3 }}>
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

                        <Grid size={{ xs: 3 }}>
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
                    </Grid>
                </AccordionSummary>
                <AccordionDetails>
                    <Box>
                        <Typography variant="h6">Tasks</Typography>
                        <Divider />

                        <TableContainer>
                            <Table aria-label="simple table">
                                <TableHead>
                                    <TableRow>
                                        <TableCell>ID</TableCell>
                                        <TableCell align="right">Host</TableCell>
                                        <TableCell align="right">State</TableCell>
                                        <TableCell align="right">
                                            <Tooltip placement="top-start" title="Pending splits">
                                                <Box display="flex" alignItems="center">
                                                    <PauseCircleIcon sx={{ color: 'text.secondary' }} />
                                                </Box>
                                            </Tooltip>
                                        </TableCell>
                                        <TableCell align="right">
                                            <Tooltip placement="top-start" title="Running splits">
                                                <Box display="flex" alignItems="center">
                                                    <PlayCircleIcon sx={{ color: 'text.secondary' }} />
                                                </Box>
                                            </Tooltip>
                                        </TableCell>
                                        <TableCell align="right">
                                            <Tooltip placement="top-start" title="Blocked splits">
                                                <Box display="flex" alignItems="center">
                                                    <RemoveCircleRoundedIcon sx={{ color: 'text.secondary' }} />
                                                </Box>
                                            </Tooltip>
                                        </TableCell>
                                        <TableCell align="right">
                                            <Tooltip placement="top-start" title="Completed splits">
                                                <Box display="flex" alignItems="center">
                                                    <CheckCircleIcon sx={{ color: 'text.secondary' }} />
                                                </Box>
                                            </Tooltip>
                                        </TableCell>
                                        <TableCell align="right">Rows</TableCell>
                                        <TableCell align="right">Rows/s</TableCell>
                                        <TableCell align="right">Bytes</TableCell>
                                        <TableCell align="right">Bytes/s</TableCell>
                                        <TableCell align="right">Elapsed</TableCell>
                                        <TableCell align="right">CPU time</TableCell>
                                        <TableCell align="right">Mem</TableCell>
                                        <TableCell align="right">Peak mem</TableCell>
                                        {taskRetriesEnabled && <TableCell align="right">Est mem</TableCell>}
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {stage.tasks.map((task) => {
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
                                                    {formatCount(task.stats.rawInputPositions)}
                                                </TableCell>
                                                <TableCell align="right">
                                                    {formatCount(
                                                        computeRate(task.stats.rawInputPositions, elapsedTime)
                                                    )}
                                                </TableCell>
                                                <TableCell align="right">
                                                    {formatDataSizeBytes(parseDataSize(task.stats.rawInputDataSize))}
                                                </TableCell>
                                                <TableCell align="right">
                                                    {formatDataSizeBytes(
                                                        computeRate(
                                                            parseDataSize(task.stats.rawInputDataSize) || 0,
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
