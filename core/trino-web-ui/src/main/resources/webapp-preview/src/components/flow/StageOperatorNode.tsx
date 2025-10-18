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
import { useState } from 'react'
import {
    Box,
    Card,
    CardContent,
    CardActionArea,
    Dialog,
    DialogTitle,
    DialogContent,
    Grid,
    Typography,
    DialogActions,
    Button,
    TableContainer,
    Table,
    TableBody,
    TableRow,
    TableCell,
    Divider,
} from '@mui/material'
import { BarChart } from '@mui/x-charts/BarChart'
import { Handle, Position } from '@xyflow/react'
import { STAGE_OPERATOR_NODE_WIDTH } from './layout'
import { QueryStageOperatorSummary, QueryTask } from '../../api/webapp/api.ts'
import {
    formatCount,
    formatDataSize,
    formatDuration,
    getTaskNumber,
    parseAndFormatDataSize,
    parseDataSize,
    parseDuration,
} from '../../utils/utils.ts'
import { LayoutDirectionType } from './types.ts'

export interface IStageOperatorNodeProps {
    data: {
        label: string
        stats: QueryStageOperatorSummary
        tasks: QueryTask[]
        layoutDirection: LayoutDirectionType
    }
}

/**
 * Represents individual execution operators within a pipeline (e.g., TableScan, Filter, Join, Aggregate).
 * These are the building blocks that show performance metrics and represent specific operations
 * that process data within a pipeline during query execution.
 *
 * Features:
 * - Displays the operator type (label) and detailed performance statistics
 * - Shows execution metrics (CPU time, wall time, input/output data rates, driver counts)
 * - Positioned as child nodes within StagePipelineNode containers
 * - Connected via edges showing data flow between operators in the pipeline
 */
export const StageOperatorNode = (props: IStageOperatorNodeProps) => {
    const { label, layoutDirection, stats, tasks } = props.data
    const [showModal, setShowModal] = useState<boolean>(false)

    const getTotalWallTime = (stats: QueryStageOperatorSummary) => {
        return (
            (parseDuration(stats.addInputWall) || 0) +
            (parseDuration(stats.getOutputWall) || 0) +
            (parseDuration(stats.finishWall) || 0) +
            (parseDuration(stats.blockedWall) || 0)
        )
    }

    const getTotalCpuTime = (stats: QueryStageOperatorSummary) => {
        return (
            (parseDuration(stats.addInputCpu) || 0) +
            (parseDuration(stats.getOutputCpu) || 0) +
            (parseDuration(stats.finishCpu) || 0)
        )
    }

    const getOperatorTasks = (
        tasks: QueryTask[],
        operatorSummary: QueryStageOperatorSummary
    ): QueryStageOperatorSummary[] => {
        return tasks
            .slice()
            .sort((taskA, taskB) => getTaskNumber(taskA.taskStatus.taskId) - getTaskNumber(taskB.taskStatus.taskId))
            .flatMap(
                (task) =>
                    task.stats.pipelines
                        .filter((pipeline) => pipeline.pipelineId === operatorSummary.pipelineId)
                        .flatMap((pipeline) =>
                            pipeline.operatorSummaries.filter(
                                (operator) => operator.operatorId === operatorSummary.operatorId
                            )
                        ) ?? []
            )
    }

    const operatorTasks = getOperatorTasks(tasks, stats)
    const totalWallTime = getTotalWallTime(stats)
    const totalCpuTime = getTotalCpuTime(stats)

    const rowInputRate = totalWallTime === 0 ? 0 : stats.inputPositions / (totalWallTime / 1000.0)
    const byteInputRate = totalWallTime === 0 ? 0 : (parseDataSize(stats.inputDataSize) || 0) / (totalWallTime / 1000.0)

    const rowOutputRate = totalWallTime === 0 ? 0 : stats.outputPositions / totalWallTime
    const byteOutputRate =
        totalWallTime === 0 ? 0 : (parseDataSize(stats.outputDataSize) || 0) / (totalWallTime / 1000.0)

    const handleClick = () => {
        setShowModal(true)
    }

    const handleModalClose = () => {
        setShowModal(false)
    }

    const StatisticRow = ({
        label,
        operatorTasks,
        supplier,
        valueFormatter,
    }: {
        label: string
        supplier: (stats: QueryStageOperatorSummary) => number | null
        operatorTasks: QueryStageOperatorSummary[]
        valueFormatter: typeof formatCount | typeof formatDataSize | typeof formatDuration
    }) => {
        const xAxisData: string[] = operatorTasks.map((_, index) => `Task ${index}`)
        const seriesData = operatorTasks.map((operatorTask) => supplier(operatorTask))

        return (
            <>
                <Grid size={{ xs: 3 }}>
                    <Typography variant="body2" color="text.secondary" sx={{ flex: 1, textAlign: 'right', pr: 2 }}>
                        {label}
                    </Typography>
                </Grid>
                <Grid size={{ xs: 9 }}>
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
                                valueFormatter: valueFormatter,
                            },
                        ]}
                        margin={{ left: 6, right: 0, top: 10, bottom: 6 }}
                        height={60}
                    />
                </Grid>
            </>
        )
    }

    const OperatorDetailDialog = () => (
        <Dialog open={showModal} onClose={handleModalClose} maxWidth="md" fullWidth>
            <DialogTitle>
                <Typography variant="subtitle2" component="span">
                    Pipeline {stats.pipelineId}
                </Typography>
                <Typography variant="h6" component="span" color="text.secondary" display="block">
                    {stats.operatorType}
                </Typography>
            </DialogTitle>
            <DialogContent>
                <Grid container spacing={3}>
                    <Grid size={{ xs: 12, md: 6 }}>
                        <TableContainer>
                            <Table aria-label="simple table">
                                <TableBody>
                                    <TableRow>
                                        <TableCell sx={{ fontWeight: 'bold' }}>Input</TableCell>
                                        <TableCell>
                                            {formatCount(stats.inputPositions) +
                                                ' rows (' +
                                                parseAndFormatDataSize(stats.inputDataSize) +
                                                ')'}
                                        </TableCell>
                                    </TableRow>
                                    <TableRow>
                                        <TableCell sx={{ fontWeight: 'bold' }}>Input rate</TableCell>
                                        <TableCell>
                                            {formatCount(rowInputRate) +
                                                ' rows/s (' +
                                                formatDataSize(byteInputRate) +
                                                '/s)'}
                                        </TableCell>
                                    </TableRow>
                                    <TableRow>
                                        <TableCell sx={{ fontWeight: 'bold' }}>Output</TableCell>
                                        <TableCell>
                                            {formatCount(stats.outputPositions) +
                                                ' rows (' +
                                                parseAndFormatDataSize(stats.outputDataSize) +
                                                ')'}
                                        </TableCell>
                                    </TableRow>
                                    <TableRow>
                                        <TableCell sx={{ fontWeight: 'bold' }}>Output rate</TableCell>
                                        <TableCell>
                                            {formatCount(rowOutputRate) +
                                                ' rows/s (' +
                                                formatDataSize(byteOutputRate) +
                                                '/s)'}
                                        </TableCell>
                                    </TableRow>
                                </TableBody>
                            </Table>
                        </TableContainer>
                    </Grid>
                    <Grid size={{ xs: 12, md: 6 }}>
                        <TableContainer>
                            <Table aria-label="simple table">
                                <TableBody>
                                    <TableRow>
                                        <TableCell sx={{ fontWeight: 'bold' }}>CPU time</TableCell>
                                        <TableCell>{formatDuration(totalCpuTime)}</TableCell>
                                    </TableRow>
                                    <TableRow>
                                        <TableCell sx={{ fontWeight: 'bold' }}>Wall time</TableCell>
                                        <TableCell>{formatDuration(totalWallTime)}</TableCell>
                                    </TableRow>
                                    <TableRow>
                                        <TableCell sx={{ fontWeight: 'bold' }}>Blocked</TableCell>
                                        <TableCell>{formatDuration(parseDuration(stats.blockedWall) || 0)}</TableCell>
                                    </TableRow>
                                    <TableRow>
                                        <TableCell sx={{ fontWeight: 'bold' }}>Drivers</TableCell>
                                        <TableCell>{stats.totalDrivers}</TableCell>
                                    </TableRow>
                                    <TableRow>
                                        <TableCell sx={{ fontWeight: 'bold' }}>Tasks</TableCell>
                                        <TableCell>{operatorTasks.length}</TableCell>
                                    </TableRow>
                                </TableBody>
                            </Table>
                        </TableContainer>
                    </Grid>
                    <Grid size={{ xs: 3 }}>
                        <Box sx={{ pt: 2 }}>
                            <Typography variant="h6">Statistics</Typography>
                            <Divider />
                        </Box>
                    </Grid>
                    <Grid size={{ xs: 9 }}>
                        <Box sx={{ pt: 2 }}>
                            <Typography variant="h6">Tasks</Typography>
                            <Divider />
                        </Box>
                    </Grid>
                    <StatisticRow
                        label="Total CPU time"
                        operatorTasks={operatorTasks}
                        supplier={getTotalCpuTime}
                        valueFormatter={formatDuration}
                    />
                    <StatisticRow
                        label="Total wall time"
                        supplier={getTotalWallTime}
                        operatorTasks={operatorTasks}
                        valueFormatter={formatDuration}
                    />
                    <StatisticRow
                        label="Input rows"
                        operatorTasks={operatorTasks}
                        supplier={(stats: QueryStageOperatorSummary) => stats.inputPositions}
                        valueFormatter={formatCount}
                    />
                    <StatisticRow
                        label="Input data size"
                        operatorTasks={operatorTasks}
                        supplier={(stats: QueryStageOperatorSummary) => parseDataSize(stats.inputDataSize)}
                        valueFormatter={formatDataSize}
                    />
                    <StatisticRow
                        label="Output rows"
                        operatorTasks={operatorTasks}
                        supplier={(stats: QueryStageOperatorSummary) => stats.outputPositions}
                        valueFormatter={formatCount}
                    />
                    <StatisticRow
                        label="Output data size"
                        operatorTasks={operatorTasks}
                        supplier={(stats: QueryStageOperatorSummary) => parseDataSize(stats.outputDataSize)}
                        valueFormatter={formatDataSize}
                    />
                </Grid>
            </DialogContent>
            <DialogActions>
                <Button onClick={handleModalClose} autoFocus>
                    Close
                </Button>
            </DialogActions>
        </Dialog>
    )

    return (
        <Box>
            <Card
                elevation={6}
                sx={{
                    border: '1px solid grey',
                    width: STAGE_OPERATOR_NODE_WIDTH,
                }}
            >
                <CardActionArea onClick={handleClick}>
                    <CardContent sx={{ p: 1 }}>
                        <Grid container spacing={1} sx={{ p: 1, textAlign: 'center' }}>
                            <Grid size={{ xs: 12 }}>
                                <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                    {label}
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 12 }}>
                                <Typography variant="body2" color="text.secondary">
                                    {formatCount(rowInputRate) + ' rows/s (' + formatDataSize(byteInputRate) + '/s)'}
                                </Typography>
                            </Grid>
                        </Grid>

                        <Grid container spacing={1}>
                            <Grid size={{ xs: 6 }}>
                                <Typography variant="body2" color="text.secondary">
                                    Output
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 6 }}>
                                <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                    {formatCount(stats.outputPositions) +
                                        ' rows (' +
                                        parseAndFormatDataSize(stats.outputDataSize) +
                                        ')'}
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 6 }}>
                                <Typography variant="body2" color="text.secondary">
                                    Drivers
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 6 }}>
                                <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                    {stats.totalDrivers}
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 6 }}>
                                <Typography variant="body2" color="text.secondary">
                                    CPU time
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 6 }}>
                                <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                    {formatDuration(totalCpuTime)}
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 6 }}>
                                <Typography variant="body2" color="text.secondary">
                                    Wall time
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 6 }}>
                                <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                    {formatDuration(totalWallTime)}
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 6 }}>
                                <Typography variant="body2" color="text.secondary">
                                    Blocked
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 6 }}>
                                <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                    {formatDuration(parseDuration(stats.blockedWall) || 0)}
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 6 }}>
                                <Typography variant="body2" color="text.secondary">
                                    Input
                                </Typography>
                            </Grid>
                            <Grid size={{ xs: 6 }}>
                                <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                    {formatCount(stats.inputPositions) +
                                        ' rows (' +
                                        parseAndFormatDataSize(stats.inputDataSize) +
                                        ')'}
                                </Typography>
                            </Grid>
                        </Grid>
                    </CardContent>
                </CardActionArea>
            </Card>

            <OperatorDetailDialog />

            <Handle
                style={{ opacity: 0 }}
                id="handle-target"
                type="target"
                position={layoutDirection === 'BT' ? Position.Bottom : Position.Right}
            />
            <Handle
                style={{ opacity: 0 }}
                id="handle-source"
                type="source"
                position={layoutDirection === 'BT' ? Position.Top : Position.Left}
            />
        </Box>
    )
}
