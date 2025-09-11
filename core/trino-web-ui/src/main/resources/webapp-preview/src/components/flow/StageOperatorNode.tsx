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
import { Box, Card, CardContent, Grid2 as Grid, Typography } from '@mui/material'
import { Handle, Position } from '@xyflow/react'
import { STAGE_OPERATOR_NODE_WIDTH } from './layout'
import { QueryStageOperatorSummary } from '../../api/webapp/api.ts'
import {
    formatCount,
    formatDataSize,
    formatDuration,
    parseAndFormatDataSize,
    parseDataSize,
    parseDuration,
} from '../../utils/utils.ts'
import { LayoutDirectionType } from './types.ts'

export interface IStageOperatorNodeProps {
    data: {
        label: string
        stats: QueryStageOperatorSummary
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
    const { label, layoutDirection, stats } = props.data

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

    const totalWallTime = getTotalWallTime(stats)
    const totalCpuTime = getTotalCpuTime(stats)

    const rowInputRate = totalWallTime === 0 ? 0 : stats.inputPositions / (totalWallTime / 1000.0)
    const byteInputRate = totalWallTime === 0 ? 0 : (parseDataSize(stats.inputDataSize) || 0) / (totalWallTime / 1000.0)

    return (
        <Box>
            <Card
                elevation={6}
                sx={{
                    border: '1px solid grey',
                    width: STAGE_OPERATOR_NODE_WIDTH,
                }}
            >
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
            </Card>

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
