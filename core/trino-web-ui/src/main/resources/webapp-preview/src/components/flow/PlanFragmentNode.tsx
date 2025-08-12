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
import { Box, Card, CardContent, CardHeader, Divider, Grid2 as Grid, Typography, Tooltip } from '@mui/material'
import Chip, { ChipProps } from '@mui/material/Chip'
import { Handle, Position } from '@xyflow/react'
import { STAGE_NODE_PADDING_TOP, STAGE_NODE_WIDTH, OPERATOR_NODE_HEIGHT } from './layout'
import { QueryStageStats } from '../../api/webapp/api.ts'
import { formatRows, parseAndFormatDataSize } from '../../utils/utils.ts'
import { LayoutDirectionType } from './types.ts'

export interface IPlanFragmentNodeProps {
    data: {
        label: string
        nrOfNodes: number
        state: string
        stats: QueryStageStats
        layoutDirection: LayoutDirectionType
    }
}

/**
 * Main container nodes in the query execution plan flow, each representing a complete plan stage.
 * PlanFragmentNodes serve as the primary organizational units that contain multiple OperatorNodes
 * and display comprehensive execution statistics and status information for the entire stage.
 *
 * Features:
 * - Contains and organizes multiple OperatorNode components within its boundaries
 * - Displays stage execution metrics (CPU time, memory, blocked time, buffered data, splits)
 * - Shows real-time status with color-coded state chips (QUEUED, RUNNING, FINISHED, FAILED)
 * - Provides input/output data statistics for the stage
 */
export const PlanFragmentNode = (props: IPlanFragmentNodeProps) => {
    const { label, nrOfNodes, stats, state, layoutDirection } = props.data
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

    return (
        <Box>
            <Card
                elevation={3}
                sx={{
                    border: '1px solid #e0e0e0',
                    borderRadius: 2,
                    width: STAGE_NODE_WIDTH,
                    height: STAGE_NODE_PADDING_TOP + nrOfNodes * OPERATOR_NODE_HEIGHT,
                    backgroundColor: 'action.hover',
                }}
            >
                <CardHeader
                    title={
                        <Typography variant="h6" sx={{ fontWeight: 600, fontSize: '1rem' }}>
                            {label}
                        </Typography>
                    }
                    action={<Chip size="small" label={state} color={getStateColor(state)} />}
                    sx={{ pb: 1, mr: 1 }}
                />
                <CardContent sx={{ pt: 0 }}>
                    <Grid container spacing={1}>
                        <Grid size={{ xs: 6 }}>
                            <Typography variant="body2" color="text.secondary">
                                CPU Time
                            </Typography>
                        </Grid>
                        <Grid size={{ xs: 6 }}>
                            <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                {stats.totalCpuTime}
                            </Typography>
                        </Grid>
                        <Grid size={{ xs: 6 }}>
                            <Typography variant="body2" color="text.secondary">
                                Scheduled Time
                            </Typography>
                        </Grid>
                        <Grid size={{ xs: 6 }}>
                            <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                {stats.totalScheduledTime}
                            </Typography>
                        </Grid>
                        <Grid size={{ xs: 6 }}>
                            <Typography variant="body2" color="text.secondary">
                                Memory
                            </Typography>
                        </Grid>
                        <Grid size={{ xs: 6 }}>
                            <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                {parseAndFormatDataSize(stats.userMemoryReservation)}
                            </Typography>
                        </Grid>
                        <Grid size={{ xs: 6 }}>
                            <Typography variant="body2" color="text.secondary">
                                Buffered Data
                            </Typography>
                        </Grid>
                        <Grid size={{ xs: 6 }}>
                            <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                {parseAndFormatDataSize(stats.bufferedDataSize)}
                            </Typography>
                        </Grid>
                        <Grid size={{ xs: 6 }}>
                            <Tooltip placement="top" title="Drivers (queued / running / finished / blocked)">
                                <Typography variant="body2" color="text.secondary">
                                    Drivers (Q / R / F / B)
                                </Typography>
                            </Tooltip>
                        </Grid>
                        <Grid size={{ xs: 6 }}>
                            <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                {`${stats.queuedDrivers} / ${stats.runningDrivers} / ${stats.completedDrivers} / ${stats.blockedDrivers}`}
                            </Typography>
                        </Grid>
                        <Grid size={{ xs: 6 }}>
                            <Tooltip placement="top" title="Tasks (running / finished / total)">
                                <Typography variant="body2" color="text.secondary">
                                    Tasks (R / F / T)
                                </Typography>
                            </Tooltip>
                        </Grid>
                        <Grid size={{ xs: 6 }}>
                            <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                {`${stats.runningTasks} / ${stats.completedTasks} / ${stats.totalTasks}`}
                            </Typography>
                        </Grid>

                        <Grid size={{ xs: 12 }}>
                            <Divider sx={{ my: 1 }} />
                        </Grid>
                        <Grid size={{ xs: 6 }}>
                            <Typography variant="body2" color="text.secondary">
                                Input
                            </Typography>
                        </Grid>
                        <Grid size={{ xs: 6 }}>
                            <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                {parseAndFormatDataSize(stats.processedInputDataSize)} /{' '}
                                {formatRows(stats.processedInputPositions)}
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
