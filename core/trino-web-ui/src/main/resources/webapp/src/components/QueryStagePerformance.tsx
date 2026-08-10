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
import {
    Alert,
    Box,
    CircularProgress,
    FormControl,
    Grid,
    InputLabel,
    MenuItem,
    Select,
    SelectChangeEvent,
} from '@mui/material'
import { type Edge, type Node, ReactFlow, useEdgesState, useNodesState, type Viewport } from '@xyflow/react'
import { QueryStage } from '../api/webapp/api.ts'
import { QueryProgressBar } from './QueryProgressBar.tsx'
import { HelpMessage } from './flow/HelpMessage'
import { nodeTypes, getLayoutedStagePerformanceElements, getViewportFocusedOnNode } from './flow/layout'
import { LayoutDirectionType } from './flow/types'
import { getStagePerformanceFlowElements } from './flow/flowUtils.ts'
import { useQueryStatus } from './QueryStatusContext'

export const QueryStagePerformance = () => {
    const { queryStatusInfo, loading, error, ended } = useQueryStatus()

    const [stagePlanIds, setStagePlanIds] = useState<string[]>([])
    const [stagePlanId, setStagePlanId] = useState<string>()
    const [stage, setStage] = useState<QueryStage>()
    const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
    const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
    const [layoutDirection, setLayoutDirection] = useState<LayoutDirectionType>('BT')
    const [viewport, setViewport] = useState<Viewport>()

    const containerRef = useRef<HTMLDivElement>(null)

    useEffect(() => {
        if (queryStatusInfo?.stages) {
            const allStagePlanIds = queryStatusInfo.stages.stages.map((stage) => stage.plan.id)
            setStagePlanIds(allStagePlanIds)
            setStagePlanId(allStagePlanIds[0])
        }
    }, [queryStatusInfo])

    useEffect(() => {
        if (ended && stagePlanId) {
            const stage = queryStatusInfo?.stages.stages.find((stage) => stage.plan.id === stagePlanId)
            setStage(stage)

            if (stage) {
                const flowElements = getStagePerformanceFlowElements(stage, layoutDirection)
                const layoutedElements = getLayoutedStagePerformanceElements(flowElements.nodes, flowElements.edges, {
                    direction: layoutDirection,
                })

                setNodes(layoutedElements.nodes)
                setEdges(layoutedElements.edges)
            }
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [queryStatusInfo, ended, stagePlanId, layoutDirection])

    const smallFormControlSx = {
        fontSize: '0.8rem',
    }

    const smallDropdownMenuPropsSx = {
        slotProps: {
            paper: {
                sx: {
                    '& .MuiMenuItem-root': smallFormControlSx,
                },
            },
        },
    }

    const focusViewportToFirstPipeline = () => {
        const viewportTarget = getViewportFocusedOnNode(nodes, {
            targetNodeId: 'pipeline-0',
            containerWidth: containerRef.current?.clientWidth ?? 0,
        })
        if (viewportTarget) {
            setViewport(viewportTarget)
        }
    }

    const handleStageIdChange = (event: SelectChangeEvent) => {
        setStagePlanId(event.target.value as string)
    }

    return (
        <>
            {loading && <CircularProgress />}
            {error && <Alert severity="error">{error}</Alert>}

            {!loading && !error && queryStatusInfo && (
                <Grid container spacing={0}>
                    <Grid size={{ xs: 12 }}>
                        <Box sx={{ pt: 2 }}>
                            <Box sx={{ width: '100%' }}>
                                <QueryProgressBar queryInfoBase={queryStatusInfo} />
                            </Box>

                            {ended ? (
                                <Grid container spacing={3}>
                                    <Grid size={{ xs: 12, md: 12 }}>
                                        <Box sx={{ pt: 2 }}>
                                            <Box
                                                ref={containerRef}
                                                sx={{ width: '100%', height: '80vh', border: '1px solid #ccc' }}
                                            >
                                                {stage ? (
                                                    <ReactFlow
                                                        nodes={nodes}
                                                        edges={edges}
                                                        onNodesChange={onNodesChange}
                                                        onEdgesChange={onEdgesChange}
                                                        nodeTypes={nodeTypes}
                                                        minZoom={0.1}
                                                        proOptions={{ hideAttribution: true }}
                                                        viewport={viewport}
                                                        onViewportChange={setViewport}
                                                        fitView
                                                    >
                                                        <HelpMessage
                                                            layoutDirection={layoutDirection}
                                                            onLayoutDirectionChange={setLayoutDirection}
                                                            onOriginClick={focusViewportToFirstPipeline}
                                                            additionalContent={
                                                                <Box sx={{ mt: 2 }}>
                                                                    <FormControl size="small" sx={{ minWidth: 200 }}>
                                                                        <InputLabel sx={smallFormControlSx}>
                                                                            Stage
                                                                        </InputLabel>
                                                                        <Select
                                                                            label="Stage"
                                                                            sx={smallFormControlSx}
                                                                            MenuProps={smallDropdownMenuPropsSx}
                                                                            value={stagePlanId}
                                                                            onChange={handleStageIdChange}
                                                                        >
                                                                            {stagePlanIds.map((stageId) => (
                                                                                <MenuItem key={stageId} value={stageId}>
                                                                                    {stageId}
                                                                                </MenuItem>
                                                                            ))}
                                                                        </Select>
                                                                    </FormControl>
                                                                </Box>
                                                            }
                                                        />
                                                    </ReactFlow>
                                                ) : (
                                                    <Alert severity="error">Stage not found.</Alert>
                                                )}
                                            </Box>
                                        </Box>
                                    </Grid>
                                </Grid>
                            ) : (
                                <>
                                    <Box sx={{ width: '100%', mt: 1 }}>
                                        <Alert severity="info">
                                            Operator graph will appear automatically when query completes.
                                        </Alert>
                                    </Box>
                                </>
                            )}
                        </Box>
                    </Grid>
                </Grid>
            )}
        </>
    )
}
