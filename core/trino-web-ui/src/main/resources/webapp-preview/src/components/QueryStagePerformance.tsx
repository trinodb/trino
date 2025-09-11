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
import {
    Alert,
    Box,
    CircularProgress,
    FormControl,
    Grid2 as Grid,
    InputLabel,
    MenuItem,
    Select,
    SelectChangeEvent,
} from '@mui/material'
import { type Edge, type Node, ReactFlow, useEdgesState, useNodesState } from '@xyflow/react'
import { queryStatusApi, QueryStatusInfo, QueryStage } from '../api/webapp/api.ts'
import { ApiResponse } from '../api/base.ts'
import { Texts } from '../constant.ts'
import { QueryProgressBar } from './QueryProgressBar.tsx'
import { HelpMessage } from './flow/HelpMessage'
import { nodeTypes, getLayoutedStagePerformanceElements } from './flow/layout'
import { LayoutDirectionType } from './flow/types'
import { getStagePerformanceFlowElements } from './flow/flowUtils.ts'

interface IQueryStatus {
    info: QueryStatusInfo | null
    ended: boolean
}

export const QueryStagePerformance = () => {
    const { queryId } = useParams()
    const initialQueryStatus: IQueryStatus = {
        info: null,
        ended: false,
    }

    const [queryStatus, setQueryStatus] = useState<IQueryStatus>(initialQueryStatus)
    const [stagePlanIds, setStagePlanIds] = useState<string[]>([])
    const [stagePlanId, setStagePlanId] = useState<string>()
    const [stage, setStage] = useState<QueryStage>()
    const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
    const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
    const [layoutDirection, setLayoutDirection] = useState<LayoutDirectionType>('BT')

    const [loading, setLoading] = useState<boolean>(true)
    const [error, setError] = useState<string | null>(null)
    const queryStatusRef = useRef(queryStatus)
    const containerRef = useRef<HTMLDivElement>(null)

    useEffect(() => {
        queryStatusRef.current = queryStatus
    }, [queryStatus])

    useEffect(() => {
        if (queryStatus.info?.stages) {
            const allStagePlanIds = queryStatus.info.stages.stages.map((stage) => stage.plan.id)
            setStagePlanIds(allStagePlanIds)
            setStagePlanId(allStagePlanIds[0])
        }
    }, [queryStatus])

    useEffect(() => {
        if (queryStatus.ended && stagePlanId) {
            const stage = queryStatus.info?.stages.stages.find((stage) => stage.plan.id === stagePlanId)
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
    }, [queryStatus, stagePlanId, layoutDirection])

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
            queryStatusApi(queryId, false).then((apiResponse: ApiResponse<QueryStatusInfo>) => {
                setLoading(false)
                if (apiResponse.status === 200 && apiResponse.data) {
                    setQueryStatus({
                        info: apiResponse.data,
                        ended: apiResponse.data.finalQueryInfo,
                    })
                    setError(null)
                } else {
                    setError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`)
                }
            })
        }
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

    const handleStageIdChange = (event: SelectChangeEvent) => {
        setStagePlanId(event.target.value as string)
    }

    return (
        <>
            {loading && <CircularProgress />}
            {error && <Alert severity="error">{Texts.Error.QueryNotFound}</Alert>}

            {!loading && !error && queryStatus.info && (
                <Grid container spacing={0}>
                    <Grid size={{ xs: 12 }}>
                        <Box sx={{ pt: 2 }}>
                            <Box sx={{ width: '100%' }}>
                                <QueryProgressBar queryInfoBase={queryStatus.info} />
                            </Box>

                            {queryStatus.ended ? (
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
                                                        defaultViewport={{ x: 200, y: 20, zoom: 0.8 }}
                                                    >
                                                        <HelpMessage
                                                            layoutDirection={layoutDirection}
                                                            onLayoutDirectionChange={setLayoutDirection}
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
