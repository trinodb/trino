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
import { Alert, Box, CircularProgress, Grid, Typography } from '@mui/material'
import { ReactFlow, type Edge, type Node, useNodesState, useEdgesState, type Viewport } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { queryPlanApi, QueryPlan } from '../api/webapp/api.ts'
import { ApiResponse } from '../api/base.ts'
import { Texts } from '../constant.ts'
import { QueryProgressBar } from './QueryProgressBar'
import { nodeTypes, getLayoutedPlanFlowElements, getViewportFocusedOnNode } from './flow/layout'
import { HelpMessage } from './flow/HelpMessage'
import { getPlanFlowElements } from './flow/flowUtils'
import { LayoutDirectionType } from './flow/types'
import { useQueryStatus } from './QueryStatusContext'

export const QueryLivePlan = () => {
    const { queryStatusInfo, loading, error } = useQueryStatus()

    const [queryPlan, setQueryPlan] = useState<QueryPlan>({})
    const [queryPlanError, setQueryPlanError] = useState<string | null>(null)
    const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
    const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
    const [layoutDirection, setLayoutDirection] = useState<LayoutDirectionType>('BT')
    const [viewport, setViewport] = useState<Viewport>()

    const containerRef = useRef<HTMLDivElement>(null)

    const queryId = queryStatusInfo?.queryId
    // The plan of a stage does not change, so the plan is fetched again only when the stages do
    const stageIds = queryStatusInfo?.stages?.stages
        .filter((stage) => stage.plan)
        .map((stage) => stage.stageId)
        .join(',')

    useEffect(() => {
        setQueryPlan({})
        setQueryPlanError(null)
        setNodes([])
        setEdges([])
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [queryId])

    useEffect(() => {
        if (!queryId || !stageIds) {
            return
        }

        let cancelled = false

        const fetchPlan = () => {
            if (cancelled) {
                return
            }
            queryPlanApi(queryId).then((apiResponse: ApiResponse<QueryPlan>) => {
                if (cancelled) {
                    return
                }
                if (apiResponse.status === 200 && apiResponse.data) {
                    setQueryPlan(apiResponse.data)
                    setQueryPlanError(null)
                    return
                }
                setQueryPlanError(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`)
                // asking again does not help when the query cannot be viewed or is gone
                if (apiResponse.status !== 403 && apiResponse.status !== 410) {
                    setTimeout(fetchPlan, 3000)
                }
            })
        }

        fetchPlan()

        return () => {
            cancelled = true
        }
    }, [queryId, stageIds])

    useEffect(() => {
        if (queryStatusInfo?.stages) {
            const flowElements = getPlanFlowElements(queryStatusInfo.stages, queryPlan, layoutDirection)
            const layoutedElements = getLayoutedPlanFlowElements(flowElements.nodes, flowElements.edges, {
                direction: layoutDirection,
            })

            setNodes(layoutedElements.nodes)
            setEdges(layoutedElements.edges)
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [queryStatusInfo, queryPlan, layoutDirection])

    const focusViewportToFirstStage = () => {
        const viewportTarget = getViewportFocusedOnNode(nodes, {
            targetNodeId: 'stage-0',
            containerWidth: containerRef.current?.clientWidth ?? 0,
        })
        if (viewportTarget) {
            setViewport(viewportTarget)
        }
    }

    return (
        <>
            {loading && <CircularProgress />}
            {error && <Alert severity="error">{error}</Alert>}
            {!error && queryPlanError && <Alert severity="error">{queryPlanError}</Alert>}

            {!loading && !error && queryStatusInfo && (
                <Grid container spacing={0}>
                    <Grid size={{ xs: 12 }}>
                        <Box sx={{ pt: 2 }}>
                            <Box sx={{ width: '100%' }}>
                                <QueryProgressBar queryInfoBase={queryStatusInfo} />
                            </Box>

                            {queryStatusInfo?.stages && !queryStatusInfo.pruned ? (
                                <Grid container spacing={3}>
                                    <Grid size={{ xs: 12, md: 12 }}>
                                        <Box sx={{ pt: 2 }}>
                                            <Box
                                                ref={containerRef}
                                                sx={{ width: '100%', height: '80vh', border: '1px solid #ccc' }}
                                            >
                                                {nodes.length > 0 ? (
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
                                                            onOriginClick={focusViewportToFirstStage}
                                                        />
                                                    </ReactFlow>
                                                ) : (
                                                    <Typography sx={{ p: 1, fontSize: 'small' }}>
                                                        Rendering...
                                                    </Typography>
                                                )}
                                            </Box>
                                        </Box>
                                    </Grid>
                                </Grid>
                            ) : (
                                <>
                                    <Box sx={{ width: '100%', mt: 1 }}>
                                        <Alert severity="info">
                                            {queryStatusInfo.pruned
                                                ? 'Live plan is no longer available for this query.'
                                                : 'Live plan will appear automatically when query starts running.'}
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
