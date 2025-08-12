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
import { Alert, Box, CircularProgress, Grid2 as Grid } from '@mui/material'
import { ReactFlow, type Edge, type Node, useNodesState, useEdgesState } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { queryStatusApi, QueryStatusInfo } from '../api/webapp/api.ts'
import { QueryProgressBar } from './QueryProgressBar'
import { nodeTypes, getLayoutedElements } from './flow/layout'
import { HelpMessage } from './flow/HelpMessage'
import { getFlowElements } from './flow/flowUtils'
import { IQueryStatus, LayoutDirectionType } from './flow/types'
import { ApiResponse } from '../api/base.ts'
import { Texts } from '../constant.ts'

export const QueryLivePlan = () => {
    const { queryId } = useParams()
    const initialQueryStatus: IQueryStatus = {
        info: null,
        ended: false,
    }

    const [queryStatus, setQueryStatus] = useState<IQueryStatus>(initialQueryStatus)
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
            const flowElements = getFlowElements(queryStatus.info.stages, layoutDirection)
            const layoutedElements = getLayoutedElements(flowElements.nodes, flowElements.edges, {
                direction: layoutDirection,
            })

            setNodes(layoutedElements.nodes)
            setEdges(layoutedElements.edges)
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [queryStatus, layoutDirection])

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

                            {queryStatus.info?.stages ? (
                                <Grid container spacing={3}>
                                    <Grid size={{ xs: 12, md: 12 }}>
                                        <Box sx={{ pt: 2 }}>
                                            <Box
                                                ref={containerRef}
                                                sx={{ width: '100%', height: '80vh', border: '1px solid #ccc' }}
                                            >
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
                                                    />
                                                </ReactFlow>
                                            </Box>
                                        </Box>
                                    </Grid>
                                </Grid>
                            ) : (
                                <>
                                    <Box sx={{ width: '100%', mt: 1 }}>
                                        <Alert severity="info">
                                            Live plan will appear automatically when query starts running.
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
