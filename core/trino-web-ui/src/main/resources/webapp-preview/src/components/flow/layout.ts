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
import Dagre from '@dagrejs/dagre'
import { type Edge, type Node, type Viewport } from '@xyflow/react'
import { RemoteExchangeNode } from './RemoteExchangeNode.tsx'
import { PlanFragmentNode } from './PlanFragmentNode.tsx'
import { OperatorNode } from './OperatorNode.tsx'
import { StagePipelineNode } from './StagePipelineNode.tsx'
import { StageOperatorNode } from './StageOperatorNode.tsx'

export const STAGE_NODE_WIDTH = 400
export const STAGE_NODE_PADDING_TOP = 280
export const OPERATOR_NODE_HEIGHT = 90
export const OPERATOR_NODE_WIDTH = 340
export const OPERATOR_NODE_PADDING_LEFT = 30
export const REMOTE_EXCHANGE_NODE_HEIGHT = 4
export const STAGE_PIPELINE_NODE_WIDTH = 400
export const STAGE_PIPELINE_NODE_GAP = 20
export const STAGE_PIPELINE_NODE_PADDING_TOP = 60
export const STAGE_PIPELINE_NODE_PADDING_LEFT = 30
export const STAGE_OPERATOR_NODE_WIDTH = 340
export const STAGE_OPERATOR_NODE_HEIGHT = 250
export const STAGE_OPERATOR_NODE_GAP = 50

/**
 * Node type definitions for the query execution plan flow visualization
 * Each node type represents a different component in the query execution hierarchy
 */
export const nodeTypes = {
    // PlanFragmentNode: Main container nodes in the flow, each representing a complete plan stage
    // Contains multiple OperatorNodes and organizes the operators within a stage
    planFragmentNode: PlanFragmentNode,

    // OperatorNode: Individual operator node within a stage (LocalMerge, PartialSort, Aggregate, etc.)
    // Building blocks that form subflows inside each StageNode, representing specific operations
    operatorNode: OperatorNode,

    // RemoteExchangeNode: Represents a specific operator that takes input from another downstream stage
    // Appears as a thin divider line connecting data flow between different stages
    remoteExchangeNode: RemoteExchangeNode,

    // StagePipelineNode: Container nodes representing a pipeline within a given stage
    // Groups multiple StageOperatorNodes and organizes operators that execute together in a pipeline
    stagePipelineNode: StagePipelineNode,

    // StageOperatorNode: Individual execution operator nodes within a pipeline (TableScan, Filter, etc.)
    // Shows performance metrics and represents specific operations that process data within a pipeline
    stageOperatorNode: StageOperatorNode,
}

export const getLayoutedPlanFlowElements = (nodes: Node[], edges: Edge[], options: { direction: string }) => {
    const g = new Dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}))
    g.setGraph({ rankdir: options.direction })

    // Only layout stage nodes - operator nodes are positioned relative to their parents
    nodes
        .filter((node) => node.type === 'planFragmentNode')
        .forEach((node) => {
            const { nrOfNodes } = node.data as { nrOfNodes: number }
            g.setNode(node.id, {
                width: STAGE_NODE_WIDTH,
                height: STAGE_NODE_PADDING_TOP + nrOfNodes * OPERATOR_NODE_HEIGHT,
            })
        })

    // Only consider edges between stages for layout
    edges
        .filter((edge) => edge.data?.remoteEdge)
        .forEach((edge) => {
            const { targetStageId } = edge.data!.remoteEdge as { targetStageId: string }
            g.setEdge(edge.source, targetStageId)
        })

    Dagre.layout(g)

    return {
        nodes: nodes.map((node) => {
            const layoutedNode = g.node(node.id)
            if (layoutedNode) {
                // Stage node - use dagre position
                const { nrOfNodes } = node.data as { nrOfNodes: number }
                const width = STAGE_NODE_WIDTH
                const height = STAGE_NODE_PADDING_TOP + nrOfNodes * OPERATOR_NODE_HEIGHT
                return {
                    ...node,
                    position: {
                        x: layoutedNode.x - width / 2,
                        y: layoutedNode.y - height / 2,
                    },
                }
            } else {
                // Operator node - position relative to parent
                const { index } = node.data as { index: number }
                return {
                    ...node,
                    position: {
                        x: OPERATOR_NODE_PADDING_LEFT,
                        y: STAGE_NODE_PADDING_TOP + index * OPERATOR_NODE_HEIGHT,
                    },
                }
            }
        }),
        edges,
    }
}

export const getLayoutedStagePerformanceElements = (nodes: Node[], edges: Edge[], options: { direction: string }) => {
    const layoutedPipelineNodes: Node[] = nodes
        .filter((node) => node.type === 'stagePipelineNode')
        .map((node) => {
            const { index } = node.data as { index: number }

            return {
                ...node,
                position:
                    options.direction == 'BT'
                        ? {
                              x: STAGE_PIPELINE_NODE_WIDTH * index + STAGE_PIPELINE_NODE_PADDING_LEFT * index,
                              y: 0,
                          }
                        : {
                              x: 0,
                              y:
                                  (STAGE_PIPELINE_NODE_PADDING_TOP +
                                      STAGE_OPERATOR_NODE_HEIGHT +
                                      STAGE_PIPELINE_NODE_GAP +
                                      STAGE_OPERATOR_NODE_GAP) *
                                  index,
                          },
            }
        })

    const layoutedOperatorNodes: Node[] = layoutedPipelineNodes.flatMap((stagePipelineNode) => {
        const stageOperatorNodes: Node[] = nodes.filter(
            (node) => node.type === 'stageOperatorNode' && node.parentId === stagePipelineNode.id
        )
        const g = new Dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}))
        g.setGraph({ rankdir: options.direction, ranksep: STAGE_OPERATOR_NODE_GAP })

        stageOperatorNodes.forEach((stageOperatorNode) => {
            g.setNode(stageOperatorNode.id, {
                width: STAGE_OPERATOR_NODE_WIDTH,
                height: STAGE_OPERATOR_NODE_HEIGHT,
            })
        })

        edges
            .filter((edge) => g.nodes().includes(edge.source))
            .forEach((edge) => {
                g.setEdge(edge.source, edge.target)
            })

        Dagre.layout(g)

        return stageOperatorNodes.map((node) => {
            const layoutedNode = g.node(node.id)
            if (layoutedNode) {
                return {
                    ...node,
                    position: {
                        x: STAGE_PIPELINE_NODE_PADDING_LEFT + layoutedNode.x - STAGE_OPERATOR_NODE_WIDTH / 2,
                        y: STAGE_PIPELINE_NODE_PADDING_TOP + layoutedNode.y - STAGE_OPERATOR_NODE_HEIGHT / 2,
                    },
                }
            } else {
                return node
            }
        })
    })

    return {
        nodes: [...layoutedPipelineNodes, ...layoutedOperatorNodes],
        edges,
    }
}

export const getViewportFocusedOnNode = (
    nodes: Node[],
    options?: { targetNodeId?: string; zoom?: number; padding?: number; containerWidth?: number }
): Viewport | undefined => {
    if (nodes.length === 0) {
        return undefined
    }
    const { targetNodeId, zoom = 0.8, padding = 20, containerWidth = 0 } = options ?? {}
    const targetNode = targetNodeId ? nodes.find((node) => node.id === targetNodeId) : undefined
    const focusNode = targetNode ?? nodes[0]
    const focusNodeWidth = focusNode.measured?.width ?? focusNode.width ?? 0
    return {
        // If width is known then center horizontally in container otherwise align to left
        x: focusNodeWidth
            ? containerWidth / 2 - (focusNode.position.x + focusNodeWidth / 2) * zoom
            : -focusNode.position.x * zoom + padding,
        y: -focusNode.position.y * zoom + padding,
        zoom,
    }
}
