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
import { type Edge, type Node } from '@xyflow/react'
import { RemoteMergeNode } from './RemoteMergeNode'
import { StageNode } from './StageNode'
import { StepNode } from './StepNode'

export const STAGE_NODE_WIDTH = 400
export const STAGE_NODE_PADDING_TOP = 260
export const STEP_NODE_HEIGHT = 90
export const STEP_NODE_WIDTH = 340
export const STEP_NODE_PADDING_LEFT = 30
export const REMOTE_MERGE_NODE_HEIGHT = 4

export const nodeTypes = {
    remoteMergeNode: RemoteMergeNode,
    stageNode: StageNode,
    stepNode: StepNode,
}

export const getLayoutedElements = (nodes: Node[], edges: Edge[], options: { direction: string }) => {
    const g = new Dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}))
    g.setGraph({ rankdir: options.direction })

    // Only layout stage nodes - step nodes are positioned relative to their parents
    nodes
        .filter((node) => node.type === 'stageNode')
        .forEach((node) => {
            const { nrOfNodes } = node.data as { nrOfNodes: number }
            g.setNode(node.id, {
                width: STAGE_NODE_WIDTH,
                height: STAGE_NODE_PADDING_TOP + nrOfNodes * STEP_NODE_HEIGHT,
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
                const height = STAGE_NODE_PADDING_TOP + nrOfNodes * STEP_NODE_HEIGHT
                return {
                    ...node,
                    position: {
                        x: layoutedNode.x - width / 2,
                        y: layoutedNode.y - height / 2,
                    },
                }
            } else {
                // Step node - position relative to parent
                const { index } = node.data as { index: number }
                return {
                    ...node,
                    position: {
                        x: STEP_NODE_PADDING_LEFT,
                        y: STAGE_NODE_PADDING_TOP + index * STEP_NODE_HEIGHT,
                    },
                }
            }
        }),
        edges,
    }
}
