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
import { MarkerType, type Edge, type Node } from '@xyflow/react'
import { QueryStage, QueryStages, QueryStagePlan } from '../../api/webapp/api'
import { formatRows, parseAndFormatDataSize } from '../../utils/utils'
import { IFlowElements, IPlanNodeProps, IStageNodeInfo, LayoutDirectionType } from './types'

export const parseRemoteSources = (sourceFragmentIds: string | undefined): string[] => {
    if (!sourceFragmentIds || sourceFragmentIds.trim() === '[]') {
        return []
    }
    try {
        const parsed = JSON.parse(sourceFragmentIds)
        return Array.isArray(parsed) ? parsed : []
    } catch {
        return sourceFragmentIds
            .replace(/[[\]]/g, '')
            .split(',')
            .map((s) => s.trim())
            .filter(Boolean)
    }
}

export const createEdge = (
    source: string,
    target: string,
    options: {
        isAnimated?: boolean
        hasArrow?: boolean
        label?: string
        remoteEdge?: { targetStageId: string }
    } = {}
): Edge => ({
    id: `${source}-${target}`,
    source,
    target,
    markerEnd: options.hasArrow ? { type: MarkerType.ArrowClosed } : undefined,
    style: { strokeWidth: 3 },
    animated: options.isAnimated,
    label: options.label,
    labelStyle: { fontSize: 16, fontWeight: 'bold' },
    data: { remoteEdge: options.remoteEdge },
})

export const createStageNode = (
    key: string,
    stageNodeInfo: IStageNodeInfo,
    layoutDirection: LayoutDirectionType
): Node => ({
    id: `stage-${key}`,
    type: 'stageNode',
    position: { x: 0, y: 0 },
    data: {
        label: `Stage ${key}`,
        nrOfNodes: stageNodeInfo.nodes.size,
        state: stageNodeInfo.state,
        stats: stageNodeInfo.stageStats,
        layoutDirection,
    },
})

export const createChildNode = (stageId: string, key: string, node: IPlanNodeProps, index: number): Node => {
    const remoteSources = parseRemoteSources(node.descriptor?.['sourceFragmentIds'])
    return {
        id: `node-${key}`,
        type: remoteSources.length === 0 ? 'stepNode' : 'remoteMergeNode',
        position: { x: 0, y: 0 },
        draggable: false,
        data: {
            index,
            label: node.name,
            descriptor: node.descriptor,
        },
        parentId: stageId,
        extent: 'parent',
    }
}

export const flattenNode = (
    rootNodeInfo: QueryStagePlan['root'],
    node: IPlanNodeProps,
    result: Map<string, IPlanNodeProps>
) => {
    result.set(node.id, {
        id: node.id,
        name: node.name,
        descriptor: node.descriptor,
        details: node.details,
        sources: node.children.map((child: IPlanNodeProps) => child.id),
        children: node.children,
    })
    if (node.children) {
        node.children.forEach((child: IPlanNodeProps) => flattenNode(rootNodeInfo, child, result))
    }
}

export const getStagesNodeInfo = (queryStages: QueryStages): Map<string, IStageNodeInfo> => {
    const stages: Map<string, IStageNodeInfo> = new Map()

    queryStages.stages.forEach((queryStage: QueryStage) => {
        const nodes: Map<string, IPlanNodeProps> = new Map()
        flattenNode(queryStage.plan.root, JSON.parse(queryStage.plan.jsonRepresentation), nodes)

        stages.set(queryStage.plan.id, {
            stageId: queryStage.stageId,
            id: queryStage.plan.id,
            root: queryStage.plan.root.id,
            stageStats: queryStage.stageStats,
            state: queryStage.state,
            nodes: nodes,
        })
    })

    return stages
}

export const getFlowElements = (queryStages: QueryStages, layoutDirection: LayoutDirectionType): IFlowElements => {
    const stages: Map<string, IStageNodeInfo> = getStagesNodeInfo(queryStages)

    const nodes: Node[] = Array.from(stages).flatMap(([key, stageNodeInfo]) => {
        const stageId: string = `stage-${key}`
        const stageNode: Node = createStageNode(key, stageNodeInfo, layoutDirection)
        const childNodes: Node[] = Array.from(stageNodeInfo.nodes).map(([childKey, node], childIndex) =>
            createChildNode(stageId, childKey, node, childIndex)
        )
        return [stageNode, ...childNodes]
    })

    const edges: Edge[] = Array.from(stages).flatMap(([key, stageNodeInfo]) => {
        const stageId: string = `stage-${key}`
        return Array.from(stageNodeInfo.nodes).flatMap(([nodeKey, node]) => {
            const targetNodeId: string = `node-${nodeKey}`

            const sourceEdges: Edge[] = Array.from(node.sources || []).map((sourceKey) =>
                createEdge(`node-${sourceKey}`, targetNodeId, { hasArrow: true })
            )

            const remoteSources = parseRemoteSources(node.descriptor?.['sourceFragmentIds'])
            const remoteSourceEdges: Edge[] = remoteSources.map((sourceKey) =>
                createEdge(`stage-${sourceKey}`, targetNodeId, {
                    isAnimated: true,
                    hasArrow: false,
                    label: `${parseAndFormatDataSize(stageNodeInfo.stageStats.outputDataSize)} / ${formatRows(stageNodeInfo.stageStats.outputPositions)}`,
                    remoteEdge: { targetStageId: stageId },
                })
            )

            return [...sourceEdges, ...remoteSourceEdges]
        })
    })

    return { nodes, edges }
}
