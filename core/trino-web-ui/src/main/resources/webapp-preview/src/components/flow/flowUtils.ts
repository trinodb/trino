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
import { QueryStage, QueryStages, QueryStageNodeInfo, QueryStageOperatorSummary } from '../../api/webapp/api'
import { formatRows, parseAndFormatDataSize } from '../../utils/utils'
import { IFlowElements, IPlanFragmentNodeInfo, IPlanNodeProps, LayoutDirectionType } from './types'

// Constants for ID generation
const ID_PREFIXES = {
    PIPELINE: 'pipeline-',
    OPERATOR: 'operator-',
    STAGE: 'stage-',
    NODE: 'node-',
} as const

const ID_SEPARATORS = {
    OPERATOR: '-operator-',
    STAGE: '-stage-',
} as const

// ID Generation Helper Functions
const IdGenerator = {
    pipeline: (key: number | string): string => `${ID_PREFIXES.PIPELINE}${key}`,
    operator: (pipelineId: string, operatorId: string): string => `${pipelineId}${ID_SEPARATORS.OPERATOR}${operatorId}`,
    stage: (key: string): string => `${ID_PREFIXES.STAGE}${key}`,
    node: (key: string): string => `${ID_PREFIXES.NODE}${key}`,
    edge: (source: string, target: string): string => `${source}-${target}`,
}

export const parseRemoteSources = (sourceFragmentIds: string | undefined): string[] => {
    if (!sourceFragmentIds || sourceFragmentIds.trim() === '[]') {
        return []
    }
    return sourceFragmentIds.replace('[', '').replace(']', '').split(', ')
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
    id: IdGenerator.edge(source, target),
    source,
    target,
    markerEnd: options.hasArrow ? { type: MarkerType.ArrowClosed } : undefined,
    style: { strokeWidth: 3 },
    animated: options.isAnimated,
    label: options.label,
    labelStyle: { fontSize: 16, fontWeight: 'bold' },
    data: { remoteEdge: options.remoteEdge },
})

export const createPlanFragmentNode = (
    key: string,
    stageNodeInfo: IPlanFragmentNodeInfo,
    layoutDirection: LayoutDirectionType
): Node => ({
    id: IdGenerator.stage(key),
    type: 'planFragmentNode',
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
        id: IdGenerator.node(key),
        type: remoteSources.length === 0 ? 'operatorNode' : 'remoteExchangeNode',
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

export const createStagePipelineNode = (
    key: string,
    nrOfNodes: number,
    index: number,
    layoutDirection: LayoutDirectionType
): Node => ({
    id: IdGenerator.pipeline(key),
    type: 'stagePipelineNode',
    position: { x: 0, y: 0 },
    data: {
        index,
        label: `Pipeline ${key}`,
        nrOfNodes,
        layoutDirection,
    },
})

export const createStageOperatorNode = (
    pipelineId: string,
    key: string,
    operatorSummary: QueryStageOperatorSummary,
    index: number,
    layoutDirection: LayoutDirectionType
): Node => ({
    id: IdGenerator.operator(pipelineId, key),
    type: 'stageOperatorNode',
    position: { x: 0, y: 0 },
    data: {
        index,
        label: operatorSummary.operatorType,
        stats: operatorSummary,
        layoutDirection,
    },
    parentId: pipelineId,
    extent: 'parent',
})

export const flattenNode = (
    rootNodeInfo: QueryStageNodeInfo,
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

export const getPlanFragmentsNodeInfo = (queryStages: QueryStages): Map<string, IPlanFragmentNodeInfo> => {
    const planFragments: Map<string, IPlanFragmentNodeInfo> = new Map()

    queryStages.stages.forEach((queryStage: QueryStage) => {
        const nodes: Map<string, IPlanNodeProps> = new Map()
        flattenNode(queryStage.plan.root, JSON.parse(queryStage.plan.jsonRepresentation), nodes)

        planFragments.set(queryStage.plan.id, {
            stageId: queryStage.stageId,
            id: queryStage.plan.id,
            root: queryStage.plan.root.id,
            stageStats: queryStage.stageStats,
            state: queryStage.state,
            nodes: nodes,
        })
    })

    return planFragments
}

export const getPlanFlowElements = (queryStages: QueryStages, layoutDirection: LayoutDirectionType): IFlowElements => {
    const stages: Map<string, IPlanFragmentNodeInfo> = getPlanFragmentsNodeInfo(queryStages)

    const nodes: Node[] = Array.from(stages).flatMap(([key, planFragmentNodeInfo]) => {
        const stageId: string = IdGenerator.stage(key)
        const stageNode: Node = createPlanFragmentNode(key, planFragmentNodeInfo, layoutDirection)
        const childNodes: Node[] = Array.from(planFragmentNodeInfo.nodes).map(([childKey, node], childIndex) =>
            createChildNode(stageId, childKey, node, childIndex)
        )
        return [stageNode, ...childNodes]
    })

    const edges: Edge[] = Array.from(stages).flatMap(([key, planFragmentNodeInfo]) => {
        const stageId: string = IdGenerator.stage(key)
        return Array.from(planFragmentNodeInfo.nodes).flatMap(([nodeKey, node]) => {
            const targetNodeId: string = IdGenerator.node(nodeKey)

            const sourceEdges: Edge[] = Array.from(node.sources || []).map((sourceKey) =>
                createEdge(IdGenerator.node(sourceKey), targetNodeId, { hasArrow: true })
            )

            const remoteSources = parseRemoteSources(node.descriptor?.['sourceFragmentIds'])
            const remoteSourceEdges: Edge[] = remoteSources.map((sourceKey) =>
                createEdge(IdGenerator.stage(sourceKey), targetNodeId, {
                    isAnimated: true,
                    hasArrow: false,
                    label: `${parseAndFormatDataSize(planFragmentNodeInfo.stageStats.outputDataSize)} / ${formatRows(planFragmentNodeInfo.stageStats.outputPositions)}`,
                    remoteEdge: { targetStageId: stageId },
                })
            )

            return [...sourceEdges, ...remoteSourceEdges]
        })
    })

    return { nodes, edges }
}

const getStageNodeChildren = (nodeInfo: QueryStageNodeInfo) => {
    // TODO: Remove this function by migrating StageDetail to use node JSON representation
    switch (nodeInfo['@type']) {
        case 'aggregation':
        case 'assignUniqueId':
        case 'cacheData':
        case 'delete':
        case 'distinctLimit':
        case 'dynamicFilterSource':
        case 'explainAnalyze':
        case 'filter':
        case 'groupId':
        case 'limit':
        case 'markDistinct':
        case 'mergeProcessor':
        case 'mergeWriter':
        case 'output':
        case 'project':
        case 'rowNumber':
        case 'sample':
        case 'scalar':
        case 'simpleTableExecuteNode':
        case 'sort':
        case 'tableCommit':
        case 'tableExecute':
        case 'tableWriter':
        case 'topN':
        case 'topNRanking':
        case 'unnest':
        case 'window':
            return [nodeInfo.source]
        case 'join':
            return [nodeInfo.left, nodeInfo.right]
        case 'semiJoin':
            return [nodeInfo.source, nodeInfo.filteringSource]
        case 'spatialJoin':
            return [nodeInfo.left, nodeInfo.right]
        case 'indexJoin':
            return [nodeInfo.probeSource, nodeInfo.indexSource]
        case 'exchange':
        case 'intersect':
        case 'union':
            return nodeInfo.sources
        case 'indexSource':
        case 'loadCachedData':
        case 'refreshMaterializedView':
        case 'remoteSource':
        case 'tableDelete':
        case 'tableScan':
        case 'tableUpdate':
        case 'values':
            break
        default:
            console.log('NOTE: Unhandled PlanNode: ' + nodeInfo['@type'])
    }

    return []
}

const computeStageOperatorMap = (stage: QueryStage) => {
    const operatorMap: Map<string, QueryStageOperatorSummary[]> = new Map()

    for (const operator of stage.stageStats.operatorSummaries ?? []) {
        const list = operatorMap.get(operator.planNodeId) ?? []
        list.push(operator)
        operatorMap.set(operator.planNodeId, list)
    }

    return operatorMap
}

// Group operators by pipeline ID
const groupOperatorsByPipeline = (operators: QueryStageOperatorSummary[]): Map<number, QueryStageOperatorSummary[]> => {
    const pipelineOperators = new Map<number, QueryStageOperatorSummary[]>()
    operators.forEach((operator) => {
        if (!pipelineOperators.has(operator.pipelineId)) {
            pipelineOperators.set(operator.pipelineId, [])
        }
        pipelineOperators.get(operator.pipelineId)!.push(operator)
    })
    return pipelineOperators
}

const sortOperatorsInPipeline = (operators: QueryStageOperatorSummary[]): QueryStageOperatorSummary[] => {
    return operators.map((operator) => ({ ...operator })).sort((a, b) => a.operatorId - b.operatorId)
}

const chainOperators = (operators: QueryStageOperatorSummary[]): QueryStageOperatorSummary => {
    if (operators.length === 0) throw new Error('Cannot chain empty operators array')

    let currentOperator = operators[0]
    operators.slice(1).forEach((nextOperator) => {
        nextOperator.child = currentOperator
        currentOperator = nextOperator
    })

    return currentOperator // Return sink operator (last in chain)
}

const mergeSourceResults = (
    sourceResults: Map<number, QueryStageOperatorSummary>,
    pipelineResults: Map<number, QueryStageOperatorSummary>
): Map<number, QueryStageOperatorSummary> => {
    const merged = new Map(pipelineResults)
    sourceResults.forEach((operator, pipelineId) => {
        if (!merged.has(pipelineId)) {
            merged.set(pipelineId, operator)
        }
    })
    return merged
}

const processNodeOperators = (
    nodeOperators: QueryStageOperatorSummary[],
    sourceResults: Map<number, QueryStageOperatorSummary>
): Map<number, QueryStageOperatorSummary> => {
    const pipelineGroups = groupOperatorsByPipeline(nodeOperators)
    const pipelineResults = new Map<number, QueryStageOperatorSummary>()

    pipelineGroups.forEach((operators, pipelineId) => {
        const sortedOperators = sortOperatorsInPipeline(operators)

        // Link with source results if available
        if (sourceResults.has(pipelineId)) {
            const sourceResult = sourceResults.get(pipelineId)
            if (sourceResult && sortedOperators.length > 0) {
                sortedOperators[0].child = sourceResult
            }
        }

        const chainedOperator = chainOperators(sortedOperators)
        pipelineResults.set(pipelineId, chainedOperator)
    })

    return pipelineResults
}

const computeStageOperatorGraphs = (
    stageNodeInfo: QueryStageNodeInfo,
    operatorMap: Map<string, QueryStageOperatorSummary[]>
): Map<number, QueryStageOperatorSummary> => {
    const sources = getStageNodeChildren(stageNodeInfo).filter((source) => source)

    // Recursively process all source nodes
    const sourceResults = new Map<number, QueryStageOperatorSummary>()
    sources.forEach((source) => {
        const sourceResult = computeStageOperatorGraphs(source, operatorMap)
        sourceResult.forEach((operator, pipelineId) => {
            if (sourceResults.has(pipelineId)) {
                console.error('Multiple sources for', stageNodeInfo['@type'], 'had the same pipeline ID')
                return
            }
            sourceResults.set(pipelineId, operator)
        })
    })

    // Get operators for current node
    const nodeOperators = operatorMap.get(stageNodeInfo.id)
    if (!nodeOperators || nodeOperators.length === 0) {
        return sourceResults
    }

    // Process current node's operators
    const pipelineResults = processNodeOperators(nodeOperators, sourceResults)

    // Merge with source results
    return mergeSourceResults(sourceResults, pipelineResults)
}

const countOperatorChainDepth = (stageOperatorSummary: QueryStageOperatorSummary | null): number => {
    if (!stageOperatorSummary || !stageOperatorSummary.child) return 0
    return countOperatorChainDepth(stageOperatorSummary.child) + 1
}

const generateStageOperatorNodes = (
    pipelineId: string,
    stageOperatorSummary: QueryStageOperatorSummary,
    childIndex: number,
    layoutDirection: LayoutDirectionType
): Node[] => {
    const nodes = [
        createStageOperatorNode(
            pipelineId,
            stageOperatorSummary.operatorId.toString(),
            stageOperatorSummary,
            childIndex,
            layoutDirection
        ),
    ]

    if (stageOperatorSummary.child) {
        nodes.push(
            ...generateStageOperatorNodes(pipelineId, stageOperatorSummary.child, childIndex + 1, layoutDirection)
        )
    }

    return nodes
}

// Generate stage operator edges recursively
const generateStageOperatorEdges = (pipelineId: string, stageOperatorSummary: QueryStageOperatorSummary): Edge[] => {
    const edges: Edge[] = []

    if (stageOperatorSummary.child) {
        const source: string = IdGenerator.operator(pipelineId, stageOperatorSummary.child.operatorId.toString())
        const target: string = IdGenerator.operator(pipelineId, stageOperatorSummary.operatorId.toString())
        edges.push(createEdge(source, target, { hasArrow: true }))
        edges.push(...generateStageOperatorEdges(pipelineId, stageOperatorSummary.child))
    }

    return edges
}

export const getStagePerformanceFlowElements = (
    stage: QueryStage,
    layoutDirection: LayoutDirectionType
): IFlowElements => {
    const operatorMap = computeStageOperatorMap(stage)
    const operatorGraphs = computeStageOperatorGraphs(stage.plan.root, operatorMap)

    const nodes: Node[] = Array.from(operatorGraphs).flatMap(([key, stageOperatorSummary]) => {
        const pipelineId: string = IdGenerator.pipeline(key)
        const pipelineStageNode: Node = createStagePipelineNode(
            key.toString(),
            countOperatorChainDepth(stageOperatorSummary) + 1,
            key,
            layoutDirection
        )
        const childNodes: Node[] = generateStageOperatorNodes(pipelineId, stageOperatorSummary, 0, layoutDirection)

        return [pipelineStageNode, ...childNodes]
    })

    const edges: Edge[] = Array.from(operatorGraphs).flatMap(([key, stageOperatorSummary]) => {
        const pipelineId: string = IdGenerator.pipeline(key)
        return generateStageOperatorEdges(pipelineId, stageOperatorSummary)
    })

    return { nodes, edges }
}
