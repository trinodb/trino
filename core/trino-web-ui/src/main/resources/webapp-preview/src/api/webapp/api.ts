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
import { api, ApiResponse } from '../base.ts'

export interface Cluster {
    nodeVersion: {
        version: string
    }
    environment: string
    uptime: string
}

export interface Stats {
    runningQueries: number
    blockedQueries: number
    queuedQueries: number
    activeCoordinators: number
    activeWorkers: number
    runningDrivers: number
    totalAvailableProcessors: number
    reservedMemory: number
    totalInputRows: number
    totalInputBytes: number
    totalCpuTimeSecs: number
}

export interface Worker {
    coordinator: boolean
    nodeId: string
    nodeIp: string
    nodeVersion: string
    state: string
}

export interface MemoryAllocationItem {
    tag: string
    allocation: number
}

export interface MemoryUsagePool {
    freeBytes: number
    maxBytes: number
    reservedBytes: number
    reservedRevocableBytes: number
    queryMemoryAllocations: {
        [key: string]: MemoryAllocationItem
    }
    queryMemoryReservations: {
        [key: string]: number
    }
    queryMemoryRevocableReservations: {
        [key: string]: number
    }
}

export interface WorkerStatusInfo {
    coordinator: boolean
    environment: string
    externalAddress: string
    heapAvailable: number
    heapUsed: number
    internalAddress: string
    memoryInfo: {
        availableProcessors: number
        pool: MemoryUsagePool
    }
    nodeId: string
    nodeVersion: {
        version: string
    }
    nonHeapUsed: number
    processCpuLoad: number
    processors: number
    systemCpuLoad: number
    uptime: string
}

export interface QueryStats {
    analysisTime: string
    blockedDrivers: number
    completedDrivers: number
    createTime: string
    cumulativeUserMemory: number
    elapsedTime: string
    endTime: string
    executionTime: string
    failedCpuTime: string
    failedCumulativeUserMemory: number
    failedScheduledTime: string
    failedTasks: number
    finishingTime: string
    fullyBlocked: boolean
    internalNetworkInputDataSize: string
    failedInternalNetworkInputDataSize: string
    peakTotalMemoryReservation: string
    peakUserMemoryReservation: string
    peakRevocableMemoryReservation: string
    physicalInputPositions: number
    failedPhysicalInputPositions: number
    physicalInputDataSize: string
    failedPhysicalInputDataSize: string
    physicalInputReadTime: string
    failedPhysicalInputReadTime: string
    physicalWrittenDataSize: string
    failedPhysicalWrittenDataSize: string
    internalNetworkInputPositions: number
    failedInternalNetworkInputPositions: number
    planningTime: string
    planningCpuTime: string
    progressPercentage: number
    queuedDrivers: number
    queuedTime: string
    processedInputPositions: number
    failedProcessedInputPositions: number
    processedInputDataSize: string
    failedProcessedInputDataSize: string
    outputPositions: number
    failedOutputPositions: number
    outputDataSize: string
    failedOutputDataSize: string
    writtenPositions: number
    logicalWrittenDataSize: string
    runningDrivers: number
    runningPercentage: number
    spilledDataSize: string
    totalCpuTime: string
    totalDrivers: number
    totalMemoryReservation: string
    totalScheduledTime: string
    userMemoryReservation: string
    blockedReasons: string[]
}

export interface Warning {
    message: string
    warningCode: {
        name: string
    }
}

export interface StackInfo {
    message: string
    type: string
    suppressed: StackInfo[]
    stack: string[]
    cause: StackInfo
}

export interface QueryInfoBase {
    queryId: string
    queryStats: QueryStats
    queryType: string
    state: string
    scheduled: boolean
    memoryPool: string
    errorType: string
    errorCode: {
        code: string
        name: string
    }
    warnings: Warning[]
    failureInfo: StackInfo
}

export interface QueryInfo extends QueryInfoBase {
    clientTags: string[]
    queryTextPreview: string
    resourceGroupId: string[]
    retryPolicy: string
    self: string
    sessionPrincipal: string
    sessionSource: string
    sessionUser: string
    queryDataEncoding: string
    traceToken: string
}

export interface Session {
    queryId: string
    transactionId: string
    clientTransactionSupport: boolean
    user: string
    originalUser: string
    groups: string[]
    originalUserGroups: string[]
    principal: string
    enabledRoles: string[]
    source: string
    catalog: string
    schema: string
    timeZoneKey: number
    locale: string
    remoteUserAddress: string
    userAgent: string
    clientTags: string[]
    clientCapabilities: string[]
    start: string
    protocolName: string
    timeZone: string
    queryDataEncoding: string
    systemProperties: { [key: string]: string | number | boolean }
    catalogProperties: { [key: string]: string | number | boolean }
}

export interface QueryTable {
    catalog: string
    schema: string
    table: string
    authorization: string
    directlyReferenced: boolean
}

export interface QueryRoutine {
    routine: string
    authorization: string
}

export interface QueryStageNodeInfo {
    '@type': string
    id: string
    source: QueryStageNodeInfo
    sources: QueryStageNodeInfo[]
    filteringSource: QueryStageNodeInfo
    probeSource: QueryStageNodeInfo
    indexSource: QueryStageNodeInfo
    left: QueryStageNodeInfo
    right: QueryStageNodeInfo
}

export interface QueryStagePlan {
    id: string
    jsonRepresentation: string
    root: QueryStageNodeInfo
}

export interface QueryStageOperatorSummary {
    pipelineId: number
    planNodeId: string
    operatorId: number
    operatorType: string
    child: QueryStageOperatorSummary
    outputPositions: number
    outputDataSize: string
    totalDrivers: number
    addInputCpu: string
    getOutputCpu: string
    finishCpu: string
    addInputWall: string
    getOutputWall: string
    finishWall: string
    blockedWall: string
    inputDataSize: string
    inputPositions: number
}

export interface QueryStageStats {
    completedDrivers: number
    fullyBlocked: boolean
    totalCpuTime: string
    totalScheduledTime: string
    userMemoryReservation: string
    queuedDrivers: number
    runningDrivers: number
    blockedDrivers: number
    runningTasks: number
    completedTasks: number
    totalTasks: number
    processedInputDataSize: string
    processedInputPositions: number
    bufferedDataSize: string
    outputDataSize: string
    outputPositions: number
    totalBlockedTime: string
    failedScheduledTime: string
    failedCpuTime: string
    cumulativeUserMemory: number
    totalBufferedBytes: number
    failedCumulativeUserMemory: number
    peakUserMemoryReservation: string
    operatorSummaries: QueryStageOperatorSummary[]
}

export interface QueryPipeline {
    pipelineId: number
    operatorSummaries: QueryStageOperatorSummary[]
}

export interface QueryTask {
    lastHeartbeat: string
    needsPlan: boolean
    estimatedMemory: string
    outputBuffers: {
        totalBufferedBytes: number
    }
    stats: {
        createTime: string
        elapsedTime: string
        fullyBlocked: boolean
        blockedDrivers: number
        completedDrivers: number
        queuedDrivers: number
        peakUserMemoryReservation: string
        runningDrivers: number
        processedInputDataSize: string
        processedInputPositions: number
        totalCpuTime: string
        totalScheduledTime: string
        userMemoryReservation: string
        pipelines: QueryPipeline[]
        firstStartTime: string
        lastStartTime: string
        lastEndTime: string
        endTime: string
    }
    taskStatus: {
        nodeId: string
        taskId: string
        self: string
        state: string
    }
}

export interface QueryStage {
    coordinatorOnly: boolean
    plan: QueryStagePlan
    stageId: string
    state: string
    stageStats: QueryStageStats
    tasks: QueryTask[]
}

export interface QueryStages {
    outputStageId: string
    stages: QueryStage[]
}

export interface QueryStatusInfo extends QueryInfoBase {
    session: Session
    query: string
    preparedQuery: string
    resourceGroupId: string[]
    retryPolicy: string
    pruned: boolean
    finalQueryInfo: boolean
    referencedTables: QueryTable[]
    routines: QueryRoutine[]
    stages: QueryStages
}

export interface WorkerTaskInfo {
    dummy: string
}

export async function clusterApi(): Promise<ApiResponse<Cluster>> {
    return await api.get<Cluster>('/ui/api/cluster')
}

export async function statsApi(): Promise<ApiResponse<Stats>> {
    return await api.get<Stats>('/ui/api/stats')
}

export async function workerApi(): Promise<ApiResponse<Worker[]>> {
    return await api.get<Worker[]>('/ui/api/worker')
}

export async function workerStatusApi(nodeId: string): Promise<ApiResponse<WorkerStatusInfo>> {
    return await api.get<WorkerStatusInfo>(`/ui/api/worker/${nodeId}/status`)
}

export async function workerTaskApi(nodeId: string, taskId: string): Promise<ApiResponse<WorkerTaskInfo>> {
    return await api.get<WorkerTaskInfo>(`/ui/api/worker/${nodeId}/task/${taskId}`)
}

export async function queryApi(): Promise<ApiResponse<QueryInfo[]>> {
    return await api.get<QueryInfo[]>('/ui/api/query')
}

export async function queryStatusApi(queryId: string, pruned: boolean = false): Promise<ApiResponse<QueryStatusInfo>> {
    return await api.get<QueryStatusInfo>(`/ui/api/query/${queryId}${pruned ? '?pruned=true' : ''}`)
}
