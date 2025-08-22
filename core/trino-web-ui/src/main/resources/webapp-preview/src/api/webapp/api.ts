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
    rawInputDataSize: string
    rawInputPositions: number
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

export interface QueryStatusInfo extends QueryInfoBase {
    session: Session
    query: string
    preparedQuery: string
    resourceGroupId: string[]
    retryPolicy: string
    pruned: boolean
    finalQueryInfo: boolean
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

export async function queryApi(): Promise<ApiResponse<QueryInfo[]>> {
    return await api.get<QueryInfo[]>('/ui/api/query')
}

export async function queryStatusApi(queryId: string): Promise<ApiResponse<QueryStatusInfo>> {
    return await api.get<QueryStatusInfo>(`/ui/api/query/${queryId}`)
}
