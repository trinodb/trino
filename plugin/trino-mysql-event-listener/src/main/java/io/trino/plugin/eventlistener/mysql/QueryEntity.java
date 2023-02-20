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
package io.trino.plugin.eventlistener.mysql;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryEntity
{
    private final String queryId;
    private final Optional<String> transactionId;

    private final String query;
    private final Optional<String> updateType;
    private final Optional<String> preparedQuery;
    private final String queryState;
    private final Optional<String> plan;
    private final Optional<String> stageInfoJson;

    private final String user;
    private final Optional<String> principal;
    private final Optional<String> traceToken;
    private final Optional<String> remoteClientAddress;
    private final Optional<String> userAgent;
    private final Optional<String> clientInfo;
    private final String clientTagsJson;
    private final Optional<String> source;

    private final Optional<String> catalog;
    private final Optional<String> schema;

    private final Optional<String> resourceGroupId;

    private final String sessionPropertiesJson;

    private final String serverAddress;
    private final String serverVersion;
    private final String environment;

    private final Optional<String> queryType;

    private final String inputsJson;
    private final Optional<String> outputJson;

    private final Optional<String> errorCode;
    private final Optional<String> errorType;
    private final Optional<String> failureType;
    private final Optional<String> failureMessage;
    private final Optional<String> failureTask;
    private final Optional<String> failureHost;
    private final Optional<String> failuresJson;

    private final String warningsJson;

    private final long cpuTimeMillis;
    private final long failedCpuTimeMillis;
    private final long wallTimeMillis;
    private final long queuedTimeMillis;
    private final long scheduledTimeMillis;
    private final long failedScheduledTimeMillis;
    private final long waitingTimeMillis;
    private final long analysisTimeMillis;
    private final long planningTimeMillis;
    private final long executionTimeMillis;
    private final long inputBlockedTimeMillis;
    private final long failedInputBlockedTimeMillis;
    private final long outputBlockedTimeMillis;
    private final long failedOutputBlockedTimeMillis;
    private final long physicalInputReadTimeMillis;

    private final long peakMemoryBytes;
    private final long peakTaskMemoryBytes;
    private final long physicalInputBytes;
    private final long physicalInputRows;
    private final long internalNetworkBytes;
    private final long internalNetworkRows;
    private final long totalBytes;
    private final long totalRows;
    private final long outputBytes;
    private final long outputRows;
    private final long writtenBytes;
    private final long writtenRows;

    private final double cumulativeMemory;
    private final double failedCumulativeMemory;

    private final int completedSplits;

    private final String retryPolicy;

    public QueryEntity(
            String queryId,
            Optional<String> transactionId,
            String query,
            Optional<String> updateType,
            Optional<String> preparedQuery,
            String queryState,
            Optional<String> plan,
            Optional<String> stageInfoJson,
            String user,
            Optional<String> principal,
            Optional<String> traceToken,
            Optional<String> remoteClientAddress,
            Optional<String> userAgent,
            Optional<String> clientInfo,
            String clientTagsJson,
            Optional<String> source,
            Optional<String> catalog,
            Optional<String> schema,
            Optional<String> resourceGroupId,
            String sessionPropertiesJson,
            String serverAddress,
            String serverVersion,
            String environment,
            Optional<String> queryType,
            String inputsJson,
            Optional<String> outputJson,
            Optional<String> errorCode,
            Optional<String> errorType,
            Optional<String> failureType,
            Optional<String> failureMessage,
            Optional<String> failureTask,
            Optional<String> failureHost,
            Optional<String> failuresJson,
            String warningsJson,
            long cpuTimeMillis,
            long failedCpuTimeMillis,
            long wallTimeMillis,
            long queuedTimeMillis,
            long scheduledTimeMillis,
            long failedScheduledTimeMillis,
            long waitingTimeMillis,
            long analysisTimeMillis,
            long planningTimeMillis,
            long executionTimeMillis,
            long inputBlockedTimeMillis,
            long failedInputBlockedTimeMillis,
            long outputBlockedTimeMillis,
            long failedOutputBlockedTimeMillis,
            long physicalInputReadTimeMillis,
            long peakMemoryBytes,
            long peakTaskMemoryBytes,
            long physicalInputBytes,
            long physicalInputRows,
            long internalNetworkBytes,
            long internalNetworkRows,
            long totalBytes,
            long totalRows,
            long outputBytes,
            long outputRows,
            long writtenBytes,
            long writtenRows,
            double cumulativeMemory,
            double failedCumulativeMemory,
            int completedSplits,
            String retryPolicy)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.query = requireNonNull(query, "query is null");
        this.updateType = requireNonNull(updateType, "updateType is null");
        this.preparedQuery = requireNonNull(preparedQuery, "preparedQuery is null");
        this.queryState = requireNonNull(queryState, "queryState is null");
        this.plan = requireNonNull(plan, "plan is null");
        this.stageInfoJson = requireNonNull(stageInfoJson, "stageInfoJson is null");
        this.user = requireNonNull(user, "user is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.traceToken = requireNonNull(traceToken, "traceToken is null");
        this.remoteClientAddress = requireNonNull(remoteClientAddress, "remoteClientAddress is null");
        this.userAgent = requireNonNull(userAgent, "userAgent is null");
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.clientTagsJson = requireNonNull(clientTagsJson, "clientTagsJson is null");
        this.source = requireNonNull(source, "source is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.resourceGroupId = requireNonNull(resourceGroupId, "resourceGroupId is null");
        this.sessionPropertiesJson = requireNonNull(sessionPropertiesJson, "sessionPropertiesJson is null");
        this.serverAddress = requireNonNull(serverAddress, "serverAddress is null");
        this.serverVersion = requireNonNull(serverVersion, "serverVersion is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.inputsJson = requireNonNull(inputsJson, "inputsJson is null");
        this.outputJson = requireNonNull(outputJson, "outputJson is null");
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
        this.errorType = requireNonNull(errorType, "errorType is null");
        this.failureType = requireNonNull(failureType, "failureType is null");
        this.failureMessage = requireNonNull(failureMessage, "failureMessage is null");
        this.failureTask = requireNonNull(failureTask, "failureTask is null");
        this.failureHost = requireNonNull(failureHost, "failureHost is null");
        this.failuresJson = requireNonNull(failuresJson, "failuresJson is null");
        this.warningsJson = requireNonNull(warningsJson, "warningsJson is null");
        this.cpuTimeMillis = cpuTimeMillis;
        this.failedCpuTimeMillis = failedCpuTimeMillis;
        this.wallTimeMillis = wallTimeMillis;
        this.queuedTimeMillis = queuedTimeMillis;
        this.scheduledTimeMillis = scheduledTimeMillis;
        this.failedScheduledTimeMillis = failedScheduledTimeMillis;
        this.waitingTimeMillis = waitingTimeMillis;
        this.analysisTimeMillis = analysisTimeMillis;
        this.planningTimeMillis = planningTimeMillis;
        this.executionTimeMillis = executionTimeMillis;
        this.inputBlockedTimeMillis = inputBlockedTimeMillis;
        this.failedInputBlockedTimeMillis = failedInputBlockedTimeMillis;
        this.outputBlockedTimeMillis = outputBlockedTimeMillis;
        this.failedOutputBlockedTimeMillis = failedOutputBlockedTimeMillis;
        this.physicalInputReadTimeMillis = physicalInputReadTimeMillis;
        this.peakMemoryBytes = peakMemoryBytes;
        this.peakTaskMemoryBytes = peakTaskMemoryBytes;
        this.physicalInputBytes = physicalInputBytes;
        this.physicalInputRows = physicalInputRows;
        this.internalNetworkBytes = internalNetworkBytes;
        this.internalNetworkRows = internalNetworkRows;
        this.totalBytes = totalBytes;
        this.totalRows = totalRows;
        this.outputBytes = outputBytes;
        this.outputRows = outputRows;
        this.writtenBytes = writtenBytes;
        this.writtenRows = writtenRows;
        this.cumulativeMemory = cumulativeMemory;
        this.failedCumulativeMemory = failedCumulativeMemory;
        this.completedSplits = completedSplits;
        this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
    }

    public String getQueryId()
    {
        return queryId;
    }

    public Optional<String> getTransactionId()
    {
        return transactionId;
    }

    public String getQuery()
    {
        return query;
    }

    public Optional<String> getUpdateType()
    {
        return updateType;
    }

    public Optional<String> getPreparedQuery()
    {
        return preparedQuery;
    }

    public String getQueryState()
    {
        return queryState;
    }

    public Optional<String> getPlan()
    {
        return plan;
    }

    public Optional<String> getStageInfoJson()
    {
        return stageInfoJson;
    }

    public String getUser()
    {
        return user;
    }

    public Optional<String> getPrincipal()
    {
        return principal;
    }

    public Optional<String> getTraceToken()
    {
        return traceToken;
    }

    public Optional<String> getRemoteClientAddress()
    {
        return remoteClientAddress;
    }

    public Optional<String> getUserAgent()
    {
        return userAgent;
    }

    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    public String getClientTagsJson()
    {
        return clientTagsJson;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    public Optional<String> getSchema()
    {
        return schema;
    }

    public Optional<String> getResourceGroupId()
    {
        return resourceGroupId;
    }

    public String getSessionPropertiesJson()
    {
        return sessionPropertiesJson;
    }

    public String getServerAddress()
    {
        return serverAddress;
    }

    public String getServerVersion()
    {
        return serverVersion;
    }

    public String getEnvironment()
    {
        return environment;
    }

    public Optional<String> getQueryType()
    {
        return queryType;
    }

    public String getInputsJson()
    {
        return inputsJson;
    }

    public Optional<String> getOutputJson()
    {
        return outputJson;
    }

    public Optional<String> getErrorCode()
    {
        return errorCode;
    }

    public Optional<String> getErrorType()
    {
        return errorType;
    }

    public Optional<String> getFailureType()
    {
        return failureType;
    }

    public Optional<String> getFailureMessage()
    {
        return failureMessage;
    }

    public Optional<String> getFailureTask()
    {
        return failureTask;
    }

    public Optional<String> getFailureHost()
    {
        return failureHost;
    }

    public Optional<String> getFailuresJson()
    {
        return failuresJson;
    }

    public String getWarningsJson()
    {
        return warningsJson;
    }

    public long getCpuTimeMillis()
    {
        return cpuTimeMillis;
    }

    public long getFailedCpuTimeMillis()
    {
        return failedCpuTimeMillis;
    }

    public long getWallTimeMillis()
    {
        return wallTimeMillis;
    }

    public long getQueuedTimeMillis()
    {
        return queuedTimeMillis;
    }

    public long getScheduledTimeMillis()
    {
        return scheduledTimeMillis;
    }

    public long getFailedScheduledTimeMillis()
    {
        return failedScheduledTimeMillis;
    }

    public long getWaitingTimeMillis()
    {
        return waitingTimeMillis;
    }

    public long getAnalysisTimeMillis()
    {
        return analysisTimeMillis;
    }

    public long getPlanningTimeMillis()
    {
        return planningTimeMillis;
    }

    public long getExecutionTimeMillis()
    {
        return executionTimeMillis;
    }

    public long getInputBlockedTimeMillis()
    {
        return inputBlockedTimeMillis;
    }

    public long getFailedInputBlockedTimeMillis()
    {
        return failedInputBlockedTimeMillis;
    }

    public long getOutputBlockedTimeMillis()
    {
        return outputBlockedTimeMillis;
    }

    public long getFailedOutputBlockedTimeMillis()
    {
        return failedOutputBlockedTimeMillis;
    }

    public long getPhysicalInputReadTimeMillis()
    {
        return physicalInputReadTimeMillis;
    }

    public long getPeakMemoryBytes()
    {
        return peakMemoryBytes;
    }

    public long getPeakTaskMemoryBytes()
    {
        return peakTaskMemoryBytes;
    }

    public long getPhysicalInputBytes()
    {
        return physicalInputBytes;
    }

    public long getPhysicalInputRows()
    {
        return physicalInputRows;
    }

    public long getInternalNetworkBytes()
    {
        return internalNetworkBytes;
    }

    public long getInternalNetworkRows()
    {
        return internalNetworkRows;
    }

    public long getTotalBytes()
    {
        return totalBytes;
    }

    public long getTotalRows()
    {
        return totalRows;
    }

    public long getOutputBytes()
    {
        return outputBytes;
    }

    public long getOutputRows()
    {
        return outputRows;
    }

    public long getWrittenBytes()
    {
        return writtenBytes;
    }

    public long getWrittenRows()
    {
        return writtenRows;
    }

    public double getCumulativeMemory()
    {
        return cumulativeMemory;
    }

    public double getFailedCumulativeMemory()
    {
        return failedCumulativeMemory;
    }

    public int getCompletedSplits()
    {
        return completedSplits;
    }

    public String getRetryPolicy()
    {
        return retryPolicy;
    }
}
