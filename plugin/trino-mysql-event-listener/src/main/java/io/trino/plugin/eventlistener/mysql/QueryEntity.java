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

public record QueryEntity(
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
        long planningCpuTimeMillis,
        long startingTimeMillis,
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
        String retryPolicy,
        Optional<String> operatorSummariesJson)
{
    public QueryEntity
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(transactionId, "transactionId is null");
        requireNonNull(query, "query is null");
        requireNonNull(updateType, "updateType is null");
        requireNonNull(preparedQuery, "preparedQuery is null");
        requireNonNull(queryState, "queryState is null");
        requireNonNull(plan, "plan is null");
        requireNonNull(stageInfoJson, "stageInfoJson is null");
        requireNonNull(user, "user is null");
        requireNonNull(principal, "principal is null");
        requireNonNull(traceToken, "traceToken is null");
        requireNonNull(remoteClientAddress, "remoteClientAddress is null");
        requireNonNull(userAgent, "userAgent is null");
        requireNonNull(clientInfo, "clientInfo is null");
        requireNonNull(clientTagsJson, "clientTagsJson is null");
        requireNonNull(source, "source is null");
        requireNonNull(catalog, "catalog is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(resourceGroupId, "resourceGroupId is null");
        requireNonNull(sessionPropertiesJson, "sessionPropertiesJson is null");
        requireNonNull(serverAddress, "serverAddress is null");
        requireNonNull(serverVersion, "serverVersion is null");
        requireNonNull(environment, "environment is null");
        requireNonNull(queryType, "queryType is null");
        requireNonNull(inputsJson, "inputsJson is null");
        requireNonNull(outputJson, "outputJson is null");
        requireNonNull(errorCode, "errorCode is null");
        requireNonNull(errorType, "errorType is null");
        requireNonNull(failureType, "failureType is null");
        requireNonNull(failureMessage, "failureMessage is null");
        requireNonNull(failureTask, "failureTask is null");
        requireNonNull(failureHost, "failureHost is null");
        requireNonNull(failuresJson, "failuresJson is null");
        requireNonNull(warningsJson, "warningsJson is null");
        requireNonNull(retryPolicy, "retryPolicy is null");
        requireNonNull(operatorSummariesJson, "operatorSummariesJson is null");
    }
}
