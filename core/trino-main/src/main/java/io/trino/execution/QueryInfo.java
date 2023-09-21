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
package io.trino.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.trino.SessionRepresentation;
import io.trino.client.NodeVersion;
import io.trino.operator.RetryPolicy;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoWarning;
import io.trino.spi.eventlistener.RoutineInfo;
import io.trino.spi.eventlistener.TableInfo;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.SelectedRole;
import io.trino.sql.analyzer.Output;
import io.trino.transaction.TransactionId;
import jakarta.annotation.Nullable;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class QueryInfo
{
    private final QueryId queryId;
    private final SessionRepresentation session;
    private final QueryState state;
    private final URI self;
    private final List<String> fieldNames;
    private final String query;
    private final Optional<String> preparedQuery;
    private final QueryStats queryStats;
    private final Optional<String> setCatalog;
    private final Optional<String> setSchema;
    private final Optional<String> setPath;
    private final Optional<String> setAuthorizationUser;
    private final boolean resetAuthorizationUser;
    private final Map<String, String> setSessionProperties;
    private final Set<String> resetSessionProperties;
    private final Map<String, SelectedRole> setRoles;
    private final Map<String, String> addedPreparedStatements;
    private final Set<String> deallocatedPreparedStatements;
    private final Optional<TransactionId> startedTransactionId;
    private final boolean clearTransactionId;
    private final String updateType;
    private final Optional<StageInfo> outputStage;
    private final List<TableInfo> referencedTables;
    private final List<RoutineInfo> routines;
    private final ExecutionFailureInfo failureInfo;
    private final ErrorType errorType;
    private final ErrorCode errorCode;
    private final List<TrinoWarning> warnings;
    private final Set<Input> inputs;
    private final Optional<Output> output;
    private final boolean finalQueryInfo;
    private final Optional<ResourceGroupId> resourceGroupId;
    private final Optional<QueryType> queryType;
    private final RetryPolicy retryPolicy;
    private final boolean pruned;
    private final NodeVersion version;

    @JsonCreator
    public QueryInfo(
            @JsonProperty("queryId") QueryId queryId,
            @JsonProperty("session") SessionRepresentation session,
            @JsonProperty("state") QueryState state,
            @JsonProperty("self") URI self,
            @JsonProperty("fieldNames") List<String> fieldNames,
            @JsonProperty("query") String query,
            @JsonProperty("preparedQuery") Optional<String> preparedQuery,
            @JsonProperty("queryStats") QueryStats queryStats,
            @JsonProperty("setCatalog") Optional<String> setCatalog,
            @JsonProperty("setSchema") Optional<String> setSchema,
            @JsonProperty("setPath") Optional<String> setPath,
            @JsonProperty("setAuthorizationUser") Optional<String> setAuthorizationUser,
            @JsonProperty("resetAuthorizationUser") boolean resetAuthorizationUser,
            @JsonProperty("setSessionProperties") Map<String, String> setSessionProperties,
            @JsonProperty("resetSessionProperties") Set<String> resetSessionProperties,
            @JsonProperty("setRoles") Map<String, SelectedRole> setRoles,
            @JsonProperty("addedPreparedStatements") Map<String, String> addedPreparedStatements,
            @JsonProperty("deallocatedPreparedStatements") Set<String> deallocatedPreparedStatements,
            @JsonProperty("startedTransactionId") Optional<TransactionId> startedTransactionId,
            @JsonProperty("clearTransactionId") boolean clearTransactionId,
            @JsonProperty("updateType") String updateType,
            @JsonProperty("outputStage") Optional<StageInfo> outputStage,
            @JsonProperty("failureInfo") ExecutionFailureInfo failureInfo,
            @JsonProperty("errorCode") ErrorCode errorCode,
            @JsonProperty("warnings") List<TrinoWarning> warnings,
            @JsonProperty("inputs") Set<Input> inputs,
            @JsonProperty("output") Optional<Output> output,
            @JsonProperty("referencedTables") List<TableInfo> referencedTables,
            @JsonProperty("routines") List<RoutineInfo> routines,
            @JsonProperty("finalQueryInfo") boolean finalQueryInfo,
            @JsonProperty("resourceGroupId") Optional<ResourceGroupId> resourceGroupId,
            @JsonProperty("queryType") Optional<QueryType> queryType,
            @JsonProperty("retryPolicy") RetryPolicy retryPolicy,
            @JsonProperty("pruned") boolean pruned,
            @JsonProperty("version") NodeVersion version)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(session, "session is null");
        requireNonNull(state, "state is null");
        requireNonNull(self, "self is null");
        requireNonNull(fieldNames, "fieldNames is null");
        requireNonNull(queryStats, "queryStats is null");
        requireNonNull(setCatalog, "setCatalog is null");
        requireNonNull(setSchema, "setSchema is null");
        requireNonNull(setPath, "setPath is null");
        requireNonNull(setAuthorizationUser, "setAuthorizationUser is null");
        requireNonNull(setSessionProperties, "setSessionProperties is null");
        requireNonNull(resetSessionProperties, "resetSessionProperties is null");
        requireNonNull(addedPreparedStatements, "addedPreparedStatements is null");
        requireNonNull(deallocatedPreparedStatements, "deallocatedPreparedStatements is null");
        requireNonNull(startedTransactionId, "startedTransactionId is null");
        requireNonNull(query, "query is null");
        requireNonNull(preparedQuery, "preparedQuery is null");
        requireNonNull(outputStage, "outputStage is null");
        requireNonNull(inputs, "inputs is null");
        requireNonNull(output, "output is null");
        requireNonNull(referencedTables, "referencedTables is null");
        requireNonNull(routines, "routines is null");
        requireNonNull(resourceGroupId, "resourceGroupId is null");
        requireNonNull(warnings, "warnings is null");
        requireNonNull(queryType, "queryType is null");
        requireNonNull(retryPolicy, "retryPolicy is null");
        requireNonNull(version, "version is null");

        this.queryId = queryId;
        this.session = session;
        this.state = state;
        this.self = self;
        this.fieldNames = ImmutableList.copyOf(fieldNames);
        this.query = query;
        this.preparedQuery = preparedQuery;
        this.queryStats = queryStats;
        this.setCatalog = setCatalog;
        this.setSchema = setSchema;
        this.setPath = setPath;
        this.setAuthorizationUser = setAuthorizationUser;
        this.resetAuthorizationUser = resetAuthorizationUser;
        this.setSessionProperties = ImmutableMap.copyOf(setSessionProperties);
        this.resetSessionProperties = ImmutableSet.copyOf(resetSessionProperties);
        this.setRoles = ImmutableMap.copyOf(setRoles);
        this.addedPreparedStatements = ImmutableMap.copyOf(addedPreparedStatements);
        this.deallocatedPreparedStatements = ImmutableSet.copyOf(deallocatedPreparedStatements);
        this.startedTransactionId = startedTransactionId;
        this.clearTransactionId = clearTransactionId;
        this.updateType = updateType;
        this.outputStage = outputStage;
        this.failureInfo = failureInfo;
        this.errorType = errorCode == null ? null : errorCode.getType();
        this.errorCode = errorCode;
        this.warnings = ImmutableList.copyOf(warnings);
        this.inputs = ImmutableSet.copyOf(inputs);
        this.output = output;
        this.referencedTables = ImmutableList.copyOf(referencedTables);
        this.routines = ImmutableList.copyOf(routines);
        this.finalQueryInfo = finalQueryInfo;
        checkArgument(!finalQueryInfo || state.isDone(), "finalQueryInfo without a terminal query state");
        this.resourceGroupId = resourceGroupId;
        this.queryType = queryType;
        this.retryPolicy = retryPolicy;
        this.pruned = pruned;
        this.version = version;
    }

    @JsonProperty
    public QueryId getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public SessionRepresentation getSession()
    {
        return session;
    }

    @JsonProperty
    public QueryState getState()
    {
        return state;
    }

    @JsonProperty
    public boolean isScheduled()
    {
        return queryStats.isScheduled();
    }

    @JsonProperty
    public OptionalDouble getProgressPercentage()
    {
        return queryStats.getProgressPercentage();
    }

    @JsonProperty
    public OptionalDouble getRunningPercentage()
    {
        return queryStats.getRunningPercentage();
    }

    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    @JsonProperty
    public List<String> getFieldNames()
    {
        return fieldNames;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public Optional<String> getPreparedQuery()
    {
        return preparedQuery;
    }

    @JsonProperty
    public QueryStats getQueryStats()
    {
        return queryStats;
    }

    @JsonProperty
    public Optional<String> getSetCatalog()
    {
        return setCatalog;
    }

    @JsonProperty
    public Optional<String> getSetSchema()
    {
        return setSchema;
    }

    @JsonProperty
    public Optional<String> getSetPath()
    {
        return setPath;
    }

    @JsonProperty
    public Optional<String> getSetAuthorizationUser()
    {
        return setAuthorizationUser;
    }

    @JsonProperty
    public boolean isResetAuthorizationUser()
    {
        return resetAuthorizationUser;
    }

    @JsonProperty
    public Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
    }

    @JsonProperty
    public Set<String> getResetSessionProperties()
    {
        return resetSessionProperties;
    }

    @JsonProperty
    public Map<String, SelectedRole> getSetRoles()
    {
        return setRoles;
    }

    @JsonProperty
    public Map<String, String> getAddedPreparedStatements()
    {
        return addedPreparedStatements;
    }

    @JsonProperty
    public Set<String> getDeallocatedPreparedStatements()
    {
        return deallocatedPreparedStatements;
    }

    @JsonProperty
    public Optional<TransactionId> getStartedTransactionId()
    {
        return startedTransactionId;
    }

    @JsonProperty
    public boolean isClearTransactionId()
    {
        return clearTransactionId;
    }

    @Nullable
    @JsonProperty
    public String getUpdateType()
    {
        return updateType;
    }

    @JsonProperty
    public Optional<StageInfo> getOutputStage()
    {
        return outputStage;
    }

    @Nullable
    @JsonProperty
    public ExecutionFailureInfo getFailureInfo()
    {
        return failureInfo;
    }

    @Nullable
    @JsonProperty
    public ErrorType getErrorType()
    {
        return errorType;
    }

    @Nullable
    @JsonProperty
    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    @JsonProperty
    public List<TrinoWarning> getWarnings()
    {
        return warnings;
    }

    @JsonProperty
    public boolean isFinalQueryInfo()
    {
        return finalQueryInfo;
    }

    @JsonProperty
    public Set<Input> getInputs()
    {
        return inputs;
    }

    @JsonProperty
    public Optional<Output> getOutput()
    {
        return output;
    }

    @JsonProperty
    public List<TableInfo> getReferencedTables()
    {
        return referencedTables;
    }

    @JsonProperty
    public List<RoutineInfo> getRoutines()
    {
        return routines;
    }

    @JsonProperty
    public Optional<ResourceGroupId> getResourceGroupId()
    {
        return resourceGroupId;
    }

    @JsonProperty
    public Optional<QueryType> getQueryType()
    {
        return queryType;
    }

    @JsonProperty
    public RetryPolicy getRetryPolicy()
    {
        return retryPolicy;
    }

    @JsonProperty
    public boolean isPruned()
    {
        return pruned;
    }

    @JsonProperty
    public NodeVersion getVersion()
    {
        return version;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("state", state)
                .add("fieldNames", fieldNames)
                .toString();
    }
}
