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
package io.trino.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.execution.BasicStageInfo;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoWarning;
import io.trino.spi.security.SelectedRole;
import io.trino.transaction.TransactionId;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public record ResultQueryInfo(
        @JsonProperty
        QueryId queryId,
        @JsonProperty
        QueryState state,
        @JsonProperty
        boolean scheduled,
        @JsonProperty
        String updateType,
        @JsonProperty
        BasicQueryStats queryStats,
        @JsonProperty
        ErrorCode errorCode,
        @JsonProperty
        Optional<BasicStageInfo> outputStage,
        @JsonProperty
        boolean finalQueryInfo,
        @JsonProperty
        ExecutionFailureInfo failureInfo,
        @JsonProperty
        Optional<String> setCatalog,
        @JsonProperty
        Optional<String> setSchema,
        @JsonProperty
        Optional<String> setPath,
        @JsonProperty
        Optional<String> setAuthorizationUser,
        @JsonProperty
        boolean resetAuthorizationUser,
        @JsonProperty
        Map<String, String> setSessionProperties,
        @JsonProperty
        Set<String> resetSessionProperties,
        @JsonProperty
        Map<String, SelectedRole> setRoles,
        @JsonProperty
        Map<String, String> addedPreparedStatements,
        @JsonProperty
        Set<String> deallocatedPreparedStatements,
        @JsonProperty
        Optional<TransactionId> startedTransactionId,
        @JsonProperty
        boolean clearTransactionId,
        @JsonProperty
        List<TrinoWarning> warnings)
{
    @JsonCreator
    public ResultQueryInfo(
            @JsonProperty("queryId") QueryId queryId,
            @JsonProperty("state") QueryState state,
            @JsonProperty("scheduled") boolean scheduled,
            @JsonProperty("updateType") String updateType,
            @JsonProperty("queryStats") BasicQueryStats queryStats,
            @JsonProperty("errorCode") ErrorCode errorCode,
            @JsonProperty("outputStage") Optional<BasicStageInfo> outputStage,
            @JsonProperty("finalQueryInfo") boolean finalQueryInfo,
            @JsonProperty("failureInfo") ExecutionFailureInfo failureInfo,
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
            @JsonProperty("warnings") List<TrinoWarning> warnings)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.state = requireNonNull(state, "state is null");
        this.scheduled = scheduled;
        this.errorCode = errorCode;
        this.updateType = updateType;
        this.queryStats = requireNonNull(queryStats, "queryStats is null");
        this.outputStage = requireNonNull(outputStage, "outputStage is null");
        this.finalQueryInfo = finalQueryInfo;
        this.failureInfo = failureInfo;
        this.setCatalog = requireNonNull(setCatalog, "setCatalog is null");
        this.setSchema = requireNonNull(setSchema, "setSchema is null");
        this.setPath = requireNonNull(setPath, "setPath is null");
        this.setAuthorizationUser = requireNonNull(setAuthorizationUser, "setAuthorizationUser is null");
        this.resetAuthorizationUser = resetAuthorizationUser;
        this.setSessionProperties = requireNonNull(setSessionProperties, "setSessionProperties is null");
        this.resetSessionProperties = requireNonNull(resetSessionProperties, "resetSessionProperties is null");
        this.addedPreparedStatements = requireNonNull(addedPreparedStatements, "addedPreparedStatements is null");
        this.deallocatedPreparedStatements = requireNonNull(deallocatedPreparedStatements, "deallocatedPreparedStatements is null");
        this.startedTransactionId = requireNonNull(startedTransactionId, "startedTransactionId is null");
        this.setRoles = requireNonNull(setRoles, "setRoles is null");
        this.clearTransactionId = clearTransactionId;
        this.warnings = requireNonNull(warnings, "warnings is null");
    }

    public ResultQueryInfo(QueryInfo queryInfo)
    {
        this(queryInfo.getQueryId(),
                queryInfo.getState(),
                queryInfo.isScheduled(),
                queryInfo.getUpdateType(),
                new BasicQueryStats(queryInfo.getQueryStats()),
                queryInfo.getErrorCode(),
                queryInfo.getOutputStage().map(BasicStageInfo::new),
                queryInfo.isFinalQueryInfo(),
                queryInfo.getFailureInfo(),
                queryInfo.getSetCatalog(),
                queryInfo.getSetSchema(),
                queryInfo.getSetPath(),
                queryInfo.getSetAuthorizationUser(),
                queryInfo.isResetAuthorizationUser(),
                queryInfo.getSetSessionProperties(),
                queryInfo.getResetSessionProperties(),
                queryInfo.getSetRoles(),
                queryInfo.getAddedPreparedStatements(),
                queryInfo.getDeallocatedPreparedStatements(),
                queryInfo.getStartedTransactionId(),
                queryInfo.isClearTransactionId(),
                queryInfo.getWarnings());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("state", state)
                .toString();
    }
}
