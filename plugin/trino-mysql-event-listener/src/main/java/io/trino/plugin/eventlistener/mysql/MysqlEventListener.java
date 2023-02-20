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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoWarning;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;

import javax.annotation.PostConstruct;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Implement an EventListener that stores information in a MySQL database
 */
public class MysqlEventListener
        implements EventListener
{
    private final QueryDao dao;
    private final JsonCodec<Set<String>> clientTagsJsonCodec;
    private final JsonCodec<Map<String, String>> sessionPropertiesJsonCodec;
    private final JsonCodec<List<QueryInputMetadata>> inputsJsonCodec;
    private final JsonCodec<QueryOutputMetadata> outputJsonCodec;
    private final JsonCodec<List<TrinoWarning>> warningsJsonCodec;

    @Inject
    public MysqlEventListener(
            QueryDao dao,
            JsonCodec<Set<String>> clientTagsJsonCodec,
            JsonCodec<Map<String, String>> sessionPropertiesJsonCodec,
            JsonCodec<List<QueryInputMetadata>> inputsJsonCodec,
            JsonCodec<QueryOutputMetadata> outputJsonCodec,
            JsonCodec<List<TrinoWarning>> warningsJsonCodec)
    {
        this.dao = requireNonNull(dao, "dao is null");
        this.clientTagsJsonCodec = requireNonNull(clientTagsJsonCodec, "clientTagsJsonCodec is null");
        this.sessionPropertiesJsonCodec = requireNonNull(sessionPropertiesJsonCodec, "sessionPropertiesJsonCodec is null");
        this.inputsJsonCodec = requireNonNull(inputsJsonCodec, "inputsJsonCodec is null");
        this.outputJsonCodec = requireNonNull(outputJsonCodec, "outputJsonCodec is null");
        this.warningsJsonCodec = requireNonNull(warningsJsonCodec, "warningsJsonCodec is null");
    }

    @PostConstruct
    public void createTable()
    {
        dao.createTable();
    }

    @Override
    public void queryCreated(QueryCreatedEvent event) {}

    @Override
    public void queryCompleted(QueryCompletedEvent event)
    {
        QueryMetadata metadata = event.getMetadata();
        QueryContext context = event.getContext();
        Optional<QueryFailureInfo> failureInfo = event.getFailureInfo();
        QueryStatistics stats = event.getStatistics();
        QueryEntity entity = new QueryEntity(
                metadata.getQueryId(),
                metadata.getTransactionId(),
                metadata.getQuery(),
                metadata.getUpdateType(),
                metadata.getPreparedQuery(),
                metadata.getQueryState(),
                metadata.getPlan(),
                metadata.getPayload(),
                context.getUser(),
                context.getPrincipal(),
                context.getTraceToken(),
                context.getRemoteClientAddress(),
                context.getUserAgent(),
                context.getClientInfo(),
                clientTagsJsonCodec.toJson(context.getClientTags()),
                context.getSource(),
                context.getCatalog(),
                context.getSchema(),
                context.getResourceGroupId().map(ResourceGroupId::toString),
                sessionPropertiesJsonCodec.toJson(context.getSessionProperties()),
                context.getServerAddress(),
                context.getServerVersion(),
                context.getEnvironment(),
                context.getQueryType().map(QueryType::name),
                inputsJsonCodec.toJson(event.getIoMetadata().getInputs()),
                event.getIoMetadata().getOutput().map(outputJsonCodec::toJson),
                failureInfo.map(QueryFailureInfo::getErrorCode).map(ErrorCode::getName),
                failureInfo.map(QueryFailureInfo::getErrorCode).map(ErrorCode::getType).map(ErrorType::name),
                failureInfo.flatMap(QueryFailureInfo::getFailureType),
                failureInfo.flatMap(QueryFailureInfo::getFailureMessage),
                failureInfo.flatMap(QueryFailureInfo::getFailureTask),
                failureInfo.flatMap(QueryFailureInfo::getFailureHost),
                failureInfo.map(QueryFailureInfo::getFailuresJson),
                warningsJsonCodec.toJson(event.getWarnings()),
                stats.getCpuTime().toMillis(),
                stats.getFailedCpuTime().toMillis(),
                stats.getWallTime().toMillis(),
                stats.getQueuedTime().toMillis(),
                stats.getScheduledTime().map(Duration::toMillis).orElse(0L),
                stats.getFailedScheduledTime().map(Duration::toMillis).orElse(0L),
                stats.getResourceWaitingTime().map(Duration::toMillis).orElse(0L),
                stats.getAnalysisTime().map(Duration::toMillis).orElse(0L),
                stats.getPlanningTime().map(Duration::toMillis).orElse(0L),
                stats.getExecutionTime().map(Duration::toMillis).orElse(0L),
                stats.getInputBlockedTime().map(Duration::toMillis).orElse(0L),
                stats.getFailedInputBlockedTime().map(Duration::toMillis).orElse(0L),
                stats.getOutputBlockedTime().map(Duration::toMillis).orElse(0L),
                stats.getFailedOutputBlockedTime().map(Duration::toMillis).orElse(0L),
                stats.getPhysicalInputReadTime().map(Duration::toMillis).orElse(0L),
                stats.getPeakUserMemoryBytes(),
                stats.getPeakTaskTotalMemory(),
                stats.getPhysicalInputBytes(),
                stats.getPhysicalInputRows(),
                stats.getInternalNetworkBytes(),
                stats.getInternalNetworkRows(),
                stats.getTotalBytes(),
                stats.getTotalRows(),
                stats.getOutputBytes(),
                stats.getOutputRows(),
                stats.getWrittenBytes(),
                stats.getWrittenRows(),
                stats.getCumulativeMemory(),
                stats.getFailedCumulativeMemory(),
                stats.getCompletedSplits(),
                context.getRetryPolicy());
        dao.store(entity);
    }

    @Override
    public void splitCompleted(SplitCompletedEvent event) {}
}
