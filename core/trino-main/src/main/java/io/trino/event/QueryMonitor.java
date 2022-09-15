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
package io.trino.event;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.stats.Distribution;
import io.airlift.stats.Distribution.DistributionSnapshot;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.SessionRepresentation;
import io.trino.client.NodeVersion;
import io.trino.cost.StatsAndCosts;
import io.trino.eventlistener.EventListenerManager;
import io.trino.execution.Column;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.Input;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStats;
import io.trino.execution.StageInfo;
import io.trino.execution.StageStats;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.operator.OperatorStats;
import io.trino.operator.RetryPolicy;
import io.trino.operator.TableFinishInfo;
import io.trino.operator.TaskStats;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import io.trino.spi.eventlistener.OutputColumnMetadata;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import io.trino.spi.eventlistener.QueryPlanDetails;
import io.trino.spi.eventlistener.QueryPlanNodeInfo;
import io.trino.spi.eventlistener.QueryPlanStageInfo;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.StageCpuDistribution;
import io.trino.spi.eventlistener.StageDetails;
import io.trino.spi.eventlistener.StageStatistics;
import io.trino.spi.eventlistener.TaskDetails;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.planprinter.Anonymizer;
import io.trino.sql.planner.planprinter.CounterBasedAnonymizer;
import io.trino.sql.planner.planprinter.NoOpAnonymizer;
import io.trino.sql.planner.planprinter.ValuePrinter;
import io.trino.transaction.TransactionId;
import org.joda.time.DateTime;

import javax.inject.Inject;

import java.math.BigDecimal;
import java.net.URI;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.StageInfo.getAllStages;
import static io.trino.sql.planner.planprinter.PlanPrinter.jsonDistributedPlan;
import static io.trino.sql.planner.planprinter.PlanPrinter.textDistributedPlan;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class QueryMonitor
{
    private static final Logger log = Logger.get(QueryMonitor.class);
    private static final JsonCodec<QueryPlanNodeInfo> PLAN_NODE_CODEC = JsonCodec.jsonCodec(QueryPlanNodeInfo.class);

    private final JsonCodec<StageInfo> stageInfoCodec;
    private final JsonCodec<OperatorStats> operatorStatsCodec;
    private final JsonCodec<ExecutionFailureInfo> executionFailureInfoCodec;
    private final JsonCodec<StatsAndCosts> statsAndCostsCodec;
    private final EventListenerManager eventListenerManager;
    private final String serverVersion;
    private final String serverAddress;
    private final String environment;
    private final SessionPropertyManager sessionPropertyManager;
    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final int maxJsonLimit;

    @Inject
    public QueryMonitor(
            JsonCodec<StageInfo> stageInfoCodec,
            JsonCodec<OperatorStats> operatorStatsCodec,
            JsonCodec<ExecutionFailureInfo> executionFailureInfoCodec,
            JsonCodec<StatsAndCosts> statsAndCostsCodec,
            EventListenerManager eventListenerManager,
            NodeInfo nodeInfo,
            NodeVersion nodeVersion,
            SessionPropertyManager sessionPropertyManager,
            Metadata metadata,
            FunctionManager functionManager,
            QueryMonitorConfig config)
    {
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
        this.stageInfoCodec = requireNonNull(stageInfoCodec, "stageInfoCodec is null");
        this.operatorStatsCodec = requireNonNull(operatorStatsCodec, "operatorStatsCodec is null");
        this.statsAndCostsCodec = requireNonNull(statsAndCostsCodec, "statsAndCostsCodec is null");
        this.executionFailureInfoCodec = requireNonNull(executionFailureInfoCodec, "executionFailureInfoCodec is null");
        this.serverVersion = nodeVersion.toString();
        this.serverAddress = nodeInfo.getExternalAddress();
        this.environment = nodeInfo.getEnvironment();
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.maxJsonLimit = toIntExact(config.getMaxOutputStageJsonSize().toBytes());
    }

    public void queryCreatedEvent(BasicQueryInfo queryInfo)
    {
        eventListenerManager.queryCreated(
                new QueryCreatedEvent(
                        queryInfo.getQueryStats().getCreateTime().toDate().toInstant(),
                        createQueryContext(
                                queryInfo.getSession(),
                                queryInfo.getResourceGroupId(),
                                queryInfo.getQueryType(),
                                queryInfo.getRetryPolicy()),
                        new QueryMetadata(
                                queryInfo.getQueryId().toString(),
                                queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                                queryInfo.getQuery(),
                                queryInfo.getUpdateType(),
                                queryInfo.getPreparedQuery(),
                                QUEUED.toString(),
                                ImmutableList.of(),
                                ImmutableList.of(),
                                queryInfo.getSelf(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty())));
    }

    public void queryImmediateFailureEvent(BasicQueryInfo queryInfo, ExecutionFailureInfo failure)
    {
        eventListenerManager.queryCompleted(requiresAnonymizedPlan -> new QueryCompletedEvent(
                new QueryMetadata(
                        queryInfo.getQueryId().toString(),
                        queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                        queryInfo.getQuery(),
                        queryInfo.getUpdateType(),
                        queryInfo.getPreparedQuery(),
                        queryInfo.getState().toString(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        queryInfo.getSelf(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(),
                        Optional.empty()),
                new QueryStatistics(
                        ofMillis(0),
                        ofMillis(0),
                        ofMillis(0),
                        ofMillis(queryInfo.getQueryStats().getQueuedTime().toMillis()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        ImmutableList.of(),
                        0,
                        true,
                        ImmutableList.of(),
                        ImmutableList.of(),
                        Optional.empty()),
                createQueryContext(
                        queryInfo.getSession(),
                        queryInfo.getResourceGroupId(),
                        queryInfo.getQueryType(),
                        queryInfo.getRetryPolicy()),
                new QueryIOMetadata(ImmutableList.of(), Optional.empty()),
                createQueryFailureInfo(failure, Optional.empty()),
                ImmutableList.of(),
                ofEpochMilli(queryInfo.getQueryStats().getCreateTime().getMillis()),
                ofEpochMilli(queryInfo.getQueryStats().getEndTime().getMillis()),
                ofEpochMilli(queryInfo.getQueryStats().getEndTime().getMillis())));

        logQueryTimeline(queryInfo);
    }

    public void queryCompletedEvent(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();
        eventListenerManager.queryCompleted(requiresAnonymizedPlan ->
                new QueryCompletedEvent(
                        createQueryMetadata(queryInfo, requiresAnonymizedPlan),
                        createQueryStatistics(queryInfo),
                        createQueryContext(
                                queryInfo.getSession(),
                                queryInfo.getResourceGroupId(),
                                queryInfo.getQueryType(),
                                queryInfo.getRetryPolicy()),
                        getQueryIOMetadata(queryInfo),
                        createQueryFailureInfo(queryInfo.getFailureInfo(), queryInfo.getOutputStage()),
                        queryInfo.getWarnings(),
                        ofEpochMilli(queryStats.getCreateTime().getMillis()),
                        ofEpochMilli(queryStats.getExecutionStartTime().getMillis()),
                        ofEpochMilli(queryStats.getEndTime() != null ? queryStats.getEndTime().getMillis() : 0)));

        logQueryTimeline(queryInfo);
    }

    private QueryMetadata createQueryMetadata(QueryInfo queryInfo, boolean requiresAnonymizedPlan)
    {
        Anonymizer anonymizer = requiresAnonymizedPlan ? new CounterBasedAnonymizer() : new NoOpAnonymizer();
        return new QueryMetadata(
                queryInfo.getQueryId().toString(),
                queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                queryInfo.getQuery(),
                Optional.ofNullable(queryInfo.getUpdateType()),
                queryInfo.getPreparedQuery(),
                queryInfo.getState().toString(),
                queryInfo.getReferencedTables(),
                queryInfo.getRoutines(),
                queryInfo.getSelf(),
                createTextQueryPlan(queryInfo, anonymizer),
                createJsonQueryPlan(queryInfo, anonymizer),
                queryInfo.getOutputStage().flatMap(stage -> stageInfoCodec.toJsonWithLengthLimit(stage, maxJsonLimit)),
                createStageDetails(queryInfo),
                createQueryPlanDetails(queryInfo));
    }

    public List<StageDetails> createStageDetails(QueryInfo query)
    {
        return getAllStages(query.getOutputStage()).stream()
                .map(QueryMonitor::toStageDetails)
                .collect(toList());
    }

    private static StageDetails toStageDetails(StageInfo stage)
    {
        StageStats stats = stage.getStageStats();
        return new StageDetails(
                String.valueOf(stage.getStageId().getId()),
                stage.getState().name(),
                getStageTasks(stage),
                stage.getStageStats().isFullyBlocked(),
                toDecimalMillis(stats.getTotalScheduledTime()),
                toDecimalMillis(stats.getTotalBlockedTime()),
                toDecimalMillis(stats.getTotalCpuTime()),
                getStageInputBuffer(stage));
    }

    private static List<TaskDetails> getStageTasks(StageInfo stage)
    {
        return stage.getTasks().stream()
                .map(QueryMonitor::toTaskDetails)
                .collect(toList());
    }

    private static long getStageInputBuffer(StageInfo stage)
    {
        // stage input buffer = sum of task output buffers across all of immediate substages
        long buffer = 0;
        for (StageInfo subStage : stage.getSubStages()) {
            for (TaskInfo task : subStage.getTasks()) {
                buffer += task.getOutputBuffers().getTotalBufferedBytes();
            }
        }
        return buffer;
    }

    private static TaskDetails toTaskDetails(TaskInfo task)
    {
        TaskStatus taskStatus = task.getTaskStatus();
        TaskStats stats = task.getStats();
        String fullTaskId = taskStatus.getTaskId().toString();
        return new TaskDetails(
                fullTaskId.substring(fullTaskId.indexOf('.') + 1),
                hostAndPort(taskStatus.getSelf()),
                taskStatus.getState().name(),
                stats.getQueuedDrivers(),
                stats.getRunningDrivers(),
                stats.getBlockedDrivers(),
                stats.getCompletedDrivers(),
                stats.getTotalDrivers(),
                stats.getRawInputPositions(),
                stats.getRawInputDataSize().toBytes(),
                task.getOutputBuffers().getTotalBufferedBytes(),
                toDecimalMillis(stats.getElapsedTime()),
                toDecimalMillis(stats.getTotalCpuTime()),
                stats.getPhysicalWrittenDataSize().toBytes(),
                stats.getPhysicalInputDataSize().toBytes());
    }

    private static String hostAndPort(URI self)
    {
        return self.getHost() + ":" + self.getPort();
    }

    private static BigDecimal toDecimalMillis(Duration dur)
    {
        return BigDecimal.valueOf(dur.roundTo(TimeUnit.NANOSECONDS))
                .divide(BigDecimal.valueOf(1_000_000L));
    }

    private Optional<QueryPlanDetails> createQueryPlanDetails(QueryInfo queryInfo)
    {
        if (isPruned(queryInfo)) {
            return Optional.empty();
        }

        Optional<StageInfo> outputStage = queryInfo.getOutputStage();
        return outputStage.map(stageInfo -> new QueryPlanDetails(queryInfo.isFinalQueryInfo(), createStageList(stageInfo)));
    }

    private static boolean isPruned(QueryInfo queryInfo)
    {
        if (!queryInfo.getState().isDone()) {
            return false;
        }

        // see QueryStateMachine#pruneQueryInfo()
        boolean prunedPlan = queryInfo.getOutputStage()
                .map(s -> s.getPlan() == null)
                .orElse(false);
        boolean prunedOperatorStats = queryInfo.getQueryStats().getTotalDrivers() > 0
                && queryInfo.getQueryStats().getOperatorSummaries().isEmpty();

        return prunedPlan && prunedOperatorStats;
    }

    private static List<QueryPlanStageInfo> createStageList(StageInfo stageInfo)
    {
        ImmutableList.Builder<QueryPlanStageInfo> stages = ImmutableList.builder();
        flattenStage(stages, stageInfo);
        return stages.build();
    }

    private static void flattenStage(ImmutableList.Builder<QueryPlanStageInfo> stages, StageInfo stageInfo)
    {
        Optional<QueryPlanNodeInfo> planNode = stageInfo.getPlan().getJsonRepresentation().map(PLAN_NODE_CODEC::fromJson);

        stages.add(new QueryPlanStageInfo(
                stageInfo.getStageId().toString(),
                createStageStatistics(stageInfo.getStageStats()),
                stageInfo.getState().toString(),
                planNode));

        stageInfo.getSubStages().forEach(subStage -> flattenStage(stages, subStage));
    }

    private static StageStatistics createStageStatistics(StageStats stageStats)
    {
        return new StageStatistics(stageStats.getQueuedDrivers(), stageStats.getRunningDrivers(), stageStats.getCompletedDrivers(),
                stageStats.getUserMemoryReservation(), stageStats.getTotalCpuTime(),
                stageStats.getTotalBlockedTime(), stageStats.isFullyBlocked(), stageStats.getRawInputDataSize(),
                stageStats.getRawInputPositions(), stageStats.getBufferedDataSize(), stageStats.getOutputDataSize(),
                stageStats.getOutputPositions());
    }

    private QueryStatistics createQueryStatistics(QueryInfo queryInfo)
    {
        List<OperatorStats> operatorStats = queryInfo.getQueryStats().getOperatorSummaries();
        ImmutableList.Builder<String> operatorSummaries = ImmutableList.builderWithExpectedSize(operatorStats.size());
        for (OperatorStats summary : operatorStats) {
            operatorSummaries.add(operatorStatsCodec.toJson(summary));
        }

        Optional<StatsAndCosts> planNodeStatsAndCosts = queryInfo.getOutputStage().map(StatsAndCosts::create);
        Optional<String> serializedPlanNodeStatsAndCosts = planNodeStatsAndCosts.map(statsAndCostsCodec::toJson);

        QueryStats queryStats = queryInfo.getQueryStats();
        return new QueryStatistics(
                ofMillis(queryStats.getTotalCpuTime().toMillis()),
                ofMillis(queryStats.getFailedCpuTime().toMillis()),
                ofMillis(queryStats.getElapsedTime().toMillis()),
                ofMillis(queryStats.getQueuedTime().toMillis()),
                Optional.of(ofMillis(queryStats.getTotalScheduledTime().toMillis())),
                Optional.of(ofMillis(queryStats.getFailedScheduledTime().toMillis())),
                Optional.of(ofMillis(queryStats.getResourceWaitingTime().toMillis())),
                Optional.of(ofMillis(queryStats.getAnalysisTime().toMillis())),
                Optional.of(ofMillis(queryStats.getPlanningTime().toMillis())),
                Optional.of(ofMillis(queryStats.getExecutionTime().toMillis())),
                Optional.of(ofMillis(queryStats.getInputBlockedTime().toMillis())),
                Optional.of(ofMillis(queryStats.getFailedInputBlockedTime().toMillis())),
                Optional.of(ofMillis(queryStats.getOutputBlockedTime().toMillis())),
                Optional.of(ofMillis(queryStats.getFailedOutputBlockedTime().toMillis())),
                queryStats.getPeakUserMemoryReservation().toBytes(),
                queryStats.getPeakTaskUserMemory().toBytes(),
                queryStats.getPeakTaskTotalMemory().toBytes(),
                queryStats.getPhysicalInputDataSize().toBytes(),
                queryStats.getPhysicalInputPositions(),
                queryStats.getProcessedInputDataSize().toBytes(),
                queryStats.getProcessedInputPositions(),
                queryStats.getInternalNetworkInputDataSize().toBytes(),
                queryStats.getInternalNetworkInputPositions(),
                queryStats.getRawInputDataSize().toBytes(),
                queryStats.getRawInputPositions(),
                queryStats.getOutputDataSize().toBytes(),
                queryStats.getOutputPositions(),
                queryStats.getLogicalWrittenDataSize().toBytes(),
                queryStats.getWrittenPositions(),
                queryStats.getCumulativeUserMemory(),
                queryStats.getFailedCumulativeUserMemory(),
                queryStats.getStageGcStatistics(),
                queryStats.getCompletedDrivers(),
                queryInfo.isFinalQueryInfo(),
                getCpuDistributions(queryInfo),
                operatorSummaries.build(),
                serializedPlanNodeStatsAndCosts);
    }

    private QueryContext createQueryContext(SessionRepresentation session, Optional<ResourceGroupId> resourceGroup, Optional<QueryType> queryType, RetryPolicy retryPolicy)
    {
        return new QueryContext(
                session.getUser(),
                session.getPrincipal(),
                session.getGroups(),
                session.getTraceToken(),
                session.getRemoteUserAddress(),
                session.getUserAgent(),
                session.getClientInfo(),
                session.getClientTags(),
                session.getClientCapabilities(),
                session.getSource(),
                session.getCatalog(),
                session.getSchema(),
                resourceGroup,
                mergeSessionAndCatalogProperties(session),
                session.getResourceEstimates(),
                serverAddress,
                serverVersion,
                environment,
                queryType,
                retryPolicy.toString());
    }

    private Optional<String> createTextQueryPlan(QueryInfo queryInfo, Anonymizer anonymizer)
    {
        try {
            if (queryInfo.getOutputStage().isPresent()) {
                return Optional.of(textDistributedPlan(
                        queryInfo.getOutputStage().get(),
                        queryInfo.getQueryStats(),
                        new ValuePrinter(metadata, functionManager, queryInfo.getSession().toSession(sessionPropertyManager)),
                        false,
                        anonymizer));
            }
        }
        catch (Exception e) {
            // Sometimes it is expected to fail. For example if generated plan is too long.
            // Don't fail to create event if the plan cannot be created.
            log.warn(e, "Error creating explain plan for query %s", queryInfo.getQueryId());
        }
        return Optional.empty();
    }

    private Optional<String> createJsonQueryPlan(QueryInfo queryInfo, Anonymizer anonymizer)
    {
        try {
            if (queryInfo.getOutputStage().isPresent()) {
                return Optional.of(jsonDistributedPlan(
                        queryInfo.getOutputStage().get(),
                        queryInfo.getSession().toSession(sessionPropertyManager),
                        metadata,
                        functionManager,
                        anonymizer));
            }
        }
        catch (Exception e) {
            // Sometimes it is expected to fail. For example if generated plan is too long.
            // Don't fail to create event if the plan cannot be created.
            log.warn(e, "Error creating anonymized json plan for query %s", queryInfo.getQueryId());
        }
        return Optional.empty();
    }

    private static QueryIOMetadata getQueryIOMetadata(QueryInfo queryInfo)
    {
        Multimap<FragmentNode, OperatorStats> planNodeStats = extractPlanNodeStats(queryInfo);

        ImmutableList.Builder<QueryInputMetadata> inputs = ImmutableList.builderWithExpectedSize(queryInfo.getInputs().size());
        for (Input input : queryInfo.getInputs()) {
            // Note: input table can be mapped to multiple operators
            Collection<OperatorStats> inputTableOperatorStats = planNodeStats.get(new FragmentNode(input.getFragmentId(), input.getPlanNodeId()));

            OptionalLong physicalInputBytes = OptionalLong.empty();
            OptionalLong physicalInputPositions = OptionalLong.empty();
            if (!inputTableOperatorStats.isEmpty()) {
                physicalInputBytes = OptionalLong.of(inputTableOperatorStats.stream()
                        .map(OperatorStats::getPhysicalInputDataSize)
                        .mapToLong(DataSize::toBytes)
                        .sum());
                physicalInputPositions = OptionalLong.of(inputTableOperatorStats.stream()
                        .mapToLong(OperatorStats::getPhysicalInputPositions)
                        .sum());
            }
            Metrics connectorMetrics = inputTableOperatorStats.stream()
                    .map(OperatorStats::getConnectorMetrics)
                    .reduce(Metrics.EMPTY, Metrics::mergeWith);

            inputs.add(new QueryInputMetadata(
                    input.getCatalogName(),
                    input.getSchema(),
                    input.getTable(),
                    input.getColumns().stream()
                            .map(Column::getName).collect(toList()),
                    input.getConnectorInfo(),
                    connectorMetrics,
                    physicalInputBytes,
                    physicalInputPositions));
        }

        Optional<QueryOutputMetadata> output = Optional.empty();
        if (queryInfo.getOutput().isPresent()) {
            Optional<TableFinishInfo> tableFinishInfo = queryInfo.getQueryStats().getOperatorSummaries().stream()
                    .map(OperatorStats::getInfo)
                    .filter(TableFinishInfo.class::isInstance)
                    .map(TableFinishInfo.class::cast)
                    .findFirst();

            Optional<List<OutputColumnMetadata>> outputColumnsMetadata = queryInfo.getOutput().get().getColumns()
                    .map(columns -> columns.stream()
                            .map(column -> new OutputColumnMetadata(
                                    column.getColumn().getName(),
                                    column.getColumn().getType(),
                                    column.getSourceColumns().stream()
                                            .map(Analysis.SourceColumn::getColumnDetail)
                                            .collect(toSet())))
                            .collect(toList()));

            output = Optional.of(
                    new QueryOutputMetadata(
                            queryInfo.getOutput().get().getCatalogName(),
                            queryInfo.getOutput().get().getSchema(),
                            queryInfo.getOutput().get().getTable(),
                            outputColumnsMetadata,
                            tableFinishInfo.map(TableFinishInfo::getConnectorOutputMetadata),
                            tableFinishInfo.map(TableFinishInfo::isJsonLengthLimitExceeded)));
        }
        return new QueryIOMetadata(inputs.build(), output);
    }

    private static Multimap<FragmentNode, OperatorStats> extractPlanNodeStats(QueryInfo queryInfo)
    {
        // Note: A plan may map a table scan to multiple operators.
        ImmutableMultimap.Builder<FragmentNode, OperatorStats> planNodeStats = ImmutableMultimap.builder();
        getAllStages(queryInfo.getOutputStage())
                .forEach(stageInfo -> extractPlanNodeStats(stageInfo, planNodeStats));
        return planNodeStats.build();
    }

    private static void extractPlanNodeStats(StageInfo stageInfo, ImmutableMultimap.Builder<FragmentNode, OperatorStats> planNodeStats)
    {
        PlanFragment fragment = stageInfo.getPlan();
        if (fragment == null) {
            return;
        }

        // Note: a plan node may be mapped to multiple operators
        Map<PlanNodeId, Collection<OperatorStats>> allOperatorStats = Multimaps.index(stageInfo.getStageStats().getOperatorSummaries(), OperatorStats::getPlanNodeId).asMap();

        // Sometimes a plan node is merged with other nodes into a single operator, and in that case,
        // use the stats of the nearest parent node with stats.
        fragment.getRoot().accept(
                new PlanVisitor<Void, Collection<OperatorStats>>()
                {
                    @Override
                    protected Void visitPlan(PlanNode node, Collection<OperatorStats> parentStats)
                    {
                        Collection<OperatorStats> operatorStats = allOperatorStats.getOrDefault(node.getId(), parentStats);
                        planNodeStats.putAll(new FragmentNode(fragment.getId(), node.getId()), operatorStats);

                        for (PlanNode child : node.getSources()) {
                            child.accept(this, operatorStats);
                        }
                        return null;
                    }
                },
                ImmutableList.of());
    }

    private Optional<QueryFailureInfo> createQueryFailureInfo(ExecutionFailureInfo failureInfo, Optional<StageInfo> outputStage)
    {
        if (failureInfo == null) {
            return Optional.empty();
        }

        Optional<TaskInfo> failedTask = outputStage.flatMap(QueryMonitor::findFailedTask);

        return Optional.of(new QueryFailureInfo(
                failureInfo.getErrorCode(),
                Optional.ofNullable(failureInfo.getType()),
                Optional.ofNullable(failureInfo.getMessage()),
                failedTask.map(task -> task.getTaskStatus().getTaskId().toString()),
                failedTask.map(task -> task.getTaskStatus().getSelf().getHost()),
                executionFailureInfoCodec.toJson(failureInfo)));
    }

    private static Optional<TaskInfo> findFailedTask(StageInfo stageInfo)
    {
        for (StageInfo subStage : stageInfo.getSubStages()) {
            Optional<TaskInfo> task = findFailedTask(subStage);
            if (task.isPresent()) {
                return task;
            }
        }
        return stageInfo.getTasks().stream()
                .filter(taskInfo -> taskInfo.getTaskStatus().getState() == TaskState.FAILED)
                .findFirst();
    }

    private static Map<String, String> mergeSessionAndCatalogProperties(SessionRepresentation session)
    {
        Map<String, String> mergedProperties = new LinkedHashMap<>(session.getSystemProperties());

        for (Map.Entry<String, Map<String, String>> catalogEntry : session.getCatalogProperties().entrySet()) {
            for (Map.Entry<String, String> entry : catalogEntry.getValue().entrySet()) {
                mergedProperties.put(catalogEntry.getKey() + "." + entry.getKey(), entry.getValue());
            }
        }
        return ImmutableMap.copyOf(mergedProperties);
    }

    private static void logQueryTimeline(QueryInfo queryInfo)
    {
        try {
            QueryStats queryStats = queryInfo.getQueryStats();
            DateTime queryStartTime = queryStats.getCreateTime();
            DateTime queryEndTime = queryStats.getEndTime();

            // query didn't finish cleanly
            if (queryStartTime == null || queryEndTime == null) {
                return;
            }

            // planning duration -- start to end of planning
            long planning = queryStats.getPlanningTime().toMillis();

            // Time spent waiting for required no. of worker nodes to be present
            long waiting = queryStats.getResourceWaitingTime().toMillis();

            List<StageInfo> stages = getAllStages(queryInfo.getOutputStage());
            long firstTaskStartTime = queryEndTime.getMillis();
            long lastTaskEndTime = queryStartTime.getMillis() + planning;
            for (StageInfo stage : stages) {
                // only consider leaf stages
                if (!stage.getSubStages().isEmpty()) {
                    continue;
                }

                for (TaskInfo taskInfo : stage.getTasks()) {
                    TaskStats taskStats = taskInfo.getStats();

                    DateTime firstStartTime = taskStats.getFirstStartTime();
                    if (firstStartTime != null) {
                        firstTaskStartTime = Math.min(firstStartTime.getMillis(), firstTaskStartTime);
                    }

                    DateTime endTime = taskStats.getEndTime();
                    if (endTime != null) {
                        lastTaskEndTime = max(endTime.getMillis(), lastTaskEndTime);
                    }
                }
            }

            long elapsed = max(queryEndTime.getMillis() - queryStartTime.getMillis(), 0);
            long scheduling = max(firstTaskStartTime - queryStartTime.getMillis() - planning, 0);
            long running = max(lastTaskEndTime - firstTaskStartTime, 0);
            long finishing = max(queryEndTime.getMillis() - lastTaskEndTime, 0);

            logQueryTimeline(
                    queryInfo.getQueryId(),
                    queryInfo.getState(),
                    Optional.ofNullable(queryInfo.getErrorCode()),
                    elapsed,
                    planning,
                    waiting,
                    scheduling,
                    running,
                    finishing,
                    queryStartTime,
                    queryEndTime);
        }
        catch (Exception e) {
            log.error(e, "Error logging query timeline");
        }
    }

    private static void logQueryTimeline(BasicQueryInfo queryInfo)
    {
        DateTime queryStartTime = queryInfo.getQueryStats().getCreateTime();
        DateTime queryEndTime = queryInfo.getQueryStats().getEndTime();

        // query didn't finish cleanly
        if (queryStartTime == null || queryEndTime == null) {
            return;
        }

        long elapsed = max(queryEndTime.getMillis() - queryStartTime.getMillis(), 0);

        logQueryTimeline(
                queryInfo.getQueryId(),
                queryInfo.getState(),
                Optional.ofNullable(queryInfo.getErrorCode()),
                elapsed,
                elapsed,
                0,
                0,
                0,
                0,
                queryStartTime,
                queryEndTime);
    }

    private static void logQueryTimeline(
            QueryId queryId,
            QueryState queryState,
            Optional<ErrorCode> errorCode,
            long elapsedMillis,
            long planningMillis,
            long waitingMillis,
            long schedulingMillis,
            long runningMillis,
            long finishingMillis,
            DateTime queryStartTime,
            DateTime queryEndTime)
    {
        log.info("TIMELINE: Query %s :: %s%s :: elapsed %sms :: planning %sms :: waiting %sms :: scheduling %sms :: running %sms :: finishing %sms :: begin %s :: end %s",
                queryId,
                queryState,
                errorCode.map(code -> " (%s)".formatted(code.getName())).orElse(""),
                elapsedMillis,
                planningMillis,
                waitingMillis,
                schedulingMillis,
                runningMillis,
                finishingMillis,
                queryStartTime,
                queryEndTime);
    }

    private static List<StageCpuDistribution> getCpuDistributions(QueryInfo queryInfo)
    {
        if (queryInfo.getOutputStage().isEmpty()) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<StageCpuDistribution> builder = ImmutableList.builder();
        populateDistribution(queryInfo.getOutputStage().get(), builder);

        return builder.build();
    }

    private static void populateDistribution(StageInfo stageInfo, ImmutableList.Builder<StageCpuDistribution> distributions)
    {
        distributions.add(computeCpuDistribution(stageInfo));
        for (StageInfo subStage : stageInfo.getSubStages()) {
            populateDistribution(subStage, distributions);
        }
    }

    private static StageCpuDistribution computeCpuDistribution(StageInfo stageInfo)
    {
        Distribution cpuDistribution = new Distribution();

        for (TaskInfo taskInfo : stageInfo.getTasks()) {
            cpuDistribution.add(taskInfo.getStats().getTotalCpuTime().toMillis());
        }

        DistributionSnapshot snapshot = cpuDistribution.snapshot();

        return new StageCpuDistribution(
                stageInfo.getStageId().getId(),
                stageInfo.getTasks().size(),
                (long) snapshot.getP25(),
                (long) snapshot.getP50(),
                (long) snapshot.getP75(),
                (long) snapshot.getP90(),
                (long) snapshot.getP95(),
                (long) snapshot.getP99(),
                (long) snapshot.getMin(),
                (long) snapshot.getMax(),
                (long) snapshot.getTotal(),
                snapshot.getTotal() / snapshot.getCount());
    }

    private static class FragmentNode
    {
        private final PlanFragmentId fragmentId;
        private final PlanNodeId nodeId;

        public FragmentNode(PlanFragmentId fragmentId, PlanNodeId nodeId)
        {
            this.fragmentId = requireNonNull(fragmentId, "fragmentId is null");
            this.nodeId = requireNonNull(nodeId, "nodeId is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FragmentNode that = (FragmentNode) o;
            return fragmentId.equals(that.fragmentId) &&
                    nodeId.equals(that.nodeId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(fragmentId, nodeId);
        }

        @Override
        public String toString()
        {
            return fragmentId + ":" + nodeId;
        }
    }
}
