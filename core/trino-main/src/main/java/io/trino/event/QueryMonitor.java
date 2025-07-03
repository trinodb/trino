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
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.stats.Distribution;
import io.airlift.stats.Distribution.DistributionSnapshot;
import io.airlift.units.DataSize;
import io.trino.SessionRepresentation;
import io.trino.client.NodeVersion;
import io.trino.cost.StatsAndCosts;
import io.trino.eventlistener.EventListenerManager;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.Input;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStats;
import io.trino.execution.StageInfo;
import io.trino.execution.StagesInfo;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskState;
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
import io.trino.spi.eventlistener.DoubleSymmetricDistribution;
import io.trino.spi.eventlistener.DynamicFilterDomainStatistics;
import io.trino.spi.eventlistener.LongDistribution;
import io.trino.spi.eventlistener.LongSymmetricDistribution;
import io.trino.spi.eventlistener.OutputColumnMetadata;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.StageCpuDistribution;
import io.trino.spi.eventlistener.StageOutputBufferMetrics;
import io.trino.spi.eventlistener.StageOutputBufferUtilization;
import io.trino.spi.eventlistener.StageTaskStatistics;
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

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.StagesInfo.getAllStages;
import static io.trino.sql.planner.planprinter.PlanPrinter.jsonDistributedPlan;
import static io.trino.sql.planner.planprinter.PlanPrinter.textDistributedPlan;
import static io.trino.util.MoreMath.firstNonNaN;
import static java.lang.Math.toIntExact;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;

public class QueryMonitor
{
    private static final Logger log = Logger.get(QueryMonitor.class);
    private static final ZoneId ZONE_ID = ZoneId.systemDefault();

    private final JsonCodec<StagesInfo> stagesInfoCodec;
    private final JsonCodec<OperatorStats> operatorStatsCodec;
    private final JsonCodec<ExecutionFailureInfo> executionFailureInfoCodec;
    private final JsonCodec<StatsAndCosts> statsAndCostsCodec;
    private final EventListenerManager eventListenerManager;
    private final NodeVersion serverVersion;
    private final String serverAddress;
    private final String environment;
    private final SessionPropertyManager sessionPropertyManager;
    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final int maxJsonLimit;

    @Inject
    public QueryMonitor(
            JsonCodec<StagesInfo> stagesInfoCodec,
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
        this.stagesInfoCodec = requireNonNull(stagesInfoCodec, "stagesInfoCodec is null");
        this.operatorStatsCodec = requireNonNull(operatorStatsCodec, "operatorStatsCodec is null");
        this.statsAndCostsCodec = requireNonNull(statsAndCostsCodec, "statsAndCostsCodec is null");
        this.executionFailureInfoCodec = requireNonNull(executionFailureInfoCodec, "executionFailureInfoCodec is null");
        this.serverVersion = nodeVersion;
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
                        queryInfo.getQueryStats().getCreateTime(),
                        createQueryContext(
                                queryInfo.getSession(),
                                queryInfo.getResourceGroupId(),
                                queryInfo.getQueryType(),
                                queryInfo.getRetryPolicy()),
                        new QueryMetadata(
                                queryInfo.getQueryId().toString(),
                                queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                                queryInfo.getSession().getQueryDataEncoding(),
                                queryInfo.getQuery(),
                                queryInfo.getUpdateType(),
                                queryInfo.getPreparedQuery(),
                                QUEUED.toString(),
                                ImmutableList.of(),
                                ImmutableList.of(),
                                queryInfo.getSelf(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional::empty)));
    }

    public void queryImmediateFailureEvent(BasicQueryInfo queryInfo, ExecutionFailureInfo failure)
    {
        eventListenerManager.queryCompleted(requiresAnonymizedPlan -> new QueryCompletedEvent(
                new QueryMetadata(
                        queryInfo.getQueryId().toString(),
                        queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                        queryInfo.getSession().getQueryDataEncoding(),
                        queryInfo.getQuery(),
                        queryInfo.getUpdateType(),
                        queryInfo.getPreparedQuery(),
                        queryInfo.getState().toString(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        queryInfo.getSelf(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional::empty),
                new QueryStatistics(
                        Duration.ZERO,
                        Duration.ZERO,
                        Duration.ZERO,
                        queryInfo.getQueryStats().getQueuedTime().toJavaTime(),
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
                        0,
                        ImmutableList.of(),
                        0,
                        true,
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(),
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
                queryInfo.getQueryStats().getCreateTime(),
                queryInfo.getQueryStats().getEndTime(),
                queryInfo.getQueryStats().getEndTime()));

        logQueryTimeline(queryInfo);
    }

    public void queryCompletedEvent(QueryInfo queryInfo)
    {
        Map<Boolean, QueryCompletedEvent> memo = new HashMap<>();
        QueryStats queryStats = queryInfo.getQueryStats();

        eventListenerManager.queryCompleted(requiresAnonymizedPlan -> {
            QueryCompletedEvent queryCompletedEvent = memo.get(requiresAnonymizedPlan);
            if (queryCompletedEvent == null) {
                queryCompletedEvent = new QueryCompletedEvent(
                        createQueryMetadata(queryInfo, requiresAnonymizedPlan),
                        createQueryStatistics(queryInfo),
                        createQueryContext(
                                queryInfo.getSession(),
                                queryInfo.getResourceGroupId(),
                                queryInfo.getQueryType(),
                                queryInfo.getRetryPolicy()),
                        getQueryIOMetadata(queryInfo),
                        createQueryFailureInfo(queryInfo.getFailureInfo(), queryInfo.getStages()),
                        queryInfo.getWarnings(),
                        queryStats.getCreateTime(),
                        queryStats.getExecutionStartTime(),
                        queryStats.getEndTime() != null ? queryStats.getEndTime() : Instant.ofEpochMilli(0));
                memo.put(requiresAnonymizedPlan, queryCompletedEvent);
            }
            return queryCompletedEvent;
        });
        logQueryTimeline(queryInfo);
    }

    private QueryMetadata createQueryMetadata(QueryInfo queryInfo, boolean requiresAnonymizedPlan)
    {
        Anonymizer anonymizer = requiresAnonymizedPlan ? new CounterBasedAnonymizer() : new NoOpAnonymizer();
        return new QueryMetadata(
                queryInfo.getQueryId().toString(),
                queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                queryInfo.getSession().getQueryDataEncoding(),
                queryInfo.getQuery(),
                Optional.ofNullable(queryInfo.getUpdateType()),
                queryInfo.getPreparedQuery(),
                queryInfo.getState().toString(),
                queryInfo.getReferencedTables(),
                queryInfo.getRoutines(),
                queryInfo.getSelf(),
                createTextQueryPlan(queryInfo, anonymizer),
                createJsonQueryPlan(queryInfo, anonymizer),
                memoize(() -> queryInfo.getStages().flatMap(stages -> stagesInfoCodec.toJsonWithLengthLimit(stages, maxJsonLimit))));
    }

    private QueryStatistics createQueryStatistics(QueryInfo queryInfo)
    {
        List<OperatorStats> operatorStats = queryInfo.getQueryStats().getOperatorSummaries();

        Optional<StatsAndCosts> planNodeStatsAndCosts = queryInfo.getStages().map(StatsAndCosts::create);
        Optional<String> serializedPlanNodeStatsAndCosts = planNodeStatsAndCosts.map(statsAndCostsCodec::toJson);

        QueryStats queryStats = queryInfo.getQueryStats();
        return new QueryStatistics(
                queryStats.getTotalCpuTime().toJavaTime(),
                queryStats.getFailedCpuTime().toJavaTime(),
                queryStats.getElapsedTime().toJavaTime(),
                queryStats.getQueuedTime().toJavaTime(),
                Optional.of(queryStats.getTotalScheduledTime().toJavaTime()),
                Optional.of(queryStats.getFailedScheduledTime().toJavaTime()),
                Optional.of(queryStats.getResourceWaitingTime().toJavaTime()),
                Optional.of(queryStats.getAnalysisTime().toJavaTime()),
                Optional.of(queryStats.getPlanningTime().toJavaTime()),
                Optional.of(queryStats.getPlanningCpuTime().toJavaTime()),
                Optional.of(queryStats.getStartingTime().toJavaTime()),
                Optional.of(queryStats.getExecutionTime().toJavaTime()),
                Optional.of(queryStats.getInputBlockedTime().toJavaTime()),
                Optional.of(queryStats.getFailedInputBlockedTime().toJavaTime()),
                Optional.of(queryStats.getOutputBlockedTime().toJavaTime()),
                Optional.of(queryStats.getFailedOutputBlockedTime().toJavaTime()),
                Optional.of(queryStats.getPhysicalInputReadTime().toJavaTime()),
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
                queryStats.getSpilledDataSize().toBytes(),
                queryStats.getCumulativeUserMemory(),
                queryStats.getFailedCumulativeUserMemory(),
                queryStats.getStageGcStatistics(),
                queryStats.getCompletedDrivers(),
                queryInfo.isFinalQueryInfo(),
                getCpuDistributions(queryInfo),
                getStageOutputBufferUtilizations(queryInfo),
                getStageOutputBufferMetrics(queryInfo),
                getStageTaskStatistics(queryInfo),
                getDynamicFilterDomainStats(queryInfo),
                memoize(() -> operatorStats.stream().map(operatorStatsCodec::toJson).toList()),
                ImmutableList.copyOf(queryInfo.getQueryStats().getOptimizerRulesSummaries()),
                serializedPlanNodeStatsAndCosts);
    }

    private static List<DynamicFilterDomainStatistics> getDynamicFilterDomainStats(QueryInfo queryInfo)
    {
        return queryInfo.getQueryStats().getDynamicFiltersStats().getDynamicFilterDomainStats()
                .stream()
                .map(stats -> new DynamicFilterDomainStatistics(
                        stats.getDynamicFilterId().toString(),
                        stats.getSimplifiedDomain(),
                        stats.getCollectionDuration().map(io.airlift.units.Duration::toJavaTime)))
                .collect(toImmutableList());
    }

    private QueryContext createQueryContext(SessionRepresentation session, Optional<ResourceGroupId> resourceGroup, Optional<QueryType> queryType, RetryPolicy retryPolicy)
    {
        return new QueryContext(
                session.getUser(),
                session.getOriginalUser(),
                session.getOriginalRoles(),
                session.getPrincipal(),
                session.getEnabledRoles(),
                session.getGroups(),
                session.getTraceToken(),
                session.getRemoteUserAddress(),
                session.getUserAgent(),
                session.getClientInfo(),
                session.getClientTags(),
                session.getClientCapabilities(),
                session.getSource(),
                session.getTimeZone(),
                session.getCatalog(),
                session.getSchema(),
                resourceGroup,
                mergeSessionAndCatalogProperties(session),
                session.getResourceEstimates(),
                serverAddress,
                serverVersion.toString(),
                environment,
                queryType,
                retryPolicy.toString());
    }

    private Optional<String> createTextQueryPlan(QueryInfo queryInfo, Anonymizer anonymizer)
    {
        try {
            if (queryInfo.getStages().isPresent()) {
                return Optional.of(textDistributedPlan(
                        queryInfo.getStages().get(),
                        queryInfo.getQueryStats(),
                        new ValuePrinter(metadata, functionManager, queryInfo.getSession().toSession(sessionPropertyManager)),
                        false,
                        anonymizer,
                        serverVersion));
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
            if (queryInfo.getStages().isPresent()) {
                return Optional.of(jsonDistributedPlan(
                        queryInfo.getStages().orElseThrow(),
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
                    input.getConnectorName(),
                    input.getCatalogName(),
                    input.getCatalogVersion(),
                    input.getSchema(),
                    input.getTable(),
                    input.getColumns().stream()
                            .map(column -> new QueryInputMetadata.Column(column.getName(), column.getType()))
                            .collect(toImmutableList()),
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
                                            .collect(toImmutableSet())))
                            .collect(toImmutableList()));

            output = Optional.of(
                    new QueryOutputMetadata(
                            queryInfo.getOutput().get().getCatalogName(),
                            queryInfo.getOutput().get().getCatalogVersion(),
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
        getAllStages(queryInfo.getStages())
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

    private Optional<QueryFailureInfo> createQueryFailureInfo(ExecutionFailureInfo failureInfo, Optional<StagesInfo> stages)
    {
        if (failureInfo == null) {
            return Optional.empty();
        }

        Optional<TaskInfo> failedTask = stages.flatMap(QueryMonitor::findFailedTask);

        return Optional.of(new QueryFailureInfo(
                failureInfo.getErrorCode(),
                Optional.ofNullable(failureInfo.getType()),
                Optional.ofNullable(failureInfo.getMessage()),
                failedTask.map(task -> task.taskStatus().getTaskId().toString()),
                failedTask.map(task -> task.taskStatus().getSelf().getHost()),
                executionFailureInfoCodec.toJson(failureInfo)));
    }

    private static Optional<TaskInfo> findFailedTask(StagesInfo stages)
    {
        for (StageInfo stageInfo : stages.getSubStagesDeepPostOrder(stages.getOutputStageId(), true)) {
            Optional<TaskInfo> failedTaskInfo = stageInfo.getTasks().stream()
                    .filter(taskInfo -> taskInfo.taskStatus().getState() == TaskState.FAILED)
                    .findFirst();
            if (failedTaskInfo.isPresent()) {
                return failedTaskInfo;
            }
        }
        return Optional.empty();
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
            Instant queryStartTime = queryStats.getCreateTime().truncatedTo(MILLIS);
            Instant queryEndTime = queryStats.getEndTime().truncatedTo(MILLIS);

            // query didn't finish cleanly
            if (queryStartTime == null || queryEndTime == null) {
                return;
            }

            // planning duration -- start to end of planning
            long planning = queryStats.getPlanningTime().toMillis();

            // Time spent waiting for required no. of worker nodes to be present
            long waiting = queryStats.getResourceWaitingTime().toMillis();

            List<StageInfo> stages = getAllStages(queryInfo.getStages());
            Instant firstTaskStartTime = queryEndTime;
            Instant lastTaskEndTime = queryStartTime.plusMillis(planning);
            for (StageInfo stage : stages) {
                // only consider leaf stages
                if (!stage.getSubStages().isEmpty()) {
                    continue;
                }

                for (TaskInfo taskInfo : stage.getTasks()) {
                    TaskStats taskStats = taskInfo.stats();

                    Instant firstStartTime = taskStats.getFirstStartTime();
                    if (firstStartTime != null) {
                        firstTaskStartTime = min(firstStartTime, firstTaskStartTime);
                    }

                    Instant endTime = taskStats.getEndTime();
                    if (endTime != null) {
                        lastTaskEndTime = max(endTime, lastTaskEndTime);
                    }
                }
            }

            long elapsed = Math.max(queryEndTime.toEpochMilli() - queryStartTime.toEpochMilli(), 0);
            long scheduling = Math.max(firstTaskStartTime.toEpochMilli() - queryStartTime.toEpochMilli() - planning, 0);
            long running = Math.max(lastTaskEndTime.toEpochMilli() - firstTaskStartTime.toEpochMilli(), 0);
            long finishing = Math.max(queryEndTime.toEpochMilli() - lastTaskEndTime.toEpochMilli(), 0);

            logQueryTimeline(
                    queryInfo.getQueryId(),
                    queryInfo.getState(),
                    queryInfo.getSession().getQueryDataEncoding(),
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
        Instant queryStartTime = queryInfo.getQueryStats().getCreateTime().truncatedTo(MILLIS);
        Instant queryEndTime = queryInfo.getQueryStats().getEndTime().truncatedTo(MILLIS);

        // query didn't finish cleanly
        if (queryStartTime == null || queryEndTime == null) {
            return;
        }

        long elapsed = Math.max(queryEndTime.toEpochMilli() - queryStartTime.toEpochMilli(), 0);

        logQueryTimeline(
                queryInfo.getQueryId(),
                queryInfo.getState(),
                queryInfo.getSession().getQueryDataEncoding(),
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
            Optional<String> encoding,
            Optional<ErrorCode> errorCode,
            long elapsedMillis,
            long planningMillis,
            long waitingMillis,
            long schedulingMillis,
            long runningMillis,
            long finishingMillis,
            Instant queryStartTime,
            Instant queryEndTime)
    {
        log.info("TIMELINE: Query %s :: %s%s :: elapsed %sms :: planning %sms :: waiting %sms :: scheduling %sms :: running %sms :: finishing %sms :: begin %s :: end %s%s",
                queryId,
                queryState,
                errorCode.map(code -> " (%s)".formatted(code.getName())).orElse(""),
                elapsedMillis,
                planningMillis,
                waitingMillis,
                schedulingMillis,
                runningMillis,
                finishingMillis,
                queryStartTime.atZone(ZONE_ID).format(ISO_OFFSET_DATE_TIME),
                queryEndTime.atZone(ZONE_ID).format(ISO_OFFSET_DATE_TIME),
                encoding.map(id -> " :: " + id).orElse(""));
    }

    private static List<StageCpuDistribution> getCpuDistributions(QueryInfo queryInfo)
    {
        return getAllStages(queryInfo.getStages()).stream()
                .map(QueryMonitor::computeCpuDistribution)
                .collect(toImmutableList());
    }

    private static StageCpuDistribution computeCpuDistribution(StageInfo stageInfo)
    {
        Distribution cpuDistribution = new Distribution();

        for (TaskInfo taskInfo : stageInfo.getTasks()) {
            cpuDistribution.add(taskInfo.stats().getTotalCpuTime().toMillis());
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
                firstNonNaN(snapshot.getTotal() / snapshot.getCount(), 0.0));
    }

    private static List<StageOutputBufferUtilization> getStageOutputBufferUtilizations(QueryInfo queryInfo)
    {
        return getAllStages(queryInfo.getStages()).stream()
                .map(QueryMonitor::computeStageOutputBufferUtilization)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
    }

    private static Optional<StageOutputBufferUtilization> computeStageOutputBufferUtilization(StageInfo stageInfo)
    {
        return stageInfo.getStageStats().getOutputBufferUtilization()
                .map(utilization -> new StageOutputBufferUtilization(
                            stageInfo.getStageId().getId(),
                            stageInfo.getTasks().size(),
                            // scale ratio to percentages
                            utilization.p01() * 100,
                            utilization.p05() * 100,
                            utilization.p10() * 100,
                            utilization.p25() * 100,
                            utilization.p50() * 100,
                            utilization.p75() * 100,
                            utilization.p90() * 100,
                            utilization.p95() * 100,
                            utilization.p99() * 100,
                            utilization.min() * 100,
                            utilization.max() * 100,
                        Duration.ofNanos(utilization.total())));
    }

    private static List<StageOutputBufferMetrics> getStageOutputBufferMetrics(QueryInfo queryInfo)
    {
        return getAllStages(queryInfo.getStages()).stream()
                .map(QueryMonitor::computeStageOutputBufferMetrics)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
    }

    private static Optional<StageOutputBufferMetrics> computeStageOutputBufferMetrics(StageInfo stageInfo)
    {
        Metrics metrics = stageInfo.getStageStats().getOutputBufferMetrics();
        if (metrics.getMetrics().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new StageOutputBufferMetrics(stageInfo.getStageId().getId(), metrics));
    }

    private List<StageTaskStatistics> getStageTaskStatistics(QueryInfo queryInfo)
    {
        return getAllStages(queryInfo.getStages()).stream()
                .map(stageInfo -> computeStageTaskStatistics(queryInfo, stageInfo))
                .collect(toImmutableList());
    }

    private StageTaskStatistics computeStageTaskStatistics(QueryInfo queryInfo, StageInfo stageInfo)
    {
        long queryCreateTimeMillis = queryInfo.getQueryStats().getCreateTime().toEpochMilli();
        LongSymmetricDistribution createTimeMillisDistribution = getTasksSymmetricDistribution(stageInfo, taskInfo -> Optional.of(taskInfo.stats().getCreateTime().toEpochMilli() - queryCreateTimeMillis));
        LongSymmetricDistribution firstStartTimeMillisDistribution = getTasksSymmetricDistribution(stageInfo, taskInfo -> Optional.ofNullable(taskInfo.stats().getFirstStartTime()).map(value -> value.toEpochMilli() - queryCreateTimeMillis));
        LongSymmetricDistribution lastStartTimeMillisDistribution = getTasksSymmetricDistribution(stageInfo, taskInfo -> Optional.ofNullable(taskInfo.stats().getLastStartTime()).map(value -> value.toEpochMilli() - queryCreateTimeMillis));
        LongSymmetricDistribution terminatingStartTimeMillisDistribution = getTasksSymmetricDistribution(stageInfo, taskInfo -> Optional.ofNullable(taskInfo.stats().getTerminatingStartTime()).map(value -> value.toEpochMilli() - queryCreateTimeMillis));
        LongSymmetricDistribution lastEndTimeMillisDistribution = getTasksSymmetricDistribution(stageInfo, taskInfo -> Optional.ofNullable(taskInfo.stats().getLastEndTime()).map(value -> value.toEpochMilli() - queryCreateTimeMillis));
        LongSymmetricDistribution endTimeMillisDistribution = getTasksSymmetricDistribution(stageInfo, taskInfo -> Optional.ofNullable(taskInfo.stats().getEndTime()).map(value -> value.toEpochMilli() - queryCreateTimeMillis));

        Optional<Long> queryExecutionTime = Optional.ofNullable(queryInfo.getQueryStats().getEndTime()).map(value -> value.toEpochMilli() - queryCreateTimeMillis);
        DoubleSymmetricDistribution createTimeScaledDistribution;
        DoubleSymmetricDistribution firstStartTimeScaledDistribution;
        DoubleSymmetricDistribution lastStartTimeScaledDistribution;
        DoubleSymmetricDistribution terminatingStartTimeScaledDistribution;
        DoubleSymmetricDistribution lastEndTimeScaledDistribution;
        DoubleSymmetricDistribution endTimeScaledDistribution;
        if (queryExecutionTime.isPresent()) {
            createTimeScaledDistribution = scaleDistribution(createTimeMillisDistribution, queryExecutionTime.orElseThrow());
            firstStartTimeScaledDistribution = scaleDistribution(firstStartTimeMillisDistribution, queryExecutionTime.orElseThrow());
            lastStartTimeScaledDistribution = scaleDistribution(lastStartTimeMillisDistribution, queryExecutionTime.orElseThrow());
            terminatingStartTimeScaledDistribution = scaleDistribution(terminatingStartTimeMillisDistribution, queryExecutionTime.orElseThrow());
            lastEndTimeScaledDistribution = scaleDistribution(lastEndTimeMillisDistribution, queryExecutionTime.orElseThrow());
            endTimeScaledDistribution = scaleDistribution(endTimeMillisDistribution, queryExecutionTime.orElseThrow());
        }
        else {
            createTimeScaledDistribution = DoubleSymmetricDistribution.ZERO;
            firstStartTimeScaledDistribution = DoubleSymmetricDistribution.ZERO;
            lastStartTimeScaledDistribution = DoubleSymmetricDistribution.ZERO;
            terminatingStartTimeScaledDistribution = DoubleSymmetricDistribution.ZERO;
            lastEndTimeScaledDistribution = DoubleSymmetricDistribution.ZERO;
            endTimeScaledDistribution = DoubleSymmetricDistribution.ZERO;
        }

        Map<String, DoubleSymmetricDistribution> getSplitDistribution = stageInfo.getStageStats().getGetSplitDistribution()
                .entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().toString(), entry -> {
                    DistributionSnapshot distributionSnapshot = entry.getValue();
                    return new DoubleSymmetricDistribution(
                            distributionSnapshot.getP01(),
                            distributionSnapshot.getP05(),
                            distributionSnapshot.getP10(),
                            distributionSnapshot.getP25(),
                            distributionSnapshot.getP50(),
                            distributionSnapshot.getP75(),
                            distributionSnapshot.getP90(),
                            distributionSnapshot.getP95(),
                            distributionSnapshot.getP99(),
                            distributionSnapshot.getMin(),
                            distributionSnapshot.getMax(),
                            distributionSnapshot.getTotal(),
                            distributionSnapshot.getCount());
                }));
        return new StageTaskStatistics(
                stageInfo.getStageId().getId(),
                stageInfo.getTasks().size(),
                getTasksDistribution(stageInfo, taskInfo -> Optional.of(taskInfo.stats().getTotalCpuTime().toMillis())),
                getTasksDistribution(stageInfo, taskInfo -> Optional.of(taskInfo.stats().getTotalScheduledTime().toMillis())),
                getTasksDistribution(stageInfo, taskInfo -> Optional.of(taskInfo.stats().getPeakUserMemoryReservation().toBytes())),
                getTasksDistribution(stageInfo, taskInfo -> taskInfo.estimatedMemory().map(DataSize::toBytes)),
                getTasksDistribution(stageInfo, taskInfo -> Optional.of(taskInfo.stats().getRawInputDataSize().toBytes())),
                getTasksDistribution(stageInfo, taskInfo -> Optional.of(taskInfo.stats().getRawInputPositions())),
                getTasksDistribution(stageInfo, taskInfo -> Optional.of(taskInfo.stats().getProcessedInputDataSize().toBytes())),
                getTasksDistribution(stageInfo, taskInfo -> Optional.of(taskInfo.stats().getProcessedInputPositions())),
                getTasksDistribution(stageInfo, taskInfo -> Optional.of(taskInfo.stats().getOutputDataSize().toBytes())),
                getTasksDistribution(stageInfo, taskInfo -> Optional.of(taskInfo.stats().getOutputPositions())),
                getTasksDistribution(stageInfo, taskInfo -> Optional.of((long) taskInfo.stats().getTotalDrivers())),
                createTimeMillisDistribution,
                firstStartTimeMillisDistribution,
                lastStartTimeMillisDistribution,
                terminatingStartTimeMillisDistribution,
                lastEndTimeMillisDistribution,
                endTimeMillisDistribution,
                createTimeScaledDistribution,
                firstStartTimeScaledDistribution,
                lastStartTimeScaledDistribution,
                terminatingStartTimeScaledDistribution,
                lastEndTimeScaledDistribution,
                endTimeScaledDistribution,
                getSplitDistribution);
    }

    private static LongDistribution getTasksDistribution(StageInfo stageInfo, Function<TaskInfo, Optional<Long>> metricFunction)
    {
        Distribution distribution = new Distribution();
        for (TaskInfo taskInfo : stageInfo.getTasks()) {
            metricFunction.apply(taskInfo).ifPresent(distribution::add);
        }
        DistributionSnapshot snapshot = distribution.snapshot();
        return new LongDistribution(
                (long) snapshot.getP25(),
                (long) snapshot.getP50(),
                (long) snapshot.getP75(),
                (long) snapshot.getP90(),
                (long) snapshot.getP95(),
                (long) snapshot.getP99(),
                (long) snapshot.getMin(),
                (long) snapshot.getMax(),
                (long) snapshot.getTotal(),
                firstNonNaN(snapshot.getTotal() / snapshot.getCount(), 0.0));
    }

    private static LongSymmetricDistribution getTasksSymmetricDistribution(StageInfo stageInfo, Function<TaskInfo, Optional<Long>> metricFunction)
    {
        Distribution distribution = new Distribution();
        for (TaskInfo taskInfo : stageInfo.getTasks()) {
            metricFunction.apply(taskInfo).ifPresent(distribution::add);
        }
        DistributionSnapshot snapshot = distribution.snapshot();
        return new LongSymmetricDistribution(
                (long) snapshot.getP01(),
                (long) snapshot.getP05(),
                (long) snapshot.getP10(),
                (long) snapshot.getP25(),
                (long) snapshot.getP50(),
                (long) snapshot.getP75(),
                (long) snapshot.getP90(),
                (long) snapshot.getP95(),
                (long) snapshot.getP99(),
                (long) snapshot.getMin(),
                (long) snapshot.getMax(),
                (long) snapshot.getTotal(),
                firstNonNaN(snapshot.getTotal() / snapshot.getCount(), 0.0));
    }

    private static DoubleSymmetricDistribution scaleDistribution(LongSymmetricDistribution distribution, long scaleFactor)
    {
        return new DoubleSymmetricDistribution(
                (double) distribution.getP01() / scaleFactor,
                (double) distribution.getP05() / scaleFactor,
                (double) distribution.getP10() / scaleFactor,
                (double) distribution.getP25() / scaleFactor,
                (double) distribution.getP50() / scaleFactor,
                (double) distribution.getP75() / scaleFactor,
                (double) distribution.getP90() / scaleFactor,
                (double) distribution.getP95() / scaleFactor,
                (double) distribution.getP99() / scaleFactor,
                (double) distribution.getMin() / scaleFactor,
                (double) distribution.getMax() / scaleFactor,
                (double) distribution.getTotal(),
                (double) distribution.getAverage() / scaleFactor);
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

    private static Instant min(Instant a, Instant b)
    {
        return a.isBefore(b) ? a : b;
    }

    private static Instant max(Instant a, Instant b)
    {
        return a.isAfter(b) ? a : b;
    }
}
