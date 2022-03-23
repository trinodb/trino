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
import io.trino.SessionRepresentation;
import io.trino.client.NodeVersion;
import io.trino.cost.StatsAndCosts;
import io.trino.eventlistener.EventListenerManager;
import io.trino.execution.Column;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.Input;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryStats;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskState;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.operator.OperatorStats;
import io.trino.operator.TableFinishInfo;
import io.trino.operator.TaskStats;
import io.trino.server.BasicQueryInfo;
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
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.StageCpuDistribution;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.planprinter.ValuePrinter;
import io.trino.transaction.TransactionId;
import org.joda.time.DateTime;

import javax.inject.Inject;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.StageInfo.getAllStages;
import static io.trino.sql.planner.planprinter.PlanPrinter.textDistributedPlan;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static java.util.Objects.requireNonNull;

public class QueryMonitor
{
    private static final Logger log = Logger.get(QueryMonitor.class);

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
        this.serverVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        this.serverAddress = requireNonNull(nodeInfo, "nodeInfo is null").getExternalAddress();
        this.environment = requireNonNull(nodeInfo, "nodeInfo is null").getEnvironment();
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.maxJsonLimit = toIntExact(requireNonNull(config, "config is null").getMaxOutputStageJsonSize().toBytes());
    }

    public void queryCreatedEvent(BasicQueryInfo queryInfo)
    {
        eventListenerManager.queryCreated(
                new QueryCreatedEvent(
                        queryInfo.getQueryStats().getCreateTime().toDate().toInstant(),
                        createQueryContext(queryInfo.getSession(), queryInfo.getResourceGroupId(), queryInfo.getQueryType()),
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
                                Optional.empty())));
    }

    public void queryImmediateFailureEvent(BasicQueryInfo queryInfo, ExecutionFailureInfo failure)
    {
        eventListenerManager.queryCompleted(new QueryCompletedEvent(
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
                createQueryContext(queryInfo.getSession(), queryInfo.getResourceGroupId(), queryInfo.getQueryType()),
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
        eventListenerManager.queryCompleted(
                new QueryCompletedEvent(
                        createQueryMetadata(queryInfo),
                        createQueryStatistics(queryInfo),
                        createQueryContext(queryInfo.getSession(), queryInfo.getResourceGroupId(), queryInfo.getQueryType()),
                        getQueryIOMetadata(queryInfo),
                        createQueryFailureInfo(queryInfo.getFailureInfo(), queryInfo.getOutputStage()),
                        queryInfo.getWarnings(),
                        ofEpochMilli(queryStats.getCreateTime().getMillis()),
                        ofEpochMilli(queryStats.getExecutionStartTime().getMillis()),
                        ofEpochMilli(queryStats.getEndTime() != null ? queryStats.getEndTime().getMillis() : 0)));

        logQueryTimeline(queryInfo);
    }

    private QueryMetadata createQueryMetadata(QueryInfo queryInfo)
    {
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
                createTextQueryPlan(queryInfo),
                queryInfo.getOutputStage().flatMap(stage -> stageInfoCodec.toJsonWithLengthLimit(stage, maxJsonLimit)));
    }

    private QueryStatistics createQueryStatistics(QueryInfo queryInfo)
    {
        ImmutableList.Builder<String> operatorSummaries = ImmutableList.builder();
        for (OperatorStats summary : queryInfo.getQueryStats().getOperatorSummaries()) {
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
                queryInfo.isCompleteInfo(),
                getCpuDistributions(queryInfo),
                operatorSummaries.build(),
                serializedPlanNodeStatsAndCosts);
    }

    private QueryContext createQueryContext(SessionRepresentation session, Optional<ResourceGroupId> resourceGroup, Optional<QueryType> queryType)
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
                queryType);
    }

    private Optional<String> createTextQueryPlan(QueryInfo queryInfo)
    {
        try {
            if (queryInfo.getOutputStage().isPresent()) {
                return Optional.of(textDistributedPlan(
                        queryInfo.getOutputStage().get(),
                        queryInfo.getQueryStats(),
                        new ValuePrinter(metadata, functionManager, queryInfo.getSession().toSession(sessionPropertyManager)),
                        false));
            }
        }
        catch (Exception e) {
            // Sometimes it is expected to fail. For example if generated plan is too long.
            // Don't fail to create event if the plan cannot be created.
            log.warn(e, "Error creating explain plan for query %s", queryInfo.getQueryId());
        }
        return Optional.empty();
    }

    private static QueryIOMetadata getQueryIOMetadata(QueryInfo queryInfo)
    {
        Multimap<FragmentNode, OperatorStats> planNodeStats = extractPlanNodeStats(queryInfo);

        ImmutableList.Builder<QueryInputMetadata> inputs = ImmutableList.builder();
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

            inputs.add(new QueryInputMetadata(
                    input.getCatalogName(),
                    input.getSchema(),
                    input.getTable(),
                    input.getColumns().stream()
                            .map(Column::getName).collect(Collectors.toList()),
                    input.getConnectorInfo(),
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
            // long lastSchedulingCompletion = 0;
            long firstTaskStartTime = queryEndTime.getMillis();
            long lastTaskStartTime = queryStartTime.getMillis() + planning;
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

                    DateTime lastStartTime = taskStats.getLastStartTime();
                    if (lastStartTime != null) {
                        lastTaskStartTime = max(lastStartTime.getMillis(), lastTaskStartTime);
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
                    queryInfo.getSession().getTransactionId().map(TransactionId::toString).orElse(""),
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
                queryInfo.getSession().getTransactionId().map(TransactionId::toString).orElse(""),
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
            String transactionId,
            long elapsedMillis,
            long planningMillis,
            long waitingMillis,
            long schedulingMillis,
            long runningMillis,
            long finishingMillis,
            DateTime queryStartTime,
            DateTime queryEndTime)
    {
        log.info("TIMELINE: Query %s :: Transaction:[%s] :: elapsed %sms :: planning %sms :: waiting %sms :: scheduling %sms :: running %sms :: finishing %sms :: begin %s :: end %s",
                queryId,
                transactionId,
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
