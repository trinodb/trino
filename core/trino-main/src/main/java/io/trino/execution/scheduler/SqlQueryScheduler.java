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
package io.trino.execution.scheduler;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.graph.Traverser;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.execution.BasicStageStats;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.RemoteTask;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.SqlStageExecution;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskStatus;
import io.trino.failuredetector.FailureDetector;
import io.trino.metadata.InternalNode;
import io.trino.server.DynamicFilterService;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.split.SplitSource;
import io.trino.sql.planner.NodePartitionMap;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.StageExecutionPlan;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.trino.SystemSessionProperties.getConcurrentLifespansPerNode;
import static io.trino.SystemSessionProperties.getWriterMinSize;
import static io.trino.connector.CatalogName.isInternalSystemConnector;
import static io.trino.execution.BasicStageStats.aggregateBasicStageStats;
import static io.trino.execution.SqlStageExecution.createSqlStageExecution;
import static io.trino.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static io.trino.execution.scheduler.StreamingStageExecution.State.ABORTED;
import static io.trino.execution.scheduler.StreamingStageExecution.State.CANCELED;
import static io.trino.execution.scheduler.StreamingStageExecution.State.FAILED;
import static io.trino.execution.scheduler.StreamingStageExecution.State.FINISHED;
import static io.trino.execution.scheduler.StreamingStageExecution.State.FLUSHING;
import static io.trino.execution.scheduler.StreamingStageExecution.State.RUNNING;
import static io.trino.execution.scheduler.StreamingStageExecution.State.SCHEDULED;
import static io.trino.execution.scheduler.StreamingStageExecution.createStreamingStageExecution;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toCollection;

public class SqlQueryScheduler
{
    private final QueryStateMachine queryStateMachine;
    private final StageExecutionPlan plan;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeScheduler nodeScheduler;
    private final int splitBatchSize;
    private final ExecutorService executor;
    private final ScheduledExecutorService schedulerExecutor;
    private final FailureDetector failureDetector;
    private final ExecutionPolicy executionPolicy;
    private final SplitSchedulerStats schedulerStats;
    private final DynamicFilterService dynamicFilterService;

    private final Map<PlanFragmentId, SqlStageExecution> stages;
    private final StageId rootStageId;
    private final Map<StageId, Set<StageId>> stageLineage;

    // all stages that could be scheduled remotely (excluding coordinator only stages)
    private final Set<StageId> remotelyScheduledStages;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicReference<StreamingScheduler> scheduler = new AtomicReference<>();

    public static SqlQueryScheduler createSqlQueryScheduler(
            QueryStateMachine queryStateMachine,
            StageExecutionPlan plan,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            boolean summarizeTaskInfo,
            int splitBatchSize,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats,
            DynamicFilterService dynamicFilterService)
    {
        SqlQueryScheduler sqlQueryScheduler = new SqlQueryScheduler(
                queryStateMachine,
                plan,
                nodePartitioningManager,
                nodeScheduler,
                remoteTaskFactory,
                summarizeTaskInfo,
                splitBatchSize,
                queryExecutor,
                schedulerExecutor,
                failureDetector,
                nodeTaskMap,
                executionPolicy,
                schedulerStats,
                dynamicFilterService);
        sqlQueryScheduler.initialize();
        return sqlQueryScheduler;
    }

    private SqlQueryScheduler(
            QueryStateMachine queryStateMachine,
            StageExecutionPlan plan,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            boolean summarizeTaskInfo,
            int splitBatchSize,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats,
            DynamicFilterService dynamicFilterService)
    {
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.plan = requireNonNull(plan, "plan is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
        this.splitBatchSize = splitBatchSize;
        this.executor = requireNonNull(queryExecutor, "queryExecutor is null");
        this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");

        stages = createStages(
                queryStateMachine.getSession(),
                remoteTaskFactory,
                nodeTaskMap,
                queryExecutor,
                schedulerStats,
                plan,
                summarizeTaskInfo);
        rootStageId = getStageId(queryStateMachine.getQueryId(), plan.getFragment().getId());
        stageLineage = getStageLineage(queryStateMachine.getQueryId(), plan);
        remotelyScheduledStages = stages.values().stream()
                .filter(stage -> !stage.getFragment().getPartitioning().isCoordinatorOnly())
                .map(SqlStageExecution::getStageId)
                .collect(toImmutableSet());
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize()
    {
        // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
        queryStateMachine.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                stages.values().forEach(SqlStageExecution::transitionToFinished);
                queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo()));
            }
        });
        for (SqlStageExecution stage : stages.values()) {
            stage.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo())));
        }
    }

    private static Map<PlanFragmentId, SqlStageExecution> createStages(
            Session session,
            RemoteTaskFactory taskFactory,
            NodeTaskMap nodeTaskMap,
            ExecutorService executor,
            SplitSchedulerStats schedulerStats,
            StageExecutionPlan planTree,
            boolean summarizeTaskInfo)
    {
        ImmutableMap.Builder<PlanFragmentId, SqlStageExecution> result = ImmutableMap.builder();
        for (StageExecutionPlan planNode : Traverser.forTree(StageExecutionPlan::getSubStages).breadthFirst(planTree)) {
            PlanFragment fragment = planNode.getFragment();
            SqlStageExecution stageExecution = createSqlStageExecution(
                    getStageId(session.getQueryId(), fragment.getId()),
                    fragment,
                    planNode.getTables(),
                    taskFactory,
                    session,
                    summarizeTaskInfo,
                    nodeTaskMap,
                    executor,
                    schedulerStats);
            result.put(fragment.getId(), stageExecution);
        }
        return result.build();
    }

    private static Map<StageId, Set<StageId>> getStageLineage(QueryId queryId, StageExecutionPlan planTree)
    {
        ImmutableMap.Builder<StageId, Set<StageId>> result = ImmutableMap.builder();
        for (StageExecutionPlan planNode : Traverser.forTree(StageExecutionPlan::getSubStages).breadthFirst(planTree)) {
            result.put(
                    getStageId(queryId, planNode.getFragment().getId()),
                    planNode.getSubStages().stream()
                            .map(stage -> getStageId(queryId, stage.getFragment().getId()))
                            .collect(toImmutableSet()));
        }
        return result.build();
    }

    private static StageId getStageId(QueryId queryId, PlanFragmentId fragmentId)
    {
        // TODO: refactor fragment id to be based on an integer
        return new StageId(queryId, parseInt(fragmentId.toString()));
    }

    public synchronized void start()
    {
        if (started.compareAndSet(false, true)) {
            if (queryStateMachine.isDone()) {
                return;
            }
            StreamingScheduler streamingScheduler = createStreamingScheduler(new ResultsConsumer()
            {
                @Override
                public void addSourceTask(PlanFragmentId fragmentId, RemoteTask task)
                {
                    Set<URI> bufferLocations = ImmutableSet.of(uriBuilderFrom(task.getTaskStatus().getSelf())
                            .appendPath("results")
                            .appendPath("0").build());
                    queryStateMachine.updateOutputLocations(bufferLocations, false);
                }

                @Override
                public void noMoreSourceTasks(PlanFragmentId fragmentId)
                {
                    queryStateMachine.updateOutputLocations(ImmutableSet.of(), true);
                }
            });
            scheduler.set(streamingScheduler);
            executor.submit(streamingScheduler::schedule);
        }
    }

    public synchronized void cancelStage(StageId stageId)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            StreamingScheduler scheduler = this.scheduler.get();
            if (scheduler != null) {
                scheduler.cancelStage(stageId);
            }
        }
    }

    public synchronized void abort()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            StreamingScheduler scheduler = this.scheduler.get();
            if (scheduler != null) {
                scheduler.abort();
            }
            stages.values().forEach(SqlStageExecution::transitionToFinished);
        }
    }

    public BasicStageStats getBasicStageStats()
    {
        List<BasicStageStats> stageStats = stages.values().stream()
                .map(SqlStageExecution::getBasicStageStats)
                .collect(toImmutableList());

        return aggregateBasicStageStats(stageStats);
    }

    public StageInfo getStageInfo()
    {
        Map<StageId, StageInfo> stageInfos = stages.values().stream()
                .map(SqlStageExecution::getStageInfo)
                .collect(toImmutableMap(StageInfo::getStageId, identity()));

        return buildStageInfo(rootStageId, stageInfos);
    }

    private StageInfo buildStageInfo(StageId stageId, Map<StageId, StageInfo> stageInfos)
    {
        StageInfo parent = stageInfos.get(stageId);
        checkArgument(parent != null, "No stageInfo for %s", parent);
        List<StageInfo> childStages = stageLineage.get(stageId).stream()
                .map(childStageId -> buildStageInfo(childStageId, stageInfos))
                .collect(toImmutableList());
        if (childStages.isEmpty()) {
            return parent;
        }
        return new StageInfo(
                parent.getStageId(),
                parent.getState(),
                parent.getPlan(),
                parent.getTypes(),
                parent.getStageStats(),
                parent.getTasks(),
                childStages,
                parent.getTables(),
                parent.getFailureCause());
    }

    public long getUserMemoryReservation()
    {
        return stages.values().stream()
                .mapToLong(SqlStageExecution::getUserMemoryReservation)
                .sum();
    }

    public long getTotalMemoryReservation()
    {
        return stages.values().stream()
                .mapToLong(SqlStageExecution::getTotalMemoryReservation)
                .sum();
    }

    public Duration getTotalCpuTime()
    {
        long millis = stages.values().stream()
                .mapToLong(stage -> stage.getTotalCpuTime().toMillis())
                .sum();
        return new Duration(millis, MILLISECONDS);
    }

    private StreamingScheduler createStreamingScheduler(ResultsConsumer resultsConsumer)
    {
        Session session = queryStateMachine.getSession();
        Map<PartitioningHandle, NodePartitionMap> partitioningCacheMap = new HashMap<>();
        Function<PartitioningHandle, NodePartitionMap> partitioningCache = partitioningHandle ->
                partitioningCacheMap.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(session, handle));
        Map<PlanFragmentId, Optional<int[]>> bucketToPartitionMap = createBucketToPartitionMap(plan, partitioningCache);
        Map<PlanFragmentId, OutputBufferManager> outputBufferManagers = createOutputBufferManagers(stages.values(), bucketToPartitionMap);
        ImmutableList.Builder<StreamingStageExecution> executionsBuilder = ImmutableList.builder();
        ImmutableMap.Builder<StageId, StageScheduler> schedulersBuilder = ImmutableMap.builder();
        StreamingStageExecution rootExecution = createStreamingExecution(
                plan,
                outputBufferManagers,
                bucketToPartitionMap,
                partitioningCache,
                resultsConsumer,
                executionsBuilder,
                schedulersBuilder);
        List<StreamingStageExecution> executions = executionsBuilder.build();
        ExecutionSchedule executionSchedule = executionPolicy.createExecutionSchedule(executions);

        rootExecution.addStateChangeListener(state -> {
            if (state == FINISHED) {
                queryStateMachine.transitionToFinishing();
            }
            else if (state == CANCELED) {
                // output stage was canceled
                queryStateMachine.transitionToCanceled();
            }
        });

        Set<StageId> finishedStages = newConcurrentHashSet();
        for (StreamingStageExecution execution : executions) {
            execution.addStateChangeListener(state -> {
                if (queryStateMachine.isDone()) {
                    return;
                }
                if (!state.canScheduleMoreTasks()) {
                    dynamicFilterService.stageCannotScheduleMoreTasks(execution.getStageId(), execution.getAllTasks().size());
                }
                if (state == FAILED) {
                    RuntimeException failureCause = execution.getFailureCause()
                            .map(ExecutionFailureInfo::toException)
                            .orElseGet(() -> new VerifyException(format("stage execution for stage %s is failed by failure cause is not present", execution.getStageId())));
                    stages.get(execution.getFragment().getId()).transitionToFailed(failureCause);
                    queryStateMachine.transitionToFailed(failureCause);
                }
                else if (state == ABORTED) {
                    // this should never happen, since abort can only be triggered in query clean up after the query is finished
                    queryStateMachine.transitionToFailed(new TrinoException(GENERIC_INTERNAL_ERROR, "Query stage was aborted"));
                }
                else if (queryStateMachine.getQueryState() == QueryState.STARTING) {
                    // if the stage has at least one task, we are running
                    if (!execution.getAllTasks().isEmpty()) {
                        queryStateMachine.transitionToRunning();
                    }
                }
                else if (state.isDone() && !state.isFailure()) {
                    finishedStages.add(execution.getStageId());
                    // Once all remotely scheduled stages complete it should be safe to transition stage execution
                    // to the finished state as at this point no further task retries are expected
                    // This is needed to make explain analyze work that requires final stage info to be available before the
                    // explain analyze stage is finished
                    if (finishedStages.containsAll(remotelyScheduledStages)) {
                        stages.values().stream()
                                .filter(stage -> finishedStages.contains(stage.getStageId()))
                                .forEach(SqlStageExecution::transitionToFinished);
                    }
                }
            });
        }

        return new StreamingScheduler(
                queryStateMachine,
                executionSchedule,
                schedulersBuilder.build(),
                schedulerStats,
                executions);
    }

    private StreamingStageExecution createStreamingExecution(
            StageExecutionPlan plan,
            Map<PlanFragmentId, OutputBufferManager> outputBufferManagers,
            Map<PlanFragmentId, Optional<int[]>> bucketToPartitionMap,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            ResultsConsumer resultsConsumer,
            ImmutableList.Builder<StreamingStageExecution> executions,
            ImmutableMap.Builder<StageId, StageScheduler> schedulers)
    {
        PlanFragment fragment = plan.getFragment();
        StreamingStageExecution execution = createStreamingStageExecution(
                stages.get(fragment.getId()),
                outputBufferManagers,
                resultsConsumer,
                failureDetector,
                executor,
                bucketToPartitionMap.get(fragment.getId()));
        executions.add(execution);
        ImmutableList.Builder<StreamingStageExecution> childExecutionsBuilder = ImmutableList.builder();
        for (StageExecutionPlan child : plan.getSubStages()) {
            childExecutionsBuilder.add(createStreamingExecution(
                    child,
                    outputBufferManagers,
                    bucketToPartitionMap,
                    partitioningCache,
                    execution,
                    executions,
                    schedulers));
        }
        ImmutableList<StreamingStageExecution> childExecutions = childExecutionsBuilder.build();
        StageScheduler scheduler = createStageScheduler(
                execution,
                plan.getSplitSources(),
                childExecutions,
                partitioningCache);
        schedulers.put(execution.getStageId(), scheduler);

        execution.addStateChangeListener(newState -> {
            if (newState == FLUSHING || newState.isDone()) {
                childExecutions.forEach(StreamingStageExecution::cancel);
            }
        });

        return execution;
    }

    private StageScheduler createStageScheduler(
            StreamingStageExecution stageExecution,
            Map<PlanNodeId, SplitSource> splitSources,
            List<StreamingStageExecution> childStages,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache)
    {
        Session session = queryStateMachine.getSession();
        PlanFragment fragment = stageExecution.getFragment();
        PartitioningHandle partitioningHandle = fragment.getPartitioning();
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
            // nodes are selected dynamically based on the constraints of the splits and the system load
            Entry<PlanNodeId, SplitSource> entry = Iterables.getOnlyElement(splitSources.entrySet());
            PlanNodeId planNodeId = entry.getKey();
            SplitSource splitSource = entry.getValue();
            Optional<CatalogName> catalogName = Optional.of(splitSource.getCatalogName())
                    .filter(catalog -> !isInternalSystemConnector(catalog));
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, catalogName);
            SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stageExecution::getAllTasks);

            checkArgument(!fragment.getStageExecutionDescriptor().isStageGroupedExecution());

            return newSourcePartitionedSchedulerAsStageScheduler(
                    stageExecution,
                    planNodeId,
                    splitSource,
                    placementPolicy,
                    splitBatchSize,
                    dynamicFilterService,
                    () -> childStages.stream().anyMatch(StreamingStageExecution::isAnyTaskBlocked));
        }
        else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            Supplier<Collection<TaskStatus>> sourceTasksProvider = () -> childStages.stream()
                    .map(StreamingStageExecution::getTaskStatuses)
                    .flatMap(List::stream)
                    .collect(toImmutableList());
            Supplier<Collection<TaskStatus>> writerTasksProvider = stageExecution::getTaskStatuses;

            ScaledWriterScheduler scheduler = new ScaledWriterScheduler(
                    stageExecution,
                    sourceTasksProvider,
                    writerTasksProvider,
                    nodeScheduler.createNodeSelector(session, Optional.empty()),
                    schedulerExecutor,
                    getWriterMinSize(session));

            whenAllStages(childStages, StreamingStageExecution.State::isDone)
                    .addListener(scheduler::finish, directExecutor());

            return scheduler;
        }
        else {
            if (!splitSources.isEmpty()) {
                // contains local source
                List<PlanNodeId> schedulingOrder = fragment.getPartitionedSources();
                Optional<CatalogName> catalogName = partitioningHandle.getConnectorId();
                checkArgument(catalogName.isPresent(), "No connector ID for partitioning handle: %s", partitioningHandle);
                List<ConnectorPartitionHandle> connectorPartitionHandles;
                boolean groupedExecutionForStage = fragment.getStageExecutionDescriptor().isStageGroupedExecution();
                if (groupedExecutionForStage) {
                    connectorPartitionHandles = nodePartitioningManager.listPartitionHandles(session, partitioningHandle);
                    checkState(!ImmutableList.of(NOT_PARTITIONED).equals(connectorPartitionHandles));
                }
                else {
                    connectorPartitionHandles = ImmutableList.of(NOT_PARTITIONED);
                }

                BucketNodeMap bucketNodeMap;
                List<InternalNode> stageNodeList;
                if (fragment.getRemoteSourceNodes().stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                    // no remote source
                    boolean dynamicLifespanSchedule = fragment.getStageExecutionDescriptor().isDynamicLifespanSchedule();
                    bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle, dynamicLifespanSchedule);

                    // verify execution is consistent with planner's decision on dynamic lifespan schedule
                    verify(bucketNodeMap.isDynamic() == dynamicLifespanSchedule);

                    stageNodeList = new ArrayList<>(nodeScheduler.createNodeSelector(session, catalogName).allNodes());
                    Collections.shuffle(stageNodeList);
                }
                else {
                    // cannot use dynamic lifespan schedule
                    verify(!fragment.getStageExecutionDescriptor().isDynamicLifespanSchedule());

                    // remote source requires nodePartitionMap
                    NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
                    if (groupedExecutionForStage) {
                        checkState(connectorPartitionHandles.size() == nodePartitionMap.getBucketToPartition().length);
                    }
                    stageNodeList = nodePartitionMap.getPartitionToNode();
                    bucketNodeMap = nodePartitionMap.asBucketNodeMap();
                }

                return new FixedSourcePartitionedScheduler(
                        stageExecution,
                        splitSources,
                        fragment.getStageExecutionDescriptor(),
                        schedulingOrder,
                        stageNodeList,
                        bucketNodeMap,
                        splitBatchSize,
                        getConcurrentLifespansPerNode(session),
                        nodeScheduler.createNodeSelector(session, catalogName),
                        connectorPartitionHandles,
                        dynamicFilterService);
            }
            else {
                // all sources are remote
                NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
                List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
                // todo this should asynchronously wait a standard timeout period before failing
                checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
                return new FixedCountScheduler(stageExecution, partitionToNode);
            }
        }
    }

    private static ListenableFuture<Void> whenAllStages(Collection<StreamingStageExecution> stages, Predicate<StreamingStageExecution.State> predicate)
    {
        checkArgument(!stages.isEmpty(), "stages is empty");
        Set<StageId> stageIds = stages.stream()
                .map(StreamingStageExecution::getStageId)
                .collect(toCollection(Sets::newConcurrentHashSet));
        SettableFuture<Void> future = SettableFuture.create();

        for (StreamingStageExecution stage : stages) {
            stage.addStateChangeListener(state -> {
                if (predicate.test(state) && stageIds.remove(stage.getStageId()) && stageIds.isEmpty()) {
                    future.set(null);
                }
            });
        }

        return future;
    }

    private static Map<PlanFragmentId, OutputBufferManager> createOutputBufferManagers(
            Collection<SqlStageExecution> stageExecutions,
            Map<PlanFragmentId, Optional<int[]>> bucketToPartitionMap)
    {
        ImmutableMap.Builder<PlanFragmentId, OutputBufferManager> result = ImmutableMap.builder();
        for (SqlStageExecution stageExecution : stageExecutions) {
            PlanFragmentId fragmentId = stageExecution.getFragment().getId();
            PartitioningHandle partitioningHandle = stageExecution.getFragment().getPartitioningScheme().getPartitioning().getHandle();
            OutputBufferManager outputBufferManager;
            if (partitioningHandle.equals(FIXED_BROADCAST_DISTRIBUTION)) {
                outputBufferManager = new BroadcastOutputBufferManager();
            }
            else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
                outputBufferManager = new ScaledOutputBufferManager();
            }
            else {
                Optional<int[]> bucketToPartition = bucketToPartitionMap.get(fragmentId);
                checkArgument(bucketToPartition.isPresent(), "bucketToPartition is expected to be present for fragment: %s", fragmentId);
                int partitionCount = Ints.max(bucketToPartition.get()) + 1;
                outputBufferManager = new PartitionedOutputBufferManager(partitioningHandle, partitionCount);
            }
            result.put(fragmentId, outputBufferManager);
        }
        return result.build();
    }

    private static Map<PlanFragmentId, Optional<int[]>> createBucketToPartitionMap(
            StageExecutionPlan plan,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache)
    {
        ImmutableMap.Builder<PlanFragmentId, Optional<int[]>> result = ImmutableMap.builder();
        // root fragment always has a single consumer
        result.put(plan.getFragment().getId(), Optional.of(new int[] {0}));
        Queue<StageExecutionPlan> queue = new ArrayDeque<>();
        queue.add(plan);
        while (!queue.isEmpty()) {
            StageExecutionPlan executionPlan = queue.poll();
            PlanFragment fragment = executionPlan.getFragment();
            Optional<int[]> bucketToPartition = getBucketToPartition(fragment.getPartitioning(), partitioningCache, fragment.getRoot(), fragment.getRemoteSourceNodes());
            for (StageExecutionPlan child : executionPlan.getSubStages()) {
                result.put(child.getFragment().getId(), bucketToPartition);
                queue.add(child);
            }
        }
        return result.build();
    }

    private static Optional<int[]> getBucketToPartition(
            PartitioningHandle partitioningHandle,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            PlanNode fragmentRoot,
            List<RemoteSourceNode> remoteSourceNodes)
    {
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION) || partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            return Optional.of(new int[1]);
        }
        else if (searchFrom(fragmentRoot).where(node -> node instanceof TableScanNode).findFirst().isPresent()) {
            if (remoteSourceNodes.stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                return Optional.empty();
            }
            else {
                // remote source requires nodePartitionMap
                NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
                return Optional.of(nodePartitionMap.getBucketToPartition());
            }
        }
        else {
            NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
            List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
            // todo this should asynchronously wait a standard timeout period before failing
            checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
            return Optional.of(nodePartitionMap.getBucketToPartition());
        }
    }

    private static class StreamingScheduler
    {
        private final QueryStateMachine queryStateMachine;
        private final ExecutionSchedule executionSchedule;
        private final Map<StageId, StageScheduler> stageSchedulers;
        private final SplitSchedulerStats schedulerStats;
        private final List<StreamingStageExecution> stageExecutions;

        private final AtomicBoolean started = new AtomicBoolean();

        private StreamingScheduler(
                QueryStateMachine queryStateMachine,
                ExecutionSchedule executionSchedule,
                Map<StageId, StageScheduler> stageSchedulers,
                SplitSchedulerStats schedulerStats,
                List<StreamingStageExecution> stageExecutions)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.executionSchedule = requireNonNull(executionSchedule, "executionSchedule is null");
            this.stageSchedulers = ImmutableMap.copyOf(requireNonNull(stageSchedulers, "stageSchedulers is null"));
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.stageExecutions = ImmutableList.copyOf(requireNonNull(stageExecutions, "stageExecutions is null"));
        }

        public void schedule()
        {
            checkState(started.compareAndSet(false, true), "already started");

            try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
                while (!executionSchedule.isFinished()) {
                    List<ListenableFuture<Void>> blockedStages = new ArrayList<>();
                    for (StreamingStageExecution stage : executionSchedule.getStagesToSchedule()) {
                        stage.beginScheduling();

                        // perform some scheduling work
                        ScheduleResult result = stageSchedulers.get(stage.getStageId())
                                .schedule();

                        // modify parent and children based on the results of the scheduling
                        if (result.isFinished()) {
                            stage.schedulingComplete();
                        }
                        else if (!result.getBlocked().isDone()) {
                            blockedStages.add(result.getBlocked());
                        }
                        schedulerStats.getSplitsScheduledPerIteration().add(result.getSplitsScheduled());
                        if (result.getBlockedReason().isPresent()) {
                            switch (result.getBlockedReason().get()) {
                                case WRITER_SCALING:
                                    // no-op
                                    break;
                                case WAITING_FOR_SOURCE:
                                    schedulerStats.getWaitingForSource().update(1);
                                    break;
                                case SPLIT_QUEUES_FULL:
                                    schedulerStats.getSplitQueuesFull().update(1);
                                    break;
                                case MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE:
                                case NO_ACTIVE_DRIVER_GROUP:
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unknown blocked reason: " + result.getBlockedReason().get());
                            }
                        }
                    }

                    // wait for a state change and then schedule again
                    if (!blockedStages.isEmpty()) {
                        try (TimeStat.BlockTimer timer = schedulerStats.getSleepTime().time()) {
                            tryGetFutureValue(whenAnyComplete(blockedStages), 1, SECONDS);
                        }
                        for (ListenableFuture<Void> blockedStage : blockedStages) {
                            blockedStage.cancel(true);
                        }
                    }
                }

                for (StreamingStageExecution stage : stageExecutions) {
                    StreamingStageExecution.State state = stage.getState();
                    if (state != SCHEDULED && state != RUNNING && state != FLUSHING && !state.isDone()) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Scheduling is complete, but stage %s is in state %s", stage.getStageId(), state));
                    }
                }
            }
            catch (Throwable t) {
                queryStateMachine.transitionToFailed(t);
                throw t;
            }
            finally {
                RuntimeException closeError = new RuntimeException();
                for (StageScheduler scheduler : stageSchedulers.values()) {
                    try {
                        scheduler.close();
                    }
                    catch (Throwable t) {
                        queryStateMachine.transitionToFailed(t);
                        // Self-suppression not permitted
                        if (closeError != t) {
                            closeError.addSuppressed(t);
                        }
                    }
                }
                if (closeError.getSuppressed().length > 0) {
                    throw closeError;
                }
            }
        }

        public void cancelStage(StageId stageId)
        {
            for (StreamingStageExecution execution : stageExecutions) {
                if (execution.getStageId().equals(stageId)) {
                    execution.cancel();
                    break;
                }
            }
        }

        public void abort()
        {
            stageExecutions.forEach(StreamingStageExecution::abort);
        }
    }
}
