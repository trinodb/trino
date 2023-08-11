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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.primitives.ImmutableLongArray;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Tracer;
import io.trino.Session;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.execution.BasicStageStats;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.RemoteTask;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.SqlStage;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.StageState;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.TableInfo;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.execution.buffer.SpoolingOutputBuffers;
import io.trino.execution.buffer.SpoolingOutputStats;
import io.trino.execution.resourcegroups.IndexedPriorityQueue;
import io.trino.execution.scheduler.NodeAllocator.NodeLease;
import io.trino.execution.scheduler.PartitionMemoryEstimator.MemoryRequirements;
import io.trino.execution.scheduler.SplitAssigner.AssignmentResult;
import io.trino.execution.scheduler.SplitAssigner.Partition;
import io.trino.execution.scheduler.SplitAssigner.PartitionUpdate;
import io.trino.failuredetector.FailureDetector;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Metadata;
import io.trino.metadata.Split;
import io.trino.operator.RetryPolicy;
import io.trino.server.DynamicFilterService;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;
import io.trino.split.RemoteSplit;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.PlanFragmentIdAllocator;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RefreshMaterializedViewNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import jakarta.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.getDone;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionDefaultCoordinatorTaskMemory;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionDefaultTaskMemory;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMaxPartitionCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMinSourceStageProgress;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionRuntimeAdaptivePartitioningMaxTaskSize;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionRuntimeAdaptivePartitioningPartitionCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionSmallStageEstimationThreshold;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionSmallStageSourceSizeMultiplier;
import static io.trino.SystemSessionProperties.getMaxTasksWaitingForExecutionPerQuery;
import static io.trino.SystemSessionProperties.getMaxTasksWaitingForNodePerStage;
import static io.trino.SystemSessionProperties.getRetryDelayScaleFactor;
import static io.trino.SystemSessionProperties.getRetryInitialDelay;
import static io.trino.SystemSessionProperties.getRetryMaxDelay;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.getTaskRetryAttemptsPerTask;
import static io.trino.SystemSessionProperties.isFaultTolerantExecutionRuntimeAdaptivePartitioningEnabled;
import static io.trino.SystemSessionProperties.isFaultTolerantExecutionSmallStageEstimationEnabled;
import static io.trino.SystemSessionProperties.isFaultTolerantExecutionSmallStageRequireNoMorePartitions;
import static io.trino.execution.BasicStageStats.aggregateBasicStageStats;
import static io.trino.execution.StageState.ABORTED;
import static io.trino.execution.StageState.PLANNED;
import static io.trino.execution.resourcegroups.IndexedPriorityQueue.PriorityOrdering.LOW_TO_HIGH;
import static io.trino.execution.scheduler.ErrorCodes.isOutOfMemoryError;
import static io.trino.execution.scheduler.Exchanges.getAllSourceHandles;
import static io.trino.execution.scheduler.SchedulingUtils.canStream;
import static io.trino.failuredetector.FailureDetector.State.GONE;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static io.trino.sql.planner.RuntimeAdaptivePartitioningRewriter.consumesHashPartitionedInput;
import static io.trino.sql.planner.RuntimeAdaptivePartitioningRewriter.getMaxPlanFragmentId;
import static io.trino.sql.planner.RuntimeAdaptivePartitioningRewriter.getMaxPlanId;
import static io.trino.sql.planner.RuntimeAdaptivePartitioningRewriter.overridePartitionCountRecursively;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.TopologicalOrderSubPlanVisitor.sortPlanInTopologicalOrder;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.util.Failures.toFailure;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class EventDrivenFaultTolerantQueryScheduler
        implements QueryScheduler
{
    private static final Logger log = Logger.get(EventDrivenFaultTolerantQueryScheduler.class);

    private final QueryStateMachine queryStateMachine;
    private final Metadata metadata;
    private final RemoteTaskFactory remoteTaskFactory;
    private final TaskDescriptorStorage taskDescriptorStorage;
    private final EventDrivenTaskSourceFactory taskSourceFactory;
    private final boolean summarizeTaskInfo;
    private final NodeTaskMap nodeTaskMap;
    private final ExecutorService queryExecutor;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Tracer tracer;
    private final SplitSchedulerStats schedulerStats;
    private final PartitionMemoryEstimatorFactory memoryEstimatorFactory;
    private final NodePartitioningManager nodePartitioningManager;
    private final ExchangeManager exchangeManager;
    private final NodeAllocatorService nodeAllocatorService;
    private final FailureDetector failureDetector;
    private final DynamicFilterService dynamicFilterService;
    private final TaskExecutionStats taskExecutionStats;
    private final SubPlan originalPlan;
    private final double minSourceStageProgress;
    private final boolean smallStageEstimationEnabled;
    private final DataSize smallStageEstimationThreshold;
    private final double smallStageSourceSizeMultiplier;
    private final DataSize smallSizePartitionSizeEstimate;
    private final boolean smallStageRequireNoMorePartitions;

    private final StageRegistry stageRegistry;

    @GuardedBy("this")
    private boolean started;
    @GuardedBy("this")
    private Scheduler scheduler;

    public EventDrivenFaultTolerantQueryScheduler(
            QueryStateMachine queryStateMachine,
            Metadata metadata,
            RemoteTaskFactory remoteTaskFactory,
            TaskDescriptorStorage taskDescriptorStorage,
            EventDrivenTaskSourceFactory taskSourceFactory,
            boolean summarizeTaskInfo,
            NodeTaskMap nodeTaskMap,
            ExecutorService queryExecutor,
            ScheduledExecutorService scheduledExecutorService,
            Tracer tracer,
            SplitSchedulerStats schedulerStats,
            PartitionMemoryEstimatorFactory memoryEstimatorFactory,
            NodePartitioningManager nodePartitioningManager,
            ExchangeManager exchangeManager,
            NodeAllocatorService nodeAllocatorService,
            FailureDetector failureDetector,
            DynamicFilterService dynamicFilterService,
            TaskExecutionStats taskExecutionStats,
            SubPlan originalPlan)
    {
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        RetryPolicy retryPolicy = getRetryPolicy(queryStateMachine.getSession());
        verify(retryPolicy == TASK, "unexpected retry policy: %s", retryPolicy);
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
        this.taskSourceFactory = requireNonNull(taskSourceFactory, "taskSourceFactory is null");
        this.summarizeTaskInfo = summarizeTaskInfo;
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
        this.scheduledExecutorService = requireNonNull(scheduledExecutorService, "scheduledExecutorService is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.memoryEstimatorFactory = requireNonNull(memoryEstimatorFactory, "memoryEstimatorFactory is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "partitioningSchemeFactory is null");
        this.exchangeManager = requireNonNull(exchangeManager, "exchangeManager is null");
        this.nodeAllocatorService = requireNonNull(nodeAllocatorService, "nodeAllocatorService is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        this.taskExecutionStats = requireNonNull(taskExecutionStats, "taskExecutionStats is null");
        this.originalPlan = requireNonNull(originalPlan, "originalPlan is null");
        this.minSourceStageProgress = getFaultTolerantExecutionMinSourceStageProgress(queryStateMachine.getSession());
        this.smallStageEstimationEnabled = isFaultTolerantExecutionSmallStageEstimationEnabled(queryStateMachine.getSession());
        this.smallStageEstimationThreshold = getFaultTolerantExecutionSmallStageEstimationThreshold(queryStateMachine.getSession());
        this.smallStageSourceSizeMultiplier = getFaultTolerantExecutionSmallStageSourceSizeMultiplier(queryStateMachine.getSession());
        this.smallSizePartitionSizeEstimate = getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin(queryStateMachine.getSession());
        this.smallStageRequireNoMorePartitions = isFaultTolerantExecutionSmallStageRequireNoMorePartitions(queryStateMachine.getSession());

        stageRegistry = new StageRegistry(queryStateMachine, originalPlan);
    }

    @Override
    public synchronized void start()
    {
        checkState(!started, "already started");
        started = true;

        if (queryStateMachine.isDone()) {
            return;
        }

        taskDescriptorStorage.initialize(queryStateMachine.getQueryId());
        queryStateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                taskDescriptorStorage.destroy(queryStateMachine.getQueryId());
            }
        });

        // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
        queryStateMachine.addStateChangeListener(state -> {
            if (!state.isDone()) {
                return;
            }
            Scheduler scheduler;
            synchronized (this) {
                scheduler = this.scheduler;
                this.scheduler = null;
            }
            if (scheduler != null) {
                scheduler.abort();
            }
            queryStateMachine.updateQueryInfo(Optional.ofNullable(stageRegistry.getStageInfo()));
        });

        Session session = queryStateMachine.getSession();
        int maxPartitionCount = getFaultTolerantExecutionMaxPartitionCount(session);
        FaultTolerantPartitioningSchemeFactory partitioningSchemeFactory = new FaultTolerantPartitioningSchemeFactory(
                nodePartitioningManager,
                session,
                maxPartitionCount);
        Closer closer = Closer.create();
        NodeAllocator nodeAllocator = closer.register(nodeAllocatorService.getNodeAllocator(session));
        try {
            scheduler = new Scheduler(
                    queryStateMachine,
                    metadata,
                    remoteTaskFactory,
                    taskDescriptorStorage,
                    taskSourceFactory,
                    summarizeTaskInfo,
                    nodeTaskMap,
                    queryExecutor,
                    scheduledExecutorService,
                    tracer,
                    schedulerStats,
                    memoryEstimatorFactory,
                    partitioningSchemeFactory,
                    exchangeManager,
                    getTaskRetryAttemptsPerTask(session) + 1,
                    getMaxTasksWaitingForNodePerStage(session),
                    getMaxTasksWaitingForExecutionPerQuery(session),
                    nodeAllocator,
                    failureDetector,
                    stageRegistry,
                    taskExecutionStats,
                    dynamicFilterService,
                    new SchedulingDelayer(
                            getRetryInitialDelay(session),
                            getRetryMaxDelay(session),
                            getRetryDelayScaleFactor(session),
                            Stopwatch.createUnstarted()),
                    originalPlan,
                    maxPartitionCount,
                    isFaultTolerantExecutionRuntimeAdaptivePartitioningEnabled(session),
                    getFaultTolerantExecutionRuntimeAdaptivePartitioningPartitionCount(session),
                    getFaultTolerantExecutionRuntimeAdaptivePartitioningMaxTaskSize(session),
                    minSourceStageProgress,
                    smallStageEstimationEnabled,
                    smallStageEstimationThreshold,
                    smallStageSourceSizeMultiplier,
                    smallSizePartitionSizeEstimate,
                    smallStageRequireNoMorePartitions);
            queryExecutor.submit(scheduler::run);
        }
        catch (Throwable t) {
            try {
                closer.close();
            }
            catch (Throwable closerFailure) {
                if (t != closerFailure) {
                    t.addSuppressed(closerFailure);
                }
            }
            throw t;
        }
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        throw new UnsupportedOperationException("partial cancel is not supported in fault tolerant mode");
    }

    @Override
    public void failTask(TaskId taskId, Throwable failureCause)
    {
        stageRegistry.failTaskRemotely(taskId, failureCause);
    }

    @Override
    public BasicStageStats getBasicStageStats()
    {
        return stageRegistry.getBasicStageStats();
    }

    @Override
    public StageInfo getStageInfo()
    {
        return stageRegistry.getStageInfo();
    }

    @Override
    public long getUserMemoryReservation()
    {
        return stageRegistry.getUserMemoryReservation();
    }

    @Override
    public long getTotalMemoryReservation()
    {
        return stageRegistry.getTotalMemoryReservation();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return stageRegistry.getTotalCpuTime();
    }

    @ThreadSafe
    private static class StageRegistry
    {
        private final QueryStateMachine queryStateMachine;
        private final AtomicReference<SubPlan> plan;
        private final Map<StageId, SqlStage> stages = new ConcurrentHashMap<>();

        public StageRegistry(QueryStateMachine queryStateMachine, SubPlan plan)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.plan = new AtomicReference<>(requireNonNull(plan, "plan is null"));
        }

        public void add(SqlStage stage)
        {
            verify(stages.putIfAbsent(stage.getStageId(), stage) == null, "stage %s is already present", stage.getStageId());
        }

        public void updatePlan(SubPlan plan)
        {
            this.plan.set(requireNonNull(plan, "plan is null"));
        }

        public StageInfo getStageInfo()
        {
            Map<PlanFragmentId, StageInfo> stageInfos = stages.values().stream()
                    .collect(toImmutableMap(stage -> stage.getFragment().getId(), SqlStage::getStageInfo));
            // make sure that plan is not staler than stageInfos since `getStageInfo` is called asynchronously
            SubPlan plan = requireNonNull(this.plan.get(), "plan is null");
            Set<PlanFragmentId> reportedFragments = new HashSet<>();
            StageInfo stageInfo = getStageInfo(plan, stageInfos, reportedFragments);
            // TODO Some stages may no longer be present in the plan when adaptive re-planning is implemented
            // TODO Figure out how to report statistics for such stages
            verify(reportedFragments.containsAll(stageInfos.keySet()), "some stages are left unreported");
            return stageInfo;
        }

        private StageInfo getStageInfo(SubPlan plan, Map<PlanFragmentId, StageInfo> infos, Set<PlanFragmentId> reportedFragments)
        {
            PlanFragmentId fragmentId = plan.getFragment().getId();
            reportedFragments.add(fragmentId);
            StageInfo info = infos.get(fragmentId);
            if (info == null) {
                info = StageInfo.createInitial(
                        queryStateMachine.getQueryId(),
                        queryStateMachine.getQueryState().isDone() ? ABORTED : PLANNED,
                        plan.getFragment());
            }
            List<StageInfo> sourceStages = plan.getChildren().stream()
                    .map(source -> getStageInfo(source, infos, reportedFragments))
                    .collect(toImmutableList());
            return info.withSubStages(sourceStages);
        }

        public BasicStageStats getBasicStageStats()
        {
            List<BasicStageStats> stageStats = stages.values().stream()
                    .map(SqlStage::getBasicStageStats)
                    .collect(toImmutableList());
            return aggregateBasicStageStats(stageStats);
        }

        public long getUserMemoryReservation()
        {
            return stages.values().stream()
                    .mapToLong(SqlStage::getUserMemoryReservation)
                    .sum();
        }

        public long getTotalMemoryReservation()
        {
            return stages.values().stream()
                    .mapToLong(SqlStage::getTotalMemoryReservation)
                    .sum();
        }

        public Duration getTotalCpuTime()
        {
            long millis = stages.values().stream()
                    .mapToLong(stage -> stage.getTotalCpuTime().toMillis())
                    .sum();
            return new Duration(millis, MILLISECONDS);
        }

        public void failTaskRemotely(TaskId taskId, Throwable failureCause)
        {
            SqlStage sqlStage = requireNonNull(stages.get(taskId.getStageId()), () -> "stage not found: %s" + taskId.getStageId());
            sqlStage.failTaskRemotely(taskId, failureCause);
        }
    }

    private static class Scheduler
            implements EventListener
    {
        private static final int EVENT_BUFFER_CAPACITY = 100;

        private final QueryStateMachine queryStateMachine;
        private final Metadata metadata;
        private final RemoteTaskFactory remoteTaskFactory;
        private final TaskDescriptorStorage taskDescriptorStorage;
        private final EventDrivenTaskSourceFactory taskSourceFactory;
        private final boolean summarizeTaskInfo;
        private final NodeTaskMap nodeTaskMap;
        private final ExecutorService queryExecutor;
        private final ScheduledExecutorService scheduledExecutorService;
        private final Tracer tracer;
        private final SplitSchedulerStats schedulerStats;
        private final PartitionMemoryEstimatorFactory memoryEstimatorFactory;
        private final FaultTolerantPartitioningSchemeFactory partitioningSchemeFactory;
        private final ExchangeManager exchangeManager;
        private final DataSize smallSizePartitionSizeEstimate;
        private final boolean smallStageRequireNoMorePartitions;
        private final int maxTaskExecutionAttempts;
        private final int maxTasksWaitingForNode;
        private final int maxTasksWaitingForExecution;
        private final NodeAllocator nodeAllocator;
        private final FailureDetector failureDetector;
        private final StageRegistry stageRegistry;
        private final TaskExecutionStats taskExecutionStats;
        private final DynamicFilterService dynamicFilterService;
        private final int maxPartitionCount;
        private final boolean runtimeAdaptivePartitioningEnabled;
        private final int runtimeAdaptivePartitioningPartitionCount;
        private final long runtimeAdaptivePartitioningMaxTaskSizeInBytes;
        private final double minSourceStageProgress;
        private final boolean smallStageEstimationEnabled;
        private final DataSize smallStageEstimationThreshold;
        private final double smallStageSourceSizeMultiplier;

        private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
        private final List<Event> eventBuffer = new ArrayList<>(EVENT_BUFFER_CAPACITY);

        private boolean started;
        private boolean runtimeAdaptivePartitioningApplied;

        private SubPlan plan;
        private List<SubPlan> planInTopologicalOrder;
        private final Map<StageId, StageExecution> stageExecutions = new HashMap<>();
        private final Map<SubPlan, IsReadyForExecutionResult> isReadyForExecutionCache = new HashMap<>();
        private final SetMultimap<StageId, StageId> stageConsumers = HashMultimap.create();

        private final SchedulingQueue schedulingQueue = new SchedulingQueue();
        private int nextSchedulingPriority;

        private final Map<ScheduledTask, PreSchedulingTaskContext> preSchedulingTaskContexts = new HashMap<>();

        private final SchedulingDelayer schedulingDelayer;

        private boolean queryOutputSet;

        public Scheduler(
                QueryStateMachine queryStateMachine,
                Metadata metadata,
                RemoteTaskFactory remoteTaskFactory,
                TaskDescriptorStorage taskDescriptorStorage,
                EventDrivenTaskSourceFactory taskSourceFactory,
                boolean summarizeTaskInfo,
                NodeTaskMap nodeTaskMap,
                ExecutorService queryExecutor,
                ScheduledExecutorService scheduledExecutorService,
                Tracer tracer,
                SplitSchedulerStats schedulerStats,
                PartitionMemoryEstimatorFactory memoryEstimatorFactory,
                FaultTolerantPartitioningSchemeFactory partitioningSchemeFactory,
                ExchangeManager exchangeManager,
                int maxTaskExecutionAttempts,
                int maxTasksWaitingForNode,
                int maxTasksWaitingForExecution,
                NodeAllocator nodeAllocator,
                FailureDetector failureDetector,
                StageRegistry stageRegistry,
                TaskExecutionStats taskExecutionStats,
                DynamicFilterService dynamicFilterService,
                SchedulingDelayer schedulingDelayer,
                SubPlan plan,
                int maxPartitionCount,
                boolean runtimeAdaptivePartitioningEnabled,
                int runtimeAdaptivePartitioningPartitionCount,
                DataSize runtimeAdaptivePartitioningMaxTaskSize,
                double minSourceStageProgress,
                boolean smallStageEstimationEnabled,
                DataSize smallStageEstimationThreshold,
                double smallStageSourceSizeMultiplier,
                DataSize smallSizePartitionSizeEstimate,
                boolean smallStageRequireNoMorePartitions)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
            this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
            this.taskSourceFactory = requireNonNull(taskSourceFactory, "taskSourceFactory is null");
            this.summarizeTaskInfo = summarizeTaskInfo;
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.scheduledExecutorService = requireNonNull(scheduledExecutorService, "scheduledExecutorService is null");
            this.tracer = requireNonNull(tracer, "tracer is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.memoryEstimatorFactory = requireNonNull(memoryEstimatorFactory, "memoryEstimatorFactory is null");
            this.partitioningSchemeFactory = requireNonNull(partitioningSchemeFactory, "partitioningSchemeFactory is null");
            this.exchangeManager = requireNonNull(exchangeManager, "exchangeManager is null");
            checkArgument(maxTaskExecutionAttempts > 0, "maxTaskExecutionAttempts must be greater than zero: %s", maxTaskExecutionAttempts);
            this.maxTaskExecutionAttempts = maxTaskExecutionAttempts;
            this.maxTasksWaitingForNode = maxTasksWaitingForNode;
            this.maxTasksWaitingForExecution = maxTasksWaitingForExecution;
            this.nodeAllocator = requireNonNull(nodeAllocator, "nodeAllocator is null");
            this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
            this.stageRegistry = requireNonNull(stageRegistry, "stageRegistry is null");
            this.taskExecutionStats = requireNonNull(taskExecutionStats, "taskExecutionStats is null");
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
            this.schedulingDelayer = requireNonNull(schedulingDelayer, "schedulingDelayer is null");
            this.plan = requireNonNull(plan, "plan is null");
            this.maxPartitionCount = maxPartitionCount;
            this.runtimeAdaptivePartitioningEnabled = runtimeAdaptivePartitioningEnabled;
            this.runtimeAdaptivePartitioningPartitionCount = runtimeAdaptivePartitioningPartitionCount;
            this.runtimeAdaptivePartitioningMaxTaskSizeInBytes = requireNonNull(runtimeAdaptivePartitioningMaxTaskSize, "runtimeAdaptivePartitioningMaxTaskSize is null").toBytes();
            this.minSourceStageProgress = minSourceStageProgress;
            this.smallStageEstimationEnabled = smallStageEstimationEnabled;
            this.smallStageEstimationThreshold = requireNonNull(smallStageEstimationThreshold, "smallStageEstimationThreshold is null");
            this.smallStageSourceSizeMultiplier = smallStageSourceSizeMultiplier;
            this.smallSizePartitionSizeEstimate = requireNonNull(smallSizePartitionSizeEstimate, "smallSizePartitionSizeEstimate is null");
            this.smallStageRequireNoMorePartitions = smallStageRequireNoMorePartitions;

            planInTopologicalOrder = sortPlanInTopologicalOrder(plan);
        }

        public void run()
        {
            checkState(!started, "already started");
            started = true;

            queryStateMachine.addStateChangeListener(state -> {
                if (state.isDone()) {
                    eventQueue.add(Event.WAKE_UP);
                }
            });

            Optional<Throwable> failure = Optional.empty();
            try {
                if (schedule()) {
                    while (processEvents()) {
                        if (schedulingDelayer.getRemainingDelayInMillis() > 0) {
                            continue;
                        }
                        if (!schedule()) {
                            break;
                        }
                    }
                }
            }
            catch (Throwable t) {
                failure = Optional.of(t);
            }

            for (StageExecution execution : stageExecutions.values()) {
                failure = closeAndAddSuppressed(failure, execution::abort);
            }
            for (PreSchedulingTaskContext context : preSchedulingTaskContexts.values()) {
                failure = closeAndAddSuppressed(failure, context.getNodeLease()::release);
            }
            preSchedulingTaskContexts.clear();
            failure = closeAndAddSuppressed(failure, nodeAllocator);

            failure.ifPresent(queryStateMachine::transitionToFailed);
        }

        private Optional<Throwable> closeAndAddSuppressed(Optional<Throwable> existingFailure, Closeable closeable)
        {
            try {
                closeable.close();
            }
            catch (Throwable t) {
                if (existingFailure.isEmpty()) {
                    return Optional.of(t);
                }
                if (existingFailure.get() != t) {
                    existingFailure.get().addSuppressed(t);
                }
            }
            return existingFailure;
        }

        private boolean processEvents()
        {
            try {
                Event event = eventQueue.poll(1, MINUTES);
                if (event == null) {
                    return true;
                }
                eventBuffer.add(event);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            while (true) {
                // poll multiple events from the queue in one shot to improve efficiency
                eventQueue.drainTo(eventBuffer, EVENT_BUFFER_CAPACITY - eventBuffer.size());
                if (eventBuffer.isEmpty()) {
                    return true;
                }
                for (Event e : eventBuffer) {
                    if (e == Event.ABORT) {
                        return false;
                    }
                    if (e == Event.WAKE_UP) {
                        continue;
                    }
                    e.accept(this);
                }
                eventBuffer.clear();
            }
        }

        private boolean schedule()
        {
            if (checkComplete()) {
                return false;
            }
            optimize();
            updateStageExecutions();
            scheduleTasks();
            processNodeAcquisitions();
            loadMoreTaskDescriptorsIfNecessary();
            return true;
        }

        private boolean checkComplete()
        {
            if (queryStateMachine.isDone()) {
                return true;
            }

            for (StageExecution execution : stageExecutions.values()) {
                if (execution.getState() == StageState.FAILED) {
                    StageInfo stageInfo = execution.getStageInfo();
                    ExecutionFailureInfo failureCause = stageInfo.getFailureCause();
                    RuntimeException failure = failureCause == null ?
                            new TrinoException(GENERIC_INTERNAL_ERROR, "stage failed due to unknown error: %s".formatted(execution.getStageId())) :
                            failureCause.toException();
                    queryStateMachine.transitionToFailed(failure);
                    return true;
                }
            }
            setQueryOutputIfReady();
            return false;
        }

        private void setQueryOutputIfReady()
        {
            StageId rootStageId = getStageId(plan.getFragment().getId());
            StageExecution rootStageExecution = stageExecutions.get(rootStageId);
            if (!queryOutputSet && rootStageExecution != null && rootStageExecution.getState() == StageState.FINISHED) {
                ListenableFuture<List<ExchangeSourceHandle>> sourceHandles = getAllSourceHandles(rootStageExecution.getExchange().getSourceHandles());
                Futures.addCallback(sourceHandles, new FutureCallback<>()
                {
                    @Override
                    public void onSuccess(List<ExchangeSourceHandle> handles)
                    {
                        try {
                            queryStateMachine.updateInputsForQueryResults(
                                    ImmutableList.of(new SpoolingExchangeInput(handles, Optional.of(rootStageExecution.getSinkOutputSelector()))),
                                    true);
                            queryStateMachine.transitionToFinishing();
                        }
                        catch (Throwable t) {
                            onFailure(t);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        queryStateMachine.transitionToFailed(t);
                    }
                }, queryExecutor);
                queryOutputSet = true;
            }
        }

        private void optimize()
        {
            SubPlan oldPlan = plan;
            plan = optimizePlan(plan);
            if (plan != oldPlan) {
                planInTopologicalOrder = sortPlanInTopologicalOrder(plan);
                stageRegistry.updatePlan(plan);
            }
        }

        private SubPlan optimizePlan(SubPlan plan)
        {
            // Re-optimize plan here based on available runtime statistics.
            // Fragments changed due to re-optimization as well as their downstream stages are expected to be assigned new fragment ids.
            plan = updateStagesPartitioning(plan);
            return plan;
        }

        private SubPlan updateStagesPartitioning(SubPlan plan)
        {
            if (!runtimeAdaptivePartitioningEnabled || runtimeAdaptivePartitioningApplied) {
                return plan;
            }

            for (SubPlan subPlan : planInTopologicalOrder) {
                PlanFragment fragment = subPlan.getFragment();
                if (!consumesHashPartitionedInput(fragment)) {
                    // no input hash partitioning present
                    continue;
                }

                StageId stageId = getStageId(fragment.getId());
                if (stageExecutions.containsKey(stageId)) {
                    // already started
                    continue;
                }

                IsReadyForExecutionResult isReadyForExecutionResult = isReadyForExecution(subPlan);
                // Caching is not only needed to avoid duplicate calls, but also to avoid the case that a stage that
                // is not ready now but becomes ready when updateStageExecutions.
                // We want to avoid starting an execution without considering changing the number of partitions.
                // TODO: think about how to eliminate the cache
                isReadyForExecutionCache.put(subPlan, isReadyForExecutionResult);
                if (!isReadyForExecutionResult.isReadyForExecution()) {
                    // not ready for execution
                    continue;
                }

                // calculate (estimated) input data size to determine if we want to change number of partitions at runtime
                List<Long> partitionedInputBytes = fragment.getRemoteSourceNodes().stream()
                        .filter(remoteSourceNode -> remoteSourceNode.getExchangeType() != REPLICATE)
                        .map(remoteSourceNode -> remoteSourceNode.getSourceFragmentIds().stream()
                                .mapToLong(sourceFragmentId -> {
                                    StageId sourceStageId = getStageId(sourceFragmentId);
                                    OutputDataSizeEstimate outputDataSizeEstimate = isReadyForExecutionResult.getSourceOutputSizeEstimates().get(sourceStageId);
                                    verify(outputDataSizeEstimate != null, "outputDataSizeEstimate not found for source stage %s", sourceStageId);
                                    return outputDataSizeEstimate.getTotalSizeInBytes();
                                })
                                .sum())
                        .collect(toImmutableList());
                // Currently the memory estimation is simplified:
                // if it's an aggregation, then we use the total input bytes as the memory consumption
                // if it involves multiple joins, conservatively we assume the smallest remote source will be streamed through
                // and use the sum of input bytes of other remote sources as the memory consumption
                // TODO: more accurate memory estimation based on context (https://github.com/trinodb/trino/issues/18698)
                long estimatedMemoryConsumptionInBytes = (partitionedInputBytes.size() == 1) ? partitionedInputBytes.get(0) :
                        partitionedInputBytes.stream().mapToLong(Long::longValue).sum() - Collections.min(partitionedInputBytes);

                int partitionCount = fragment.getPartitionCount().orElse(maxPartitionCount);
                if (estimatedMemoryConsumptionInBytes > runtimeAdaptivePartitioningMaxTaskSizeInBytes * partitionCount) {
                    log.info("Stage %s has an estimated memory consumption of %s, changing partition count from %s to %s",
                            stageId, succinctBytes(estimatedMemoryConsumptionInBytes), partitionCount, runtimeAdaptivePartitioningPartitionCount);
                    runtimeAdaptivePartitioningApplied = true;
                    PlanFragmentIdAllocator planFragmentIdAllocator = new PlanFragmentIdAllocator(getMaxPlanFragmentId(planInTopologicalOrder) + 1);
                    PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator(getMaxPlanId(planInTopologicalOrder) + 1);
                    return overridePartitionCountRecursively(
                            plan,
                            partitionCount,
                            runtimeAdaptivePartitioningPartitionCount,
                            planFragmentIdAllocator,
                            planNodeIdAllocator,
                            planInTopologicalOrder.stream()
                                    .map(SubPlan::getFragment)
                                    .map(PlanFragment::getId)
                                    .filter(planFragmentId -> stageExecutions.containsKey(getStageId(planFragmentId)))
                                    .collect(toImmutableSet()));
                }
            }

            return plan;
        }

        private void updateStageExecutions()
        {
            Set<StageId> currentPlanStages = new HashSet<>();
            PlanFragmentId rootFragmentId = plan.getFragment().getId();
            for (SubPlan subPlan : planInTopologicalOrder) {
                PlanFragmentId fragmentId = subPlan.getFragment().getId();
                StageId stageId = getStageId(fragmentId);
                currentPlanStages.add(stageId);
                StageExecution stageExecution = stageExecutions.get(stageId);
                if (stageExecution == null) {
                    IsReadyForExecutionResult result = isReadyForExecutionCache.computeIfAbsent(subPlan, ignored -> isReadyForExecution(subPlan));
                    if (result.isReadyForExecution()) {
                        createStageExecution(subPlan, fragmentId.equals(rootFragmentId), result.getSourceOutputSizeEstimates(), nextSchedulingPriority++);
                    }
                }
                if (stageExecution != null && stageExecution.getState().equals(StageState.FINISHED) && !stageExecution.isExchangeClosed()) {
                    // we are ready to close its source exchanges
                    closeSourceExchanges(subPlan);
                }
            }
            stageExecutions.forEach((stageId, stageExecution) -> {
                if (!currentPlanStages.contains(stageId)) {
                    // stage got re-written during re-optimization
                    stageExecution.abort();
                }
            });
            isReadyForExecutionCache.clear();
        }

        private static class IsReadyForExecutionResult
        {
            private final boolean readyForExecution;
            private final Optional<Map<StageId, OutputDataSizeEstimate>> sourceOutputSizeEstimates;

            @CheckReturnValue
            public static IsReadyForExecutionResult ready(Map<StageId, OutputDataSizeEstimate> sourceOutputSizeEstimates)
            {
                return new IsReadyForExecutionResult(true, Optional.of(sourceOutputSizeEstimates));
            }

            @CheckReturnValue
            public static IsReadyForExecutionResult notReady()
            {
                return new IsReadyForExecutionResult(false, Optional.empty());
            }

            private IsReadyForExecutionResult(boolean readyForExecution, Optional<Map<StageId, OutputDataSizeEstimate>> sourceOutputSizeEstimates)
            {
                requireNonNull(sourceOutputSizeEstimates, "sourceOutputSizeEstimates is null");
                if (readyForExecution) {
                    checkArgument(sourceOutputSizeEstimates.isPresent(), "expected sourceOutputSizeEstimates to be set");
                }
                if (!readyForExecution) {
                    checkArgument(sourceOutputSizeEstimates.isEmpty(), "expected sourceOutputSizeEstimates to be not set");
                }
                this.readyForExecution = readyForExecution;
                this.sourceOutputSizeEstimates = sourceOutputSizeEstimates.map(ImmutableMap::copyOf);
            }

            public boolean isReadyForExecution()
            {
                return readyForExecution;
            }

            public Map<StageId, OutputDataSizeEstimate> getSourceOutputSizeEstimates()
            {
                return sourceOutputSizeEstimates.orElseThrow();
            }
        }

        private IsReadyForExecutionResult isReadyForExecution(SubPlan subPlan)
        {
            boolean nonSpeculativeTasksInQueue = schedulingQueue.getNonSpeculativeTaskCount() > 0;
            boolean nonSpeculativeTasksWaitingForNode = preSchedulingTaskContexts.values().stream()
                    .anyMatch(task -> !task.isSpeculative() && !task.getNodeLease().getNode().isDone());

            // Do not start a speculative stage if there is non-speculative work still to be done.
            // Do not start a speculative stage after partition count has been changed at runtime, as when we estimate
            // by progress, repartition tasks will produce very uneven output for different output partitions, which
            // will result in very bad task bin-packing results; also the fact that runtime adaptive partitioning
            // happened already suggests that there is plenty work ahead.
            boolean canScheduleSpeculative = !nonSpeculativeTasksInQueue && !nonSpeculativeTasksWaitingForNode && !runtimeAdaptivePartitioningApplied;
            boolean speculative = false;
            int finishedSourcesCount = 0;
            int estimatedByProgressSourcesCount = 0;
            int estimatedBySmallInputSourcesCount = 0;

            ImmutableMap.Builder<StageId, OutputDataSizeEstimate> sourceOutputSizeEstimates = ImmutableMap.builder();

            boolean someSourcesMadeProgress = false;

            for (SubPlan source : subPlan.getChildren()) {
                StageExecution sourceStageExecution = stageExecutions.get(getStageId(source.getFragment().getId()));
                if (sourceStageExecution == null) {
                    // source stage did not yet start
                    return IsReadyForExecutionResult.notReady();
                }

                if (sourceStageExecution.getState() != StageState.FINISHED) {
                    if (!exchangeManager.supportsConcurrentReadAndWrite()) {
                        // speculative execution not supported by Exchange implementation
                        return IsReadyForExecutionResult.notReady();
                    }
                    if (!canScheduleSpeculative) {
                        return IsReadyForExecutionResult.notReady();
                    }
                    speculative = true;
                }
                else {
                    // source stage finished; no more checks needed
                    OutputDataSizeEstimateResult result = sourceStageExecution.getOutputDataSize(stageExecutions::get).orElseThrow();
                    verify(result.getStatus() == OutputDataSizeEstimateStatus.FINISHED, "expected FINISHED status but got %s", result.getStatus());
                    finishedSourcesCount++;
                    sourceOutputSizeEstimates.put(sourceStageExecution.getStageId(), result.getOutputDataSizeEstimate());
                    someSourcesMadeProgress = true;
                    continue;
                }

                if (!canOutputDataEarly(source)) {
                    // no point in starting stage if source stage needs to complete before we can get any input data to make progress
                    return IsReadyForExecutionResult.notReady();
                }

                if (!canStream(subPlan, source)) {
                    // only allow speculative execution of stage if all source stages for which we cannot stream data are finished
                    return IsReadyForExecutionResult.notReady();
                }

                Optional<OutputDataSizeEstimateResult> result = sourceStageExecution.getOutputDataSize(stageExecutions::get);
                if (result.isEmpty()) {
                    return IsReadyForExecutionResult.notReady();
                }

                switch (result.orElseThrow().getStatus()) {
                    case ESTIMATED_BY_PROGRESS -> {
                        estimatedByProgressSourcesCount++;
                    }
                    case ESTIMATED_BY_SMALL_INPUT -> {
                        estimatedBySmallInputSourcesCount++;
                    }
                    default -> {
                        // FINISHED handled above
                        throw new IllegalStateException(format("unexpected status %s", result.orElseThrow().getStatus()));
                    }
                }

                sourceOutputSizeEstimates.put(sourceStageExecution.getStageId(), result.orElseThrow().getOutputDataSizeEstimate());
                someSourcesMadeProgress = someSourcesMadeProgress || sourceStageExecution.isSomeProgressMade();
            }

            if (!subPlan.getChildren().isEmpty() && !someSourcesMadeProgress) {
                return IsReadyForExecutionResult.notReady();
            }

            if (speculative) {
                log.debug("scheduling speculative %s/%s; sources: finished=%s; estimatedByProgress=%s; estimatedSmall=%s",
                        queryStateMachine.getQueryId(),
                        subPlan.getFragment().getId(),
                        finishedSourcesCount,
                        estimatedByProgressSourcesCount,
                        estimatedBySmallInputSourcesCount);
            }
            return IsReadyForExecutionResult.ready(sourceOutputSizeEstimates.buildOrThrow());
        }

        /**
         * Verify if source plan is expected to output data as its tasks are progressing.
         * E.g. tasks building final aggregation would not output any data until task completes; all data
         * for partition task is responsible for must be processed.
         * <br/>
         * Note that logic here is conservative. It is still possible that stage produces output data before it is
         * finished because some tasks finish sooner than the other.
         */
        private boolean canOutputDataEarly(SubPlan source)
        {
            PlanFragment fragment = source.getFragment();
            return canOutputDataEarly(fragment.getRoot());
        }

        private boolean canOutputDataEarly(PlanNode node)
        {
            if (node instanceof AggregationNode aggregationNode) {
                return aggregationNode.getStep().isOutputPartial();
            }
            // todo filter out more (window?)
            return node.getSources().stream().allMatch(this::canOutputDataEarly);
        }

        private void closeSourceExchanges(SubPlan subPlan)
        {
            for (SubPlan source : subPlan.getChildren()) {
                StageExecution sourceStageExecution = stageExecutions.get(getStageId(source.getFragment().getId()));
                if (sourceStageExecution != null) {
                    sourceStageExecution.closeExchange();
                }
            }
        }

        private void createStageExecution(SubPlan subPlan, boolean rootFragment, Map<StageId, OutputDataSizeEstimate> sourceOutputSizeEstimates, int schedulingPriority)
        {
            Closer closer = Closer.create();

            try {
                PlanFragment fragment = subPlan.getFragment();
                Session session = queryStateMachine.getSession();

                StageId stageId = getStageId(fragment.getId());
                SqlStage stage = SqlStage.createSqlStage(
                        stageId,
                        fragment,
                        TableInfo.extract(session, metadata, fragment),
                        remoteTaskFactory,
                        session,
                        summarizeTaskInfo,
                        nodeTaskMap,
                        queryStateMachine.getStateMachineExecutor(),
                        tracer,
                        schedulerStats);
                closer.register(stage::abort);
                stageRegistry.add(stage);
                stage.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.ofNullable(stageRegistry.getStageInfo())));

                ImmutableMap.Builder<PlanFragmentId, Exchange> sourceExchanges = ImmutableMap.builder();
                Map<PlanFragmentId, OutputDataSizeEstimate> sourceOutputEstimatesByFragmentId = new HashMap<>();
                for (SubPlan source : subPlan.getChildren()) {
                    PlanFragmentId sourceFragmentId = source.getFragment().getId();
                    StageId sourceStageId = getStageId(sourceFragmentId);
                    StageExecution sourceStageExecution = getStageExecution(sourceStageId);
                    sourceExchanges.put(sourceFragmentId, sourceStageExecution.getExchange());
                    OutputDataSizeEstimate outputDataSizeResult = sourceOutputSizeEstimates.get(sourceStageId);
                    verify(outputDataSizeResult != null, "No output data size estimate in %s map for stage %s", sourceOutputSizeEstimates, sourceStageId);
                    sourceOutputEstimatesByFragmentId.put(sourceFragmentId, outputDataSizeResult);
                    stageConsumers.put(sourceStageExecution.getStageId(), stageId);
                }

                ImmutableMap.Builder<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates = ImmutableMap.builder();
                for (RemoteSourceNode remoteSource : stage.getFragment().getRemoteSourceNodes()) {
                    List<OutputDataSizeEstimate> estimates = new ArrayList<>();
                    for (PlanFragmentId fragmentId : remoteSource.getSourceFragmentIds()) {
                        OutputDataSizeEstimate fragmentEstimate = sourceOutputEstimatesByFragmentId.get(fragmentId);
                        verify(fragmentEstimate != null, "fragmentEstimate not found for fragment %s", fragmentId);
                        estimates.add(fragmentEstimate);
                    }
                    // merge estimates for all source fragments of a single remote source
                    outputDataSizeEstimates.put(remoteSource.getId(), OutputDataSizeEstimate.merge(estimates));
                }

                EventDrivenTaskSource taskSource = closer.register(taskSourceFactory.create(
                        session,
                        stage.getStageSpan(),
                        fragment,
                        sourceExchanges.buildOrThrow(),
                        partitioningSchemeFactory.get(fragment.getPartitioning(), fragment.getPartitionCount()),
                        stage::recordGetSplitTime,
                        outputDataSizeEstimates.buildOrThrow()));

                FaultTolerantPartitioningScheme sinkPartitioningScheme = partitioningSchemeFactory.get(
                        fragment.getOutputPartitioningScheme().getPartitioning().getHandle(),
                        fragment.getOutputPartitioningScheme().getPartitionCount());
                ExchangeContext exchangeContext = new ExchangeContext(queryStateMachine.getQueryId(), new ExchangeId("external-exchange-" + stage.getStageId().getId()));

                boolean preserveOrderWithinPartition = rootFragment && stage.getFragment().getPartitioning().equals(SINGLE_DISTRIBUTION);
                Exchange exchange = closer.register(exchangeManager.createExchange(
                        exchangeContext,
                        sinkPartitioningScheme.getPartitionCount(),
                        preserveOrderWithinPartition));

                boolean coordinatorStage = stage.getFragment().getPartitioning().equals(COORDINATOR_DISTRIBUTION);

                StageExecution execution = new StageExecution(
                        queryStateMachine,
                        taskDescriptorStorage,
                        stage,
                        taskSource,
                        sinkPartitioningScheme,
                        exchange,
                        memoryEstimatorFactory.createPartitionMemoryEstimator(),
                        // do not retry coordinator only tasks
                        coordinatorStage ? 1 : maxTaskExecutionAttempts,
                        schedulingPriority,
                        dynamicFilterService,
                        minSourceStageProgress,
                        smallStageEstimationEnabled,
                        smallStageEstimationThreshold,
                        smallStageSourceSizeMultiplier,
                        smallSizePartitionSizeEstimate,
                        smallStageRequireNoMorePartitions);

                stageExecutions.put(execution.getStageId(), execution);

                for (SubPlan source : subPlan.getChildren()) {
                    PlanFragmentId sourceFragmentId = source.getFragment().getId();
                    StageExecution sourceExecution = getStageExecution(getStageId(sourceFragmentId));
                    execution.setSourceOutputSelector(sourceFragmentId, sourceExecution.getSinkOutputSelector());
                }
            }
            catch (Throwable t) {
                try {
                    closer.close();
                }
                catch (Throwable closerFailure) {
                    if (closerFailure != t) {
                        t.addSuppressed(closerFailure);
                    }
                }
                throw t;
            }
        }

        private StageId getStageId(PlanFragmentId fragmentId)
        {
            return StageId.create(queryStateMachine.getQueryId(), fragmentId);
        }

        private void scheduleTasks()
        {
            long speculativeTasksWaitingForNode = preSchedulingTaskContexts.values().stream()
                    .filter(context -> !context.getNodeLease().getNode().isDone())
                    .filter(PreSchedulingTaskContext::isSpeculative)
                    .count();

            long nonSpeculativeTasksWaitingForNode = preSchedulingTaskContexts.values().stream()
                    .filter(context -> !context.getNodeLease().getNode().isDone())
                    .filter(preSchedulingTaskContext -> !preSchedulingTaskContext.isSpeculative())
                    .count();

            while (!schedulingQueue.isEmpty()) {
                if (nonSpeculativeTasksWaitingForNode >= maxTasksWaitingForNode) {
                    break;
                }

                PrioritizedScheduledTask scheduledTask = schedulingQueue.peekOrThrow();

                if (scheduledTask.isSpeculative() && nonSpeculativeTasksWaitingForNode > 0) {
                    // do not handle any speculative tasks if there are non-speculative waiting
                    break;
                }

                if (scheduledTask.isSpeculative() && speculativeTasksWaitingForNode >= maxTasksWaitingForNode) {
                    // too many speculative tasks waiting for node
                    break;
                }

                verify(schedulingQueue.pollOrThrow().equals(scheduledTask));

                StageExecution stageExecution = getStageExecution(scheduledTask.task().stageId());
                if (stageExecution.getState().isDone()) {
                    continue;
                }
                int partitionId = scheduledTask.task().partitionId();
                Optional<NodeRequirements> nodeRequirements = stageExecution.getNodeRequirements(partitionId);
                if (nodeRequirements.isEmpty()) {
                    // execution finished
                    continue;
                }
                MemoryRequirements memoryRequirements = stageExecution.getMemoryRequirements(partitionId);
                NodeLease lease = nodeAllocator.acquire(nodeRequirements.get(), memoryRequirements.getRequiredMemory(), scheduledTask.isSpeculative());
                lease.getNode().addListener(() -> eventQueue.add(Event.WAKE_UP), queryExecutor);
                preSchedulingTaskContexts.put(scheduledTask.task(), new PreSchedulingTaskContext(lease, scheduledTask.isSpeculative()));

                if (scheduledTask.isSpeculative()) {
                    speculativeTasksWaitingForNode++;
                }
                else {
                    nonSpeculativeTasksWaitingForNode++;
                }
            }
        }

        private void processNodeAcquisitions()
        {
            Iterator<Map.Entry<ScheduledTask, PreSchedulingTaskContext>> iterator = preSchedulingTaskContexts.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ScheduledTask, PreSchedulingTaskContext> entry = iterator.next();
                ScheduledTask scheduledTask = entry.getKey();
                PreSchedulingTaskContext context = entry.getValue();
                if (context.isWaitingForSinkInstanceHandle()) {
                    verify(context.getNodeLease().getNode().isDone(), "isWaitingForSinkInstanceHandle true but node not set");
                    continue; // this entry is already in the isWaitingForSinkInstanceHandle phase
                }

                NodeLease nodeLease = context.getNodeLease();
                StageExecution stageExecution = getStageExecution(scheduledTask.stageId());
                if (stageExecution.getState().isDone()) {
                    iterator.remove();
                    nodeLease.release();
                }
                else if (nodeLease.getNode().isDone()) {
                    context.setWaitingForSinkInstanceHandle(true);
                    Optional<GetExchangeSinkInstanceHandleResult> getExchangeSinkInstanceHandleResult = stageExecution.getExchangeSinkInstanceHandle(scheduledTask.partitionId());
                    if (getExchangeSinkInstanceHandleResult.isPresent()) {
                        CompletableFuture<ExchangeSinkInstanceHandle> sinkInstanceHandleFuture = getExchangeSinkInstanceHandleResult.get().exchangeSinkInstanceHandleFuture();
                        sinkInstanceHandleFuture.whenComplete((sinkInstanceHandle, throwable) -> {
                            if (throwable != null) {
                                eventQueue.add(new StageFailureEvent(scheduledTask.stageId, throwable));
                            }
                            else {
                                eventQueue.add(new SinkInstanceHandleAcquiredEvent(
                                        scheduledTask.stageId(),
                                        scheduledTask.partitionId(),
                                        nodeLease,
                                        getExchangeSinkInstanceHandleResult.get().attempt(),
                                        sinkInstanceHandle));
                            }
                        });
                    }
                    else {
                        iterator.remove();
                        nodeLease.release();
                    }
                }
            }
        }

        @Override
        public void onSinkInstanceHandleAcquired(SinkInstanceHandleAcquiredEvent sinkInstanceHandleAcquiredEvent)
        {
            ScheduledTask scheduledTask = new ScheduledTask(sinkInstanceHandleAcquiredEvent.getStageId(), sinkInstanceHandleAcquiredEvent.getPartitionId());
            PreSchedulingTaskContext context = preSchedulingTaskContexts.remove(scheduledTask);
            verify(context != null, "expected %s in preSchedulingTaskContexts", scheduledTask);
            verify(context.getNodeLease().getNode().isDone(), "expected node set for %s", scheduledTask);
            verify(context.isWaitingForSinkInstanceHandle(), "expected isWaitingForSinkInstanceHandle set for %s", scheduledTask);
            NodeLease nodeLease = sinkInstanceHandleAcquiredEvent.getNodeLease();
            int partitionId = sinkInstanceHandleAcquiredEvent.getPartitionId();
            StageId stageId = sinkInstanceHandleAcquiredEvent.getStageId();
            int attempt = sinkInstanceHandleAcquiredEvent.getAttempt();
            ExchangeSinkInstanceHandle sinkInstanceHandle = sinkInstanceHandleAcquiredEvent.getSinkInstanceHandle();
            StageExecution stageExecution = getStageExecution(stageId);

            try {
                InternalNode node = getDone(nodeLease.getNode());
                Optional<RemoteTask> remoteTask = stageExecution.schedule(partitionId, sinkInstanceHandle, attempt, node, context.isSpeculative());
                remoteTask.ifPresent(task -> {
                    task.addStateChangeListener(createExchangeSinkInstanceHandleUpdateRequiredListener());
                    task.addStateChangeListener(taskStatus -> {
                        if (taskStatus.getState().isDone()) {
                            nodeLease.release();
                        }
                    });
                    task.addFinalTaskInfoListener(taskExecutionStats::update);
                    task.addFinalTaskInfoListener(taskInfo -> eventQueue.add(new RemoteTaskCompletedEvent(taskInfo.getTaskStatus())));
                    nodeLease.attachTaskId(task.getTaskId());
                    task.start();
                    if (queryStateMachine.getQueryState() == QueryState.STARTING) {
                        queryStateMachine.transitionToRunning();
                    }
                });
                if (remoteTask.isEmpty()) {
                    nodeLease.release();
                }
            }
            catch (ExecutionException e) {
                throw new UncheckedExecutionException(e);
            }
        }

        private StateChangeListener<TaskStatus> createExchangeSinkInstanceHandleUpdateRequiredListener()
        {
            AtomicLong respondedToVersion = new AtomicLong(-1);
            return taskStatus -> {
                OutputBufferStatus outputBufferStatus = taskStatus.getOutputBufferStatus();
                if (outputBufferStatus.getOutputBuffersVersion().isEmpty()) {
                    return;
                }
                if (!outputBufferStatus.isExchangeSinkInstanceHandleUpdateRequired()) {
                    return;
                }
                long remoteVersion = outputBufferStatus.getOutputBuffersVersion().getAsLong();
                while (true) {
                    long localVersion = respondedToVersion.get();
                    if (remoteVersion <= localVersion) {
                        // version update is scheduled or sent already but got not propagated yet
                        break;
                    }
                    if (respondedToVersion.compareAndSet(localVersion, remoteVersion)) {
                        eventQueue.add(new RemoteTaskExchangeSinkUpdateRequiredEvent(taskStatus));
                        break;
                    }
                }
            };
        }

        private void loadMoreTaskDescriptorsIfNecessary()
        {
            boolean schedulingQueueIsFull = schedulingQueue.getNonSpeculativeTaskCount() >= maxTasksWaitingForExecution;
            for (StageExecution stageExecution : stageExecutions.values()) {
                if (!schedulingQueueIsFull || stageExecution.hasOpenTaskRunning()) {
                    stageExecution.loadMoreTaskDescriptors().ifPresent(future -> Futures.addCallback(future, new FutureCallback<>()
                    {
                        @Override
                        public void onSuccess(AssignmentResult result)
                        {
                            eventQueue.add(new SplitAssignmentEvent(stageExecution.getStageId(), result));
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            eventQueue.add(new StageFailureEvent(stageExecution.getStageId(), t));
                        }
                    }, queryExecutor));
                }
            }
        }

        public void abort()
        {
            eventQueue.clear();
            eventQueue.add(Event.ABORT);
        }

        @Override
        public void onRemoteTaskCompleted(RemoteTaskCompletedEvent event)
        {
            TaskStatus taskStatus = event.getTaskStatus();
            TaskId taskId = taskStatus.getTaskId();
            TaskState taskState = taskStatus.getState();
            StageExecution stageExecution = getStageExecution(taskId.getStageId());
            if (taskState == TaskState.FINISHED) {
                stageExecution.taskFinished(taskId, taskStatus);
            }
            else if (taskState == TaskState.FAILED) {
                ExecutionFailureInfo failureInfo = taskStatus.getFailures().stream()
                        .findFirst()
                        .map(this::rewriteTransportFailure)
                        .orElseGet(() -> toFailure(new TrinoException(GENERIC_INTERNAL_ERROR, "A task failed for an unknown reason")));

                List<PrioritizedScheduledTask> replacementTasks = stageExecution.taskFailed(taskId, failureInfo, taskStatus);
                replacementTasks.forEach(schedulingQueue::addOrUpdate);

                if (shouldDelayScheduling(failureInfo.getErrorCode())) {
                    schedulingDelayer.startOrProlongDelayIfNecessary();
                    scheduledExecutorService.schedule(() -> eventQueue.add(Event.WAKE_UP), schedulingDelayer.getRemainingDelayInMillis(), MILLISECONDS);
                }
            }

            // update output selectors
            ExchangeSourceOutputSelector outputSelector = stageExecution.getSinkOutputSelector();
            for (StageId consumerStageId : stageConsumers.get(stageExecution.getStageId())) {
                getStageExecution(consumerStageId).setSourceOutputSelector(stageExecution.getStageFragmentId(), outputSelector);
            }
        }

        @Override
        public void onRemoteTaskExchangeSinkUpdateRequired(RemoteTaskExchangeSinkUpdateRequiredEvent event)
        {
            TaskId taskId = event.getTaskStatus().getTaskId();
            StageExecution stageExecution = getStageExecution(taskId.getStageId());
            stageExecution.initializeUpdateOfExchangeSinkInstanceHandle(taskId, eventQueue);
        }

        @Override
        public void onRemoteTaskExchangeUpdatedSinkAcquired(RemoteTaskExchangeUpdatedSinkAcquired event)
        {
            TaskId taskId = event.getTaskId();
            StageExecution stageExecution = getStageExecution(taskId.getStageId());
            stageExecution.finalizeUpdateOfExchangeSinkInstanceHandle(taskId, event.getExchangeSinkInstanceHandle());
        }

        @Override
        public void onSplitAssignment(SplitAssignmentEvent event)
        {
            StageExecution stageExecution = getStageExecution(event.getStageId());
            AssignmentResult assignment = event.getAssignmentResult();
            for (Partition partition : assignment.partitionsAdded()) {
                stageExecution.addPartition(partition.partitionId(), partition.nodeRequirements());
            }
            for (PartitionUpdate partitionUpdate : assignment.partitionUpdates()) {
                Optional<PrioritizedScheduledTask> scheduledTask = stageExecution.updatePartition(
                        partitionUpdate.partitionId(),
                        partitionUpdate.planNodeId(),
                        partitionUpdate.readyForScheduling(),
                        partitionUpdate.splits(),
                        partitionUpdate.noMoreSplits());
                scheduledTask.ifPresent(schedulingQueue::addOrUpdate);
            }
            assignment.sealedPartitions().forEach(partitionId -> {
                Optional<PrioritizedScheduledTask> scheduledTask = stageExecution.sealPartition(partitionId);
                scheduledTask.ifPresent(prioritizedTask -> {
                    PreSchedulingTaskContext context = preSchedulingTaskContexts.get(prioritizedTask.task());
                    if (context != null) {
                        // task is already waiting for node or for sink instance handle
                        // update speculative flag
                        context.setSpeculative(prioritizedTask.isSpeculative());
                        context.getNodeLease().setSpeculative(prioritizedTask.isSpeculative());
                        return;
                    }
                    schedulingQueue.addOrUpdate(prioritizedTask);
                });
            });
            if (assignment.noMorePartitions()) {
                stageExecution.noMorePartitions();
            }
            stageExecution.taskDescriptorLoadingComplete();
        }

        @Override
        public void onStageFailure(StageFailureEvent event)
        {
            StageExecution stageExecution = getStageExecution(event.getStageId());
            stageExecution.fail(event.getFailure());
        }

        private StageExecution getStageExecution(StageId stageId)
        {
            StageExecution execution = stageExecutions.get(stageId);
            checkState(execution != null, "stage execution does not exist for stage: %s", stageId);
            return execution;
        }

        private boolean shouldDelayScheduling(@Nullable ErrorCode errorCode)
        {
            return errorCode == null || errorCode.getType() == INTERNAL_ERROR || errorCode.getType() == EXTERNAL;
        }

        private ExecutionFailureInfo rewriteTransportFailure(ExecutionFailureInfo executionFailureInfo)
        {
            if (executionFailureInfo.getRemoteHost() == null || failureDetector.getState(executionFailureInfo.getRemoteHost()) != GONE) {
                return executionFailureInfo;
            }

            return new ExecutionFailureInfo(
                    executionFailureInfo.getType(),
                    executionFailureInfo.getMessage(),
                    executionFailureInfo.getCause(),
                    executionFailureInfo.getSuppressed(),
                    executionFailureInfo.getStack(),
                    executionFailureInfo.getErrorLocation(),
                    REMOTE_HOST_GONE.toErrorCode(),
                    executionFailureInfo.getRemoteHost());
        }
    }

    private static class StageExecution
    {
        private final QueryStateMachine queryStateMachine;
        private final TaskDescriptorStorage taskDescriptorStorage;

        private final SqlStage stage;
        private final EventDrivenTaskSource taskSource;
        private final FaultTolerantPartitioningScheme sinkPartitioningScheme;
        private final Exchange exchange;
        private final PartitionMemoryEstimator partitionMemoryEstimator;
        private final int maxTaskExecutionAttempts;
        private final int schedulingPriority;
        private final DynamicFilterService dynamicFilterService;
        private final long[] outputDataSize;

        private final Int2ObjectMap<StagePartition> partitions = new Int2ObjectOpenHashMap<>();
        private boolean noMorePartitions;

        private final IntSet runningPartitions = new IntOpenHashSet();
        private final IntSet remainingPartitions = new IntOpenHashSet();

        private ExchangeSourceOutputSelector.Builder sinkOutputSelectorBuilder;
        private ExchangeSourceOutputSelector finalSinkOutputSelector;

        private final Set<PlanNodeId> remoteSourceIds;
        private final Map<PlanFragmentId, RemoteSourceNode> remoteSources;
        private final Map<PlanFragmentId, ExchangeSourceOutputSelector> sourceOutputSelectors = new HashMap<>();

        private boolean taskDescriptorLoadingActive;
        private boolean exchangeClosed;

        private final double minSourceStageProgress;
        private final boolean smallStageEstimationEnabled;
        private final DataSize smallStageEstimationThreshold;
        private final double smallStageSourceSizeMultiplier;
        private final DataSize smallSizePartitionSizeEstimate;
        private final boolean smallStageRequireNoMorePartitions;

        private StageExecution(
                QueryStateMachine queryStateMachine,
                TaskDescriptorStorage taskDescriptorStorage,
                SqlStage stage,
                EventDrivenTaskSource taskSource,
                FaultTolerantPartitioningScheme sinkPartitioningScheme,
                Exchange exchange,
                PartitionMemoryEstimator partitionMemoryEstimator,
                int maxTaskExecutionAttempts,
                int schedulingPriority,
                DynamicFilterService dynamicFilterService,
                double minSourceStageProgress,
                boolean smallStageEstimationEnabled,
                DataSize smallStageEstimationThreshold,
                double smallStageSourceSizeMultiplier,
                DataSize smallSizePartitionSizeEstimate,
                boolean smallStageRequireNoMorePartitions)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
            this.stage = requireNonNull(stage, "stage is null");
            this.taskSource = requireNonNull(taskSource, "taskSource is null");
            this.sinkPartitioningScheme = requireNonNull(sinkPartitioningScheme, "sinkPartitioningScheme is null");
            this.exchange = requireNonNull(exchange, "exchange is null");
            this.partitionMemoryEstimator = requireNonNull(partitionMemoryEstimator, "partitionMemoryEstimator is null");
            this.maxTaskExecutionAttempts = maxTaskExecutionAttempts;
            this.schedulingPriority = schedulingPriority;
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
            outputDataSize = new long[sinkPartitioningScheme.getPartitionCount()];
            sinkOutputSelectorBuilder = ExchangeSourceOutputSelector.builder(ImmutableSet.of(exchange.getId()));
            ImmutableMap.Builder<PlanFragmentId, RemoteSourceNode> remoteSources = ImmutableMap.builder();
            ImmutableSet.Builder<PlanNodeId> remoteSourceIds = ImmutableSet.builder();
            for (RemoteSourceNode remoteSource : stage.getFragment().getRemoteSourceNodes()) {
                remoteSourceIds.add(remoteSource.getId());
                remoteSource.getSourceFragmentIds().forEach(fragmentId -> remoteSources.put(fragmentId, remoteSource));
            }
            this.remoteSourceIds = remoteSourceIds.build();
            this.remoteSources = remoteSources.buildOrThrow();
            this.minSourceStageProgress = minSourceStageProgress;
            this.smallStageEstimationEnabled = smallStageEstimationEnabled;
            this.smallStageEstimationThreshold = requireNonNull(smallStageEstimationThreshold, "smallStageEstimationThreshold is null");
            this.smallStageSourceSizeMultiplier = smallStageSourceSizeMultiplier;
            this.smallSizePartitionSizeEstimate = requireNonNull(smallSizePartitionSizeEstimate, "smallSizePartitionSizeEstimate is null");
            this.smallStageRequireNoMorePartitions = smallStageRequireNoMorePartitions;
        }

        public StageId getStageId()
        {
            return stage.getStageId();
        }

        public PlanFragmentId getStageFragmentId()
        {
            return stage.getFragment().getId();
        }

        public StageState getState()
        {
            return stage.getState();
        }

        public StageInfo getStageInfo()
        {
            return stage.getStageInfo();
        }

        public Exchange getExchange()
        {
            return exchange;
        }

        public boolean isExchangeClosed()
        {
            return exchangeClosed;
        }

        public void addPartition(int partitionId, NodeRequirements nodeRequirements)
        {
            if (getState().isDone()) {
                return;
            }

            ExchangeSinkHandle exchangeSinkHandle = exchange.addSink(partitionId);
            Session session = queryStateMachine.getSession();
            DataSize defaultTaskMemory = stage.getFragment().getPartitioning().equals(COORDINATOR_DISTRIBUTION) ?
                    getFaultTolerantExecutionDefaultCoordinatorTaskMemory(session) :
                    getFaultTolerantExecutionDefaultTaskMemory(session);
            if (stage.getFragment().getRoot().getSources().stream()
                    .anyMatch(planNode -> planNode instanceof RefreshMaterializedViewNode)) {
                // REFRESH MATERIALIZED VIEW will issue other SQL commands under the hood. If its task memory is
                // non-zero, then a deadlock scenario is possible if we only have a single node in the cluster.
                defaultTaskMemory = DataSize.ofBytes(0);
            }
            StagePartition partition = new StagePartition(
                    taskDescriptorStorage,
                    stage.getStageId(),
                    partitionId,
                    exchangeSinkHandle,
                    remoteSourceIds,
                    nodeRequirements,
                    partitionMemoryEstimator.getInitialMemoryRequirements(session, defaultTaskMemory),
                    maxTaskExecutionAttempts);
            checkState(partitions.putIfAbsent(partitionId, partition) == null, "partition with id %s already exist in stage %s", partitionId, stage.getStageId());
            getSourceOutputSelectors().forEach((partition::updateExchangeSourceOutputSelector));
            remainingPartitions.add(partitionId);
        }

        public Optional<PrioritizedScheduledTask> updatePartition(
                int partitionId,
                PlanNodeId planNodeId,
                boolean readyForScheduling,
                List<Split> splits,
                boolean noMoreSplits)
        {
            if (getState().isDone()) {
                return Optional.empty();
            }

            StagePartition partition = getStagePartition(partitionId);
            partition.addSplits(planNodeId, splits, noMoreSplits);
            if (readyForScheduling && !partition.isTaskScheduled()) {
                partition.setTaskScheduled(true);
                return Optional.of(PrioritizedScheduledTask.createSpeculative(stage.getStageId(), partitionId, schedulingPriority));
            }
            else {
                return Optional.empty();
            }
        }

        public Optional<PrioritizedScheduledTask> sealPartition(int partitionId)
        {
            if (getState().isDone()) {
                return Optional.empty();
            }

            StagePartition partition = getStagePartition(partitionId);
            partition.seal();

            if (!partition.isRunning()) {
                // if partition is not yet running update its priority as it is no longer speculative
                return Optional.of(PrioritizedScheduledTask.create(stage.getStageId(), partitionId, schedulingPriority));
            }

            // TODO: split into smaller partitions here if necessary (for example if a task for a given partition failed with out of memory)

            return Optional.empty();
        }

        public void noMorePartitions()
        {
            noMorePartitions = true;
            if (getState().isDone()) {
                return;
            }

            if (remainingPartitions.isEmpty()) {
                stage.finish();
                // TODO close exchange early
                taskSource.close();
            }
        }

        public boolean isNoMorePartitions()
        {
            return noMorePartitions;
        }

        public int getPartitionsCount()
        {
            checkState(noMorePartitions, "noMorePartitions not set yet");
            return partitions.size();
        }

        public int getRemainingPartitionsCount()
        {
            checkState(noMorePartitions, "noMorePartitions not set yet");
            return remainingPartitions.size();
        }

        public void closeExchange()
        {
            if (exchangeClosed) {
                return;
            }

            exchange.close();
            exchangeClosed = true;
        }

        public Optional<GetExchangeSinkInstanceHandleResult> getExchangeSinkInstanceHandle(int partitionId)
        {
            if (getState().isDone()) {
                return Optional.empty();
            }

            StagePartition partition = getStagePartition(partitionId);
            verify(partition.getRemainingAttempts() >= 0, "remaining attempts is expected to be greater than or equal to zero: %s", partition.getRemainingAttempts());

            if (partition.isFinished()) {
                return Optional.empty();
            }

            int attempt = maxTaskExecutionAttempts - partition.getRemainingAttempts();
            return Optional.of(new GetExchangeSinkInstanceHandleResult(
                    exchange.instantiateSink(partition.getExchangeSinkHandle(), attempt),
                    attempt));
        }

        public Optional<RemoteTask> schedule(int partitionId, ExchangeSinkInstanceHandle exchangeSinkInstanceHandle, int attempt, InternalNode node, boolean speculative)
        {
            if (getState().isDone()) {
                return Optional.empty();
            }

            StagePartition partition = getStagePartition(partitionId);
            verify(partition.getRemainingAttempts() >= 0, "remaining attempts is expected to be greater than or equal to zero: %s", partition.getRemainingAttempts());

            if (partition.isFinished()) {
                return Optional.empty();
            }

            Map<PlanNodeId, ExchangeSourceOutputSelector> outputSelectors = getSourceOutputSelectors();

            ListMultimap<PlanNodeId, Split> splits = ArrayListMultimap.create();
            splits.putAll(partition.getSplits());
            outputSelectors.forEach((planNodeId, outputSelector) -> splits.put(planNodeId, createOutputSelectorSplit(outputSelector)));

            Set<PlanNodeId> noMoreSplits = new HashSet<>();
            for (RemoteSourceNode remoteSource : stage.getFragment().getRemoteSourceNodes()) {
                ExchangeSourceOutputSelector selector = outputSelectors.get(remoteSource.getId());
                if (selector != null && selector.isFinal() && partition.isNoMoreSplits(remoteSource.getId())) {
                    noMoreSplits.add(remoteSource.getId());
                }
            }
            for (PlanNodeId partitionedSource : stage.getFragment().getPartitionedSources()) {
                if (partition.isNoMoreSplits(partitionedSource)) {
                    noMoreSplits.add(partitionedSource);
                }
            }

            SpoolingOutputBuffers outputBuffers = SpoolingOutputBuffers.createInitial(exchangeSinkInstanceHandle, sinkPartitioningScheme.getPartitionCount());
            Optional<RemoteTask> task = stage.createTask(
                    node,
                    partitionId,
                    attempt,
                    sinkPartitioningScheme.getBucketToPartitionMap(),
                    outputBuffers,
                    splits,
                    noMoreSplits,
                    Optional.of(partition.getMemoryRequirements().getRequiredMemory()),
                    speculative);
            task.ifPresent(remoteTask -> {
                partition.addTask(remoteTask, outputBuffers);
                runningPartitions.add(partitionId);
            });
            return task;
        }

        public boolean hasOpenTaskRunning()
        {
            if (getState().isDone()) {
                return false;
            }

            if (runningPartitions.isEmpty()) {
                return false;
            }

            for (int partitionId : runningPartitions) {
                StagePartition partition = getStagePartition(partitionId);
                if (!partition.isSealed()) {
                    return true;
                }
            }

            return false;
        }

        public Optional<ListenableFuture<AssignmentResult>> loadMoreTaskDescriptors()
        {
            if (getState().isDone() || taskDescriptorLoadingActive) {
                return Optional.empty();
            }
            taskDescriptorLoadingActive = true;
            return Optional.of(taskSource.process());
        }

        public void taskDescriptorLoadingComplete()
        {
            taskDescriptorLoadingActive = false;
        }

        private Map<PlanNodeId, ExchangeSourceOutputSelector> getSourceOutputSelectors()
        {
            ImmutableMap.Builder<PlanNodeId, ExchangeSourceOutputSelector> result = ImmutableMap.builder();
            for (RemoteSourceNode remoteSource : stage.getFragment().getRemoteSourceNodes()) {
                ExchangeSourceOutputSelector mergedSelector = null;
                for (PlanFragmentId sourceFragmentId : remoteSource.getSourceFragmentIds()) {
                    ExchangeSourceOutputSelector sourceFragmentSelector = sourceOutputSelectors.get(sourceFragmentId);
                    if (sourceFragmentSelector == null) {
                        continue;
                    }
                    if (mergedSelector == null) {
                        mergedSelector = sourceFragmentSelector;
                    }
                    else {
                        mergedSelector = mergedSelector.merge(sourceFragmentSelector);
                    }
                }
                if (mergedSelector != null) {
                    result.put(remoteSource.getId(), mergedSelector);
                }
            }
            return result.buildOrThrow();
        }

        public void initializeUpdateOfExchangeSinkInstanceHandle(TaskId taskId, BlockingQueue<Event> eventQueue)
        {
            if (getState().isDone()) {
                return;
            }
            StagePartition partition = getStagePartition(taskId.getPartitionId());
            CompletableFuture<ExchangeSinkInstanceHandle> exchangeSinkInstanceHandleFuture = exchange.updateSinkInstanceHandle(partition.getExchangeSinkHandle(), taskId.getAttemptId());

            exchangeSinkInstanceHandleFuture.whenComplete((sinkInstanceHandle, throwable) -> {
                if (throwable != null) {
                    eventQueue.add(new StageFailureEvent(taskId.getStageId(), throwable));
                }
                else {
                    eventQueue.add(new RemoteTaskExchangeUpdatedSinkAcquired(taskId, sinkInstanceHandle));
                }
            });
        }

        public void finalizeUpdateOfExchangeSinkInstanceHandle(TaskId taskId, ExchangeSinkInstanceHandle updatedExchangeSinkInstanceHandle)
        {
            if (getState().isDone()) {
                return;
            }
            StagePartition partition = getStagePartition(taskId.getPartitionId());
            partition.updateExchangeSinkInstanceHandle(taskId, updatedExchangeSinkInstanceHandle);
        }

        public void taskFinished(TaskId taskId, TaskStatus taskStatus)
        {
            if (getState().isDone()) {
                return;
            }

            int partitionId = taskId.getPartitionId();
            StagePartition partition = getStagePartition(partitionId);
            exchange.sinkFinished(partition.getExchangeSinkHandle(), taskId.getAttemptId());
            SpoolingOutputStats.Snapshot outputStats = partition.taskFinished(taskId);

            if (!partition.isRunning()) {
                runningPartitions.remove(partitionId);
            }

            if (!remainingPartitions.remove(partitionId)) {
                // a different task for the same partition finished before
                return;
            }

            updateOutputSize(outputStats);

            partitionMemoryEstimator.registerPartitionFinished(
                    queryStateMachine.getSession(),
                    partition.getMemoryRequirements(),
                    taskStatus.getPeakMemoryReservation(),
                    true,
                    Optional.empty());

            sinkOutputSelectorBuilder.include(exchange.getId(), taskId.getPartitionId(), taskId.getAttemptId());

            if (noMorePartitions && remainingPartitions.isEmpty() && !stage.getState().isDone()) {
                dynamicFilterService.stageCannotScheduleMoreTasks(stage.getStageId(), 0, partitions.size());
                exchange.noMoreSinks();
                exchange.allRequiredSinksFinished();
                verify(finalSinkOutputSelector == null, "finalOutputSelector is already set");
                sinkOutputSelectorBuilder.setPartitionCount(exchange.getId(), partitions.size());
                sinkOutputSelectorBuilder.setFinal();
                finalSinkOutputSelector = sinkOutputSelectorBuilder.build();
                sinkOutputSelectorBuilder = null;
                stage.finish();
            }
        }

        private void updateOutputSize(SpoolingOutputStats.Snapshot taskOutputStats)
        {
            for (int partitionId = 0; partitionId < sinkPartitioningScheme.getPartitionCount(); partitionId++) {
                long partitionSizeInBytes = taskOutputStats.getPartitionSizeInBytes(partitionId);
                checkArgument(partitionSizeInBytes >= 0, "partitionSizeInBytes must be greater than or equal to zero: %s", partitionSizeInBytes);
                outputDataSize[partitionId] += partitionSizeInBytes;
            }
        }

        public List<PrioritizedScheduledTask> taskFailed(TaskId taskId, ExecutionFailureInfo failureInfo, TaskStatus taskStatus)
        {
            if (getState().isDone()) {
                return ImmutableList.of();
            }

            int partitionId = taskId.getPartitionId();
            StagePartition partition = getStagePartition(partitionId);
            partition.taskFailed(taskId);

            if (!partition.isRunning()) {
                runningPartitions.remove(partitionId);
            }

            RuntimeException failure = failureInfo.toException();
            ErrorCode errorCode = failureInfo.getErrorCode();
            partitionMemoryEstimator.registerPartitionFinished(
                    queryStateMachine.getSession(),
                    partition.getMemoryRequirements(),
                    taskStatus.getPeakMemoryReservation(),
                    false,
                    Optional.ofNullable(errorCode));

            // update memory limits for next attempt
            MemoryRequirements currentMemoryLimits = partition.getMemoryRequirements();
            MemoryRequirements newMemoryLimits = partitionMemoryEstimator.getNextRetryMemoryRequirements(
                    queryStateMachine.getSession(),
                    partition.getMemoryRequirements(),
                    taskStatus.getPeakMemoryReservation(),
                    errorCode);
            partition.setMemoryRequirements(newMemoryLimits);
            log.debug(
                    "Computed next memory requirements for task from stage %s; previous=%s; new=%s; peak=%s; estimator=%s",
                    stage.getStageId(),
                    currentMemoryLimits,
                    newMemoryLimits,
                    taskStatus.getPeakMemoryReservation(),
                    partitionMemoryEstimator);

            if (errorCode != null && isOutOfMemoryError(errorCode) && newMemoryLimits.getRequiredMemory().toBytes() * 0.99 <= taskStatus.getPeakMemoryReservation().toBytes()) {
                String message = format(
                        "Cannot allocate enough memory for task %s. Reported peak memory reservation: %s. Maximum possible reservation: %s.",
                        taskId,
                        taskStatus.getPeakMemoryReservation(),
                        newMemoryLimits.getRequiredMemory());
                stage.fail(new TrinoException(() -> errorCode, message, failure));
                return ImmutableList.of();
            }

            if (partition.getRemainingAttempts() == 0 || (errorCode != null && errorCode.getType() == USER_ERROR)) {
                stage.fail(failure);
                // stage failed, don't reschedule
                return ImmutableList.of();
            }

            if (!partition.isSealed()) {
                // don't reschedule speculative tasks
                return ImmutableList.of();
            }

            // TODO[https://github.com/trinodb/trino/issues/18025]: split into smaller partitions here if necessary (for example if a task for a given partition failed with out of memory)

            // reschedule a task
            return ImmutableList.of(PrioritizedScheduledTask.create(stage.getStageId(), partitionId, schedulingPriority));
        }

        public MemoryRequirements getMemoryRequirements(int partitionId)
        {
            return getStagePartition(partitionId).getMemoryRequirements();
        }

        public Optional<NodeRequirements> getNodeRequirements(int partitionId)
        {
            return getStagePartition(partitionId).getNodeRequirements();
        }

        public Optional<OutputDataSizeEstimateResult> getOutputDataSize(Function<StageId, StageExecution> stageExecutionLookup)
        {
            if (stage.getState() == StageState.FINISHED) {
                return Optional.of(new OutputDataSizeEstimateResult(
                        new OutputDataSizeEstimate(ImmutableLongArray.copyOf(outputDataSize)), OutputDataSizeEstimateStatus.FINISHED));
            }
            return getEstimatedOutputDataSize().or(() -> getEstimatedSmallStageOutputDataSize(stageExecutionLookup));
        }

        public boolean isSomeProgressMade()
        {
            return partitions.size() > 0 && remainingPartitions.size() < partitions.size();
        }

        private Optional<OutputDataSizeEstimateResult> getEstimatedOutputDataSize()
        {
            if (!isNoMorePartitions()) {
                return Optional.empty();
            }

            int allPartitionsCount = getPartitionsCount();
            int remainingPartitionsCount = getRemainingPartitionsCount();

            if (remainingPartitionsCount == allPartitionsCount) {
                return Optional.empty();
            }

            double progress = (double) (allPartitionsCount - remainingPartitionsCount) / allPartitionsCount;

            if (progress < minSourceStageProgress) {
                return Optional.empty();
            }

            ImmutableLongArray.Builder estimateBuilder = ImmutableLongArray.builder(outputDataSize.length);

            for (long partitionSize : outputDataSize) {
                estimateBuilder.add((long) (partitionSize / progress));
            }
            return Optional.of(new OutputDataSizeEstimateResult(new OutputDataSizeEstimate(estimateBuilder.build()), OutputDataSizeEstimateStatus.ESTIMATED_BY_PROGRESS));
        }

        private Optional<OutputDataSizeEstimateResult> getEstimatedSmallStageOutputDataSize(Function<StageId, StageExecution> stageExecutionLookup)
        {
            if (!smallStageEstimationEnabled) {
                return Optional.empty();
            }

            if (smallStageRequireNoMorePartitions && !isNoMorePartitions()) {
                return Optional.empty();
            }

            long currentOutputDataSize = 0;
            for (long partitionOutputDataSize : outputDataSize) {
                currentOutputDataSize += partitionOutputDataSize;
            }
            if (currentOutputDataSize > smallStageEstimationThreshold.toBytes()) {
                // our output is too big already
                return Optional.empty();
            }

            PlanFragment planFragment = this.getStageInfo().getPlan();
            boolean hasPartitionedSources = planFragment.getPartitionedSources().size() > 0;
            List<RemoteSourceNode> remoteSourceNodes = planFragment.getRemoteSourceNodes();

            long partitionedInputSizeEstimate = 0;
            if (hasPartitionedSources) {
                if (!isNoMorePartitions()) {
                    // stage is reading directly from table
                    // for leaf stages require all tasks to be enumerated
                    return Optional.empty();
                }
                // estimate partitioned input based on number of task partitions
                partitionedInputSizeEstimate += this.getPartitionsCount() * smallSizePartitionSizeEstimate.toBytes();
            }

            long remoteInputSizeEstimate = 0;
            for (RemoteSourceNode remoteSourceNode : remoteSourceNodes) {
                for (PlanFragmentId sourceFragmentId : remoteSourceNode.getSourceFragmentIds()) {
                    StageId sourceStageId = StageId.create(queryStateMachine.getQueryId(), sourceFragmentId);

                    StageExecution sourceStage = stageExecutionLookup.apply(sourceStageId);
                    requireNonNull(sourceStage, "sourceStage is null");
                    Optional<OutputDataSizeEstimateResult> sourceStageOutputDataSize = sourceStage.getOutputDataSize(stageExecutionLookup);

                    if (sourceStageOutputDataSize.isEmpty()) {
                        // cant estimate size of one of sources; should not happen in practice
                        return Optional.empty();
                    }

                    remoteInputSizeEstimate += sourceStageOutputDataSize.orElseThrow().getOutputDataSizeEstimate().getTotalSizeInBytes();
                }
            }

            long inputSizeEstimate = (long) ((partitionedInputSizeEstimate + remoteInputSizeEstimate) * smallStageSourceSizeMultiplier);
            if (inputSizeEstimate > smallStageEstimationThreshold.toBytes()) {
                return Optional.empty();
            }

            int outputPartitionsCount = sinkPartitioningScheme.getPartitionCount();
            ImmutableLongArray.Builder estimateBuilder = ImmutableLongArray.builder(outputPartitionsCount);
            for (int i = 0; i < outputPartitionsCount; ++i) {
                // assume uniform distribution
                // TODO; should we use distribution as in this.outputDataSize if we have some data there already?
                estimateBuilder.add(inputSizeEstimate / outputPartitionsCount);
            }
            return Optional.of(new OutputDataSizeEstimateResult(estimateBuilder.build(), OutputDataSizeEstimateStatus.ESTIMATED_BY_SMALL_INPUT));
        }

        public ExchangeSourceOutputSelector getSinkOutputSelector()
        {
            if (finalSinkOutputSelector != null) {
                return finalSinkOutputSelector;
            }
            return sinkOutputSelectorBuilder.build();
        }

        public void setSourceOutputSelector(PlanFragmentId sourceFragmentId, ExchangeSourceOutputSelector selector)
        {
            sourceOutputSelectors.put(sourceFragmentId, selector);
            RemoteSourceNode remoteSourceNode = remoteSources.get(sourceFragmentId);
            verify(remoteSourceNode != null, "remoteSourceNode is null for fragment: %s", sourceFragmentId);
            ExchangeSourceOutputSelector mergedSelector = selector;
            for (PlanFragmentId fragmentId : remoteSourceNode.getSourceFragmentIds()) {
                if (fragmentId.equals(sourceFragmentId)) {
                    continue;
                }
                ExchangeSourceOutputSelector fragmentSelector = sourceOutputSelectors.get(fragmentId);
                if (fragmentSelector != null) {
                    mergedSelector = mergedSelector.merge(fragmentSelector);
                }
            }
            ExchangeSourceOutputSelector finalMergedSelector = mergedSelector;
            remainingPartitions.forEach((IntConsumer) value -> {
                StagePartition partition = partitions.get(value);
                verify(partition != null, "partition not found: %s", value);
                partition.updateExchangeSourceOutputSelector(remoteSourceNode.getId(), finalMergedSelector);
            });
        }

        public void abort()
        {
            Closer closer = createStageExecutionCloser();
            closer.register(stage::abort);
            try {
                closer.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public void fail(Throwable t)
        {
            Closer closer = createStageExecutionCloser();
            closer.register(() -> stage.fail(t));
            try {
                closer.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            taskDescriptorLoadingComplete();
        }

        private Closer createStageExecutionCloser()
        {
            Closer closer = Closer.create();
            closer.register(taskSource);
            closer.register(this::closeExchange);
            return closer;
        }

        private StagePartition getStagePartition(int partitionId)
        {
            StagePartition partition = partitions.get(partitionId);
            checkState(partition != null, "partition with id %s does not exist in stage %s", partitionId, stage.getStageId());
            return partition;
        }
    }

    private enum OutputDataSizeEstimateStatus {
        FINISHED,
        ESTIMATED_BY_PROGRESS,
        ESTIMATED_BY_SMALL_INPUT
    }

    private static class OutputDataSizeEstimateResult
    {
        private final OutputDataSizeEstimate outputDataSizeEstimate;
        private final OutputDataSizeEstimateStatus status;

        public OutputDataSizeEstimateResult(ImmutableLongArray partitionDataSizes, OutputDataSizeEstimateStatus status)
        {
            this(new OutputDataSizeEstimate(partitionDataSizes), status);
        }

        private OutputDataSizeEstimateResult(OutputDataSizeEstimate outputDataSizeEstimate, OutputDataSizeEstimateStatus status)
        {
            this.outputDataSizeEstimate = requireNonNull(outputDataSizeEstimate, "outputDataSizeEstimate is null");
            this.status = requireNonNull(status, "status is null");
        }

        public OutputDataSizeEstimate getOutputDataSizeEstimate()
        {
            return outputDataSizeEstimate;
        }

        public OutputDataSizeEstimateStatus getStatus()
        {
            return status;
        }
    }

    private static class StagePartition
    {
        private final TaskDescriptorStorage taskDescriptorStorage;
        private final StageId stageId;
        private final int partitionId;
        private final ExchangeSinkHandle exchangeSinkHandle;
        private final Set<PlanNodeId> remoteSourceIds;

        // empty when task descriptor is closed and stored in TaskDescriptorStorage
        private Optional<OpenTaskDescriptor> openTaskDescriptor;
        private MemoryRequirements memoryRequirements;
        private int remainingAttempts;

        private final Map<TaskId, RemoteTask> tasks = new HashMap<>();
        private final Map<TaskId, SpoolingOutputBuffers> taskOutputBuffers = new HashMap<>();
        private final Set<TaskId> runningTasks = new HashSet<>();
        private final Set<PlanNodeId> finalSelectors = new HashSet<>();
        private final Set<PlanNodeId> noMoreSplits = new HashSet<>();
        private boolean taskScheduled;
        private boolean finished;

        public StagePartition(
                TaskDescriptorStorage taskDescriptorStorage,
                StageId stageId,
                int partitionId,
                ExchangeSinkHandle exchangeSinkHandle,
                Set<PlanNodeId> remoteSourceIds,
                NodeRequirements nodeRequirements,
                MemoryRequirements memoryRequirements,
                int maxTaskExecutionAttempts)
        {
            this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
            this.stageId = requireNonNull(stageId, "stageId is null");
            this.partitionId = partitionId;
            this.exchangeSinkHandle = requireNonNull(exchangeSinkHandle, "exchangeSinkHandle is null");
            this.remoteSourceIds = ImmutableSet.copyOf(requireNonNull(remoteSourceIds, "remoteSourceIds is null"));
            requireNonNull(nodeRequirements, "nodeRequirements is null");
            this.openTaskDescriptor = Optional.of(new OpenTaskDescriptor(ImmutableListMultimap.of(), ImmutableSet.of(), nodeRequirements));
            this.memoryRequirements = requireNonNull(memoryRequirements, "memoryRequirements is null");
            this.remainingAttempts = maxTaskExecutionAttempts;
        }

        public int getPartitionId()
        {
            return partitionId;
        }

        public ExchangeSinkHandle getExchangeSinkHandle()
        {
            return exchangeSinkHandle;
        }

        public void addSplits(PlanNodeId planNodeId, List<Split> splits, boolean noMoreSplits)
        {
            checkState(openTaskDescriptor.isPresent(), "openTaskDescriptor is empty");
            openTaskDescriptor = Optional.of(openTaskDescriptor.get().update(planNodeId, splits, noMoreSplits));
            if (noMoreSplits) {
                this.noMoreSplits.add(planNodeId);
            }
            for (RemoteTask task : tasks.values()) {
                task.addSplits(ImmutableListMultimap.<PlanNodeId, Split>builder()
                        .putAll(planNodeId, splits)
                        .build());
                if (noMoreSplits && isFinalOutputSelectorDelivered(planNodeId)) {
                    task.noMoreSplits(planNodeId);
                }
            }
        }

        private boolean isFinalOutputSelectorDelivered(PlanNodeId planNodeId)
        {
            if (!remoteSourceIds.contains(planNodeId)) {
                // not a remote source; input selector concept not applicable
                return true;
            }
            return finalSelectors.contains(planNodeId);
        }

        public void seal()
        {
            checkState(openTaskDescriptor.isPresent(), "openTaskDescriptor is empty");
            TaskDescriptor taskDescriptor = openTaskDescriptor.get().createTaskDescriptor(partitionId);
            openTaskDescriptor = Optional.empty();
            // a task may finish before task descriptor is sealed
            if (!finished) {
                taskDescriptorStorage.put(stageId, taskDescriptor);

                // update speculative flag for running tasks
                for (TaskId runningTaskId : runningTasks) {
                    RemoteTask runningTask = tasks.get(runningTaskId);
                    runningTask.setSpeculative(false);
                }
            }
        }

        public ListMultimap<PlanNodeId, Split> getSplits()
        {
            if (finished) {
                return ImmutableListMultimap.of();
            }
            return openTaskDescriptor.map(OpenTaskDescriptor::getSplits)
                    .or(() -> taskDescriptorStorage.get(stageId, partitionId).map(TaskDescriptor::getSplits))
                    // execution is finished
                    .orElse(ImmutableListMultimap.of());
        }

        public boolean isNoMoreSplits(PlanNodeId planNodeId)
        {
            if (finished) {
                return true;
            }
            return openTaskDescriptor.map(taskDescriptor -> taskDescriptor.getNoMoreSplits().contains(planNodeId))
                    // task descriptor is sealed, no more splits are expected
                    .orElse(true);
        }

        public boolean isSealed()
        {
            return openTaskDescriptor.isEmpty();
        }

        /**
         * Returns {@link Optional#empty()} when execution is finished
         */
        public Optional<NodeRequirements> getNodeRequirements()
        {
            if (finished) {
                return Optional.empty();
            }
            if (openTaskDescriptor.isPresent()) {
                return openTaskDescriptor.map(OpenTaskDescriptor::getNodeRequirements);
            }
            Optional<TaskDescriptor> taskDescriptor = taskDescriptorStorage.get(stageId, partitionId);
            if (taskDescriptor.isPresent()) {
                return taskDescriptor.map(TaskDescriptor::getNodeRequirements);
            }
            return Optional.empty();
        }

        public MemoryRequirements getMemoryRequirements()
        {
            return memoryRequirements;
        }

        public void setMemoryRequirements(MemoryRequirements memoryRequirements)
        {
            this.memoryRequirements = requireNonNull(memoryRequirements, "memoryRequirements is null");
        }

        public int getRemainingAttempts()
        {
            return remainingAttempts;
        }

        public void addTask(RemoteTask remoteTask, SpoolingOutputBuffers outputBuffers)
        {
            TaskId taskId = remoteTask.getTaskId();
            tasks.put(taskId, remoteTask);
            taskOutputBuffers.put(taskId, outputBuffers);
            runningTasks.add(taskId);
        }

        public SpoolingOutputStats.Snapshot taskFinished(TaskId taskId)
        {
            RemoteTask remoteTask = tasks.get(taskId);
            checkArgument(remoteTask != null, "task not found: %s", taskId);
            SpoolingOutputStats.Snapshot outputStats = remoteTask.retrieveAndDropSpoolingOutputStats();
            runningTasks.remove(taskId);
            tasks.values().forEach(RemoteTask::abort);
            finished = true;
            // task descriptor has been created
            if (isSealed()) {
                taskDescriptorStorage.remove(stageId, partitionId);
            }
            return outputStats;
        }

        public void taskFailed(TaskId taskId)
        {
            runningTasks.remove(taskId);
            remainingAttempts--;
        }

        public void updateExchangeSinkInstanceHandle(TaskId taskId, ExchangeSinkInstanceHandle handle)
        {
            SpoolingOutputBuffers outputBuffers = taskOutputBuffers.get(taskId);
            checkArgument(outputBuffers != null, "output buffers not found: %s", taskId);
            RemoteTask remoteTask = tasks.get(taskId);
            checkArgument(remoteTask != null, "task not found: %s", taskId);
            SpoolingOutputBuffers updatedOutputBuffers = outputBuffers.withExchangeSinkInstanceHandle(handle);
            taskOutputBuffers.put(taskId, updatedOutputBuffers);
            remoteTask.setOutputBuffers(updatedOutputBuffers);
        }

        public void updateExchangeSourceOutputSelector(PlanNodeId planNodeId, ExchangeSourceOutputSelector selector)
        {
            if (selector.isFinal()) {
                finalSelectors.add(planNodeId);
            }
            for (TaskId taskId : runningTasks) {
                RemoteTask task = tasks.get(taskId);
                verify(task != null, "task is null: %s", taskId);
                task.addSplits(ImmutableListMultimap.of(
                        planNodeId,
                        createOutputSelectorSplit(selector)));
                if (selector.isFinal() && noMoreSplits.contains(planNodeId)) {
                    task.noMoreSplits(planNodeId);
                }
            }
        }

        public boolean isRunning()
        {
            return !runningTasks.isEmpty();
        }

        public boolean isTaskScheduled()
        {
            return taskScheduled;
        }

        public void setTaskScheduled(boolean taskScheduled)
        {
            checkArgument(taskScheduled, "taskScheduled must be true");
            this.taskScheduled = taskScheduled;
        }

        public boolean isFinished()
        {
            return finished;
        }
    }

    private static Split createOutputSelectorSplit(ExchangeSourceOutputSelector selector)
    {
        return new Split(REMOTE_CATALOG_HANDLE, new RemoteSplit(new SpoolingExchangeInput(ImmutableList.of(), Optional.of(selector))));
    }

    private static class OpenTaskDescriptor
    {
        private final ListMultimap<PlanNodeId, Split> splits;
        private final Set<PlanNodeId> noMoreSplits;
        private final NodeRequirements nodeRequirements;

        private OpenTaskDescriptor(ListMultimap<PlanNodeId, Split> splits, Set<PlanNodeId> noMoreSplits, NodeRequirements nodeRequirements)
        {
            this.splits = ImmutableListMultimap.copyOf(requireNonNull(splits, "splits is null"));
            this.noMoreSplits = ImmutableSet.copyOf(requireNonNull(noMoreSplits, "noMoreSplits is null"));
            this.nodeRequirements = requireNonNull(nodeRequirements, "nodeRequirements is null");
        }

        public ListMultimap<PlanNodeId, Split> getSplits()
        {
            return splits;
        }

        public Set<PlanNodeId> getNoMoreSplits()
        {
            return noMoreSplits;
        }

        public NodeRequirements getNodeRequirements()
        {
            return nodeRequirements;
        }

        public OpenTaskDescriptor update(PlanNodeId planNodeId, List<Split> splits, boolean noMoreSplits)
        {
            ListMultimap<PlanNodeId, Split> updatedSplits = ImmutableListMultimap.<PlanNodeId, Split>builder()
                    .putAll(this.splits)
                    .putAll(planNodeId, splits)
                    .build();

            Set<PlanNodeId> updatedNoMoreSplits = this.noMoreSplits;
            if (noMoreSplits && !updatedNoMoreSplits.contains(planNodeId)) {
                updatedNoMoreSplits = ImmutableSet.<PlanNodeId>builder()
                        .addAll(this.noMoreSplits)
                        .add(planNodeId)
                        .build();
            }
            return new OpenTaskDescriptor(
                    updatedSplits,
                    updatedNoMoreSplits,
                    nodeRequirements);
        }

        public TaskDescriptor createTaskDescriptor(int partitionId)
        {
            Set<PlanNodeId> missingNoMoreSplits = Sets.difference(splits.keySet(), noMoreSplits);
            checkState(missingNoMoreSplits.isEmpty(), "missing no more splits for plan nodes: %s", missingNoMoreSplits);
            return new TaskDescriptor(
                    partitionId,
                    splits,
                    nodeRequirements);
        }
    }

    private record ScheduledTask(StageId stageId, int partitionId)
    {
        private ScheduledTask
        {
            requireNonNull(stageId, "stageId is null");
            checkArgument(partitionId >= 0, "partitionId must be greater than or equal to zero: %s", partitionId);
        }
    }

    private record PrioritizedScheduledTask(ScheduledTask task, int priority)
    {
        private static final int SPECULATIVE_EXECUTION_PRIORITY = 1_000_000_000;

        private PrioritizedScheduledTask
        {
            requireNonNull(task, "task is null");
            checkArgument(priority >= 0, "priority must be greater than or equal to zero: %s", priority);
        }

        public static PrioritizedScheduledTask create(StageId stageId, int partitionId, int priority)
        {
            checkArgument(priority < SPECULATIVE_EXECUTION_PRIORITY, "priority is expected to be less than %s: %s", SPECULATIVE_EXECUTION_PRIORITY, priority);
            return new PrioritizedScheduledTask(new ScheduledTask(stageId, partitionId), priority);
        }

        public static PrioritizedScheduledTask createSpeculative(StageId stageId, int partitionId, int priority)
        {
            checkArgument(priority < SPECULATIVE_EXECUTION_PRIORITY, "priority is expected to be less than %s: %s", SPECULATIVE_EXECUTION_PRIORITY, priority);
            return new PrioritizedScheduledTask(new ScheduledTask(stageId, partitionId), priority + SPECULATIVE_EXECUTION_PRIORITY);
        }

        public boolean isSpeculative()
        {
            return priority >= SPECULATIVE_EXECUTION_PRIORITY;
        }

        @Override
        public String toString()
        {
            return "" + task.stageId() + "/" + task.partitionId() + "[" + priority + "]";
        }
    }

    private static class SchedulingQueue
    {
        private final IndexedPriorityQueue<ScheduledTask> queue = new IndexedPriorityQueue<>(LOW_TO_HIGH);
        private int nonSpeculativeTaskCount;

        public boolean isEmpty()
        {
            return queue.isEmpty();
        }

        public int getNonSpeculativeTaskCount()
        {
            return nonSpeculativeTaskCount;
        }

        public PrioritizedScheduledTask pollOrThrow()
        {
            IndexedPriorityQueue.Prioritized<ScheduledTask> task = queue.pollPrioritized();
            checkState(task != null, "queue is empty");
            PrioritizedScheduledTask prioritizedTask = getPrioritizedTask(task);
            if (!prioritizedTask.isSpeculative()) {
                nonSpeculativeTaskCount--;
            }
            return prioritizedTask;
        }

        public PrioritizedScheduledTask peekOrThrow()
        {
            IndexedPriorityQueue.Prioritized<ScheduledTask> task = queue.peekPrioritized();
            checkState(task != null, "queue is empty");
            return getPrioritizedTask(task);
        }

        public void addOrUpdate(PrioritizedScheduledTask prioritizedTask)
        {
            IndexedPriorityQueue.Prioritized<ScheduledTask> previousTask = queue.getPrioritized(prioritizedTask.task());
            PrioritizedScheduledTask previousPrioritizedTask = null;
            if (previousTask != null) {
                previousPrioritizedTask = getPrioritizedTask(previousTask);
            }

            if (!prioritizedTask.isSpeculative() && (previousPrioritizedTask == null || previousPrioritizedTask.isSpeculative())) {
                // number of non-speculative tasks increased
                nonSpeculativeTaskCount++;
            }

            queue.addOrUpdate(prioritizedTask.task(), prioritizedTask.priority());
        }

        private static PrioritizedScheduledTask getPrioritizedTask(IndexedPriorityQueue.Prioritized<ScheduledTask> task)
        {
            return new PrioritizedScheduledTask(task.getValue(), toIntExact(task.getPriority()));
        }
    }

    private static class SchedulingDelayer
    {
        private final long minRetryDelayInMillis;
        private final long maxRetryDelayInMillis;
        private final double retryDelayScaleFactor;
        private final Stopwatch stopwatch;

        private long currentDelayInMillis;

        private SchedulingDelayer(Duration minRetryDelay, Duration maxRetryDelay, double retryDelayScaleFactor, Stopwatch stopwatch)
        {
            this.minRetryDelayInMillis = requireNonNull(minRetryDelay, "minRetryDelay is null").toMillis();
            this.maxRetryDelayInMillis = requireNonNull(maxRetryDelay, "maxRetryDelay is null").toMillis();
            checkArgument(retryDelayScaleFactor >= 1, "retryDelayScaleFactor is expected to be greater than or equal to 1: %s", retryDelayScaleFactor);
            this.retryDelayScaleFactor = retryDelayScaleFactor;
            this.stopwatch = requireNonNull(stopwatch, "stopwatch is null");
        }

        public void startOrProlongDelayIfNecessary()
        {
            if (stopwatch.isRunning()) {
                if (stopwatch.elapsed(MILLISECONDS) > currentDelayInMillis) {
                    // we are past previous delay period and still getting failures; let's make it longer
                    stopwatch.reset().start();
                    currentDelayInMillis = min(round(currentDelayInMillis * retryDelayScaleFactor), maxRetryDelayInMillis);
                }
            }
            else {
                // initialize delaying of tasks scheduling
                stopwatch.start();
                currentDelayInMillis = minRetryDelayInMillis;
            }
        }

        public long getRemainingDelayInMillis()
        {
            if (stopwatch.isRunning()) {
                return max(0, currentDelayInMillis - stopwatch.elapsed(MILLISECONDS));
            }
            return 0;
        }
    }

    private interface Event
    {
        Event ABORT = listener -> {
            throw new UnsupportedOperationException();
        };

        Event WAKE_UP = listener -> {
            throw new UnsupportedOperationException();
        };

        void accept(EventListener listener);
    }

    private interface EventListener
    {
        void onRemoteTaskCompleted(RemoteTaskCompletedEvent event);

        void onRemoteTaskExchangeSinkUpdateRequired(RemoteTaskExchangeSinkUpdateRequiredEvent event);

        void onRemoteTaskExchangeUpdatedSinkAcquired(RemoteTaskExchangeUpdatedSinkAcquired event);

        void onSplitAssignment(SplitAssignmentEvent event);

        void onStageFailure(StageFailureEvent event);

        void onSinkInstanceHandleAcquired(SinkInstanceHandleAcquiredEvent sinkInstanceHandleAcquiredEvent);
    }

    private static class SinkInstanceHandleAcquiredEvent
            implements Event
    {
        private final StageId stageId;
        private final int partitionId;
        private final NodeLease nodeLease;
        private final int attempt;
        private final ExchangeSinkInstanceHandle sinkInstanceHandle;

        public SinkInstanceHandleAcquiredEvent(StageId stageId, int partitionId, NodeLease nodeLease, int attempt, ExchangeSinkInstanceHandle sinkInstanceHandle)
        {
            this.stageId = requireNonNull(stageId, "stageId is null");
            this.partitionId = partitionId;
            this.nodeLease = requireNonNull(nodeLease, "nodeLease is null");
            this.attempt = attempt;
            this.sinkInstanceHandle = requireNonNull(sinkInstanceHandle, "sinkInstanceHandle is null");
        }

        public StageId getStageId()
        {
            return stageId;
        }

        public int getPartitionId()
        {
            return partitionId;
        }

        public NodeLease getNodeLease()
        {
            return nodeLease;
        }

        public int getAttempt()
        {
            return attempt;
        }

        public ExchangeSinkInstanceHandle getSinkInstanceHandle()
        {
            return sinkInstanceHandle;
        }

        @Override
        public void accept(EventListener listener)
        {
            listener.onSinkInstanceHandleAcquired(this);
        }
    }

    private static class RemoteTaskCompletedEvent
            extends RemoteTaskEvent
    {
        public RemoteTaskCompletedEvent(TaskStatus taskStatus)
        {
            super(taskStatus);
        }

        @Override
        public void accept(EventListener listener)
        {
            listener.onRemoteTaskCompleted(this);
        }
    }

    private static class RemoteTaskExchangeSinkUpdateRequiredEvent
            extends RemoteTaskEvent
    {
        protected RemoteTaskExchangeSinkUpdateRequiredEvent(TaskStatus taskStatus)
        {
            super(taskStatus);
        }

        @Override
        public void accept(EventListener listener)
        {
            listener.onRemoteTaskExchangeSinkUpdateRequired(this);
        }
    }

    private static class RemoteTaskExchangeUpdatedSinkAcquired
            implements Event
    {
        private final TaskId taskId;
        private final ExchangeSinkInstanceHandle exchangeSinkInstanceHandle;

        private RemoteTaskExchangeUpdatedSinkAcquired(TaskId taskId, ExchangeSinkInstanceHandle exchangeSinkInstanceHandle)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.exchangeSinkInstanceHandle = requireNonNull(exchangeSinkInstanceHandle, "exchangeSinkInstanceHandle is null");
        }

        @Override
        public void accept(EventListener listener)
        {
            listener.onRemoteTaskExchangeUpdatedSinkAcquired(this);
        }

        public TaskId getTaskId()
        {
            return taskId;
        }

        public ExchangeSinkInstanceHandle getExchangeSinkInstanceHandle()
        {
            return exchangeSinkInstanceHandle;
        }
    }

    private abstract static class RemoteTaskEvent
            implements Event
    {
        private final TaskStatus taskStatus;

        protected RemoteTaskEvent(TaskStatus taskStatus)
        {
            this.taskStatus = requireNonNull(taskStatus, "taskStatus is null");
        }

        public TaskStatus getTaskStatus()
        {
            return taskStatus;
        }
    }

    private static class SplitAssignmentEvent
            extends StageEvent
    {
        private final AssignmentResult assignmentResult;

        public SplitAssignmentEvent(StageId stageId, AssignmentResult assignmentResult)
        {
            super(stageId);
            this.assignmentResult = requireNonNull(assignmentResult, "assignmentResult is null");
        }

        public AssignmentResult getAssignmentResult()
        {
            return assignmentResult;
        }

        @Override
        public void accept(EventListener listener)
        {
            listener.onSplitAssignment(this);
        }
    }

    private static class StageFailureEvent
            extends StageEvent
    {
        private final Throwable failure;

        public StageFailureEvent(StageId stageId, Throwable failure)
        {
            super(stageId);
            this.failure = requireNonNull(failure, "failure is null");
        }

        public Throwable getFailure()
        {
            return failure;
        }

        @Override
        public void accept(EventListener listener)
        {
            listener.onStageFailure(this);
        }
    }

    private abstract static class StageEvent
            implements Event
    {
        private final StageId stageId;

        protected StageEvent(StageId stageId)
        {
            this.stageId = requireNonNull(stageId, "stageId is null");
        }

        public StageId getStageId()
        {
            return stageId;
        }
    }

    private record GetExchangeSinkInstanceHandleResult(CompletableFuture<ExchangeSinkInstanceHandle> exchangeSinkInstanceHandleFuture, int attempt)
    {
        public GetExchangeSinkInstanceHandleResult
        {
            requireNonNull(exchangeSinkInstanceHandleFuture, "exchangeSinkInstanceHandleFuture is null");
        }
    }

    private static class PreSchedulingTaskContext
    {
        private final NodeLease nodeLease;
        private boolean speculative;
        private boolean waitingForSinkInstanceHandle;

        public PreSchedulingTaskContext(NodeLease nodeLease, boolean speculative)
        {
            this.nodeLease = requireNonNull(nodeLease, "nodeLease is null");
            this.speculative = speculative;
        }

        public NodeLease getNodeLease()
        {
            return nodeLease;
        }

        public boolean isSpeculative()
        {
            return speculative;
        }

        public void setSpeculative(boolean speculative)
        {
            checkArgument(!speculative || this.speculative, "cannot change speculative flag false -> true");
            this.speculative = speculative;
        }

        public boolean isWaitingForSinkInstanceHandle()
        {
            return waitingForSinkInstanceHandle;
        }

        public void setWaitingForSinkInstanceHandle(boolean waitingForSinkInstanceHandle)
        {
            this.waitingForSinkInstanceHandle = waitingForSinkInstanceHandle;
        }
    }
}
