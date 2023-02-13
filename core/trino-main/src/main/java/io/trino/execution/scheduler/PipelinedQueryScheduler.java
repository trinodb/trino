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
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.exchange.DirectExchangeInput;
import io.trino.execution.BasicStageStats;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.RemoteTask;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.SqlStage;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.StateMachine;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.TaskFailureListener;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStatus;
import io.trino.execution.scheduler.policy.ExecutionPolicy;
import io.trino.execution.scheduler.policy.ExecutionSchedule;
import io.trino.execution.scheduler.policy.StagesScheduleResult;
import io.trino.failuredetector.FailureDetector;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Metadata;
import io.trino.operator.RetryPolicy;
import io.trino.server.DynamicFilterService;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.split.SplitSource;
import io.trino.sql.planner.NodePartitionMap;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SplitSourceFactory;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableScanNode;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.trino.SystemSessionProperties.getQueryRetryAttempts;
import static io.trino.SystemSessionProperties.getRetryDelayScaleFactor;
import static io.trino.SystemSessionProperties.getRetryInitialDelay;
import static io.trino.SystemSessionProperties.getRetryMaxDelay;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.getWriterMinSize;
import static io.trino.execution.QueryState.STARTING;
import static io.trino.execution.scheduler.PipelinedStageExecution.createPipelinedStageExecution;
import static io.trino.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static io.trino.execution.scheduler.StageExecution.State.ABORTED;
import static io.trino.execution.scheduler.StageExecution.State.CANCELED;
import static io.trino.execution.scheduler.StageExecution.State.FAILED;
import static io.trino.execution.scheduler.StageExecution.State.FINISHED;
import static io.trino.execution.scheduler.StageExecution.State.FLUSHING;
import static io.trino.execution.scheduler.StageExecution.State.RUNNING;
import static io.trino.execution.scheduler.StageExecution.State.SCHEDULED;
import static io.trino.operator.RetryPolicy.NONE;
import static io.trino.operator.RetryPolicy.QUERY;
import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_FAILED;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.Failures.toFailure;
import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toCollection;

public class PipelinedQueryScheduler
        implements QueryScheduler
{
    private static final Logger log = Logger.get(PipelinedQueryScheduler.class);

    private final QueryStateMachine queryStateMachine;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeScheduler nodeScheduler;
    private final int splitBatchSize;
    private final ExecutorService executor;
    private final ScheduledExecutorService schedulerExecutor;
    private final FailureDetector failureDetector;
    private final ExecutionPolicy executionPolicy;
    private final SplitSchedulerStats schedulerStats;
    private final DynamicFilterService dynamicFilterService;
    private final TableExecuteContextManager tableExecuteContextManager;
    private final SplitSourceFactory splitSourceFactory;

    private final StageManager stageManager;
    private final CoordinatorStagesScheduler coordinatorStagesScheduler;

    private final RetryPolicy retryPolicy;
    private final int maxQueryRetryAttempts;
    private final AtomicInteger currentAttempt = new AtomicInteger();
    private final Duration retryInitialDelay;
    private final Duration retryMaxDelay;
    private final double retryDelayScaleFactor;

    @GuardedBy("this")
    private boolean started;

    @GuardedBy("this")
    private final AtomicReference<DistributedStagesScheduler> distributedStagesScheduler = new AtomicReference<>();
    @GuardedBy("this")
    private Future<Void> distributedStagesSchedulingTask;

    public PipelinedQueryScheduler(
            QueryStateMachine queryStateMachine,
            SubPlan plan,
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
            DynamicFilterService dynamicFilterService,
            TableExecuteContextManager tableExecuteContextManager,
            Metadata metadata,
            SplitSourceFactory splitSourceFactory,
            SqlTaskManager coordinatorTaskManager)
    {
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
        this.splitBatchSize = splitBatchSize;
        this.executor = requireNonNull(queryExecutor, "queryExecutor is null");
        this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");

        stageManager = StageManager.create(
                queryStateMachine,
                metadata,
                remoteTaskFactory,
                nodeTaskMap,
                schedulerStats,
                plan,
                summarizeTaskInfo);

        coordinatorStagesScheduler = CoordinatorStagesScheduler.create(
                queryStateMachine,
                nodeScheduler,
                stageManager,
                failureDetector,
                schedulerExecutor,
                distributedStagesScheduler,
                coordinatorTaskManager);

        retryPolicy = getRetryPolicy(queryStateMachine.getSession());
        verify(retryPolicy == NONE || retryPolicy == QUERY, "unexpected retry policy: %s", retryPolicy);
        maxQueryRetryAttempts = getQueryRetryAttempts(queryStateMachine.getSession());
        retryInitialDelay = getRetryInitialDelay(queryStateMachine.getSession());
        retryMaxDelay = getRetryMaxDelay(queryStateMachine.getSession());
        retryDelayScaleFactor = getRetryDelayScaleFactor(queryStateMachine.getSession());
    }

    @Override
    public synchronized void start()
    {
        if (started) {
            return;
        }
        started = true;

        if (queryStateMachine.isDone()) {
            return;
        }

        // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
        queryStateMachine.addStateChangeListener(state -> {
            if (!state.isDone()) {
                return;
            }

            DistributedStagesScheduler distributedStagesScheduler;
            // synchronize to wait on distributed scheduler creation if it is currently in process
            synchronized (this) {
                distributedStagesScheduler = this.distributedStagesScheduler.get();
            }

            if (state == QueryState.FINISHED) {
                coordinatorStagesScheduler.cancel();
                if (distributedStagesScheduler != null) {
                    distributedStagesScheduler.cancel();
                }
                stageManager.finish();
            }
            else if (state == QueryState.FAILED) {
                coordinatorStagesScheduler.abort();
                if (distributedStagesScheduler != null) {
                    distributedStagesScheduler.abort();
                }
                stageManager.abort();
            }

            queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo()));
        });

        Optional<DistributedStagesScheduler> distributedStagesScheduler = createDistributedStagesScheduler(currentAttempt.get());

        coordinatorStagesScheduler.schedule();
        distributedStagesScheduler.ifPresent(scheduler -> distributedStagesSchedulingTask = executor.submit(scheduler::schedule, null));
    }

    private synchronized Optional<DistributedStagesScheduler> createDistributedStagesScheduler(int attempt)
    {
        verify(attempt == 0 || retryPolicy == RetryPolicy.QUERY, "unexpected attempt %s for retry policy %s", attempt, retryPolicy);
        if (queryStateMachine.isDone()) {
            return Optional.empty();
        }
        DistributedStagesScheduler distributedStagesScheduler;
        switch (retryPolicy) {
            case QUERY:
            case NONE:
                if (attempt > 0) {
                    dynamicFilterService.registerQueryRetry(queryStateMachine.getQueryId(), attempt);
                }
                distributedStagesScheduler = DistributedStagesScheduler.create(
                        queryStateMachine,
                        schedulerStats,
                        nodeScheduler,
                        nodePartitioningManager,
                        stageManager,
                        coordinatorStagesScheduler,
                        executionPolicy,
                        failureDetector,
                        schedulerExecutor,
                        splitSourceFactory,
                        splitBatchSize,
                        dynamicFilterService,
                        tableExecuteContextManager,
                        retryPolicy,
                        attempt);
                break;
            default:
                throw new IllegalArgumentException("Unexpected retry policy: " + retryPolicy);
        }

        this.distributedStagesScheduler.set(distributedStagesScheduler);
        distributedStagesScheduler.addStateChangeListener(state -> {
            if (queryStateMachine.getQueryState() == QueryState.STARTING && (state == DistributedStagesSchedulerState.RUNNING || state.isDone())) {
                queryStateMachine.transitionToRunning();
            }

            if (state.isDone() && !state.isFailure()) {
                stageManager.getDistributedStagesInTopologicalOrder().forEach(stage -> stageManager.get(stage.getStageId()).finish());
            }

            if (stageManager.getCoordinatorStagesInTopologicalOrder().isEmpty()) {
                // if there are no coordinator stages (e.g., simple select query) and the distributed stages are finished, do the query transitioning
                // otherwise defer query transitioning to the coordinator stages
                if (state == DistributedStagesSchedulerState.FINISHED) {
                    queryStateMachine.transitionToFinishing();
                }
                else if (state == DistributedStagesSchedulerState.CANCELED) {
                    // output stage was canceled
                    queryStateMachine.transitionToCanceled();
                }
            }

            if (state == DistributedStagesSchedulerState.FAILED) {
                StageFailureInfo stageFailureInfo = distributedStagesScheduler.getFailureCause()
                        .orElseGet(() -> new StageFailureInfo(toFailure(new VerifyException("distributedStagesScheduler failed but failure cause is not present")), Optional.empty()));
                ErrorCode errorCode = stageFailureInfo.getFailureInfo().getErrorCode();
                if (shouldRetry(errorCode)) {
                    long delayInMillis = min(retryInitialDelay.toMillis() * ((long) pow(retryDelayScaleFactor, currentAttempt.get())), retryMaxDelay.toMillis());
                    currentAttempt.incrementAndGet();
                    scheduleRetryWithDelay(delayInMillis);
                }
                else {
                    stageManager.getDistributedStagesInTopologicalOrder().forEach(stage -> {
                        if (stageFailureInfo.getFailedStageId().isPresent() && stageFailureInfo.getFailedStageId().get().equals(stage.getStageId())) {
                            stage.fail(stageFailureInfo.getFailureInfo().toException());
                        }
                        else {
                            stage.abort();
                        }
                    });
                    queryStateMachine.transitionToFailed(stageFailureInfo.getFailureInfo().toException());
                }
            }
        });
        return Optional.of(distributedStagesScheduler);
    }

    private boolean shouldRetry(ErrorCode errorCode)
    {
        return retryPolicy == RetryPolicy.QUERY && currentAttempt.get() < maxQueryRetryAttempts && isRetryableErrorCode(errorCode);
    }

    private static boolean isRetryableErrorCode(ErrorCode errorCode)
    {
        return errorCode == null
                || errorCode.getType() == INTERNAL_ERROR
                || errorCode.getType() == EXTERNAL
                || errorCode.getCode() == CLUSTER_OUT_OF_MEMORY.toErrorCode().getCode();
    }

    private void scheduleRetryWithDelay(long delayInMillis)
    {
        try {
            schedulerExecutor.schedule(this::scheduleRetry, delayInMillis, MILLISECONDS);
        }
        catch (Throwable t) {
            queryStateMachine.transitionToFailed(t);
        }
    }

    private synchronized void scheduleRetry()
    {
        try {
            checkState(distributedStagesSchedulingTask != null, "schedulingTask is expected to be set");

            // give current scheduler some time to terminate, usually it is expected to be done right away
            distributedStagesSchedulingTask.get(5, MINUTES);

            Optional<DistributedStagesScheduler> distributedStagesScheduler = createDistributedStagesScheduler(currentAttempt.get());
            distributedStagesScheduler.ifPresent(scheduler -> distributedStagesSchedulingTask = executor.submit(scheduler::schedule, null));
        }
        catch (Throwable t) {
            queryStateMachine.transitionToFailed(t);
        }
    }

    @Override
    public synchronized void cancelStage(StageId stageId)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            coordinatorStagesScheduler.cancelStage(stageId);
            DistributedStagesScheduler distributedStagesScheduler = this.distributedStagesScheduler.get();
            if (distributedStagesScheduler != null) {
                distributedStagesScheduler.cancelStage(stageId);
            }
        }
    }

    @Override
    public void failTask(TaskId taskId, Throwable failureCause)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            stageManager.failTaskRemotely(taskId, failureCause);
        }
    }

    @Override
    public BasicStageStats getBasicStageStats()
    {
        return stageManager.getBasicStageStats();
    }

    @Override
    public StageInfo getStageInfo()
    {
        return stageManager.getStageInfo();
    }

    @Override
    public long getUserMemoryReservation()
    {
        return stageManager.getUserMemoryReservation();
    }

    @Override
    public long getTotalMemoryReservation()
    {
        return stageManager.getTotalMemoryReservation();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return stageManager.getTotalCpuTime();
    }

    private static class QueryOutputTaskLifecycleListener
            implements TaskLifecycleListener
    {
        private final QueryStateMachine queryStateMachine;

        private QueryOutputTaskLifecycleListener(QueryStateMachine queryStateMachine)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        }

        @Override
        public void taskCreated(PlanFragmentId fragmentId, RemoteTask task)
        {
            URI taskUri = uriBuilderFrom(task.getTaskStatus().getSelf())
                    .appendPath("results")
                    .appendPath("0").build();
            DirectExchangeInput input = new DirectExchangeInput(task.getTaskId(), taskUri.toString());
            queryStateMachine.updateInputsForQueryResults(ImmutableList.of(input), false);
        }

        @Override
        public void noMoreTasks(PlanFragmentId fragmentId)
        {
            queryStateMachine.updateInputsForQueryResults(ImmutableList.of(), true);
        }
    }

    /**
     * Scheduler for stages that must be executed on coordinator.
     * <p>
     * Scheduling for coordinator only stages must be represented as a separate entity to
     * ensure the coordinator stages/tasks are never restarted in an event of a failure.
     * <p>
     * Coordinator only tasks cannot be restarted due to the nature of operations
     * they perform. For example commit operations for DML statements are performed as a
     * coordinator only task (via {@link io.trino.operator.TableFinishOperator}). Today it is
     * not required for a commit operation to be side effect free and idempotent what makes it
     * impossible to safely retry.
     */
    private static class CoordinatorStagesScheduler
    {
        private static final int[] SINGLE_PARTITION = new int[] {0};

        private final QueryStateMachine queryStateMachine;
        private final NodeScheduler nodeScheduler;
        private final Map<PlanFragmentId, PipelinedOutputBufferManager> outputBuffersForStagesConsumedByCoordinator;
        private final Map<PlanFragmentId, Optional<int[]>> bucketToPartitionForStagesConsumedByCoordinator;
        private final TaskLifecycleListener taskLifecycleListener;
        private final StageManager stageManager;
        private final List<StageExecution> stageExecutions;
        private final AtomicReference<DistributedStagesScheduler> distributedStagesScheduler;
        private final SqlTaskManager coordinatorTaskManager;

        private final AtomicBoolean scheduled = new AtomicBoolean();

        public static CoordinatorStagesScheduler create(
                QueryStateMachine queryStateMachine,
                NodeScheduler nodeScheduler,
                StageManager stageManager,
                FailureDetector failureDetector,
                Executor executor,
                AtomicReference<DistributedStagesScheduler> distributedStagesScheduler,
                SqlTaskManager coordinatorTaskManager)
        {
            Map<PlanFragmentId, PipelinedOutputBufferManager> outputBuffersForStagesConsumedByCoordinator = createOutputBuffersForStagesConsumedByCoordinator(stageManager);
            Map<PlanFragmentId, Optional<int[]>> bucketToPartitionForStagesConsumedByCoordinator = createBucketToPartitionForStagesConsumedByCoordinator(stageManager);

            TaskLifecycleListener taskLifecycleListener = new QueryOutputTaskLifecycleListener(queryStateMachine);
            // create executions
            ImmutableList.Builder<StageExecution> stageExecutions = ImmutableList.builder();
            for (SqlStage stage : stageManager.getCoordinatorStagesInTopologicalOrder()) {
                StageExecution stageExecution = createPipelinedStageExecution(
                        stage,
                        outputBuffersForStagesConsumedByCoordinator,
                        taskLifecycleListener,
                        failureDetector,
                        executor,
                        bucketToPartitionForStagesConsumedByCoordinator.get(stage.getFragment().getId()),
                        0);
                stageExecutions.add(stageExecution);
                taskLifecycleListener = stageExecution.getTaskLifecycleListener();
            }

            CoordinatorStagesScheduler coordinatorStagesScheduler = new CoordinatorStagesScheduler(
                    queryStateMachine,
                    nodeScheduler,
                    outputBuffersForStagesConsumedByCoordinator,
                    bucketToPartitionForStagesConsumedByCoordinator,
                    taskLifecycleListener,
                    stageManager,
                    stageExecutions.build(),
                    distributedStagesScheduler,
                    coordinatorTaskManager);
            coordinatorStagesScheduler.initialize();

            return coordinatorStagesScheduler;
        }

        private static Map<PlanFragmentId, PipelinedOutputBufferManager> createOutputBuffersForStagesConsumedByCoordinator(StageManager stageManager)
        {
            ImmutableMap.Builder<PlanFragmentId, PipelinedOutputBufferManager> result = ImmutableMap.builder();

            // create output buffer for output stage
            SqlStage outputStage = stageManager.getOutputStage();
            result.put(outputStage.getFragment().getId(), createSingleStreamOutputBuffer(outputStage));

            // create output buffers for stages consumed by coordinator
            for (SqlStage coordinatorStage : stageManager.getCoordinatorStagesInTopologicalOrder()) {
                for (SqlStage childStage : stageManager.getChildren(coordinatorStage.getStageId())) {
                    result.put(childStage.getFragment().getId(), createSingleStreamOutputBuffer(childStage));
                }
            }

            return result.buildOrThrow();
        }

        private static PipelinedOutputBufferManager createSingleStreamOutputBuffer(SqlStage stage)
        {
            PartitioningHandle partitioningHandle = stage.getFragment().getOutputPartitioningScheme().getPartitioning().getHandle();
            checkArgument(partitioningHandle.isSingleNode(), "partitioning is expected to be single node: " + partitioningHandle);
            return new PartitionedPipelinedOutputBufferManager(partitioningHandle, 1);
        }

        private static Map<PlanFragmentId, Optional<int[]>> createBucketToPartitionForStagesConsumedByCoordinator(StageManager stageManager)
        {
            ImmutableMap.Builder<PlanFragmentId, Optional<int[]>> result = ImmutableMap.builder();

            SqlStage outputStage = stageManager.getOutputStage();
            result.put(outputStage.getFragment().getId(), Optional.of(SINGLE_PARTITION));

            for (SqlStage coordinatorStage : stageManager.getCoordinatorStagesInTopologicalOrder()) {
                for (SqlStage childStage : stageManager.getChildren(coordinatorStage.getStageId())) {
                    result.put(childStage.getFragment().getId(), Optional.of(SINGLE_PARTITION));
                }
            }

            return result.buildOrThrow();
        }

        private CoordinatorStagesScheduler(
                QueryStateMachine queryStateMachine,
                NodeScheduler nodeScheduler,
                Map<PlanFragmentId, PipelinedOutputBufferManager> outputBuffersForStagesConsumedByCoordinator,
                Map<PlanFragmentId, Optional<int[]>> bucketToPartitionForStagesConsumedByCoordinator,
                TaskLifecycleListener taskLifecycleListener,
                StageManager stageManager,
                List<StageExecution> stageExecutions,
                AtomicReference<DistributedStagesScheduler> distributedStagesScheduler,
                SqlTaskManager coordinatorTaskManager)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.outputBuffersForStagesConsumedByCoordinator = ImmutableMap.copyOf(requireNonNull(outputBuffersForStagesConsumedByCoordinator, "outputBuffersForStagesConsumedByCoordinator is null"));
            this.bucketToPartitionForStagesConsumedByCoordinator = ImmutableMap.copyOf(requireNonNull(bucketToPartitionForStagesConsumedByCoordinator, "bucketToPartitionForStagesConsumedByCoordinator is null"));
            this.taskLifecycleListener = requireNonNull(taskLifecycleListener, "taskLifecycleListener is null");
            this.stageManager = requireNonNull(stageManager, "stageManager is null");
            this.stageExecutions = ImmutableList.copyOf(requireNonNull(stageExecutions, "stageExecutions is null"));
            this.distributedStagesScheduler = requireNonNull(distributedStagesScheduler, "distributedStagesScheduler is null");
            this.coordinatorTaskManager = requireNonNull(coordinatorTaskManager, "coordinatorTaskManager is null");
        }

        private void initialize()
        {
            for (StageExecution stageExecution : stageExecutions) {
                stageExecution.addStateChangeListener(state -> {
                    if (queryStateMachine.isDone()) {
                        return;
                    }
                    // if any coordinator stage failed transition directly to failure
                    if (state == FAILED) {
                        RuntimeException failureCause = stageExecution.getFailureCause()
                                .map(ExecutionFailureInfo::toException)
                                .orElseGet(() -> new VerifyException(format("stage execution for stage %s is failed by failure cause is not present", stageExecution.getStageId())));
                        stageManager.get(stageExecution.getStageId()).fail(failureCause);
                        queryStateMachine.transitionToFailed(failureCause);
                    }
                    else if (state == ABORTED) {
                        // this should never happen, since abort can only be triggered in query clean up after the query is finished
                        stageManager.get(stageExecution.getStageId()).abort();
                        queryStateMachine.transitionToFailed(new TrinoException(GENERIC_INTERNAL_ERROR, "Query stage was aborted"));
                    }
                    else if (state.isDone()) {
                        stageManager.get(stageExecution.getStageId()).finish();
                    }
                });
            }

            for (int currentIndex = 0, nextIndex = 1; nextIndex < stageExecutions.size(); currentIndex++, nextIndex++) {
                StageExecution stageExecution = stageExecutions.get(currentIndex);
                StageExecution childStageExecution = stageExecutions.get(nextIndex);
                Set<SqlStage> childStages = stageManager.getChildren(stageExecution.getStageId());
                verify(childStages.size() == 1, "exactly one child stage is expected");
                SqlStage childStage = getOnlyElement(childStages);
                verify(childStage.getStageId().equals(childStageExecution.getStageId()), "stage execution order doesn't match the stage order");
                stageExecution.addStateChangeListener(newState -> {
                    if (newState == FLUSHING || newState.isDone()) {
                        childStageExecution.cancel();
                    }
                });
            }

            Optional<StageExecution> root = Optional.ofNullable(getFirst(stageExecutions, null));
            root.ifPresent(stageExecution -> stageExecution.addStateChangeListener(state -> {
                if (state == FINISHED) {
                    queryStateMachine.transitionToFinishing();
                }
                else if (state == CANCELED) {
                    // output stage was canceled
                    queryStateMachine.transitionToCanceled();
                }
            }));

            Optional<StageExecution> last = Optional.ofNullable(getLast(stageExecutions, null));
            last.ifPresent(stageExecution -> stageExecution.addStateChangeListener(newState -> {
                if (newState == FLUSHING || newState.isDone()) {
                    DistributedStagesScheduler distributedStagesScheduler = this.distributedStagesScheduler.get();
                    if (distributedStagesScheduler != null) {
                        distributedStagesScheduler.cancel();
                    }
                }
            }));
        }

        public synchronized void schedule()
        {
            if (!scheduled.compareAndSet(false, true)) {
                return;
            }

            /*
             * Tasks have 2 communication links:
             *
             * Task <-> Coordinator (for status updates)
             * Task <-> Downstream Task (for exchanging the task results)
             *
             * In a scenario when a link between a task and a downstream task is broken (while the link between a
             * task and coordinator is not) without failure recovery enabled the downstream task would discover
             * that the communication link is broken and fail a query.
             *
             * However with failure recovery enabled a downstream task is configured to ignore the failures to survive an
             * upstream task failure. That may result into a "deadlock", when the coordinator thinks that a task is active,
             * but since the communication link between the task and it's downstream task is broken nobody is pooling
             * the results leaving it in a blocked state. Thus it is important to notify the scheduler about such
             * communication failures so the scheduler can react and re-schedule a task.
             *
             * Currently only "coordinator" tasks have to survive an upstream task failure (for example a task that performs
             * table commit). Restarting a table commit task introduces another set of challenges (such as making sure the commit
             * operation is always idempotent). Given that only coordinator tasks have to survive a failure there's a shortcut in
             * implementation of the error reporting. The assumption is that scheduling also happens on coordinator, thus no RPC is
             * involved in notifying the coordinator. Whenever it is needed to separate scheduling and coordinator tasks on different
             * nodes an RPC mechanism for this notification has to be implemented.
             *
             * Note: For queries that don't have any coordinator stages the situation is still similar. The exchange client that
             * pulls the final query results has to propagate the same notification if the communication link between the exchange client
             * and one of the output tasks is broken.
             */
            TaskFailureReporter failureReporter = new TaskFailureReporter(distributedStagesScheduler);
            queryStateMachine.addOutputTaskFailureListener(failureReporter);

            InternalNode coordinator = nodeScheduler.createNodeSelector(queryStateMachine.getSession(), Optional.empty()).selectCurrentNode();
            for (StageExecution stageExecution : stageExecutions) {
                Optional<RemoteTask> remoteTask = stageExecution.scheduleTask(
                        coordinator,
                        0,
                        ImmutableMultimap.of());
                stageExecution.schedulingComplete();
                remoteTask.ifPresent(task -> coordinatorTaskManager.addSourceTaskFailureListener(task.getTaskId(), failureReporter));
                if (queryStateMachine.getQueryState() == STARTING && remoteTask.isPresent()) {
                    queryStateMachine.transitionToRunning();
                }
            }
        }

        public Map<PlanFragmentId, PipelinedOutputBufferManager> getOutputBuffersForStagesConsumedByCoordinator()
        {
            return outputBuffersForStagesConsumedByCoordinator;
        }

        public Map<PlanFragmentId, Optional<int[]>> getBucketToPartitionForStagesConsumedByCoordinator()
        {
            return bucketToPartitionForStagesConsumedByCoordinator;
        }

        public TaskLifecycleListener getTaskLifecycleListener()
        {
            return taskLifecycleListener;
        }

        public void cancelStage(StageId stageId)
        {
            for (StageExecution stageExecution : stageExecutions) {
                if (stageExecution.getStageId().equals(stageId)) {
                    stageExecution.cancel();
                }
            }
        }

        public void cancel()
        {
            stageExecutions.forEach(StageExecution::cancel);
        }

        public void abort()
        {
            stageExecutions.forEach(StageExecution::abort);
        }
    }

    private static class TaskFailureReporter
            implements TaskFailureListener
    {
        private final AtomicReference<DistributedStagesScheduler> distributedStagesScheduler;

        private TaskFailureReporter(AtomicReference<DistributedStagesScheduler> distributedStagesScheduler)
        {
            this.distributedStagesScheduler = distributedStagesScheduler;
        }

        @Override
        public void onTaskFailed(TaskId taskId, Throwable failure)
        {
            if (failure instanceof TrinoException && REMOTE_TASK_FAILED.toErrorCode().equals(((TrinoException) failure).getErrorCode())) {
                // This error indicates that a downstream task was trying to fetch results from an upstream task that is marked as failed
                // Instead of failing a downstream task let the coordinator handle and report the failure of an upstream task to ensure correct error reporting
                log.debug("Task failure discovered while fetching task results: %s", taskId);
                return;
            }
            log.warn(failure, "Reported task failure: %s", taskId);
            DistributedStagesScheduler scheduler = this.distributedStagesScheduler.get();
            if (scheduler != null) {
                scheduler.reportTaskFailure(taskId, failure);
            }
        }
    }

    /**
     * Scheduler for stages executed on workers.
     * <p>
     * As opposed to {@link CoordinatorStagesScheduler} this component is designed
     * to facilitate failure recovery.
     * <p>
     * In an event of a failure the system may decide to terminate an active scheduler
     * and create a new one to initiate a new query attempt.
     * <p>
     * Stages scheduled by this scheduler are assumed to be safe to retry.
     * <p>
     * The implementation is responsible for task creation and orchestration as well as
     * split enumeration, split assignment and state transitioning for the tasks scheduled.
     */
    private static class DistributedStagesScheduler
    {
        private final DistributedStagesSchedulerStateMachine stateMachine;
        private final QueryStateMachine queryStateMachine;
        private final SplitSchedulerStats schedulerStats;
        private final StageManager stageManager;
        private final ExecutionSchedule executionSchedule;
        private final Map<StageId, StageScheduler> stageSchedulers;
        private final Map<StageId, StageExecution> stageExecutions;
        private final DynamicFilterService dynamicFilterService;

        private final AtomicBoolean started = new AtomicBoolean();

        public static DistributedStagesScheduler create(
                QueryStateMachine queryStateMachine,
                SplitSchedulerStats schedulerStats,
                NodeScheduler nodeScheduler,
                NodePartitioningManager nodePartitioningManager,
                StageManager stageManager,
                CoordinatorStagesScheduler coordinatorStagesScheduler,
                ExecutionPolicy executionPolicy,
                FailureDetector failureDetector,
                ScheduledExecutorService executor,
                SplitSourceFactory splitSourceFactory,
                int splitBatchSize,
                DynamicFilterService dynamicFilterService,
                TableExecuteContextManager tableExecuteContextManager,
                RetryPolicy retryPolicy,
                int attempt)
        {
            DistributedStagesSchedulerStateMachine stateMachine = new DistributedStagesSchedulerStateMachine(queryStateMachine.getQueryId(), executor);

            Map<PartitioningHandle, NodePartitionMap> partitioningCacheMap = new HashMap<>();
            BiFunction<PartitioningHandle, Optional<Integer>, NodePartitionMap> partitioningCache = (partitioningHandle, partitionCount) ->
                    partitioningCacheMap.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(
                            queryStateMachine.getSession(),
                            // TODO: support hash distributed writer scaling (https://github.com/trinodb/trino/issues/10791)
                            handle.equals(SCALED_WRITER_HASH_DISTRIBUTION) ? FIXED_HASH_DISTRIBUTION : handle,
                            partitionCount));

            Map<PlanFragmentId, Optional<int[]>> bucketToPartitionMap = createBucketToPartitionMap(
                    coordinatorStagesScheduler.getBucketToPartitionForStagesConsumedByCoordinator(),
                    stageManager,
                    partitioningCache);
            Map<PlanFragmentId, PipelinedOutputBufferManager> outputBufferManagers = createOutputBufferManagers(
                    coordinatorStagesScheduler.getOutputBuffersForStagesConsumedByCoordinator(),
                    stageManager,
                    bucketToPartitionMap);

            TaskLifecycleListener coordinatorTaskLifecycleListener = coordinatorStagesScheduler.getTaskLifecycleListener();
            if (retryPolicy != RetryPolicy.NONE) {
                // when retries are enabled only close exchange clients on coordinator when the query is finished
                TaskLifecycleListenerBridge taskLifecycleListenerBridge = new TaskLifecycleListenerBridge(coordinatorTaskLifecycleListener);
                coordinatorTaskLifecycleListener = taskLifecycleListenerBridge;
                stateMachine.addStateChangeListener(state -> {
                    if (state == DistributedStagesSchedulerState.FINISHED) {
                        taskLifecycleListenerBridge.notifyNoMoreSourceTasks();
                    }
                });
            }

            // Preserve topological ordering in stageExecutionsMap
            Map<StageId, StageExecution> stageExecutions = new LinkedHashMap<>();
            for (SqlStage stage : stageManager.getDistributedStagesInTopologicalOrder()) {
                Optional<SqlStage> parentStage = stageManager.getParent(stage.getStageId());
                TaskLifecycleListener taskLifecycleListener;
                if (parentStage.isEmpty() || parentStage.get().getFragment().getPartitioning().isCoordinatorOnly()) {
                    // output will be consumed by coordinator
                    taskLifecycleListener = coordinatorTaskLifecycleListener;
                }
                else {
                    StageId parentStageId = parentStage.get().getStageId();
                    StageExecution parentStageExecution = requireNonNull(stageExecutions.get(parentStageId), () -> "execution is null for stage: " + parentStageId);
                    taskLifecycleListener = parentStageExecution.getTaskLifecycleListener();
                }

                PlanFragment fragment = stage.getFragment();
                StageExecution stageExecution = createPipelinedStageExecution(
                        stageManager.get(fragment.getId()),
                        outputBufferManagers,
                        taskLifecycleListener,
                        failureDetector,
                        executor,
                        bucketToPartitionMap.get(fragment.getId()),
                        attempt);
                stageExecutions.put(stage.getStageId(), stageExecution);
            }

            ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers = ImmutableMap.builder();
            for (StageExecution stageExecution : stageExecutions.values()) {
                List<StageExecution> children = stageManager.getChildren(stageExecution.getStageId()).stream()
                        .map(stage -> requireNonNull(stageExecutions.get(stage.getStageId()), () -> "stage execution not found for stage: " + stage))
                        .collect(toImmutableList());
                StageScheduler scheduler = createStageScheduler(
                        queryStateMachine,
                        stageExecution,
                        splitSourceFactory,
                        children,
                        partitioningCache,
                        nodeScheduler,
                        nodePartitioningManager,
                        splitBatchSize,
                        dynamicFilterService,
                        executor,
                        tableExecuteContextManager);
                stageSchedulers.put(stageExecution.getStageId(), scheduler);
            }

            DistributedStagesScheduler distributedStagesScheduler = new DistributedStagesScheduler(
                    stateMachine,
                    queryStateMachine,
                    schedulerStats,
                    stageManager,
                    executionPolicy.createExecutionSchedule(stageExecutions.values()),
                    stageSchedulers.buildOrThrow(),
                    ImmutableMap.copyOf(stageExecutions),
                    dynamicFilterService);
            distributedStagesScheduler.initialize();
            return distributedStagesScheduler;
        }

        private static Map<PlanFragmentId, Optional<int[]>> createBucketToPartitionMap(
                Map<PlanFragmentId, Optional<int[]>> bucketToPartitionForStagesConsumedByCoordinator,
                StageManager stageManager,
                BiFunction<PartitioningHandle, Optional<Integer>, NodePartitionMap> partitioningCache)
        {
            ImmutableMap.Builder<PlanFragmentId, Optional<int[]>> result = ImmutableMap.builder();
            result.putAll(bucketToPartitionForStagesConsumedByCoordinator);
            for (SqlStage stage : stageManager.getDistributedStagesInTopologicalOrder()) {
                PlanFragment fragment = stage.getFragment();
                Optional<int[]> bucketToPartition = getBucketToPartition(
                        fragment.getPartitioning(),
                        partitioningCache,
                        fragment.getRoot(),
                        fragment.getRemoteSourceNodes(),
                        fragment.getPartitionCount());
                for (SqlStage childStage : stageManager.getChildren(stage.getStageId())) {
                    result.put(childStage.getFragment().getId(), bucketToPartition);
                }
            }
            return result.buildOrThrow();
        }

        private static Optional<int[]> getBucketToPartition(
                PartitioningHandle partitioningHandle,
                BiFunction<PartitioningHandle, Optional<Integer>, NodePartitionMap> partitioningCache,
                PlanNode fragmentRoot,
                List<RemoteSourceNode> remoteSourceNodes,
                Optional<Integer> partitionCount)
        {
            if (partitioningHandle.equals(SOURCE_DISTRIBUTION) || partitioningHandle.equals(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION)) {
                return Optional.of(new int[1]);
            }
            if (searchFrom(fragmentRoot).where(node -> node instanceof TableScanNode).findFirst().isPresent()) {
                if (remoteSourceNodes.stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                    return Optional.empty();
                }
                // remote source requires nodePartitionMap
                NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle, partitionCount);
                return Optional.of(nodePartitionMap.getBucketToPartition());
            }
            NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle, partitionCount);
            List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
            // todo this should asynchronously wait a standard timeout period before failing
            checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
            return Optional.of(nodePartitionMap.getBucketToPartition());
        }

        private static Map<PlanFragmentId, PipelinedOutputBufferManager> createOutputBufferManagers(
                Map<PlanFragmentId, PipelinedOutputBufferManager> outputBuffersForStagesConsumedByCoordinator,
                StageManager stageManager,
                Map<PlanFragmentId, Optional<int[]>> bucketToPartitionMap)
        {
            ImmutableMap.Builder<PlanFragmentId, PipelinedOutputBufferManager> result = ImmutableMap.builder();
            result.putAll(outputBuffersForStagesConsumedByCoordinator);
            for (SqlStage parentStage : stageManager.getDistributedStagesInTopologicalOrder()) {
                for (SqlStage childStage : stageManager.getChildren(parentStage.getStageId())) {
                    PlanFragmentId fragmentId = childStage.getFragment().getId();
                    PartitioningHandle partitioningHandle = childStage.getFragment().getOutputPartitioningScheme().getPartitioning().getHandle();

                    PipelinedOutputBufferManager outputBufferManager;
                    if (partitioningHandle.equals(FIXED_BROADCAST_DISTRIBUTION)) {
                        outputBufferManager = new BroadcastPipelinedOutputBufferManager();
                    }
                    else if (partitioningHandle.equals(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION)) {
                        outputBufferManager = new ScaledPipelinedOutputBufferManager();
                    }
                    else {
                        Optional<int[]> bucketToPartition = bucketToPartitionMap.get(fragmentId);
                        checkArgument(bucketToPartition.isPresent(), "bucketToPartition is expected to be present for fragment: %s", fragmentId);
                        int partitionCount = Ints.max(bucketToPartition.get()) + 1;
                        outputBufferManager = new PartitionedPipelinedOutputBufferManager(partitioningHandle, partitionCount);
                    }
                    result.put(fragmentId, outputBufferManager);
                }
            }
            return result.buildOrThrow();
        }

        private static StageScheduler createStageScheduler(
                QueryStateMachine queryStateMachine,
                StageExecution stageExecution,
                SplitSourceFactory splitSourceFactory,
                List<StageExecution> childStageExecutions,
                BiFunction<PartitioningHandle, Optional<Integer>, NodePartitionMap> partitioningCache,
                NodeScheduler nodeScheduler,
                NodePartitioningManager nodePartitioningManager,
                int splitBatchSize,
                DynamicFilterService dynamicFilterService,
                ScheduledExecutorService executor,
                TableExecuteContextManager tableExecuteContextManager)
        {
            Session session = queryStateMachine.getSession();
            PlanFragment fragment = stageExecution.getFragment();
            PartitioningHandle partitioningHandle = fragment.getPartitioning();
            Optional<Integer> partitionCount = fragment.getPartitionCount();
            Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(session, fragment);
            if (!splitSources.isEmpty()) {
                queryStateMachine.addStateChangeListener(new StateChangeListener<>()
                {
                    private final AtomicReference<Collection<SplitSource>> splitSourcesReference = new AtomicReference<>(splitSources.values());

                    @Override
                    public void stateChanged(QueryState newState)
                    {
                        if (newState.isDone()) {
                            // ensure split sources are closed and release memory
                            Collection<SplitSource> sources = splitSourcesReference.getAndSet(null);
                            if (sources != null) {
                                closeSplitSources(sources);
                            }
                        }
                    }
                });
            }

            if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
                // nodes are selected dynamically based on the constraints of the splits and the system load
                Entry<PlanNodeId, SplitSource> entry = getOnlyElement(splitSources.entrySet());
                PlanNodeId planNodeId = entry.getKey();
                SplitSource splitSource = entry.getValue();
                Optional<CatalogHandle> catalogHandle = Optional.of(splitSource.getCatalogHandle())
                        .filter(catalog -> !catalog.getType().isInternal());
                NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, catalogHandle);
                SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stageExecution::getAllTasks);

                return newSourcePartitionedSchedulerAsStageScheduler(
                        stageExecution,
                        planNodeId,
                        splitSource,
                        placementPolicy,
                        splitBatchSize,
                        dynamicFilterService,
                        tableExecuteContextManager,
                        () -> childStageExecutions.stream().anyMatch(StageExecution::isAnyTaskBlocked));
            }

            if (partitioningHandle.equals(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION)) {
                Supplier<Collection<TaskStatus>> sourceTasksProvider = () -> childStageExecutions.stream()
                        .map(StageExecution::getTaskStatuses)
                        .flatMap(List::stream)
                        .collect(toImmutableList());
                Supplier<Collection<TaskStatus>> writerTasksProvider = stageExecution::getTaskStatuses;

                ScaledWriterScheduler scheduler = new ScaledWriterScheduler(
                        stageExecution,
                        sourceTasksProvider,
                        writerTasksProvider,
                        nodeScheduler.createNodeSelector(session, Optional.empty()),
                        executor,
                        getWriterMinSize(session));

                whenAllStages(childStageExecutions, StageExecution.State::isDone)
                        .addListener(scheduler::finish, directExecutor());

                return scheduler;
            }

            if (splitSources.isEmpty()) {
                // all sources are remote
                NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle, partitionCount);
                List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
                // todo this should asynchronously wait a standard timeout period before failing
                checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
                return new FixedCountScheduler(stageExecution, partitionToNode);
            }

            // contains local source
            List<PlanNodeId> schedulingOrder = fragment.getPartitionedSources();
            Optional<CatalogHandle> catalogHandle = partitioningHandle.getCatalogHandle();
            checkArgument(catalogHandle.isPresent(), "No catalog handle for partitioning handle: %s", partitioningHandle);

            BucketNodeMap bucketNodeMap;
            List<InternalNode> stageNodeList;
            if (fragment.getRemoteSourceNodes().stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                // no remote source
                bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle);
                stageNodeList = new ArrayList<>(nodeScheduler.createNodeSelector(session, catalogHandle).allNodes());
                Collections.shuffle(stageNodeList);
            }
            else {
                // remote source requires nodePartitionMap
                NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle, partitionCount);
                stageNodeList = nodePartitionMap.getPartitionToNode();
                bucketNodeMap = nodePartitionMap.asBucketNodeMap();
            }

            return new FixedSourcePartitionedScheduler(
                    stageExecution,
                    splitSources,
                    schedulingOrder,
                    stageNodeList,
                    bucketNodeMap,
                    splitBatchSize,
                    nodeScheduler.createNodeSelector(session, catalogHandle),
                    dynamicFilterService,
                    tableExecuteContextManager);
        }

        private static void closeSplitSources(Collection<SplitSource> splitSources)
        {
            for (SplitSource source : splitSources) {
                try {
                    source.close();
                }
                catch (Throwable t) {
                    log.warn(t, "Error closing split source");
                }
            }
        }

        private static ListenableFuture<Void> whenAllStages(Collection<StageExecution> stages, Predicate<StageExecution.State> predicate)
        {
            checkArgument(!stages.isEmpty(), "stages is empty");
            Set<StageId> stageIds = stages.stream()
                    .map(StageExecution::getStageId)
                    .collect(toCollection(Sets::newConcurrentHashSet));
            SettableFuture<Void> future = SettableFuture.create();

            for (StageExecution stageExecution : stages) {
                stageExecution.addStateChangeListener(state -> {
                    if (predicate.test(state) && stageIds.remove(stageExecution.getStageId()) && stageIds.isEmpty()) {
                        future.set(null);
                    }
                });
            }

            return future;
        }

        private DistributedStagesScheduler(
                DistributedStagesSchedulerStateMachine stateMachine,
                QueryStateMachine queryStateMachine,
                SplitSchedulerStats schedulerStats,
                StageManager stageManager,
                ExecutionSchedule executionSchedule,
                Map<StageId, StageScheduler> stageSchedulers,
                Map<StageId, StageExecution> stageExecutions,
                DynamicFilterService dynamicFilterService)
        {
            this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.stageManager = requireNonNull(stageManager, "stageManager is null");
            this.executionSchedule = requireNonNull(executionSchedule, "executionSchedule is null");
            this.stageSchedulers = ImmutableMap.copyOf(requireNonNull(stageSchedulers, "stageSchedulers is null"));
            this.stageExecutions = ImmutableMap.copyOf(requireNonNull(stageExecutions, "stageExecutions is null"));
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        }

        private void initialize()
        {
            for (StageExecution stageExecution : stageExecutions.values()) {
                List<StageExecution> childStageExecutions = stageManager.getChildren(stageExecution.getStageId()).stream()
                        .map(stage -> requireNonNull(stageExecutions.get(stage.getStageId()), () -> "stage execution not found for stage: " + stage))
                        .collect(toImmutableList());
                if (!childStageExecutions.isEmpty()) {
                    stageExecution.addStateChangeListener(newState -> {
                        if (newState == FLUSHING || newState.isDone()) {
                            childStageExecutions.forEach(StageExecution::cancel);
                        }
                    });
                }
            }

            Set<StageId> finishedStages = newConcurrentHashSet();
            for (StageExecution stageExecution : stageExecutions.values()) {
                stageExecution.addStateChangeListener(state -> {
                    if (stateMachine.getState().isDone()) {
                        return;
                    }
                    int numberOfTasks = stageExecution.getAllTasks().size();
                    if (!state.canScheduleMoreTasks()) {
                        dynamicFilterService.stageCannotScheduleMoreTasks(stageExecution.getStageId(), stageExecution.getAttemptId(), numberOfTasks);
                    }
                    if (state == FAILED) {
                        RuntimeException failureCause = stageExecution.getFailureCause()
                                .map(ExecutionFailureInfo::toException)
                                .orElseGet(() -> new VerifyException(format("stage execution for stage %s is failed by failure cause is not present", stageExecution.getStageId())));
                        fail(failureCause, Optional.of(stageExecution.getStageId()));
                    }
                    else if (state.isDone()) {
                        finishedStages.add(stageExecution.getStageId());
                        if (finishedStages.containsAll(stageExecutions.keySet())) {
                            stateMachine.transitionToFinished();
                        }
                    }
                });
            }
        }

        public void schedule()
        {
            checkState(started.compareAndSet(false, true), "already started");

            try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
                stageSchedulers.values().forEach(StageScheduler::start);
                while (!executionSchedule.isFinished()) {
                    List<ListenableFuture<Void>> blockedStages = new ArrayList<>();
                    StagesScheduleResult stagesScheduleResult = executionSchedule.getStagesToSchedule();
                    for (StageExecution stageExecution : stagesScheduleResult.getStagesToSchedule()) {
                        stageExecution.beginScheduling();

                        // perform some scheduling work
                        ScheduleResult result = stageSchedulers.get(stageExecution.getStageId())
                                .schedule();

                        if (stateMachine.getState() == DistributedStagesSchedulerState.PLANNED && stageExecution.getAllTasks().size() > 0) {
                            stateMachine.transitionToRunning();
                        }

                        // modify parent and children based on the results of the scheduling
                        if (result.isFinished()) {
                            stageExecution.schedulingComplete();
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
                                default:
                                    throw new UnsupportedOperationException("Unknown blocked reason: " + result.getBlockedReason().get());
                            }
                        }
                    }

                    // wait for a state change and then schedule again
                    if (!blockedStages.isEmpty()) {
                        ImmutableList.Builder<ListenableFuture<Void>> futures = ImmutableList.builder();
                        futures.addAll(blockedStages);
                        // allow for schedule to resume scheduling (e.g. when some active stage completes
                        // and dependent stages can be started)
                        stagesScheduleResult.getRescheduleFuture().ifPresent(futures::add);
                        try (TimeStat.BlockTimer timer = schedulerStats.getSleepTime().time()) {
                            tryGetFutureValue(whenAnyComplete(futures.build()), 1, SECONDS);
                        }
                        for (ListenableFuture<Void> blockedStage : blockedStages) {
                            blockedStage.cancel(true);
                        }
                    }
                }

                for (StageExecution stageExecution : stageExecutions.values()) {
                    StageExecution.State state = stageExecution.getState();
                    if (state != SCHEDULED && state != RUNNING && state != FLUSHING && !state.isDone()) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Scheduling is complete, but stage %s is in state %s", stageExecution.getStageId(), state));
                    }
                }
            }
            catch (Throwable t) {
                fail(t, Optional.empty());
            }
            finally {
                RuntimeException closeError = new RuntimeException();
                for (StageScheduler scheduler : stageSchedulers.values()) {
                    try {
                        scheduler.close();
                    }
                    catch (Throwable t) {
                        fail(t, Optional.empty());
                        // Self-suppression not permitted
                        if (closeError != t) {
                            closeError.addSuppressed(t);
                        }
                    }
                }
            }
        }

        public void cancelStage(StageId stageId)
        {
            StageExecution stageExecution = stageExecutions.get(stageId);
            if (stageExecution != null) {
                stageExecution.cancel();
            }
        }

        public void cancel()
        {
            stateMachine.transitionToCanceled();
            stageExecutions.values().forEach(StageExecution::cancel);
        }

        public void abort()
        {
            stateMachine.transitionToAborted();
            stageExecutions.values().forEach(StageExecution::abort);
        }

        public void fail(Throwable failureCause, Optional<StageId> failedStageId)
        {
            stateMachine.transitionToFailed(failureCause, failedStageId);
            stageExecutions.values().forEach(StageExecution::abort);
        }

        public void reportTaskFailure(TaskId taskId, Throwable failureCause)
        {
            StageExecution stageExecution = stageExecutions.get(taskId.getStageId());
            if (stageExecution == null) {
                return;
            }

            List<RemoteTask> tasks = stageExecution.getAllTasks();
            if (tasks.stream().noneMatch(task -> task.getTaskId().equals(taskId))) {
                return;
            }

            stageExecution.failTask(taskId, failureCause);
            stateMachine.transitionToFailed(failureCause, Optional.of(taskId.getStageId()));
            stageExecutions.values().forEach(StageExecution::abort);
        }

        public void addStateChangeListener(StateChangeListener<DistributedStagesSchedulerState> stateChangeListener)
        {
            stateMachine.addStateChangeListener(stateChangeListener);
        }

        public Optional<StageFailureInfo> getFailureCause()
        {
            return stateMachine.getFailureCause();
        }
    }

    private enum DistributedStagesSchedulerState
    {
        PLANNED(false, false),
        RUNNING(false, false),
        FINISHED(true, false),
        CANCELED(true, false),
        ABORTED(true, true),
        FAILED(true, true);

        public static final Set<DistributedStagesSchedulerState> TERMINAL_STATES = Stream.of(DistributedStagesSchedulerState.values()).filter(DistributedStagesSchedulerState::isDone).collect(toImmutableSet());

        private final boolean doneState;
        private final boolean failureState;

        DistributedStagesSchedulerState(boolean doneState, boolean failureState)
        {
            checkArgument(!failureState || doneState, "%s is a non-done failure state", name());
            this.doneState = doneState;
            this.failureState = failureState;
        }

        /**
         * Is this a terminal state.
         */
        public boolean isDone()
        {
            return doneState;
        }

        /**
         * Is this a non-success terminal state.
         */
        public boolean isFailure()
        {
            return failureState;
        }
    }

    private static class DistributedStagesSchedulerStateMachine
    {
        private final QueryId queryId;
        private final StateMachine<DistributedStagesSchedulerState> state;
        private final AtomicReference<StageFailureInfo> failureCause = new AtomicReference<>();

        public DistributedStagesSchedulerStateMachine(QueryId queryId, Executor executor)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            requireNonNull(executor, "executor is null");
            state = new StateMachine<>("Distributed stages scheduler", executor, DistributedStagesSchedulerState.PLANNED, DistributedStagesSchedulerState.TERMINAL_STATES);
        }

        public DistributedStagesSchedulerState getState()
        {
            return state.get();
        }

        public boolean transitionToRunning()
        {
            return state.setIf(DistributedStagesSchedulerState.RUNNING, currentState -> !currentState.isDone());
        }

        public boolean transitionToFinished()
        {
            return state.setIf(DistributedStagesSchedulerState.FINISHED, currentState -> !currentState.isDone());
        }

        public boolean transitionToCanceled()
        {
            return state.setIf(DistributedStagesSchedulerState.CANCELED, currentState -> !currentState.isDone());
        }

        public boolean transitionToAborted()
        {
            return state.setIf(DistributedStagesSchedulerState.ABORTED, currentState -> !currentState.isDone());
        }

        public boolean transitionToFailed(Throwable throwable, Optional<StageId> failedStageId)
        {
            requireNonNull(throwable, "throwable is null");

            failureCause.compareAndSet(null, new StageFailureInfo(toFailure(throwable), failedStageId));
            boolean failed = state.setIf(DistributedStagesSchedulerState.FAILED, currentState -> !currentState.isDone());
            if (failed) {
                log.error(throwable, "Failure in distributed stage for query %s", queryId);
            }
            else {
                log.debug(throwable, "Failure in distributed stage for query %s after finished", queryId);
            }
            return failed;
        }

        public Optional<StageFailureInfo> getFailureCause()
        {
            return Optional.ofNullable(failureCause.get());
        }

        /**
         * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
         * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
         * possible notifications are observed out of order due to the asynchronous execution.
         */
        public void addStateChangeListener(StateChangeListener<DistributedStagesSchedulerState> stateChangeListener)
        {
            state.addStateChangeListener(stateChangeListener);
        }
    }

    private static class TaskLifecycleListenerBridge
            implements TaskLifecycleListener
    {
        private final TaskLifecycleListener listener;

        @GuardedBy("this")
        private final Set<PlanFragmentId> noMoreSourceTasks = new HashSet<>();
        @GuardedBy("this")
        private boolean done;

        private TaskLifecycleListenerBridge(TaskLifecycleListener listener)
        {
            this.listener = requireNonNull(listener, "listener is null");
        }

        @Override
        public synchronized void taskCreated(PlanFragmentId fragmentId, RemoteTask task)
        {
            checkState(!done, "unexpected state");
            listener.taskCreated(fragmentId, task);
        }

        @Override
        public synchronized void noMoreTasks(PlanFragmentId fragmentId)
        {
            checkState(!done, "unexpected state");
            noMoreSourceTasks.add(fragmentId);
        }

        public synchronized void notifyNoMoreSourceTasks()
        {
            checkState(!done, "unexpected state");
            done = true;
            noMoreSourceTasks.forEach(listener::noMoreTasks);
        }
    }

    private static class StageFailureInfo
    {
        private final ExecutionFailureInfo failureInfo;
        private final Optional<StageId> failedStageId;

        private StageFailureInfo(ExecutionFailureInfo failureInfo, Optional<StageId> failedStageId)
        {
            this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
            this.failedStageId = requireNonNull(failedStageId, "failedStageId is null");
        }

        public ExecutionFailureInfo getFailureInfo()
        {
            return failureInfo;
        }

        public Optional<StageId> getFailedStageId()
        {
            return failedStageId;
        }
    }
}
