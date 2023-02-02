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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.units.Duration;
import io.trino.event.SplitMonitor;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.executor.TaskExecutor;
import io.trino.execution.executor.TaskHandle;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.operator.DriverStats;
import io.trino.operator.PipelineContext;
import io.trino.operator.TaskContext;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.SystemSessionProperties.getInitialSplitsPerNode;
import static io.trino.SystemSessionProperties.getMaxDriversPerTask;
import static io.trino.SystemSessionProperties.getSplitConcurrencyAdjustmentInterval;
import static io.trino.execution.SqlTaskExecution.SplitsState.ADDING_SPLITS;
import static io.trino.execution.SqlTaskExecution.SplitsState.FINISHED;
import static io.trino.execution.SqlTaskExecution.SplitsState.NO_MORE_SPLITS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class SqlTaskExecution
{
    private final TaskId taskId;
    private final TaskStateMachine taskStateMachine;
    private final TaskContext taskContext;
    private final OutputBuffer outputBuffer;

    private final TaskHandle taskHandle;
    private final TaskExecutor taskExecutor;

    private final Executor notificationExecutor;

    private final SplitMonitor splitMonitor;

    private final Map<PlanNodeId, DriverSplitRunnerFactory> driverRunnerFactoriesWithSplitLifeCycle;
    private final List<DriverSplitRunnerFactory> driverRunnerFactoriesWithTaskLifeCycle;
    private final Map<PlanNodeId, DriverSplitRunnerFactory> driverRunnerFactoriesWithRemoteSource;

    @GuardedBy("this")
    private final Map<PlanNodeId, Long> maxAcknowledgedSplitByPlanNode = new HashMap<>();

    @GuardedBy("this")
    private final List<PlanNodeId> sourceStartOrder;
    @GuardedBy("this")
    private int schedulingPlanNodeOrdinal;

    @GuardedBy("this")
    private final Map<PlanNodeId, PendingSplitsForPlanNode> pendingSplitsByPlanNode;

    // number of created PrioritizedSplitRunners that haven't yet finished
    private final AtomicLong remainingSplitRunners = new AtomicLong();

    public SqlTaskExecution(
            TaskStateMachine taskStateMachine,
            TaskContext taskContext,
            OutputBuffer outputBuffer,
            LocalExecutionPlan localExecutionPlan,
            TaskExecutor taskExecutor,
            SplitMonitor splitMonitor,
            Executor notificationExecutor)
    {
        this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null");
        this.taskId = taskStateMachine.getTaskId();
        this.taskContext = requireNonNull(taskContext, "taskContext is null");
        this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");

        this.taskExecutor = requireNonNull(taskExecutor, "taskExecutor is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");

        this.splitMonitor = requireNonNull(splitMonitor, "splitMonitor is null");

        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            // index driver factories
            Set<PlanNodeId> partitionedSources = ImmutableSet.copyOf(localExecutionPlan.getPartitionedSourceOrder());
            ImmutableMap.Builder<PlanNodeId, DriverSplitRunnerFactory> driverRunnerFactoriesWithSplitLifeCycle = ImmutableMap.builder();
            ImmutableList.Builder<DriverSplitRunnerFactory> driverRunnerFactoriesWithTaskLifeCycle = ImmutableList.builder();
            ImmutableMap.Builder<PlanNodeId, DriverSplitRunnerFactory> driverRunnerFactoriesWithRemoteSource = ImmutableMap.builder();
            for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
                Optional<PlanNodeId> sourceId = driverFactory.getSourceId();
                if (sourceId.isPresent() && partitionedSources.contains(sourceId.get())) {
                    driverRunnerFactoriesWithSplitLifeCycle.put(sourceId.get(), new DriverSplitRunnerFactory(driverFactory, true));
                }
                else {
                    DriverSplitRunnerFactory runnerFactory = new DriverSplitRunnerFactory(driverFactory, false);
                    sourceId.ifPresent(planNodeId -> driverRunnerFactoriesWithRemoteSource.put(planNodeId, runnerFactory));
                    driverRunnerFactoriesWithTaskLifeCycle.add(runnerFactory);
                }
            }
            this.driverRunnerFactoriesWithSplitLifeCycle = driverRunnerFactoriesWithSplitLifeCycle.buildOrThrow();
            this.driverRunnerFactoriesWithTaskLifeCycle = driverRunnerFactoriesWithTaskLifeCycle.build();
            this.driverRunnerFactoriesWithRemoteSource = driverRunnerFactoriesWithRemoteSource.buildOrThrow();

            this.pendingSplitsByPlanNode = this.driverRunnerFactoriesWithSplitLifeCycle.keySet().stream()
                    .collect(toImmutableMap(identity(), ignore -> new PendingSplitsForPlanNode()));
            sourceStartOrder = localExecutionPlan.getPartitionedSourceOrder();

            checkArgument(this.driverRunnerFactoriesWithSplitLifeCycle.keySet().equals(partitionedSources),
                    "Fragment is partitioned, but not all partitioned drivers were found");

            // don't register the task if it is already completed (most likely failed during planning above)
            if (!taskStateMachine.getState().isDone()) {
                taskHandle = createTaskHandle(taskStateMachine, taskContext, outputBuffer, localExecutionPlan, taskExecutor);
            }
            else {
                taskHandle = null;
            }
        }
    }

    public void start()
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", getTaskId())) {
            // Task handle was not created because the task is already done, nothing to do
            if (taskHandle == null) {
                return;
            }
            // The scheduleDriversForTaskLifeCycle method calls enqueueDriverSplitRunner, which registers a callback with access to this object.
            // The call back is accessed from another thread, so this code cannot be placed in the constructor. This must also happen before outputBuffer
            // callbacks are registered to prevent a task completion check before task lifecycle splits are created
            scheduleDriversForTaskLifeCycle();
            // Output buffer state change listener callback must not run in the constructor to avoid leaking a reference to "this" across to another thread
            outputBuffer.addStateChangeListener(new CheckTaskCompletionOnBufferFinish(SqlTaskExecution.this));
        }
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private static TaskHandle createTaskHandle(
            TaskStateMachine taskStateMachine,
            TaskContext taskContext,
            OutputBuffer outputBuffer,
            LocalExecutionPlan localExecutionPlan,
            TaskExecutor taskExecutor)
    {
        TaskHandle taskHandle = taskExecutor.addTask(
                taskStateMachine.getTaskId(),
                outputBuffer::getUtilization,
                getInitialSplitsPerNode(taskContext.getSession()),
                getSplitConcurrencyAdjustmentInterval(taskContext.getSession()),
                getMaxDriversPerTask(taskContext.getSession()));
        taskStateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                taskExecutor.removeTask(taskHandle);
                for (DriverFactory factory : localExecutionPlan.getDriverFactories()) {
                    factory.noMoreDrivers();
                }
            }
        });
        return taskHandle;
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public TaskContext getTaskContext()
    {
        return taskContext;
    }

    public void addSplitAssignments(List<SplitAssignment> splitAssignments)
    {
        requireNonNull(splitAssignments, "splitAssignments is null");
        checkState(!Thread.holdsLock(this), "Cannot add split assignments while holding a lock on the %s", getClass().getSimpleName());

        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            // update our record of split assignments and schedule drivers for new partitioned splits
            Set<PlanNodeId> updatedUnpartitionedSources = updateSplitAssignments(splitAssignments);
            for (PlanNodeId planNodeId : updatedUnpartitionedSources) {
                DriverSplitRunnerFactory factory = driverRunnerFactoriesWithRemoteSource.get(planNodeId);
                // schedule splits outside the lock
                factory.scheduleSplits();
            }
            // we may have transitioned to no more splits, so check for completion
            checkTaskCompletion();
        }
    }

    private synchronized Set<PlanNodeId> updateSplitAssignments(List<SplitAssignment> splitAssignments)
    {
        ImmutableSet.Builder<PlanNodeId> updatedUnpartitionedSources = ImmutableSet.builder();
        List<SplitAssignment> unacknowledgedSplitAssignment = new ArrayList<>(splitAssignments.size());

        // first remove any split that was already acknowledged
        for (SplitAssignment splitAssignment : splitAssignments) {
            // drop assignments containing no unacknowledged splits
            // the noMoreSplits signal acknowledgement is not tracked but it is okay to deliver it more than once
            if (!splitAssignment.getSplits().isEmpty() || splitAssignment.isNoMoreSplits()) {
                PlanNodeId planNodeId = splitAssignment.getPlanNodeId();
                long currentMaxAcknowledgedSplit = maxAcknowledgedSplitByPlanNode.getOrDefault(planNodeId, Long.MIN_VALUE);
                long maxAcknowledgedSplit = currentMaxAcknowledgedSplit;
                ImmutableSet.Builder<ScheduledSplit> builder = ImmutableSet.builderWithExpectedSize(splitAssignment.getSplits().size());
                for (ScheduledSplit split : splitAssignment.getSplits()) {
                    long sequenceId = split.getSequenceId();
                    // previously acknowledged splits can be included in source
                    if (sequenceId > currentMaxAcknowledgedSplit) {
                        builder.add(split);
                    }
                    if (sequenceId > maxAcknowledgedSplit) {
                        maxAcknowledgedSplit = sequenceId;
                    }
                }
                if (maxAcknowledgedSplit > currentMaxAcknowledgedSplit) {
                    maxAcknowledgedSplitByPlanNode.put(planNodeId, maxAcknowledgedSplit);
                }

                Set<ScheduledSplit> newSplits = builder.build();
                // We may have filtered all splits out, so only proceed with updates if new splits are
                // present or noMoreSplits is set
                if (!newSplits.isEmpty() || splitAssignment.isNoMoreSplits()) {
                    unacknowledgedSplitAssignment.add(new SplitAssignment(splitAssignment.getPlanNodeId(), newSplits, splitAssignment.isNoMoreSplits()));
                }
            }
        }

        // update task with new sources
        for (SplitAssignment splitAssignment : unacknowledgedSplitAssignment) {
            if (driverRunnerFactoriesWithSplitLifeCycle.containsKey(splitAssignment.getPlanNodeId())) {
                schedulePartitionedSource(splitAssignment);
            }
            else {
                // tell existing drivers about the new splits
                DriverSplitRunnerFactory factory = driverRunnerFactoriesWithRemoteSource.get(splitAssignment.getPlanNodeId());
                factory.enqueueSplits(splitAssignment.getSplits(), splitAssignment.isNoMoreSplits());
                updatedUnpartitionedSources.add(splitAssignment.getPlanNodeId());
            }
        }

        return updatedUnpartitionedSources.build();
    }

    @GuardedBy("this")
    private void mergeIntoPendingSplits(PlanNodeId planNodeId, Set<ScheduledSplit> scheduledSplits, boolean noMoreSplits)
    {
        checkHoldsLock();

        DriverSplitRunnerFactory partitionedDriverFactory = driverRunnerFactoriesWithSplitLifeCycle.get(planNodeId);
        PendingSplitsForPlanNode pendingSplitsForPlanNode = pendingSplitsByPlanNode.get(planNodeId);

        partitionedDriverFactory.splitsAdded(scheduledSplits.size(), SplitWeight.rawValueSum(scheduledSplits, scheduledSplit -> scheduledSplit.getSplit().getSplitWeight()));
        for (ScheduledSplit scheduledSplit : scheduledSplits) {
            pendingSplitsForPlanNode.addSplit(scheduledSplit);
        }
        if (noMoreSplits) {
            pendingSplitsForPlanNode.setNoMoreSplits();
        }
    }

    private synchronized void schedulePartitionedSource(SplitAssignment splitAssignmentUpdate)
    {
        mergeIntoPendingSplits(splitAssignmentUpdate.getPlanNodeId(), splitAssignmentUpdate.getSplits(), splitAssignmentUpdate.isNoMoreSplits());

        while (schedulingPlanNodeOrdinal < sourceStartOrder.size()) {
            PlanNodeId schedulingPlanNode = sourceStartOrder.get(schedulingPlanNodeOrdinal);

            DriverSplitRunnerFactory partitionedDriverRunnerFactory = driverRunnerFactoriesWithSplitLifeCycle.get(schedulingPlanNode);

            PendingSplitsForPlanNode pendingSplits = pendingSplitsByPlanNode.get(schedulingPlanNode);

            // Enqueue driver runners with split lifecycle for this plan node and driver life cycle combination.
            Set<ScheduledSplit> removed = pendingSplits.removeAllSplits();
            ImmutableList.Builder<DriverSplitRunner> runners = ImmutableList.builderWithExpectedSize(removed.size());
            for (ScheduledSplit scheduledSplit : removed) {
                // create a new driver for the split
                runners.add(partitionedDriverRunnerFactory.createDriverRunner(scheduledSplit));
            }
            enqueueDriverSplitRunner(false, runners.build());

            // If all driver runners have been enqueued for this plan node and driver life cycle combination,
            // move on to the next plan node.
            if (pendingSplits.getState() != NO_MORE_SPLITS) {
                break;
            }
            partitionedDriverRunnerFactory.noMoreDriverRunner();
            pendingSplits.markAsCleanedUp();

            schedulingPlanNodeOrdinal++;
        }
    }

    private void scheduleDriversForTaskLifeCycle()
    {
        // This method is called at the beginning of the task.
        // It schedules drivers for all the pipelines that have task life cycle.
        List<DriverSplitRunner> runners = new ArrayList<>();
        for (DriverSplitRunnerFactory driverRunnerFactory : driverRunnerFactoriesWithTaskLifeCycle) {
            for (int i = 0; i < driverRunnerFactory.getDriverInstances().orElse(1); i++) {
                runners.add(driverRunnerFactory.createDriverRunner(null));
            }
        }
        enqueueDriverSplitRunner(true, runners);
        for (DriverSplitRunnerFactory driverRunnerFactory : driverRunnerFactoriesWithTaskLifeCycle) {
            driverRunnerFactory.noMoreDriverRunner();
            verify(driverRunnerFactory.isNoMoreDriverRunner());
        }
        checkTaskCompletion();
    }

    private synchronized void enqueueDriverSplitRunner(boolean forceRunSplit, List<DriverSplitRunner> runners)
    {
        // schedule driver to be executed
        List<ListenableFuture<Void>> finishedFutures = taskExecutor.enqueueSplits(taskHandle, forceRunSplit, runners);
        checkState(finishedFutures.size() == runners.size(), "Expected %s futures but got %s", runners.size(), finishedFutures.size());

        // record new split runners
        remainingSplitRunners.addAndGet(runners.size());

        // when split runner completes, update state and fire events
        for (int i = 0; i < finishedFutures.size(); i++) {
            ListenableFuture<Void> finishedFuture = finishedFutures.get(i);
            DriverSplitRunner splitRunner = runners.get(i);

            Futures.addCallback(finishedFuture, new FutureCallback<Object>()
            {
                @Override
                public void onSuccess(Object result)
                {
                    try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
                        // record driver is finished
                        if (remainingSplitRunners.decrementAndGet() == 0) {
                            checkTaskCompletion();
                        }

                        splitMonitor.splitCompletedEvent(taskId, getDriverStats());
                    }
                }

                @Override
                public void onFailure(Throwable cause)
                {
                    try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
                        taskStateMachine.failed(cause);

                        // record driver is finished
                        if (remainingSplitRunners.decrementAndGet() == 0) {
                            checkTaskCompletion();
                        }

                        // fire failed event with cause
                        splitMonitor.splitFailedEvent(taskId, getDriverStats(), cause);
                    }
                }

                private DriverStats getDriverStats()
                {
                    DriverContext driverContext = splitRunner.getDriverContext();
                    DriverStats driverStats;
                    if (driverContext != null) {
                        driverStats = driverContext.getDriverStats();
                    }
                    else {
                        // split runner did not start successfully
                        driverStats = new DriverStats();
                    }

                    return driverStats;
                }
            }, notificationExecutor);
        }
    }

    public synchronized Set<PlanNodeId> getNoMoreSplits()
    {
        ImmutableSet.Builder<PlanNodeId> noMoreSplits = ImmutableSet.builder();
        for (Map.Entry<PlanNodeId, DriverSplitRunnerFactory> entry : driverRunnerFactoriesWithSplitLifeCycle.entrySet()) {
            if (entry.getValue().isNoMoreDriverRunner()) {
                noMoreSplits.add(entry.getKey());
            }
        }
        for (Map.Entry<PlanNodeId, DriverSplitRunnerFactory> entry : driverRunnerFactoriesWithRemoteSource.entrySet()) {
            if (entry.getValue().isNoMoreSplits()) {
                noMoreSplits.add(entry.getKey());
            }
        }
        return noMoreSplits.build();
    }

    private synchronized void checkTaskCompletion()
    {
        if (taskStateMachine.getState().isDone()) {
            return;
        }

        // are there more drivers expected?
        for (DriverSplitRunnerFactory driverSplitRunnerFactory : concat(driverRunnerFactoriesWithTaskLifeCycle, driverRunnerFactoriesWithSplitLifeCycle.values())) {
            if (!driverSplitRunnerFactory.isNoMoreDrivers()) {
                return;
            }
        }
        // do we still have running tasks?
        if (remainingSplitRunners.get() != 0) {
            return;
        }

        // no more output will be created
        outputBuffer.setNoMorePages();

        BufferState bufferState = outputBuffer.getState();
        if (!bufferState.isTerminal()) {
            taskStateMachine.transitionToFlushing();
            return;
        }

        if (bufferState == BufferState.FINISHED) {
            // Cool! All done!
            taskStateMachine.finished();
            return;
        }

        if (bufferState == BufferState.FAILED) {
            Throwable failureCause = outputBuffer.getFailureCause()
                    .orElseGet(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "Output buffer is failed but the failure cause is missing"));
            taskStateMachine.failed(failureCause);
            return;
        }

        // The only terminal state that remains is ABORTED.
        // Buffer is expected to be aborted only if the task itself is aborted. In this scenario the following statement is expected to be noop.
        taskStateMachine.failed(new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected buffer state: " + bufferState));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskId)
                .add("remainingSplitRunners", remainingSplitRunners.get())
                .toString();
    }

    private void checkHoldsLock()
    {
        // This method serves a similar purpose at runtime as GuardedBy on method serves during static analysis.
        // This method should not have significant performance impact. If it does, it may be reasonably to remove this method.
        // This intentionally does not use checkState.
        if (!Thread.holdsLock(this)) {
            throw new IllegalStateException(format("Thread must hold a lock on the %s", getClass().getSimpleName()));
        }
    }

    // Splits for a particular plan node
    @NotThreadSafe
    private static class PendingSplitsForPlanNode
    {
        private Set<ScheduledSplit> splits = new HashSet<>();
        private SplitsState state = ADDING_SPLITS;
        private boolean noMoreSplits;

        public void setNoMoreSplits()
        {
            if (noMoreSplits) {
                return;
            }
            noMoreSplits = true;
            if (state == ADDING_SPLITS) {
                state = NO_MORE_SPLITS;
            }
        }

        public SplitsState getState()
        {
            return state;
        }

        public void addSplit(ScheduledSplit scheduledSplit)
        {
            checkState(state == ADDING_SPLITS);
            splits.add(scheduledSplit);
        }

        public Set<ScheduledSplit> removeAllSplits()
        {
            checkState(state == ADDING_SPLITS || state == NO_MORE_SPLITS);
            Set<ScheduledSplit> result = splits;
            splits = new HashSet<>();
            return result;
        }

        public void markAsCleanedUp()
        {
            checkState(splits.isEmpty());
            checkState(state == NO_MORE_SPLITS);
            state = FINISHED;
        }
    }

    enum SplitsState
    {
        ADDING_SPLITS,
        // All splits have been received from scheduler.
        // No more splits will be added to the pendingSplits set.
        NO_MORE_SPLITS,
        // All splits has been turned into DriverSplitRunner.
        FINISHED,
    }

    private class DriverSplitRunnerFactory
    {
        private final DriverFactory driverFactory;
        private final PipelineContext pipelineContext;

        // number of created DriverSplitRunners that haven't created underlying Driver
        private final AtomicInteger pendingCreations = new AtomicInteger();
        // true if no more DriverSplitRunners will be created
        private final AtomicBoolean noMoreDriverRunner = new AtomicBoolean();

        private final List<WeakReference<Driver>> driverReferences = new CopyOnWriteArrayList<>();
        private final Queue<ScheduledSplit> queuedSplits = new ConcurrentLinkedQueue<>();
        private final AtomicLong inFlightSplits = new AtomicLong();
        private final AtomicBoolean noMoreSplits = new AtomicBoolean();

        private DriverSplitRunnerFactory(DriverFactory driverFactory, boolean partitioned)
        {
            this.driverFactory = driverFactory;
            this.pipelineContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), partitioned);
        }

        // TODO: split this method into two: createPartitionedDriverRunner and createUnpartitionedDriverRunner.
        // The former will take two arguments, and the latter will take one. This will simplify the signature quite a bit.
        public DriverSplitRunner createDriverRunner(@Nullable ScheduledSplit partitionedSplit)
        {
            checkState(!noMoreDriverRunner.get(), "noMoreDriverRunner is set");
            pendingCreations.incrementAndGet();
            // create driver context immediately so the driver existence is recorded in the stats
            // the number of drivers is used to balance work across nodes
            long splitWeight = partitionedSplit == null ? 0 : partitionedSplit.getSplit().getSplitWeight().getRawValue();
            DriverContext driverContext = pipelineContext.addDriverContext(splitWeight);
            return new DriverSplitRunner(this, driverContext, partitionedSplit);
        }

        public Driver createDriver(DriverContext driverContext, @Nullable ScheduledSplit partitionedSplit)
        {
            Driver driver = driverFactory.createDriver(driverContext);

            try {
                if (partitionedSplit != null) {
                    // TableScanOperator requires partitioned split to be added before the first call to process
                    driver.updateSplitAssignment(new SplitAssignment(partitionedSplit.getPlanNodeId(), ImmutableSet.of(partitionedSplit), true));
                }

                if (pendingCreations.decrementAndGet() == 0) {
                    closeDriverFactoryIfFullyCreated();
                }

                if (driverFactory.getSourceId().isPresent() && partitionedSplit == null) {
                    driverReferences.add(new WeakReference<>(driver));
                    scheduleSplits();
                }

                return driver;
            }
            catch (Throwable failure) {
                try {
                    driver.close();
                }
                catch (Throwable closeFailure) {
                    if (failure != closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
                throw failure;
            }
        }

        public void enqueueSplits(Set<ScheduledSplit> splits, boolean noMoreSplits)
        {
            verify(driverFactory.getSourceId().isPresent(), "not a source driver");
            verify(!this.noMoreSplits.get() || splits.isEmpty(), "cannot add splits after noMoreSplits is set");
            queuedSplits.addAll(splits);
            verify(!this.noMoreSplits.get() || noMoreSplits, "cannot unset noMoreSplits");
            if (noMoreSplits) {
                this.noMoreSplits.set(true);
            }
        }

        public void scheduleSplits()
        {
            if (driverReferences.isEmpty()) {
                return;
            }

            PlanNodeId sourceId = driverFactory.getSourceId().orElseThrow();
            while (!queuedSplits.isEmpty()) {
                int activeDriversCount = 0;
                for (WeakReference<Driver> driverReference : driverReferences) {
                    Driver driver = driverReference.get();
                    if (driver == null) {
                        continue;
                    }
                    activeDriversCount++;
                    inFlightSplits.incrementAndGet();
                    ScheduledSplit split = queuedSplits.poll();
                    if (split == null) {
                        inFlightSplits.decrementAndGet();
                        break;
                    }
                    driver.updateSplitAssignment(new SplitAssignment(sourceId, ImmutableSet.of(split), false));
                    inFlightSplits.decrementAndGet();
                }
                if (activeDriversCount == 0) {
                    break;
                }
            }

            if (noMoreSplits.get() && queuedSplits.isEmpty() && inFlightSplits.get() == 0) {
                for (WeakReference<Driver> driverReference : driverReferences) {
                    Driver driver = driverReference.get();
                    if (driver != null) {
                        driver.updateSplitAssignment(new SplitAssignment(sourceId, ImmutableSet.of(), true));
                    }
                }
            }
        }

        public boolean isNoMoreSplits()
        {
            return noMoreSplits.get();
        }

        public void noMoreDriverRunner()
        {
            noMoreDriverRunner.set(true);
            closeDriverFactoryIfFullyCreated();
        }

        public boolean isNoMoreDriverRunner()
        {
            return noMoreDriverRunner.get();
        }

        public void closeDriverFactoryIfFullyCreated()
        {
            if (driverFactory.isNoMoreDrivers()) {
                return;
            }
            if (isNoMoreDriverRunner() && pendingCreations.get() == 0) {
                driverFactory.noMoreDrivers();
            }
        }

        public boolean isNoMoreDrivers()
        {
            return driverFactory.isNoMoreDrivers();
        }

        public OptionalInt getDriverInstances()
        {
            return driverFactory.getDriverInstances();
        }

        public void splitsAdded(int count, long weightSum)
        {
            pipelineContext.splitsAdded(count, weightSum);
        }
    }

    private static class DriverSplitRunner
            implements SplitRunner
    {
        private final DriverSplitRunnerFactory driverSplitRunnerFactory;
        private final DriverContext driverContext;

        @GuardedBy("this")
        private boolean closed;

        @Nullable
        private final ScheduledSplit partitionedSplit;

        @GuardedBy("this")
        private Driver driver;

        private DriverSplitRunner(DriverSplitRunnerFactory driverSplitRunnerFactory, DriverContext driverContext, @Nullable ScheduledSplit partitionedSplit)
        {
            this.driverSplitRunnerFactory = requireNonNull(driverSplitRunnerFactory, "driverSplitRunnerFactory is null");
            this.driverContext = requireNonNull(driverContext, "driverContext is null");
            this.partitionedSplit = partitionedSplit;
        }

        public synchronized DriverContext getDriverContext()
        {
            if (driver == null) {
                return null;
            }
            return driver.getDriverContext();
        }

        @Override
        public synchronized boolean isFinished()
        {
            if (closed) {
                return true;
            }

            return driver != null && driver.isFinished();
        }

        @Override
        public ListenableFuture<Void> processFor(Duration duration)
        {
            Driver driver;
            synchronized (this) {
                // if close() was called before we get here, there's not point in even creating the driver
                if (closed) {
                    return immediateVoidFuture();
                }

                if (this.driver == null) {
                    this.driver = driverSplitRunnerFactory.createDriver(driverContext, partitionedSplit);
                }

                driver = this.driver;
            }

            return driver.processForDuration(duration);
        }

        @Override
        public String getInfo()
        {
            return (partitionedSplit == null) ? "" : partitionedSplit.getSplit().getInfo().toString();
        }

        @Override
        public void close()
        {
            Driver driver;
            synchronized (this) {
                closed = true;
                driver = this.driver;
            }

            if (driver != null) {
                driver.close();
            }
        }
    }

    private static final class CheckTaskCompletionOnBufferFinish
            implements StateChangeListener<BufferState>
    {
        private final WeakReference<SqlTaskExecution> sqlTaskExecutionReference;

        public CheckTaskCompletionOnBufferFinish(SqlTaskExecution sqlTaskExecution)
        {
            // we are only checking for completion of the task, so don't hold up GC if the task is dead
            this.sqlTaskExecutionReference = new WeakReference<>(sqlTaskExecution);
        }

        @Override
        public void stateChanged(BufferState newState)
        {
            if (newState.isTerminal()) {
                SqlTaskExecution sqlTaskExecution = sqlTaskExecutionReference.get();
                if (sqlTaskExecution != null) {
                    sqlTaskExecution.checkTaskCompletion();
                }
            }
        }
    }
}
