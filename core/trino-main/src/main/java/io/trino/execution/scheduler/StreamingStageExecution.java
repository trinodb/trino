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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.Lifespan;
import io.trino.execution.RemoteTask;
import io.trino.execution.SqlStageExecution;
import io.trino.execution.StageId;
import io.trino.execution.StateMachine;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.OutputBuffers.OutputBufferId;
import io.trino.failuredetector.FailureDetector;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.spi.TrinoException;
import io.trino.split.RemoteSplit;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.util.Failures;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.trino.execution.scheduler.StreamingStageExecution.State.ABORTED;
import static io.trino.execution.scheduler.StreamingStageExecution.State.CANCELED;
import static io.trino.execution.scheduler.StreamingStageExecution.State.FAILED;
import static io.trino.execution.scheduler.StreamingStageExecution.State.FINISHED;
import static io.trino.execution.scheduler.StreamingStageExecution.State.FLUSHING;
import static io.trino.execution.scheduler.StreamingStageExecution.State.PLANNED;
import static io.trino.execution.scheduler.StreamingStageExecution.State.RUNNING;
import static io.trino.execution.scheduler.StreamingStageExecution.State.SCHEDULED;
import static io.trino.execution.scheduler.StreamingStageExecution.State.SCHEDULING;
import static io.trino.execution.scheduler.StreamingStageExecution.State.SCHEDULING_SPLITS;
import static io.trino.failuredetector.FailureDetector.State.GONE;
import static io.trino.operator.ExchangeOperator.REMOTE_CONNECTOR_ID;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static java.util.Objects.requireNonNull;

public class StreamingStageExecution
        implements ResultsConsumer
{
    private static final Logger log = Logger.get(StreamingStageExecution.class);

    private final StreamingStageStateMachine stateMachine;
    private final SqlStageExecution stageExecution;
    private final Map<PlanFragmentId, OutputBufferManager> outputBufferManagers;
    private final ResultsConsumer parent;
    private final FailureDetector failureDetector;
    private final Executor executor;
    private final Optional<int[]> bucketToPartition;
    private final Map<PlanFragmentId, RemoteSourceNode> exchangeSources;

    private final Map<Integer, RemoteTask> tasks = new ConcurrentHashMap<>();

    // current stage task tracking
    @GuardedBy("this")
    private final Set<TaskId> allTasks = newConcurrentHashSet();
    @GuardedBy("this")
    private final Set<TaskId> finishedTasks = newConcurrentHashSet();
    @GuardedBy("this")
    private final Set<TaskId> flushingTasks = newConcurrentHashSet();

    // source task tracking
    @GuardedBy("this")
    private final Multimap<PlanFragmentId, RemoteTask> sourceTasks = HashMultimap.create();
    @GuardedBy("this")
    private final Set<PlanFragmentId> completeSourceFragments = newConcurrentHashSet();
    @GuardedBy("this")
    private final Set<PlanNodeId> completeSources = newConcurrentHashSet();

    // lifespan tracking
    private final Set<Lifespan> completedDriverGroups = new HashSet<>();
    private final ListenerManager<Set<Lifespan>> completedLifespansChangeListeners = new ListenerManager<>();

    public static StreamingStageExecution createStreamingStageExecution(
            SqlStageExecution stageExecution,
            Map<PlanFragmentId, OutputBufferManager> outputBufferManagers,
            ResultsConsumer parent,
            FailureDetector failureDetector,
            Executor executor,
            Optional<int[]> bucketToPartition)
    {
        StreamingStageStateMachine stateMachine = new StreamingStageStateMachine(stageExecution.getStageId(), executor);
        ImmutableMap.Builder<PlanFragmentId, RemoteSourceNode> exchangeSources = ImmutableMap.builder();
        for (RemoteSourceNode remoteSourceNode : stageExecution.getFragment().getRemoteSourceNodes()) {
            for (PlanFragmentId planFragmentId : remoteSourceNode.getSourceFragmentIds()) {
                exchangeSources.put(planFragmentId, remoteSourceNode);
            }
        }
        StreamingStageExecution execution = new StreamingStageExecution(
                stateMachine,
                stageExecution,
                outputBufferManagers,
                parent,
                failureDetector,
                executor,
                bucketToPartition,
                exchangeSources.build());
        execution.initialize();
        return execution;
    }

    private StreamingStageExecution(
            StreamingStageStateMachine stateMachine,
            SqlStageExecution stageExecution,
            Map<PlanFragmentId, OutputBufferManager> outputBufferManagers,
            ResultsConsumer parent,
            FailureDetector failureDetector,
            Executor executor,
            Optional<int[]> bucketToPartition,
            Map<PlanFragmentId, RemoteSourceNode> exchangeSources)
    {
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.stageExecution = requireNonNull(stageExecution, "stageExecution is null");
        this.outputBufferManagers = ImmutableMap.copyOf(requireNonNull(outputBufferManagers, "outputBufferManagers is null"));
        this.parent = requireNonNull(parent, "parent is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.bucketToPartition = requireNonNull(bucketToPartition, "bucketToPartition is null");
        this.exchangeSources = ImmutableMap.copyOf(requireNonNull(exchangeSources, "exchangeSources is null"));
    }

    private void initialize()
    {
        stateMachine.addStateChangeListener(state -> {
            if (!state.canScheduleMoreTasks()) {
                // notify parent stage
                parent.noMoreSourceTasks(stageExecution.getFragment().getId());

                // update output buffers
                for (PlanFragmentId sourceFragment : exchangeSources.keySet()) {
                    OutputBufferManager outputBufferManager = outputBufferManagers.get(sourceFragment);
                    outputBufferManager.noMoreBuffers();
                    for (RemoteTask sourceTask : sourceTasks.get(stageExecution.getFragment().getId())) {
                        sourceTask.setOutputBuffers(outputBufferManager.getOutputBuffers());
                    }
                }
            }
        });
    }

    public State getState()
    {
        return stateMachine.getState();
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor.
     */
    public void addStateChangeListener(StateChangeListener<State> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    public void addCompletedDriverGroupsChangedListener(Consumer<Set<Lifespan>> newlyCompletedDriverGroupConsumer)
    {
        completedLifespansChangeListeners.addListener(newlyCompletedDriverGroupConsumer);
    }

    public synchronized void beginScheduling()
    {
        stateMachine.transitionToScheduling();
    }

    public synchronized void transitionToSchedulingSplits()
    {
        stateMachine.transitionToSchedulingSplits();
    }

    public synchronized void schedulingComplete()
    {
        if (!stateMachine.transitionToScheduled()) {
            return;
        }

        if (isFlushing()) {
            stateMachine.transitionToFlushing();
        }
        if (finishedTasks.containsAll(allTasks)) {
            stateMachine.transitionToFinished();
        }

        for (PlanNodeId partitionedSource : stageExecution.getFragment().getPartitionedSources()) {
            schedulingComplete(partitionedSource);
        }
    }

    private synchronized boolean isFlushing()
    {
        // to transition to flushing, there must be at least one flushing task, and all others must be flushing or finished.
        return !flushingTasks.isEmpty()
                && allTasks.stream().allMatch(taskId -> finishedTasks.contains(taskId) || flushingTasks.contains(taskId));
    }

    public synchronized void schedulingComplete(PlanNodeId partitionedSource)
    {
        for (RemoteTask task : getAllTasks()) {
            task.noMoreSplits(partitionedSource);
        }
        completeSources.add(partitionedSource);
    }

    public synchronized void cancel()
    {
        stateMachine.transitionToCanceled();
        getAllTasks().forEach(RemoteTask::cancel);
    }

    public synchronized void abort()
    {
        stateMachine.transitionToAborted();
        getAllTasks().forEach(RemoteTask::abort);
    }

    public synchronized Optional<RemoteTask> scheduleTask(
            InternalNode node,
            int partition,
            Multimap<PlanNodeId, Split> initialSplits,
            Multimap<PlanNodeId, Lifespan> noMoreSplitsForLifespan)
    {
        if (stateMachine.getState().isDone()) {
            return Optional.empty();
        }

        checkArgument(!tasks.containsKey(partition), "A task for partition %s already exists", partition);

        OutputBuffers outputBuffers = outputBufferManagers.get(stageExecution.getFragment().getId()).getOutputBuffers();

        Optional<RemoteTask> optionalTask = stageExecution.createTask(
                node,
                partition,
                bucketToPartition,
                outputBuffers,
                initialSplits,
                ImmutableMultimap.of(),
                ImmutableSet.of());

        if (optionalTask.isEmpty()) {
            return Optional.empty();
        }

        RemoteTask task = optionalTask.get();

        tasks.put(partition, task);

        ImmutableMultimap.Builder<PlanNodeId, Split> exchangeSplits = ImmutableMultimap.builder();
        sourceTasks.forEach((fragmentId, sourceTask) -> {
            TaskStatus status = sourceTask.getTaskStatus();
            if (status.getState() != TaskState.FINISHED) {
                PlanNodeId planNodeId = exchangeSources.get(fragmentId).getId();
                exchangeSplits.put(planNodeId, createExchangeSplit(task, sourceTask));
            }
        });

        allTasks.add(task.getTaskId());

        task.addSplits(exchangeSplits.build());
        noMoreSplitsForLifespan.forEach(task::noMoreSplits);
        completeSources.forEach(task::noMoreSplits);

        task.addStateChangeListener(this::updateTaskStatus);
        task.addStateChangeListener(this::updateCompletedDriverGroups);

        task.start();

        // update parent stage
        parent.addSourceTask(stageExecution.getFragment().getId(), task);

        // update output buffers
        OutputBufferId outputBufferId = new OutputBufferId(task.getTaskId().getId());
        for (PlanFragmentId sourceFragment : exchangeSources.keySet()) {
            OutputBufferManager outputBufferManager = outputBufferManagers.get(sourceFragment);
            outputBufferManager.addOutputBuffer(outputBufferId);
            for (RemoteTask sourceTask : sourceTasks.get(stageExecution.getFragment().getId())) {
                sourceTask.setOutputBuffers(outputBufferManager.getOutputBuffers());
            }
        }

        return Optional.of(task);
    }

    private synchronized void updateTaskStatus(TaskStatus taskStatus)
    {
        State stageState = stateMachine.getState();
        if (stageState.isDone()) {
            return;
        }

        TaskState taskState = taskStatus.getState();

        switch (taskState) {
            case FAILED:
                RuntimeException failure = taskStatus.getFailures().stream()
                        .findFirst()
                        .map(this::rewriteTransportFailure)
                        .map(ExecutionFailureInfo::toException)
                        .orElse(new TrinoException(GENERIC_INTERNAL_ERROR, "A task failed for an unknown reason"));
                stateMachine.transitionToFailed(failure);
                break;
            case ABORTED:
                // A task should only be in the aborted state if the STAGE is done (ABORTED or FAILED)
                stateMachine.transitionToFailed(new TrinoException(GENERIC_INTERNAL_ERROR, "A task is in the ABORTED state but stage is " + stageState));
                break;
            case FLUSHING:
                flushingTasks.add(taskStatus.getTaskId());
                break;
            case FINISHED:
                finishedTasks.add(taskStatus.getTaskId());
                flushingTasks.remove(taskStatus.getTaskId());
                break;
            default:
        }

        if (stageState == SCHEDULED || stageState == RUNNING || stageState == FLUSHING) {
            if (taskState == TaskState.RUNNING) {
                stateMachine.transitionToRunning();
            }
            if (isFlushing()) {
                stateMachine.transitionToFlushing();
            }
            if (finishedTasks.containsAll(allTasks)) {
                stateMachine.transitionToFinished();
            }
        }
    }

    private synchronized void updateCompletedDriverGroups(TaskStatus taskStatus)
    {
        // Sets.difference returns a view.
        // Once we add the difference into `completedDriverGroups`, the view will be empty.
        // `completedLifespansChangeListeners.invoke` happens asynchronously.
        // As a result, calling the listeners before updating `completedDriverGroups` doesn't make a difference.
        // That's why a copy must be made here.
        Set<Lifespan> newlyCompletedDriverGroups = ImmutableSet.copyOf(Sets.difference(taskStatus.getCompletedDriverGroups(), this.completedDriverGroups));
        if (newlyCompletedDriverGroups.isEmpty()) {
            return;
        }
        completedLifespansChangeListeners.invoke(newlyCompletedDriverGroups, executor);
        // newlyCompletedDriverGroups is a view.
        // Making changes to completedDriverGroups will change newlyCompletedDriverGroups.
        completedDriverGroups.addAll(newlyCompletedDriverGroups);
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

    @Override
    public synchronized void addSourceTask(PlanFragmentId fragmentId, RemoteTask sourceTask)
    {
        requireNonNull(fragmentId, "fragmentId is null");

        RemoteSourceNode remoteSource = exchangeSources.get(fragmentId);
        checkArgument(remoteSource != null, "Unknown remote source %s. Known sources are %s", fragmentId, exchangeSources.keySet());

        sourceTasks.put(fragmentId, sourceTask);

        OutputBufferManager outputBufferManager = outputBufferManagers.get(fragmentId);
        sourceTask.setOutputBuffers(outputBufferManager.getOutputBuffers());

        for (RemoteTask destinationTask : getAllTasks()) {
            destinationTask.addSplits(ImmutableMultimap.of(remoteSource.getId(), createExchangeSplit(destinationTask, sourceTask)));
        }
    }

    @Override
    public synchronized void noMoreSourceTasks(PlanFragmentId fragmentId)
    {
        RemoteSourceNode remoteSource = exchangeSources.get(fragmentId);
        checkArgument(remoteSource != null, "Unknown remote source %s. Known sources are %s", fragmentId, exchangeSources.keySet());

        completeSourceFragments.add(fragmentId);

        // is the source now complete?
        if (completeSourceFragments.containsAll(remoteSource.getSourceFragmentIds())) {
            completeSources.add(remoteSource.getId());
            for (RemoteTask task : getAllTasks()) {
                task.noMoreSplits(remoteSource.getId());
            }
        }
    }

    public List<RemoteTask> getAllTasks()
    {
        return ImmutableList.copyOf(tasks.values());
    }

    public List<TaskStatus> getTaskStatuses()
    {
        return getAllTasks().stream()
                .map(RemoteTask::getTaskStatus)
                .collect(toImmutableList());
    }

    public boolean isAnyTaskBlocked()
    {
        return getTaskStatuses().stream().anyMatch(TaskStatus::isOutputBufferOverutilized);
    }

    public void recordGetSplitTime(long start)
    {
        stageExecution.recordGetSplitTime(start);
    }

    public StageId getStageId()
    {
        return stageExecution.getStageId();
    }

    public PlanFragment getFragment()
    {
        return stageExecution.getFragment();
    }

    public Optional<ExecutionFailureInfo> getFailureCause()
    {
        return stateMachine.getFailureCause();
    }

    private static Split createExchangeSplit(RemoteTask destinationTask, RemoteTask sourceTask)
    {
        // Fetch the results from the buffer assigned to the task based on id
        URI exchangeLocation = sourceTask.getTaskStatus().getSelf();
        URI splitLocation = uriBuilderFrom(exchangeLocation).appendPath("results").appendPath(String.valueOf(destinationTask.getTaskId().getId())).build();
        return new Split(REMOTE_CONNECTOR_ID, new RemoteSplit(splitLocation), Lifespan.taskWide());
    }

    public enum State
    {
        /**
         * Stage is planned but has not been scheduled yet.  A stage will
         * be in the planned state until, the dependencies of the stage
         * have begun producing output.
         */
        PLANNED(false, false),
        /**
         * Stage tasks are being scheduled on nodes.
         */
        SCHEDULING(false, false),
        /**
         * All stage tasks have been scheduled, but splits are still being scheduled.
         */
        SCHEDULING_SPLITS(false, false),
        /**
         * Stage has been scheduled on nodes and ready to execute, but all tasks are still queued.
         */
        SCHEDULED(false, false),
        /**
         * Stage is running.
         */
        RUNNING(false, false),
        /**
         * Stage has finished executing and output being consumed.
         * In this state, at-least one of the tasks is flushing and the non-flushing tasks are finished
         */
        FLUSHING(false, false),
        /**
         * Stage has finished executing and all output has been consumed.
         */
        FINISHED(true, false),
        /**
         * Stage was canceled by a user.
         */
        CANCELED(true, false),
        /**
         * Stage was aborted due to a failure in the query.  The failure
         * was not in this stage.
         */
        ABORTED(true, true),
        /**
         * Stage execution failed.
         */
        FAILED(true, true);

        private final boolean doneState;
        private final boolean failureState;

        State(boolean doneState, boolean failureState)
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

        public boolean canScheduleMoreTasks()
        {
            switch (this) {
                case PLANNED:
                case SCHEDULING:
                    // workers are still being added to the query
                    return true;
                case SCHEDULING_SPLITS:
                case SCHEDULED:
                case RUNNING:
                case FLUSHING:
                case FINISHED:
                case CANCELED:
                    // no more workers will be added to the query
                    return false;
                case ABORTED:
                case FAILED:
                    // DO NOT complete a FAILED or ABORTED stage.  This will cause the
                    // stage above to finish normally, which will result in a query
                    // completing successfully when it should fail..
                    return true;
            }
            throw new IllegalStateException("Unhandled state: " + this);
        }
    }

    private static class StreamingStageStateMachine
    {
        private static final Set<State> TERMINAL_STAGE_STATES = Stream.of(State.values()).filter(State::isDone).collect(toImmutableSet());

        private final StageId stageId;
        private final StateMachine<State> state;
        private final AtomicReference<DateTime> schedulingComplete = new AtomicReference<>();
        private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

        private StreamingStageStateMachine(StageId stageId, Executor executor)
        {
            this.stageId = requireNonNull(stageId, "stageId is null");

            state = new StateMachine<>("Streaming stage execution " + stageId, executor, PLANNED, TERMINAL_STAGE_STATES);
            state.addStateChangeListener(state -> log.debug("Streaming stage execution %s is %s", stageId, state));
        }

        public State getState()
        {
            return state.get();
        }

        public boolean transitionToScheduling()
        {
            return state.compareAndSet(PLANNED, SCHEDULING);
        }

        public boolean transitionToSchedulingSplits()
        {
            return state.setIf(SCHEDULING_SPLITS, currentState -> currentState == PLANNED || currentState == SCHEDULING);
        }

        public boolean transitionToScheduled()
        {
            schedulingComplete.compareAndSet(null, DateTime.now());
            return state.setIf(SCHEDULED, currentState -> currentState == PLANNED || currentState == SCHEDULING || currentState == SCHEDULING_SPLITS);
        }

        public boolean transitionToRunning()
        {
            return state.setIf(RUNNING, currentState -> currentState != RUNNING && currentState != FLUSHING && !currentState.isDone());
        }

        public boolean transitionToFlushing()
        {
            return state.setIf(FLUSHING, currentState -> currentState != FLUSHING && !currentState.isDone());
        }

        public boolean transitionToFinished()
        {
            return state.setIf(FINISHED, currentState -> !currentState.isDone());
        }

        public boolean transitionToCanceled()
        {
            return state.setIf(CANCELED, currentState -> !currentState.isDone());
        }

        public boolean transitionToAborted()
        {
            return state.setIf(ABORTED, currentState -> !currentState.isDone());
        }

        public boolean transitionToFailed(Throwable throwable)
        {
            requireNonNull(throwable, "throwable is null");

            failureCause.compareAndSet(null, Failures.toFailure(throwable));
            boolean failed = state.setIf(FAILED, currentState -> !currentState.isDone());
            if (failed) {
                log.error(throwable, "Streaming stage execution for stage %s failed", stageId);
            }
            else {
                log.debug(throwable, "Failure in streaming stage execution for stage %s after finished", stageId);
            }
            return failed;
        }

        public Optional<ExecutionFailureInfo> getFailureCause()
        {
            return Optional.ofNullable(failureCause.get());
        }

        /**
         * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
         * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
         * possible notifications are observed out of order due to the asynchronous execution.
         */
        public void addStateChangeListener(StateChangeListener<State> stateChangeListener)
        {
            state.addStateChangeListener(stateChangeListener);
        }
    }

    private static class ListenerManager<T>
    {
        private final List<Consumer<T>> listeners = new ArrayList<>();
        private boolean frozen;

        public synchronized void addListener(Consumer<T> listener)
        {
            checkState(!frozen, "Listeners have been invoked");
            listeners.add(listener);
        }

        public synchronized void invoke(T payload, Executor executor)
        {
            frozen = true;
            for (Consumer<T> listener : listeners) {
                executor.execute(() -> listener.accept(payload));
            }
        }
    }
}
