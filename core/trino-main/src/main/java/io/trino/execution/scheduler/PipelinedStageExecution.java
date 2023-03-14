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
import io.airlift.log.Logger;
import io.trino.exchange.DirectExchangeInput;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.RemoteTask;
import io.trino.execution.SqlStage;
import io.trino.execution.StageId;
import io.trino.execution.StateMachine;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
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

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.trino.execution.scheduler.StageExecution.State.ABORTED;
import static io.trino.execution.scheduler.StageExecution.State.CANCELED;
import static io.trino.execution.scheduler.StageExecution.State.FAILED;
import static io.trino.execution.scheduler.StageExecution.State.FINISHED;
import static io.trino.execution.scheduler.StageExecution.State.FLUSHING;
import static io.trino.execution.scheduler.StageExecution.State.PLANNED;
import static io.trino.execution.scheduler.StageExecution.State.RUNNING;
import static io.trino.execution.scheduler.StageExecution.State.SCHEDULED;
import static io.trino.execution.scheduler.StageExecution.State.SCHEDULING;
import static io.trino.execution.scheduler.StageExecution.State.SCHEDULING_SPLITS;
import static io.trino.failuredetector.FailureDetector.State.GONE;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class is designed to facilitate the pipelined mode of execution.
 * <p>
 * In the pipeline mode the tasks are executed in all-or-nothing fashion with all
 * the intermediate data being "piped" between stages in a streaming way.
 * <p>
 * This class has two main responsibilities:
 * <p>
 * 1. Linking pipelined stages together. If a new task is scheduled the implementation
 * notifies upstream stages to add an additional output buffer for the task as well as
 * it notifies the downstream stage to update a list of source tasks. It is also
 * responsible of notifying both upstream and downstream stages when no more tasks will
 * be added.
 * <p>
 * 2. Facilitates state transitioning for a pipelined stage execution according to the
 * all-or-noting model. If any of the tasks fail the implementation is responsible for
 * terminating all remaining tasks as well as propagating the original error. If all
 * the tasks finish successfully the implementation is responsible for notifying the
 * scheduler about a successful completion of a given stage.
 */
public class PipelinedStageExecution
        implements StageExecution
{
    private static final Logger log = Logger.get(PipelinedStageExecution.class);

    private final PipelinedStageStateMachine stateMachine;
    private final SqlStage stage;
    private final Map<PlanFragmentId, PipelinedOutputBufferManager> outputBufferManagers;
    private final TaskLifecycleListener taskLifecycleListener;
    private final FailureDetector failureDetector;
    private final Optional<int[]> bucketToPartition;
    private final Map<PlanFragmentId, RemoteSourceNode> exchangeSources;
    private final int attempt;

    private final Map<Integer, RemoteTask> tasks = new ConcurrentHashMap<>();

    // current stage task tracking
    @GuardedBy("this")
    private final Set<TaskId> allTasks = new HashSet<>();
    private final Set<TaskId> finishedTasks = ConcurrentHashMap.newKeySet();
    private final Set<TaskId> flushingTasks = ConcurrentHashMap.newKeySet();

    // source task tracking
    @GuardedBy("this")
    private final Multimap<PlanFragmentId, RemoteTask> sourceTasks = HashMultimap.create();
    @GuardedBy("this")
    private final Set<PlanFragmentId> completeSourceFragments = new HashSet<>();
    @GuardedBy("this")
    private final Set<PlanNodeId> completeSources = new HashSet<>();

    public static PipelinedStageExecution createPipelinedStageExecution(
            SqlStage stage,
            Map<PlanFragmentId, PipelinedOutputBufferManager> outputBufferManagers,
            TaskLifecycleListener taskLifecycleListener,
            FailureDetector failureDetector,
            Executor executor,
            Optional<int[]> bucketToPartition,
            int attempt)
    {
        PipelinedStageStateMachine stateMachine = new PipelinedStageStateMachine(stage.getStageId(), executor);
        ImmutableMap.Builder<PlanFragmentId, RemoteSourceNode> exchangeSources = ImmutableMap.builder();
        for (RemoteSourceNode remoteSourceNode : stage.getFragment().getRemoteSourceNodes()) {
            for (PlanFragmentId planFragmentId : remoteSourceNode.getSourceFragmentIds()) {
                exchangeSources.put(planFragmentId, remoteSourceNode);
            }
        }
        PipelinedStageExecution execution = new PipelinedStageExecution(
                stateMachine,
                stage,
                outputBufferManagers,
                taskLifecycleListener,
                failureDetector,
                bucketToPartition,
                exchangeSources.buildOrThrow(),
                attempt);
        execution.initialize();
        return execution;
    }

    private PipelinedStageExecution(
            PipelinedStageStateMachine stateMachine,
            SqlStage stage,
            Map<PlanFragmentId, PipelinedOutputBufferManager> outputBufferManagers,
            TaskLifecycleListener taskLifecycleListener,
            FailureDetector failureDetector,
            Optional<int[]> bucketToPartition,
            Map<PlanFragmentId, RemoteSourceNode> exchangeSources,
            int attempt)
    {
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.stage = requireNonNull(stage, "stage is null");
        this.outputBufferManagers = ImmutableMap.copyOf(requireNonNull(outputBufferManagers, "outputBufferManagers is null"));
        this.taskLifecycleListener = requireNonNull(taskLifecycleListener, "taskLifecycleListener is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.bucketToPartition = requireNonNull(bucketToPartition, "bucketToPartition is null");
        this.exchangeSources = ImmutableMap.copyOf(requireNonNull(exchangeSources, "exchangeSources is null"));
        this.attempt = attempt;
    }

    private void initialize()
    {
        stateMachine.addStateChangeListener(state -> {
            if (!state.canScheduleMoreTasks()) {
                taskLifecycleListener.noMoreTasks(stage.getFragment().getId());
                updateSourceTasksOutputBuffers(PipelinedOutputBufferManager::noMoreBuffers);
            }
        });
    }

    @Override
    public State getState()
    {
        return stateMachine.getState();
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor.
     */
    @Override
    public void addStateChangeListener(StateChangeListener<State> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public synchronized void beginScheduling()
    {
        stateMachine.transitionToScheduling();
    }

    @Override
    public synchronized void transitionToSchedulingSplits()
    {
        stateMachine.transitionToSchedulingSplits();
    }

    @Override
    public void schedulingComplete()
    {
        if (!stateMachine.transitionToScheduled()) {
            return;
        }

        if (isStageFlushing()) {
            stateMachine.transitionToFlushing();
        }
        if (isStageFinished()) {
            stateMachine.transitionToFinished();
        }

        for (PlanNodeId partitionedSource : stage.getFragment().getPartitionedSources()) {
            schedulingComplete(partitionedSource);
        }
    }

    @Override
    public synchronized void schedulingComplete(PlanNodeId partitionedSource)
    {
        for (RemoteTask task : getAllTasks()) {
            task.noMoreSplits(partitionedSource);
        }
        completeSources.add(partitionedSource);
    }

    @Override
    public synchronized void cancel()
    {
        // Only send tasks a cancel command if the stage is successfully cancelled and not already failed
        if (stateMachine.transitionToCanceled()) {
            tasks.values().forEach(RemoteTask::cancel);
        }
    }

    @Override
    public synchronized void abort()
    {
        stateMachine.transitionToAborted();
        tasks.values().forEach(RemoteTask::abort);
    }

    public synchronized void fail(Throwable failureCause)
    {
        stateMachine.transitionToFailed(failureCause);
        tasks.values().forEach(RemoteTask::abort);
    }

    @Override
    public synchronized void failTask(TaskId taskId, Throwable failureCause)
    {
        RemoteTask task = requireNonNull(tasks.get(taskId.getPartitionId()), () -> "task not found: " + taskId);
        task.failLocallyImmediately(failureCause);
        fail(failureCause);
    }

    @Override
    public synchronized Optional<RemoteTask> scheduleTask(
            InternalNode node,
            int partition,
            Multimap<PlanNodeId, Split> initialSplits)
    {
        if (stateMachine.getState().isDone()) {
            return Optional.empty();
        }

        checkArgument(!tasks.containsKey(partition), "A task for partition %s already exists", partition);

        OutputBuffers outputBuffers = outputBufferManagers.get(stage.getFragment().getId()).getOutputBuffers();

        Optional<RemoteTask> optionalTask = stage.createTask(
                node,
                partition,
                attempt,
                bucketToPartition,
                outputBuffers,
                initialSplits,
                ImmutableSet.of(),
                Optional.empty());

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
                exchangeSplits.put(planNodeId, createExchangeSplit(sourceTask, task));
            }
        });

        allTasks.add(task.getTaskId());

        task.addSplits(exchangeSplits.build());
        completeSources.forEach(task::noMoreSplits);

        task.addStateChangeListener(this::updateTaskStatus);

        task.start();

        taskLifecycleListener.taskCreated(stage.getFragment().getId(), task);

        // update output buffers
        OutputBufferId outputBufferId = new OutputBufferId(task.getTaskId().getPartitionId());
        updateSourceTasksOutputBuffers(outputBufferManager -> outputBufferManager.addOutputBuffer(outputBufferId));

        return Optional.of(task);
    }

    private void updateTaskStatus(TaskStatus taskStatus)
    {
        if (stateMachine.getState().isDone()) {
            return;
        }
        boolean newFlushingOrFinishedTaskObserved = false;
        TaskState taskState = taskStatus.getState();

        switch (taskState) {
            case FAILING:
            case FAILED:
                RuntimeException failure = taskStatus.getFailures().stream()
                        .findFirst()
                        .map(this::rewriteTransportFailure)
                        .map(ExecutionFailureInfo::toException)
                        // task is failed or failing, so we need to create a synthetic exception to fail the stage now
                        .orElseGet(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "A task failed for an unknown reason"));
                fail(failure);
                break;
            case CANCELING:
            case CANCELED:
            case ABORTING:
            case ABORTED:
                // A task should only be in the aborting, aborted, canceling, or canceled state if the STAGE is done (ABORTED or FAILED)
                fail(new TrinoException(GENERIC_INTERNAL_ERROR, format("A task is in the %s state but stage is %s", taskState, stateMachine.getState())));
                break;
            case FLUSHING:
                newFlushingOrFinishedTaskObserved = addFlushingTask(taskStatus.getTaskId());
                break;
            case FINISHED:
                newFlushingOrFinishedTaskObserved = addFinishedTask(taskStatus.getTaskId());
                break;
            default:
        }

        // Only allow stage state to transition to RUNNING, FLUSHING or FINISHED state
        // when allTasks list is complete.
        // If scheduling of tasks completes and all tasks are already finished then
        // stage state will also be updated by schedulingComplete method.
        State stageState = stateMachine.getState();
        if (stageState == SCHEDULED || stageState == RUNNING || stageState == FLUSHING) {
            if (taskState == TaskState.RUNNING) {
                stateMachine.transitionToRunning();
            }
            // avoid extra synchronization if no new flushing or finished task was observed
            if (newFlushingOrFinishedTaskObserved) {
                if (isStageFlushing()) {
                    stateMachine.transitionToFlushing();
                }
                if (isStageFinished()) {
                    stateMachine.transitionToFinished();
                }
            }
        }
    }

    private synchronized boolean isStageFlushing()
    {
        // to transition to flushing, there must be at least one flushing task, and all others must be flushing or finished.
        return !flushingTasks.isEmpty() && allTasks.size() == finishedTasks.size() + flushingTasks.size();
    }

    private synchronized boolean isStageFinished()
    {
        boolean finished = finishedTasks.size() == allTasks.size();
        if (finished) {
            checkState(finishedTasks.containsAll(allTasks), "Finished tasks should contain all tasks");
        }
        return finished;
    }

    private boolean addFlushingTask(TaskId taskId)
    {
        if (!flushingTasks.contains(taskId) && !finishedTasks.contains(taskId)) {
            synchronized (this) {
                // We need to check whether that task is not already finished. It could happen because of out of order of
                // task status events
                if (!finishedTasks.contains(taskId)) {
                    return flushingTasks.add(taskId);
                }
            }
        }
        return false;
    }

    private boolean addFinishedTask(TaskId taskId)
    {
        if (!finishedTasks.contains(taskId)) {
            synchronized (this) {
                boolean added = finishedTasks.add(taskId);
                flushingTasks.remove(taskId);
                return added;
            }
        }
        return false;
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
    public TaskLifecycleListener getTaskLifecycleListener()
    {
        return new TaskLifecycleListener()
        {
            @Override
            public void taskCreated(PlanFragmentId fragmentId, RemoteTask task)
            {
                sourceTaskCreated(fragmentId, task);
            }

            @Override
            public void noMoreTasks(PlanFragmentId fragmentId)
            {
                noMoreSourceTasks(fragmentId);
            }
        };
    }

    private synchronized void sourceTaskCreated(PlanFragmentId fragmentId, RemoteTask sourceTask)
    {
        requireNonNull(fragmentId, "fragmentId is null");

        RemoteSourceNode remoteSource = exchangeSources.get(fragmentId);
        checkArgument(remoteSource != null, "Unknown remote source %s. Known sources are %s", fragmentId, exchangeSources.keySet());

        sourceTasks.put(fragmentId, sourceTask);

        PipelinedOutputBufferManager outputBufferManager = outputBufferManagers.get(fragmentId);
        sourceTask.setOutputBuffers(outputBufferManager.getOutputBuffers());

        for (RemoteTask destinationTask : getAllTasks()) {
            destinationTask.addSplits(ImmutableMultimap.of(remoteSource.getId(), createExchangeSplit(sourceTask, destinationTask)));
        }
    }

    private synchronized void noMoreSourceTasks(PlanFragmentId fragmentId)
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

    private synchronized void updateSourceTasksOutputBuffers(Consumer<PipelinedOutputBufferManager> updater)
    {
        for (PlanFragmentId sourceFragment : exchangeSources.keySet()) {
            PipelinedOutputBufferManager outputBufferManager = outputBufferManagers.get(sourceFragment);
            updater.accept(outputBufferManager);
            for (RemoteTask sourceTask : sourceTasks.get(sourceFragment)) {
                sourceTask.setOutputBuffers(outputBufferManager.getOutputBuffers());
            }
        }
    }

    @Override
    public List<RemoteTask> getAllTasks()
    {
        return ImmutableList.copyOf(tasks.values());
    }

    @Override
    public List<TaskStatus> getTaskStatuses()
    {
        return getAllTasks().stream()
                .map(RemoteTask::getTaskStatus)
                .collect(toImmutableList());
    }

    @Override
    public boolean isAnyTaskBlocked()
    {
        return getTaskStatuses().stream()
                .map(TaskStatus::getOutputBufferStatus)
                .anyMatch(OutputBufferStatus::isOverutilized);
    }

    @Override
    public void recordGetSplitTime(long start)
    {
        stage.recordGetSplitTime(start);
    }

    @Override
    public StageId getStageId()
    {
        return stage.getStageId();
    }

    @Override
    public int getAttemptId()
    {
        return attempt;
    }

    @Override
    public PlanFragment getFragment()
    {
        return stage.getFragment();
    }

    @Override
    public Optional<ExecutionFailureInfo> getFailureCause()
    {
        return stateMachine.getFailureCause();
    }

    @Override
    public String toString()
    {
        return stateMachine.toString();
    }

    private static Split createExchangeSplit(RemoteTask sourceTask, RemoteTask destinationTask)
    {
        // Fetch the results from the buffer assigned to the task based on id
        URI exchangeLocation = sourceTask.getTaskStatus().getSelf();
        URI splitLocation = uriBuilderFrom(exchangeLocation).appendPath("results").appendPath(String.valueOf(destinationTask.getTaskId().getPartitionId())).build();
        return new Split(REMOTE_CATALOG_HANDLE, new RemoteSplit(new DirectExchangeInput(sourceTask.getTaskId(), splitLocation.toString())));
    }

    private static class PipelinedStageStateMachine
    {
        private static final Set<State> TERMINAL_STAGE_STATES = Stream.of(State.values()).filter(State::isDone).collect(toImmutableSet());

        private final StageId stageId;
        private final StateMachine<State> state;
        private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

        private PipelinedStageStateMachine(StageId stageId, Executor executor)
        {
            this.stageId = requireNonNull(stageId, "stageId is null");

            state = new StateMachine<>("Pipelined stage execution " + stageId, executor, PLANNED, TERMINAL_STAGE_STATES);
            state.addStateChangeListener(state -> log.debug("Pipelined stage execution %s is %s", stageId, state));
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
                log.error(throwable, "Pipelined stage execution for stage %s failed", stageId);
            }
            else {
                log.debug(throwable, "Failure in pipelined stage execution for stage %s after finished", stageId);
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

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("stageId", stageId)
                    .add("state", state)
                    .toString();
        }
    }
}
