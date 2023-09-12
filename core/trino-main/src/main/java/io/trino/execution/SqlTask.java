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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.trino.Session;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.LazyOutputBuffer;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.memory.QueryContext;
import io.trino.operator.PipelineContext;
import io.trino.operator.PipelineStatus;
import io.trino.operator.TaskContext;
import io.trino.operator.TaskStats;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.tracing.TrinoAttributes;
import jakarta.annotation.Nullable;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTERS_VERSION;
import static io.trino.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTER_DOMAINS;
import static io.trino.execution.TaskState.FAILED;
import static io.trino.execution.TaskState.FAILING;
import static io.trino.execution.TaskState.RUNNING;
import static io.trino.util.Failures.toFailures;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SqlTask
{
    private static final Logger log = Logger.get(SqlTask.class);

    private final TaskId taskId;
    private final String taskInstanceId;
    private final URI location;
    private final String nodeId;
    private final AtomicBoolean speculative = new AtomicBoolean(false);
    private final TaskStateMachine taskStateMachine;
    private final OutputBuffer outputBuffer;
    private final QueryContext queryContext;
    private final Tracer tracer;

    private final SqlTaskExecutionFactory sqlTaskExecutionFactory;
    private final Executor taskNotificationExecutor;

    private final AtomicReference<DateTime> lastHeartbeat = new AtomicReference<>(DateTime.now());
    private final AtomicLong taskStatusVersion = new AtomicLong(TaskStatus.STARTING_VERSION);
    private final FutureStateChange<?> taskStatusVersionChange = new FutureStateChange<>();
    // Must be synchronized when updating the current task holder reference, but not when only reading the current reference value
    private final Object taskHolderLock = new Object();
    @GuardedBy("taskHolderLock")
    private final AtomicReference<TaskHolder> taskHolderReference = new AtomicReference<>(new TaskHolder());
    private final AtomicBoolean needsPlan = new AtomicBoolean(true);
    private final AtomicReference<Span> taskSpan = new AtomicReference<>(Span.getInvalid());
    private final AtomicReference<String> traceToken = new AtomicReference<>();
    private final AtomicReference<Set<CatalogHandle>> catalogs = new AtomicReference<>();

    public static SqlTask createSqlTask(
            TaskId taskId,
            URI location,
            String nodeId,
            QueryContext queryContext,
            Tracer tracer,
            SqlTaskExecutionFactory sqlTaskExecutionFactory,
            ExecutorService taskNotificationExecutor,
            Consumer<SqlTask> onDone,
            DataSize maxBufferSize,
            DataSize maxBroadcastBufferSize,
            ExchangeManagerRegistry exchangeManagerRegistry,
            CounterStat failedTasks)
    {
        SqlTask sqlTask = new SqlTask(taskId, location, nodeId, queryContext, tracer, sqlTaskExecutionFactory, taskNotificationExecutor, maxBufferSize, maxBroadcastBufferSize, exchangeManagerRegistry);
        sqlTask.initialize(onDone, failedTasks);
        return sqlTask;
    }

    private SqlTask(
            TaskId taskId,
            URI location,
            String nodeId,
            QueryContext queryContext,
            Tracer tracer,
            SqlTaskExecutionFactory sqlTaskExecutionFactory,
            ExecutorService taskNotificationExecutor,
            DataSize maxBufferSize,
            DataSize maxBroadcastBufferSize,
            ExchangeManagerRegistry exchangeManagerRegistry)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskInstanceId = UUID.randomUUID().toString();
        this.location = requireNonNull(location, "location is null");
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.queryContext = requireNonNull(queryContext, "queryContext is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.sqlTaskExecutionFactory = requireNonNull(sqlTaskExecutionFactory, "sqlTaskExecutionFactory is null");
        this.taskNotificationExecutor = requireNonNull(taskNotificationExecutor, "taskNotificationExecutor is null");
        requireNonNull(maxBufferSize, "maxBufferSize is null");

        outputBuffer = new LazyOutputBuffer(
                taskId,
                taskInstanceId,
                taskNotificationExecutor,
                maxBufferSize,
                maxBroadcastBufferSize,
                // Pass a memory context supplier instead of a memory context to the output buffer,
                // because we haven't created the task context that holds the memory context yet.
                () -> queryContext.getTaskContextByTaskId(taskId).localMemoryContext(),
                this::notifyStatusChanged,
                exchangeManagerRegistry);
        taskStateMachine = new TaskStateMachine(taskId, taskNotificationExecutor);
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize(Consumer<SqlTask> onDone, CounterStat failedTasks)
    {
        requireNonNull(onDone, "onDone is null");
        requireNonNull(failedTasks, "failedTasks is null");

        AtomicBoolean outputBufferCleanedUp = new AtomicBoolean();
        taskStateMachine.addStateChangeListener(newState -> {
            taskSpan.get().addEvent("task_state", Attributes.of(
                    TrinoAttributes.EVENT_STATE, newState.toString()));

            if (newState.isTerminatingOrDone()) {
                if (newState.isTerminating()) {
                    // This section must be synchronized to lock out any threads that might be attempting to create a SqlTaskExecution
                    synchronized (taskHolderLock) {
                        // If a SqlTaskExecution exists, it decides when termination is complete. Otherwise, we can mark termination completed immediately
                        if (taskHolderReference.get().getTaskExecution() == null) {
                            taskStateMachine.terminationComplete();
                        }
                    }
                }
                else if (newState.isDone()) {
                    // Update failed tasks counter
                    if (newState == FAILED) {
                        failedTasks.update(1);
                    }
                    // store final task info
                    boolean finished = false;
                    synchronized (taskHolderLock) {
                        TaskHolder taskHolder = taskHolderReference.get();
                        if (!taskHolder.isFinished()) {
                            TaskHolder newHolder = new TaskHolder(
                                    createTaskInfo(taskHolder),
                                    taskHolder.getIoStats(),
                                    taskHolder.getDynamicFilterDomains());
                            checkState(taskHolderReference.compareAndSet(taskHolder, newHolder), "unsynchronized concurrent task holder update");
                            finished = true;
                        }
                    }
                    // Successfully set the final task info, cleanup the output buffer and call the completion handler
                    if (finished) {
                        try {
                            onDone.accept(this);
                        }
                        catch (Exception e) {
                            log.warn(e, "Error running task cleanup callback %s", SqlTask.this.taskId);
                        }
                    }
                }
                // make sure buffers are cleaned up
                if (outputBufferCleanedUp.compareAndSet(false, true)) {
                    switch (newState) {
                        // don't close buffers for a failed query
                        // closed buffers signal to upstream tasks that everything finished cleanly
                        case FAILED, FAILING, ABORTED, ABORTING ->
                                outputBuffer.abort();
                        case FINISHED, CANCELED, CANCELING ->
                                outputBuffer.destroy();
                        default ->
                                throw new IllegalStateException(format("Invalid state for output buffer destruction: %s", newState));
                    }
                }
            }

            // notify that task state changed (apart from initial RUNNING state notification)
            if (newState != RUNNING) {
                notifyStatusChanged();
            }

            if (newState.isDone()) {
                taskSpan.get().end();
            }
        });
    }

    public SqlTaskIoStats getIoStats()
    {
        return taskHolderReference.get().getIoStats();
    }

    public TaskState getTaskState()
    {
        return taskStateMachine.getState();
    }

    public DateTime getTaskCreatedTime()
    {
        return taskStateMachine.getCreatedTime();
    }

    public TaskId getTaskId()
    {
        return taskStateMachine.getTaskId();
    }

    public String getTaskInstanceId()
    {
        return taskInstanceId;
    }

    public void recordHeartbeat()
    {
        lastHeartbeat.set(DateTime.now());
    }

    public TaskInfo getTaskInfo()
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            return createTaskInfo(taskHolderReference.get());
        }
    }

    public TaskStatus getTaskStatus()
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            return createTaskStatus(taskHolderReference.get());
        }
    }

    public Optional<Set<CatalogHandle>> getCatalogs()
    {
        return Optional.ofNullable(catalogs.get());
    }

    public boolean setCatalogs(Set<CatalogHandle> catalogs)
    {
        requireNonNull(catalogs, "catalogs is null");
        return this.catalogs.compareAndSet(null, requireNonNull(catalogs, "catalogs is null"));
    }

    public VersionedDynamicFilterDomains acknowledgeAndGetNewDynamicFilterDomains(long callersDynamicFiltersVersion)
    {
        return taskHolderReference.get().acknowledgeAndGetNewDynamicFilterDomains(callersDynamicFiltersVersion);
    }

    private synchronized void notifyStatusChanged()
    {
        taskStatusVersion.incrementAndGet();
        taskStatusVersionChange.complete(null, taskNotificationExecutor);
    }

    private TaskStatus createTaskStatus(TaskHolder taskHolder)
    {
        // Obtain task status version before building actual TaskStatus object.
        // This way any task updates won't be lost since all updates happen
        // before version number is increased.
        long versionNumber = taskStatusVersion.get();

        TaskState state = taskStateMachine.getState();
        List<ExecutionFailureInfo> failures = ImmutableList.of();
        if (state == FAILED || state == FAILING) {
            failures = toFailures(taskStateMachine.getFailureCauses());
        }

        int queuedPartitionedDrivers = 0;
        long queuedPartitionedSplitsWeight = 0L;
        int runningPartitionedDrivers = 0;
        long runningPartitionedSplitsWeight = 0L;
        DataSize outputDataSize = DataSize.ofBytes(0);
        DataSize writerInputDataSize = DataSize.ofBytes(0);
        DataSize physicalWrittenDataSize = DataSize.ofBytes(0);
        Optional<Integer> writerCount = Optional.empty();
        DataSize userMemoryReservation = DataSize.ofBytes(0);
        DataSize peakUserMemoryReservation = DataSize.ofBytes(0);
        DataSize revocableMemoryReservation = DataSize.ofBytes(0);
        long fullGcCount = 0;
        Duration fullGcTime = new Duration(0, MILLISECONDS);
        long dynamicFiltersVersion = INITIAL_DYNAMIC_FILTERS_VERSION;
        if (taskHolder.getFinalTaskInfo() != null) {
            TaskInfo taskInfo = taskHolder.getFinalTaskInfo();
            TaskStats taskStats = taskInfo.getStats();
            queuedPartitionedDrivers = taskStats.getQueuedPartitionedDrivers();
            queuedPartitionedSplitsWeight = taskStats.getQueuedPartitionedSplitsWeight();
            runningPartitionedDrivers = taskStats.getRunningPartitionedDrivers();
            runningPartitionedSplitsWeight = taskStats.getRunningPartitionedSplitsWeight();
            writerInputDataSize = taskStats.getWriterInputDataSize();
            physicalWrittenDataSize = taskStats.getPhysicalWrittenDataSize();
            writerCount = taskStats.getMaxWriterCount();
            userMemoryReservation = taskStats.getUserMemoryReservation();
            peakUserMemoryReservation = taskStats.getPeakUserMemoryReservation();
            revocableMemoryReservation = taskStats.getRevocableMemoryReservation();
            outputDataSize = taskStats.getOutputDataSize();
            fullGcCount = taskStats.getFullGcCount();
            fullGcTime = taskStats.getFullGcTime();
            dynamicFiltersVersion = taskHolder.getDynamicFiltersVersion();
        }
        else if (taskHolder.getTaskExecution() != null) {
            long physicalWrittenBytes = 0;
            TaskContext taskContext = taskHolder.getTaskExecution().getTaskContext();
            for (PipelineContext pipelineContext : taskContext.getPipelineContexts()) {
                PipelineStatus pipelineStatus = pipelineContext.getPipelineStatus();
                queuedPartitionedDrivers += pipelineStatus.getQueuedPartitionedDrivers();
                queuedPartitionedSplitsWeight += pipelineStatus.getQueuedPartitionedSplitsWeight();
                runningPartitionedDrivers += pipelineStatus.getRunningPartitionedDrivers();
                runningPartitionedSplitsWeight += pipelineStatus.getRunningPartitionedSplitsWeight();
                physicalWrittenBytes += pipelineContext.getPhysicalWrittenDataSize();
            }
            writerInputDataSize = succinctBytes(taskContext.getWriterInputDataSize());
            physicalWrittenDataSize = succinctBytes(physicalWrittenBytes);
            writerCount = taskContext.getMaxWriterCount();
            userMemoryReservation = taskContext.getMemoryReservation();
            peakUserMemoryReservation = taskContext.getPeakMemoryReservation();
            revocableMemoryReservation = taskContext.getRevocableMemoryReservation();
            outputDataSize = DataSize.ofBytes(taskContext.getOutputDataSize().getTotalCount());
            fullGcCount = taskContext.getFullGcCount();
            fullGcTime = taskContext.getFullGcTime();
            dynamicFiltersVersion = taskContext.getDynamicFiltersVersion();
        }

        return new TaskStatus(
                taskStateMachine.getTaskId(),
                taskInstanceId,
                versionNumber,
                state,
                location,
                nodeId,
                speculative.get(),
                failures,
                queuedPartitionedDrivers,
                runningPartitionedDrivers,
                outputBuffer.getStatus(),
                outputDataSize,
                writerInputDataSize,
                physicalWrittenDataSize,
                writerCount,
                userMemoryReservation,
                peakUserMemoryReservation,
                revocableMemoryReservation,
                fullGcCount,
                fullGcTime,
                dynamicFiltersVersion,
                queuedPartitionedSplitsWeight,
                runningPartitionedSplitsWeight);
    }

    private TaskStats getTaskStats(TaskHolder taskHolder)
    {
        TaskInfo finalTaskInfo = taskHolder.getFinalTaskInfo();
        if (finalTaskInfo != null) {
            return finalTaskInfo.getStats();
        }
        SqlTaskExecution taskExecution = taskHolder.getTaskExecution();
        if (taskExecution != null) {
            return taskExecution.getTaskContext().getTaskStats();
        }
        // if the task completed without creation, set end time
        DateTime endTime = taskStateMachine.getState().isDone() ? DateTime.now() : null;
        return new TaskStats(taskStateMachine.getCreatedTime(), endTime);
    }

    private static Set<PlanNodeId> getNoMoreSplits(TaskHolder taskHolder)
    {
        TaskInfo finalTaskInfo = taskHolder.getFinalTaskInfo();
        if (finalTaskInfo != null) {
            return finalTaskInfo.getNoMoreSplits();
        }
        SqlTaskExecution taskExecution = taskHolder.getTaskExecution();
        if (taskExecution != null) {
            return taskExecution.getNoMoreSplits();
        }
        return ImmutableSet.of();
    }

    private TaskInfo createTaskInfo(TaskHolder taskHolder)
    {
        // create task status first to prevent potentially seeing incomplete stats for a done task state
        TaskStatus taskStatus = createTaskStatus(taskHolder);
        TaskStats taskStats = getTaskStats(taskHolder);
        Set<PlanNodeId> noMoreSplits = getNoMoreSplits(taskHolder);

        return new TaskInfo(
                taskStatus,
                lastHeartbeat.get(),
                outputBuffer.getInfo(),
                noMoreSplits,
                taskStats,
                Optional.empty(),
                needsPlan.get());
    }

    public synchronized ListenableFuture<TaskStatus> getTaskStatus(long callersCurrentVersion)
    {
        if (callersCurrentVersion < taskStatusVersion.get() || taskHolderReference.get().isFinished()) {
            // return immediately if caller has older task status version or final task info is available
            return immediateFuture(getTaskStatus());
        }

        // At this point taskHolderReference.get().isFinished() might become true. However notifyStatusChanged()
        // is synchronized therefore notification for new listener won't be lost.
        return Futures.transform(taskStatusVersionChange.createNewListener(), input -> getTaskStatus(), directExecutor());
    }

    public synchronized ListenableFuture<TaskInfo> getTaskInfo(long callersCurrentVersion)
    {
        if (callersCurrentVersion < taskStatusVersion.get() || taskHolderReference.get().isFinished()) {
            // return immediately if caller has older task status version or final task info is available
            return immediateFuture(getTaskInfo());
        }

        // At this point taskHolderReference.get().isFinished() might become true. However notifyStatusChanged()
        // is synchronized therefore notification for new listener won't be lost.
        return Futures.transform(taskStatusVersionChange.createNewListener(), input -> getTaskInfo(), directExecutor());
    }

    public TaskInfo updateTask(
            Session session,
            Span stageSpan,
            Optional<PlanFragment> fragment,
            List<SplitAssignment> splitAssignments,
            OutputBuffers outputBuffers,
            Map<DynamicFilterId, Domain> dynamicFilterDomains,
            boolean speculative)
    {
        try {
            // trace token must be set first to make sure failure injection for getTaskResults requests works as expected
            session.getTraceToken().ifPresent(traceToken::set);

            // The LazyOutput buffer does not support write methods, so the actual
            // output buffer must be established before drivers are created (e.g.
            // a VALUES query).
            outputBuffer.setOutputBuffers(outputBuffers);

            // is task already complete?
            TaskHolder taskHolder = taskHolderReference.get();
            if (taskHolder.isFinished()) {
                return taskHolder.getFinalTaskInfo();
            }

            SqlTaskExecution taskExecution = taskHolder.getTaskExecution();
            if (taskExecution == null) {
                checkState(fragment.isPresent(), "fragment must be present");
                taskExecution = tryCreateSqlTaskExecution(session, stageSpan, fragment.get());
            }
            // taskExecution can still be null if the creation was skipped
            if (taskExecution != null) {
                taskExecution.getTaskContext().addDynamicFilter(dynamicFilterDomains);
                taskExecution.addSplitAssignments(splitAssignments);
            }

            // update speculative flag
            this.speculative.set(speculative);
        }
        catch (Error e) {
            failed(e);
            throw e;
        }
        catch (RuntimeException e) {
            return failed(e);
        }

        return getTaskInfo();
    }

    @Nullable
    private SqlTaskExecution tryCreateSqlTaskExecution(Session session, Span stageSpan, PlanFragment fragment)
    {
        synchronized (taskHolderLock) {
            // Recheck holder for task execution after acquiring the lock
            TaskHolder taskHolder = taskHolderReference.get();
            if (taskHolder.isFinished()) {
                return null;
            }
            SqlTaskExecution execution = taskHolder.getTaskExecution();
            if (execution != null) {
                return execution;
            }

            // Don't create SqlTaskExecution once termination has started
            if (taskStateMachine.getState().isTerminatingOrDone()) {
                return null;
            }

            taskSpan.set(tracer.spanBuilder("task")
                    .setParent(Context.current().with(stageSpan))
                    .setAttribute(TrinoAttributes.QUERY_ID, taskId.getQueryId().toString())
                    .setAttribute(TrinoAttributes.STAGE_ID, taskId.getStageId().toString())
                    .setAttribute(TrinoAttributes.TASK_ID, taskId.toString())
                    .startSpan());

            execution = sqlTaskExecutionFactory.create(
                    session,
                    taskSpan.get(),
                    queryContext,
                    taskStateMachine,
                    outputBuffer,
                    fragment,
                    this::notifyStatusChanged);
            needsPlan.set(false);
            execution.start();
            // this must happen after taskExecution.start(), otherwise it could become visible to a
            // concurrent update without being fully initialized
            checkState(taskHolderReference.compareAndSet(taskHolder, new TaskHolder(execution)), "unsynchronized concurrent task holder update");
            return execution;
        }
    }

    public ListenableFuture<BufferResult> getTaskResults(PipelinedOutputBuffers.OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return outputBuffer.get(bufferId, startingSequenceId, maxSize);
    }

    public void acknowledgeTaskResults(PipelinedOutputBuffers.OutputBufferId bufferId, long sequenceId)
    {
        requireNonNull(bufferId, "bufferId is null");

        outputBuffer.acknowledge(bufferId, sequenceId);
    }

    public TaskInfo destroyTaskResults(PipelinedOutputBuffers.OutputBufferId bufferId)
    {
        requireNonNull(bufferId, "bufferId is null");

        log.debug("Aborting task %s output %s", taskId, bufferId);
        outputBuffer.destroy(bufferId);

        return getTaskInfo();
    }

    public TaskInfo failed(Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        taskStateMachine.failed(cause);
        return getTaskInfo();
    }

    public TaskInfo cancel()
    {
        taskStateMachine.cancel();
        return getTaskInfo();
    }

    public TaskInfo abort()
    {
        taskStateMachine.abort();
        return getTaskInfo();
    }

    @Override
    public String toString()
    {
        return taskId.toString();
    }

    private static final class TaskHolder
    {
        private final SqlTaskExecution taskExecution;
        private final TaskInfo finalTaskInfo;
        private final SqlTaskIoStats finalIoStats;
        private final VersionedDynamicFilterDomains finalDynamicFilterDomains;

        private TaskHolder()
        {
            this.taskExecution = null;
            this.finalTaskInfo = null;
            this.finalIoStats = null;
            this.finalDynamicFilterDomains = null;
        }

        private TaskHolder(SqlTaskExecution taskExecution)
        {
            this.taskExecution = requireNonNull(taskExecution, "taskExecution is null");
            this.finalTaskInfo = null;
            this.finalIoStats = null;
            this.finalDynamicFilterDomains = null;
        }

        private TaskHolder(TaskInfo finalTaskInfo, SqlTaskIoStats finalIoStats, VersionedDynamicFilterDomains finalDynamicFilterDomains)
        {
            this.taskExecution = null;
            this.finalTaskInfo = requireNonNull(finalTaskInfo, "finalTaskInfo is null");
            this.finalIoStats = requireNonNull(finalIoStats, "finalIoStats is null");
            this.finalDynamicFilterDomains = requireNonNull(finalDynamicFilterDomains, "finalDynamicFilterDomains is null");
        }

        public boolean isFinished()
        {
            return finalTaskInfo != null;
        }

        @Nullable
        public SqlTaskExecution getTaskExecution()
        {
            return taskExecution;
        }

        @Nullable
        public TaskInfo getFinalTaskInfo()
        {
            return finalTaskInfo;
        }

        public SqlTaskIoStats getIoStats()
        {
            // if we are finished, return the final IoStats
            if (finalIoStats != null) {
                return finalIoStats;
            }
            // if we haven't started yet, return an empty IoStats
            if (taskExecution == null) {
                return new SqlTaskIoStats();
            }
            // get IoStats from the current task execution
            TaskContext taskContext = taskExecution.getTaskContext();
            return new SqlTaskIoStats(taskContext.getProcessedInputDataSize(), taskContext.getInputPositions(), taskContext.getOutputDataSize(), taskContext.getOutputPositions());
        }

        public VersionedDynamicFilterDomains acknowledgeAndGetNewDynamicFilterDomains(long callersSummaryVersion)
        {
            // if we are finished, return the final VersionedDynamicFilterDomains
            if (finalDynamicFilterDomains != null) {
                return finalDynamicFilterDomains;
            }
            // if we haven't started yet, return an empty VersionedDynamicFilterDomains
            if (taskExecution == null) {
                return INITIAL_DYNAMIC_FILTER_DOMAINS;
            }
            // get VersionedDynamicFilterDomains from the current task execution
            TaskContext taskContext = taskExecution.getTaskContext();
            return taskContext.acknowledgeAndGetNewDynamicFilterDomains(callersSummaryVersion);
        }

        public long getDynamicFiltersVersion()
        {
            // if we are finished, return the version of the final VersionedDynamicFilterDomains
            if (finalDynamicFilterDomains != null) {
                return finalDynamicFilterDomains.getVersion();
            }
            requireNonNull(taskExecution, "taskExecution is null");
            return taskExecution.getTaskContext().getDynamicFiltersVersion();
        }

        public VersionedDynamicFilterDomains getDynamicFilterDomains()
        {
            verify(finalDynamicFilterDomains == null, "finalDynamicFilterDomains has already been set");
            // Task was aborted or failed before taskExecution was created, return an empty VersionedDynamicFilterDomains
            if (taskExecution == null) {
                return INITIAL_DYNAMIC_FILTER_DOMAINS;
            }
            // get VersionedDynamicFilterDomains from the current task execution
            return taskExecution.getTaskContext().getCurrentDynamicFilterDomains();
        }
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateChangeListener<TaskState> stateChangeListener)
    {
        taskStateMachine.addStateChangeListener(stateChangeListener);
    }

    public void addSourceTaskFailureListener(TaskFailureListener listener)
    {
        taskStateMachine.addSourceTaskFailureListener(listener);
    }

    public QueryContext getQueryContext()
    {
        return queryContext;
    }

    public Optional<TaskContext> getTaskContext()
    {
        SqlTaskExecution taskExecution = taskHolderReference.get().getTaskExecution();
        if (taskExecution == null) {
            return Optional.empty();
        }
        return Optional.of(taskExecution.getTaskContext());
    }

    public Optional<String> getTraceToken()
    {
        return Optional.ofNullable(traceToken.get());
    }
}
