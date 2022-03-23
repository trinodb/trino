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
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.stats.GcMonitor;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.DynamicFiltersCollector;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.Lifespan;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStateMachine;
import io.trino.execution.buffer.LazyOutputBuffer;
import io.trino.memory.QueryContext;
import io.trino.memory.QueryContextVisitor;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.LocalDynamicFiltersCollector;
import io.trino.sql.planner.plan.DynamicFilterId;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class TaskContext
{
    private final QueryContext queryContext;
    private final TaskStateMachine taskStateMachine;
    private final GcMonitor gcMonitor;
    private final Executor notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final Session session;

    private final long createNanos = System.nanoTime();

    private final AtomicLong startNanos = new AtomicLong();
    private final AtomicLong startFullGcCount = new AtomicLong(-1);
    private final AtomicLong startFullGcTimeNanos = new AtomicLong(-1);
    private final AtomicLong endNanos = new AtomicLong();
    private final AtomicLong endFullGcCount = new AtomicLong(-1);
    private final AtomicLong endFullGcTimeNanos = new AtomicLong(-1);

    private final AtomicLong currentPeakUserMemoryReservation = new AtomicLong(0);

    private final AtomicReference<DateTime> executionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> lastExecutionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> executionEndTime = new AtomicReference<>();

    private final Set<Lifespan> completedDriverGroups = newConcurrentHashSet();

    private final List<PipelineContext> pipelineContexts = new CopyOnWriteArrayList<>();

    private final boolean perOperatorCpuTimerEnabled;
    private final boolean cpuTimerEnabled;

    private final Object cumulativeMemoryLock = new Object();
    private final AtomicDouble cumulativeUserMemory = new AtomicDouble(0.0);

    @GuardedBy("cumulativeMemoryLock")
    private long lastUserMemoryReservation;

    @GuardedBy("cumulativeMemoryLock")
    private long lastTaskStatCallNanos;

    private final MemoryTrackingContext taskMemoryContext;
    private final DynamicFiltersCollector dynamicFiltersCollector;

    // The collector is shared for dynamic filters collected from coordinator
    // as well as from local build-side of replicated joins. It is also shared with
    // with multiple table scans (e.g. co-located joins).
    private final LocalDynamicFiltersCollector localDynamicFiltersCollector;

    public static TaskContext createTaskContext(
            QueryContext queryContext,
            TaskStateMachine taskStateMachine,
            GcMonitor gcMonitor,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            Session session,
            MemoryTrackingContext taskMemoryContext,
            Runnable notifyStatusChanged,
            boolean perOperatorCpuTimerEnabled,
            boolean cpuTimerEnabled)
    {
        TaskContext taskContext = new TaskContext(
                queryContext,
                taskStateMachine,
                gcMonitor,
                notificationExecutor,
                yieldExecutor,
                session,
                taskMemoryContext,
                notifyStatusChanged,
                perOperatorCpuTimerEnabled,
                cpuTimerEnabled);
        taskContext.initialize();
        return taskContext;
    }

    private TaskContext(
            QueryContext queryContext,
            TaskStateMachine taskStateMachine,
            GcMonitor gcMonitor,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            Session session,
            MemoryTrackingContext taskMemoryContext,
            Runnable notifyStatusChanged,
            boolean perOperatorCpuTimerEnabled,
            boolean cpuTimerEnabled)
    {
        this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null");
        this.gcMonitor = requireNonNull(gcMonitor, "gcMonitor is null");
        this.queryContext = requireNonNull(queryContext, "queryContext is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.yieldExecutor = requireNonNull(yieldExecutor, "yieldExecutor is null");
        this.session = session;
        this.taskMemoryContext = requireNonNull(taskMemoryContext, "taskMemoryContext is null");

        // Initialize the local memory contexts with the LazyOutputBuffer tag as LazyOutputBuffer will do the local memory allocations
        this.taskMemoryContext.initializeLocalMemoryContexts(LazyOutputBuffer.class.getSimpleName());
        this.dynamicFiltersCollector = new DynamicFiltersCollector(notifyStatusChanged);
        this.localDynamicFiltersCollector = new LocalDynamicFiltersCollector(session);
        this.perOperatorCpuTimerEnabled = perOperatorCpuTimerEnabled;
        this.cpuTimerEnabled = cpuTimerEnabled;
    }

    // the state change listener is added here in a separate initialize() method
    // instead of the constructor to prevent leaking the "this" reference to
    // another thread, which will cause unsafe publication of this instance.
    private void initialize()
    {
        taskStateMachine.addStateChangeListener(this::updateStatsIfDone);
    }

    public TaskId getTaskId()
    {
        return taskStateMachine.getTaskId();
    }

    public PipelineContext addPipelineContext(int pipelineId, boolean inputPipeline, boolean outputPipeline, boolean partitioned)
    {
        PipelineContext pipelineContext = new PipelineContext(
                pipelineId,
                this,
                notificationExecutor,
                yieldExecutor,
                taskMemoryContext.newMemoryTrackingContext(),
                inputPipeline,
                outputPipeline,
                partitioned);
        pipelineContexts.add(pipelineContext);
        return pipelineContext;
    }

    public Session getSession()
    {
        return session;
    }

    public void start()
    {
        DateTime now = DateTime.now();
        executionStartTime.compareAndSet(null, now);
        startNanos.compareAndSet(0, System.nanoTime());
        startFullGcCount.compareAndSet(-1, gcMonitor.getMajorGcCount());
        startFullGcTimeNanos.compareAndSet(-1, gcMonitor.getMajorGcTime().roundTo(NANOSECONDS));

        // always update last execution start time
        lastExecutionStartTime.set(now);
    }

    private void updateStatsIfDone(TaskState newState)
    {
        if (newState.isDone()) {
            DateTime now = DateTime.now();
            long majorGcCount = gcMonitor.getMajorGcCount();
            long majorGcTime = gcMonitor.getMajorGcTime().roundTo(NANOSECONDS);

            // before setting the end times, make sure a start has been recorded
            executionStartTime.compareAndSet(null, now);
            startNanos.compareAndSet(0, System.nanoTime());
            startFullGcCount.compareAndSet(-1, majorGcCount);
            startFullGcTimeNanos.compareAndSet(-1, majorGcTime);

            // Only update last start time, if the nothing was started
            lastExecutionStartTime.compareAndSet(null, now);

            // use compare and set from initial value to avoid overwriting if there
            // were a duplicate notification, which shouldn't happen
            executionEndTime.compareAndSet(null, now);
            endNanos.compareAndSet(0, System.nanoTime());
            endFullGcCount.compareAndSet(-1, majorGcCount);
            endFullGcTimeNanos.compareAndSet(-1, majorGcTime);
        }
    }

    public void failed(Throwable cause)
    {
        taskStateMachine.failed(cause);
    }

    public boolean isDone()
    {
        return taskStateMachine.getState().isDone();
    }

    public TaskState getState()
    {
        return taskStateMachine.getState();
    }

    public DataSize getMemoryReservation()
    {
        return DataSize.ofBytes(taskMemoryContext.getUserMemory());
    }

    public DataSize getRevocableMemoryReservation()
    {
        return DataSize.ofBytes(taskMemoryContext.getRevocableMemory());
    }

    /**
     * Returns the completed driver groups (excluding taskWide).
     * A driver group is considered complete if all drivers associated with it
     * has completed, and no new drivers associated with it will be created.
     */
    public Set<Lifespan> getCompletedDriverGroups()
    {
        return completedDriverGroups;
    }

    public void addCompletedDriverGroup(Lifespan driverGroup)
    {
        checkArgument(!driverGroup.isTaskWide(), "driverGroup is task-wide, not a driver group.");
        completedDriverGroups.add(driverGroup);
    }

    public List<PipelineContext> getPipelineContexts()
    {
        return pipelineContexts;
    }

    public synchronized ListenableFuture<Void> reserveSpill(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        return queryContext.reserveSpill(bytes);
    }

    public synchronized void freeSpill(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        queryContext.freeSpill(bytes);
    }

    public LocalMemoryContext localMemoryContext()
    {
        return taskMemoryContext.localUserMemoryContext();
    }

    public boolean isPerOperatorCpuTimerEnabled()
    {
        return perOperatorCpuTimerEnabled;
    }

    public boolean isCpuTimerEnabled()
    {
        return cpuTimerEnabled;
    }

    public CounterStat getProcessedInputDataSize()
    {
        CounterStat stat = new CounterStat();
        for (PipelineContext pipelineContext : pipelineContexts) {
            if (pipelineContext.isInputPipeline()) {
                stat.merge(pipelineContext.getProcessedInputDataSize());
            }
        }
        return stat;
    }

    public CounterStat getInputPositions()
    {
        CounterStat stat = new CounterStat();
        for (PipelineContext pipelineContext : pipelineContexts) {
            if (pipelineContext.isInputPipeline()) {
                stat.merge(pipelineContext.getInputPositions());
            }
        }
        return stat;
    }

    public CounterStat getOutputDataSize()
    {
        CounterStat stat = new CounterStat();
        for (PipelineContext pipelineContext : pipelineContexts) {
            if (pipelineContext.isOutputPipeline()) {
                stat.merge(pipelineContext.getOutputDataSize());
            }
        }
        return stat;
    }

    public CounterStat getOutputPositions()
    {
        CounterStat stat = new CounterStat();
        for (PipelineContext pipelineContext : pipelineContexts) {
            if (pipelineContext.isOutputPipeline()) {
                stat.merge(pipelineContext.getOutputPositions());
            }
        }
        return stat;
    }

    public Duration getFullGcTime()
    {
        long startFullGcTimeNanos = this.startFullGcTimeNanos.get();
        if (startFullGcTimeNanos < 0) {
            return new Duration(0, MILLISECONDS);
        }

        long endFullGcTimeNanos = this.endFullGcTimeNanos.get();
        if (endFullGcTimeNanos < 0) {
            endFullGcTimeNanos = gcMonitor.getMajorGcTime().roundTo(NANOSECONDS);
        }
        return new Duration(max(0, endFullGcTimeNanos - startFullGcTimeNanos), NANOSECONDS);
    }

    public int getFullGcCount()
    {
        long startFullGcCount = this.startFullGcCount.get();
        if (startFullGcCount < 0) {
            return 0;
        }

        long endFullGcCount = this.endFullGcCount.get();
        if (endFullGcCount <= 0) {
            endFullGcCount = gcMonitor.getMajorGcCount();
        }
        return toIntExact(max(0, endFullGcCount - startFullGcCount));
    }

    public void updateDomains(Map<DynamicFilterId, Domain> dynamicFilterDomains)
    {
        dynamicFiltersCollector.updateDomains(dynamicFilterDomains);
    }

    public long getDynamicFiltersVersion()
    {
        return dynamicFiltersCollector.getDynamicFiltersVersion();
    }

    public VersionedDynamicFilterDomains acknowledgeAndGetNewDynamicFilterDomains(long callersCurrentVersion)
    {
        return dynamicFiltersCollector.acknowledgeAndGetNewDomains(callersCurrentVersion);
    }

    public TaskStats getTaskStats()
    {
        // check for end state to avoid callback ordering problems
        updateStatsIfDone(taskStateMachine.getState());

        List<PipelineStats> pipelineStats = ImmutableList.copyOf(transform(pipelineContexts, PipelineContext::getPipelineStats));

        long lastExecutionEndTime = 0;

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int queuedPartitionedDrivers = 0;
        long queuedPartitionedSplitsWeight = 0;
        int runningDrivers = 0;
        int runningPartitionedDrivers = 0;
        long runningPartitionedSplitsWeight = 0;
        int blockedDrivers = 0;
        int completedDrivers = 0;

        long totalScheduledTime = 0;
        long totalCpuTime = 0;
        long totalBlockedTime = 0;

        long physicalInputDataSize = 0;
        long physicalInputPositions = 0;
        long physicalInputReadTime = 0;

        long internalNetworkInputDataSize = 0;
        long internalNetworkInputPositions = 0;

        long rawInputDataSize = 0;
        long rawInputPositions = 0;

        long processedInputDataSize = 0;
        long processedInputPositions = 0;

        long inputBlockedTime = 0;

        long outputDataSize = 0;
        long outputPositions = 0;

        long outputBlockedTime = 0;

        long physicalWrittenDataSize = 0;

        boolean hasRunningPipelines = false;
        boolean runningPipelinesFullyBlocked = true;
        ImmutableSet.Builder<BlockedReason> blockedReasons = ImmutableSet.builder();

        for (PipelineStats pipeline : pipelineStats) {
            if (pipeline.getLastEndTime() != null) {
                lastExecutionEndTime = max(pipeline.getLastEndTime().getMillis(), lastExecutionEndTime);
            }
            if (pipeline.getRunningDrivers() > 0 || pipeline.getRunningPartitionedDrivers() > 0 || pipeline.getBlockedDrivers() > 0) {
                // pipeline is running
                hasRunningPipelines = true;
                runningPipelinesFullyBlocked &= pipeline.isFullyBlocked();
                blockedReasons.addAll(pipeline.getBlockedReasons());
            }

            totalDrivers += pipeline.getTotalDrivers();
            queuedDrivers += pipeline.getQueuedDrivers();
            queuedPartitionedDrivers += pipeline.getQueuedPartitionedDrivers();
            queuedPartitionedSplitsWeight += pipeline.getQueuedPartitionedSplitsWeight();
            runningDrivers += pipeline.getRunningDrivers();
            runningPartitionedDrivers += pipeline.getRunningPartitionedDrivers();
            runningPartitionedSplitsWeight += pipeline.getRunningPartitionedSplitsWeight();
            blockedDrivers += pipeline.getBlockedDrivers();
            completedDrivers += pipeline.getCompletedDrivers();

            totalScheduledTime += pipeline.getTotalScheduledTime().roundTo(NANOSECONDS);
            totalCpuTime += pipeline.getTotalCpuTime().roundTo(NANOSECONDS);
            totalBlockedTime += pipeline.getTotalBlockedTime().roundTo(NANOSECONDS);

            if (pipeline.isInputPipeline()) {
                physicalInputDataSize += pipeline.getPhysicalInputDataSize().toBytes();
                physicalInputPositions += pipeline.getPhysicalInputPositions();
                physicalInputReadTime += pipeline.getPhysicalInputReadTime().roundTo(NANOSECONDS);

                internalNetworkInputDataSize += pipeline.getInternalNetworkInputDataSize().toBytes();
                internalNetworkInputPositions += pipeline.getInternalNetworkInputPositions();

                rawInputDataSize += pipeline.getRawInputDataSize().toBytes();
                rawInputPositions += pipeline.getRawInputPositions();

                processedInputDataSize += pipeline.getProcessedInputDataSize().toBytes();
                processedInputPositions += pipeline.getProcessedInputPositions();

                inputBlockedTime += pipeline.getInputBlockedTime().roundTo(NANOSECONDS);
            }

            if (pipeline.isOutputPipeline()) {
                outputDataSize += pipeline.getOutputDataSize().toBytes();
                outputPositions += pipeline.getOutputPositions();

                outputBlockedTime += pipeline.getOutputBlockedTime().roundTo(NANOSECONDS);
            }

            physicalWrittenDataSize += pipeline.getPhysicalWrittenDataSize().toBytes();
        }

        long startNanos = this.startNanos.get();
        if (startNanos == 0) {
            startNanos = System.nanoTime();
        }
        Duration queuedTime = new Duration(startNanos - createNanos, NANOSECONDS);

        long endNanos = this.endNanos.get();
        Duration elapsedTime;
        if (endNanos >= startNanos) {
            elapsedTime = new Duration(endNanos - createNanos, NANOSECONDS);
        }
        else {
            elapsedTime = new Duration(System.nanoTime() - createNanos, NANOSECONDS);
        }

        int fullGcCount = getFullGcCount();
        Duration fullGcTime = getFullGcTime();

        long userMemory = taskMemoryContext.getUserMemory();
        currentPeakUserMemoryReservation.updateAndGet(oldValue -> max(oldValue, userMemory));

        synchronized (cumulativeMemoryLock) {
            long currentTimeNanos = System.nanoTime();
            double sinceLastPeriodMillis = (currentTimeNanos - lastTaskStatCallNanos) / 1_000_000.0;
            long averageUserMemoryForLastPeriod = (userMemory + lastUserMemoryReservation) / 2;
            cumulativeUserMemory.addAndGet(averageUserMemoryForLastPeriod * sinceLastPeriodMillis);

            lastTaskStatCallNanos = currentTimeNanos;
            lastUserMemoryReservation = userMemory;
        }

        boolean fullyBlocked = hasRunningPipelines && runningPipelinesFullyBlocked;

        return new TaskStats(
                taskStateMachine.getCreatedTime(),
                executionStartTime.get(),
                lastExecutionStartTime.get(),
                lastExecutionEndTime == 0 ? null : new DateTime(lastExecutionEndTime),
                executionEndTime.get(),
                elapsedTime.convertToMostSuccinctTimeUnit(),
                queuedTime.convertToMostSuccinctTimeUnit(),
                totalDrivers,
                queuedDrivers,
                queuedPartitionedDrivers,
                queuedPartitionedSplitsWeight,
                runningDrivers,
                runningPartitionedDrivers,
                runningPartitionedSplitsWeight,
                blockedDrivers,
                completedDrivers,
                cumulativeUserMemory.get(),
                succinctBytes(userMemory),
                succinctBytes(currentPeakUserMemoryReservation.get()),
                succinctBytes(taskMemoryContext.getRevocableMemory()),
                new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                fullyBlocked && (runningDrivers > 0 || runningPartitionedDrivers > 0),
                blockedReasons.build(),
                succinctBytes(physicalInputDataSize),
                physicalInputPositions,
                new Duration(physicalInputReadTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                succinctBytes(internalNetworkInputDataSize),
                internalNetworkInputPositions,
                succinctBytes(rawInputDataSize),
                rawInputPositions,
                succinctBytes(processedInputDataSize),
                processedInputPositions,
                new Duration(inputBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                succinctBytes(outputDataSize),
                outputPositions,
                new Duration(outputBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                succinctBytes(physicalWrittenDataSize),
                fullGcCount,
                fullGcTime,
                pipelineStats);
    }

    public <C, R> R accept(QueryContextVisitor<C, R> visitor, C context)
    {
        return visitor.visitTaskContext(this, context);
    }

    public <C, R> List<R> acceptChildren(QueryContextVisitor<C, R> visitor, C context)
    {
        return pipelineContexts.stream()
                .map(pipelineContext -> pipelineContext.accept(visitor, context))
                .collect(toList());
    }

    @VisibleForTesting
    public synchronized MemoryTrackingContext getTaskMemoryContext()
    {
        return taskMemoryContext;
    }

    @VisibleForTesting
    public QueryContext getQueryContext()
    {
        return queryContext;
    }

    public LocalDynamicFiltersCollector getLocalDynamicFiltersCollector()
    {
        return localDynamicFiltersCollector;
    }

    public void addDynamicFilter(Map<DynamicFilterId, Domain> dynamicFilterDomains)
    {
        localDynamicFiltersCollector.collectDynamicFilterDomains(dynamicFilterDomains);
    }

    public void sourceTaskFailed(TaskId taskId, Throwable failure)
    {
        taskStateMachine.sourceTaskFailed(taskId, failure);
    }
}
