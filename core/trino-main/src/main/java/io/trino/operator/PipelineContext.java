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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.stats.CounterStat;
import io.airlift.stats.Distribution;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.TaskId;
import io.trino.memory.QueryContextVisitor;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.spi.metrics.Metrics;
import org.joda.time.DateTime;

import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class PipelineContext
{
    private final TaskContext taskContext;
    private final Executor notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final int pipelineId;

    private final boolean inputPipeline;
    private final boolean outputPipeline;
    private final boolean partitioned;

    private final List<DriverContext> drivers = new CopyOnWriteArrayList<>();

    private final AtomicInteger totalSplits = new AtomicInteger();
    private final AtomicLong totalSplitsWeight = new AtomicLong();
    private final AtomicInteger completedDrivers = new AtomicInteger();
    private final AtomicLong completedSplitsWeight = new AtomicLong();

    private final AtomicReference<DateTime> executionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> lastExecutionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> lastExecutionEndTime = new AtomicReference<>();

    private final Distribution queuedTime = new Distribution();
    private final Distribution elapsedTime = new Distribution();

    private final AtomicLong totalScheduledTime = new AtomicLong();
    private final AtomicLong totalCpuTime = new AtomicLong();
    private final AtomicLong totalBlockedTime = new AtomicLong();

    private final CounterStat physicalInputDataSize = new CounterStat();
    private final CounterStat physicalInputPositions = new CounterStat();
    private final AtomicLong physicalInputReadTime = new AtomicLong();

    private final CounterStat internalNetworkInputDataSize = new CounterStat();
    private final CounterStat internalNetworkInputPositions = new CounterStat();

    private final CounterStat rawInputDataSize = new CounterStat();
    private final CounterStat rawInputPositions = new CounterStat();

    private final CounterStat processedInputDataSize = new CounterStat();
    private final CounterStat processedInputPositions = new CounterStat();

    private final AtomicLong inputBlockedTime = new AtomicLong();

    private final CounterStat outputDataSize = new CounterStat();
    private final CounterStat outputPositions = new CounterStat();

    private final AtomicLong outputBlockedTime = new AtomicLong();

    private final AtomicLong physicalWrittenDataSize = new AtomicLong();

    private final ConcurrentMap<AlternativeOperatorId, OperatorStats> operatorSummaries = new ConcurrentHashMap<>();
    // pre-merged metrics which are shared among instances of given operator within pipeline
    private final ConcurrentMap<AlternativeOperatorId, Metrics> pipelineOperatorMetrics = new ConcurrentHashMap<>();

    private final MemoryTrackingContext pipelineMemoryContext;

    public PipelineContext(int pipelineId, TaskContext taskContext, Executor notificationExecutor, ScheduledExecutorService yieldExecutor, ScheduledExecutorService timeoutExecutor, MemoryTrackingContext pipelineMemoryContext, boolean inputPipeline, boolean outputPipeline, boolean partitioned)
    {
        this.pipelineId = pipelineId;
        this.inputPipeline = inputPipeline;
        this.outputPipeline = outputPipeline;
        this.partitioned = partitioned;
        this.taskContext = requireNonNull(taskContext, "taskContext is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.yieldExecutor = requireNonNull(yieldExecutor, "yieldExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.pipelineMemoryContext = requireNonNull(pipelineMemoryContext, "pipelineMemoryContext is null");
        // Initialize the local memory contexts with the ExchangeOperator tag as ExchangeOperator will do the local memory allocations
        pipelineMemoryContext.initializeLocalMemoryContexts(ExchangeOperator.class.getSimpleName());
    }

    public TaskContext getTaskContext()
    {
        return taskContext;
    }

    public TaskId getTaskId()
    {
        return taskContext.getTaskId();
    }

    public int getPipelineId()
    {
        return pipelineId;
    }

    public boolean isInputPipeline()
    {
        return inputPipeline;
    }

    public boolean isOutputPipeline()
    {
        return outputPipeline;
    }

    public DriverContext addDriverContext()
    {
        return addDriverContext(0);
    }

    public DriverContext addDriverContext(long splitWeight)
    {
        checkArgument(partitioned || splitWeight == 0, "Only partitioned splits should have weights");
        DriverContext driverContext = new DriverContext(
                this,
                notificationExecutor,
                yieldExecutor,
                timeoutExecutor,
                pipelineMemoryContext.newMemoryTrackingContext(),
                splitWeight);
        drivers.add(driverContext);
        return driverContext;
    }

    public Session getSession()
    {
        return taskContext.getSession();
    }

    public void splitsAdded(int count, long weightSum)
    {
        checkArgument(count >= 0 && weightSum >= 0);
        totalSplits.addAndGet(count);
        if (partitioned && weightSum != 0) {
            totalSplitsWeight.addAndGet(weightSum);
        }
    }

    public void setPipelineOperatorMetrics(int operatorId, int alternativeId, Metrics metrics)
    {
        pipelineOperatorMetrics.put(new AlternativeOperatorId(operatorId, alternativeId), metrics);
    }

    public void driverFinished(DriverContext driverContext)
    {
        requireNonNull(driverContext, "driverContext is null");

        if (!drivers.remove(driverContext)) {
            throw new IllegalArgumentException("Unknown driver " + driverContext);
        }

        // always update last execution end time
        lastExecutionEndTime.set(DateTime.now());

        DriverStats driverStats = driverContext.getDriverStats();

        completedDrivers.getAndIncrement();
        if (partitioned) {
            completedSplitsWeight.addAndGet(driverContext.getSplitWeight());
        }

        queuedTime.add(driverStats.getQueuedTime().roundTo(NANOSECONDS));
        elapsedTime.add(driverStats.getElapsedTime().roundTo(NANOSECONDS));

        totalScheduledTime.getAndAdd(driverStats.getTotalScheduledTime().roundTo(NANOSECONDS));
        totalCpuTime.getAndAdd(driverStats.getTotalCpuTime().roundTo(NANOSECONDS));

        totalBlockedTime.getAndAdd(driverStats.getTotalBlockedTime().roundTo(NANOSECONDS));

        // merge the operator stats into the operator summary
        List<OperatorStats> operators = driverStats.getOperatorStats();
        for (OperatorStats operator : operators) {
            AlternativeOperatorId alternativeOperatorId = new AlternativeOperatorId(operator.getOperatorId(), operator.getAlternativeId());
            Metrics pipelineLevelMetrics = pipelineOperatorMetrics.getOrDefault(alternativeOperatorId, Metrics.EMPTY);
            operatorSummaries.merge(alternativeOperatorId, operator, (first, second) -> first.addFillingPipelineMetrics(second, pipelineLevelMetrics));
        }

        physicalInputDataSize.update(driverStats.getPhysicalInputDataSize().toBytes());
        physicalInputPositions.update(driverStats.getPhysicalInputPositions());
        physicalInputReadTime.getAndAdd(driverStats.getPhysicalInputReadTime().roundTo(NANOSECONDS));

        internalNetworkInputDataSize.update(driverStats.getInternalNetworkInputDataSize().toBytes());
        internalNetworkInputPositions.update(driverStats.getInternalNetworkInputPositions());

        rawInputDataSize.update(driverStats.getRawInputDataSize().toBytes());
        rawInputPositions.update(driverStats.getRawInputPositions());

        processedInputDataSize.update(driverStats.getProcessedInputDataSize().toBytes());
        processedInputPositions.update(driverStats.getProcessedInputPositions());

        inputBlockedTime.getAndAdd(driverStats.getInputBlockedTime().roundTo(NANOSECONDS));

        outputDataSize.update(driverStats.getOutputDataSize().toBytes());
        outputPositions.update(driverStats.getOutputPositions());

        outputBlockedTime.getAndAdd(driverStats.getOutputBlockedTime().roundTo(NANOSECONDS));

        physicalWrittenDataSize.getAndAdd(driverStats.getPhysicalWrittenDataSize().toBytes());
    }

    public void start()
    {
        DateTime now = DateTime.now();
        executionStartTime.compareAndSet(null, now);
        // always update last execution start time
        lastExecutionStartTime.set(now);

        taskContext.start();
    }

    public void driverFailed(Throwable cause)
    {
        taskContext.failed(cause);
    }

    public boolean isTerminatingOrDone()
    {
        return taskContext.isTerminatingOrDone();
    }

    public synchronized ListenableFuture<Void> reserveSpill(long bytes)
    {
        return taskContext.reserveSpill(bytes);
    }

    public synchronized void freeSpill(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        taskContext.freeSpill(bytes);
    }

    public LocalMemoryContext localMemoryContext()
    {
        return pipelineMemoryContext.localUserMemoryContext();
    }

    public boolean isPerOperatorCpuTimerEnabled()
    {
        return taskContext.isPerOperatorCpuTimerEnabled();
    }

    public boolean isCpuTimerEnabled()
    {
        return taskContext.isCpuTimerEnabled();
    }

    public CounterStat getProcessedInputDataSize()
    {
        CounterStat stat = new CounterStat();
        stat.merge(processedInputDataSize);
        for (DriverContext driver : drivers) {
            stat.merge(driver.getInputDataSize());
        }
        return stat;
    }

    public CounterStat getInputPositions()
    {
        CounterStat stat = new CounterStat();
        stat.merge(processedInputPositions);
        for (DriverContext driver : drivers) {
            stat.merge(driver.getInputPositions());
        }
        return stat;
    }

    public CounterStat getOutputDataSize()
    {
        CounterStat stat = new CounterStat();
        stat.merge(outputDataSize);
        for (DriverContext driver : drivers) {
            stat.merge(driver.getOutputDataSize());
        }
        return stat;
    }

    public CounterStat getOutputPositions()
    {
        CounterStat stat = new CounterStat();
        stat.merge(outputPositions);
        for (DriverContext driver : drivers) {
            stat.merge(driver.getOutputPositions());
        }
        return stat;
    }

    public long getWriterInputDataSize()
    {
        // Avoid using stream api due to performance reasons
        long writerInputDataSize = 0;
        for (DriverContext context : drivers) {
            writerInputDataSize += context.getWriterInputDataSize();
        }
        return writerInputDataSize;
    }

    public long getPhysicalWrittenDataSize()
    {
        // Avoid using stream api due to performance reasons
        long physicalWrittenBytes = 0;
        for (DriverContext context : drivers) {
            physicalWrittenBytes += context.getPhysicalWrittenDataSize();
        }
        return physicalWrittenBytes;
    }

    public PipelineStatus getPipelineStatus()
    {
        return getPipelineStatus(drivers.iterator(), totalSplits.get(), completedDrivers.get(), getActivePartitionedSplitsWeight(), partitioned);
    }

    private long getActivePartitionedSplitsWeight()
    {
        if (partitioned) {
            return totalSplitsWeight.get() - completedSplitsWeight.get();
        }
        return 0;
    }

    public PipelineStats getPipelineStats()
    {
        // check for end state to avoid callback ordering problems
        if (taskContext.getState().isDone()) {
            DateTime now = DateTime.now();
            executionStartTime.compareAndSet(null, now);
            lastExecutionStartTime.compareAndSet(null, now);
            lastExecutionEndTime.compareAndSet(null, now);
        }

        int completedDrivers = this.completedDrivers.get();
        List<DriverContext> driverContexts = ImmutableList.copyOf(this.drivers);
        int totalSplits = this.totalSplits.get();
        PipelineStatusBuilder pipelineStatusBuilder = new PipelineStatusBuilder(totalSplits, completedDrivers, getActivePartitionedSplitsWeight(), partitioned);

        int totalDrivers = completedDrivers + driverContexts.size();

        Distribution queuedTime = this.queuedTime.duplicate();
        Distribution elapsedTime = this.elapsedTime.duplicate();

        long totalScheduledTime = this.totalScheduledTime.get();
        long totalCpuTime = this.totalCpuTime.get();
        long totalBlockedTime = this.totalBlockedTime.get();

        long physicalInputDataSize = this.physicalInputDataSize.getTotalCount();
        long physicalInputPositions = this.physicalInputPositions.getTotalCount();

        long internalNetworkInputDataSize = this.internalNetworkInputDataSize.getTotalCount();
        long internalNetworkInputPositions = this.internalNetworkInputPositions.getTotalCount();

        long rawInputDataSize = this.rawInputDataSize.getTotalCount();
        long rawInputPositions = this.rawInputPositions.getTotalCount();

        long processedInputDataSize = this.processedInputDataSize.getTotalCount();
        long processedInputPositions = this.processedInputPositions.getTotalCount();
        long physicalInputReadTime = this.physicalInputReadTime.get();

        long inputBlockedTime = this.inputBlockedTime.get();

        long outputDataSize = this.outputDataSize.getTotalCount();
        long outputPositions = this.outputPositions.getTotalCount();

        long outputBlockedTime = this.outputBlockedTime.get();

        long physicalWrittenDataSize = this.physicalWrittenDataSize.get();

        ImmutableSet.Builder<BlockedReason> blockedReasons = ImmutableSet.builder();
        boolean hasUnfinishedDrivers = false;
        boolean unfinishedDriversFullyBlocked = true;

        TreeMap<AlternativeOperatorId, OperatorStats> operatorSummaries = new TreeMap<>(this.operatorSummaries);
        // Expect the same number of operators as existing summaries, with one operator per driver context in the resulting multimap
        ListMultimap<AlternativeOperatorId, OperatorStats> runningOperators = ArrayListMultimap.create(operatorSummaries.size(), driverContexts.size());
        ImmutableList.Builder<DriverStats> drivers = ImmutableList.builderWithExpectedSize(driverContexts.size());
        for (DriverContext driverContext : driverContexts) {
            DriverStats driverStats = driverContext.getDriverStats();
            drivers.add(driverStats);
            pipelineStatusBuilder.accumulate(driverStats, driverContext.getSplitWeight());
            if (driverStats.getStartTime() != null && driverStats.getEndTime() == null) {
                // driver has started running, but not yet completed
                hasUnfinishedDrivers = true;
                unfinishedDriversFullyBlocked &= driverStats.isFullyBlocked();
                blockedReasons.addAll(driverStats.getBlockedReasons());
            }

            queuedTime.add(driverStats.getQueuedTime().roundTo(NANOSECONDS));
            elapsedTime.add(driverStats.getElapsedTime().roundTo(NANOSECONDS));

            totalScheduledTime += driverStats.getTotalScheduledTime().roundTo(NANOSECONDS);
            totalCpuTime += driverStats.getTotalCpuTime().roundTo(NANOSECONDS);
            totalBlockedTime += driverStats.getTotalBlockedTime().roundTo(NANOSECONDS);

            for (OperatorStats operatorStats : driverStats.getOperatorStats()) {
                runningOperators.put(new AlternativeOperatorId(operatorStats.getOperatorId(), operatorStats.getAlternativeId()), operatorStats);
            }

            physicalInputDataSize += driverStats.getPhysicalInputDataSize().toBytes();
            physicalInputPositions += driverStats.getPhysicalInputPositions();
            physicalInputReadTime += driverStats.getPhysicalInputReadTime().roundTo(NANOSECONDS);

            internalNetworkInputDataSize += driverStats.getInternalNetworkInputDataSize().toBytes();
            internalNetworkInputPositions += driverStats.getInternalNetworkInputPositions();

            rawInputDataSize += driverStats.getRawInputDataSize().toBytes();
            rawInputPositions += driverStats.getRawInputPositions();

            processedInputDataSize += driverStats.getProcessedInputDataSize().toBytes();
            processedInputPositions += driverStats.getProcessedInputPositions();

            inputBlockedTime += driverStats.getInputBlockedTime().roundTo(NANOSECONDS);

            outputDataSize += driverStats.getOutputDataSize().toBytes();
            outputPositions += driverStats.getOutputPositions();

            outputBlockedTime += driverStats.getOutputBlockedTime().roundTo(NANOSECONDS);

            physicalWrittenDataSize += driverStats.getPhysicalWrittenDataSize().toBytes();
        }

        // Computes the combined stats from existing completed operators and those still running
        BiFunction<AlternativeOperatorId, OperatorStats, OperatorStats> combineOperatorStats = (operatorId, current) -> {
            List<OperatorStats> runningStats = runningOperators.get(operatorId);
            if (runningStats.isEmpty()) {
                return current;
            }
            Metrics pipelineLevelMetrics = pipelineOperatorMetrics.getOrDefault(operatorId, Metrics.EMPTY);
            if (current != null) {
                return current.addFillingPipelineMetrics(runningStats, pipelineLevelMetrics);
            }
            else {
                OperatorStats combined = runningStats.get(0);
                if (runningStats.size() > 1) {
                    combined = combined.addFillingPipelineMetrics(runningStats.subList(1, runningStats.size()), pipelineLevelMetrics);
                }
                else if (pipelineLevelMetrics != Metrics.EMPTY) {
                    combined = combined.withPipelineMetrics(pipelineLevelMetrics);
                }
                return combined;
            }
        };
        for (AlternativeOperatorId operatorId : runningOperators.keySet()) {
            operatorSummaries.compute(operatorId, combineOperatorStats);
        }

        PipelineStatus pipelineStatus = pipelineStatusBuilder.build();
        boolean fullyBlocked = hasUnfinishedDrivers && unfinishedDriversFullyBlocked;

        return new PipelineStats(
                pipelineId,

                executionStartTime.get(),
                lastExecutionStartTime.get(),
                lastExecutionEndTime.get(),

                inputPipeline,
                outputPipeline,

                totalDrivers,
                pipelineStatus.getQueuedDrivers(),
                pipelineStatus.getQueuedPartitionedDrivers(),
                pipelineStatus.getQueuedPartitionedSplitsWeight(),
                pipelineStatus.getRunningDrivers(),
                pipelineStatus.getRunningPartitionedDrivers(),
                pipelineStatus.getRunningPartitionedSplitsWeight(),
                pipelineStatus.getBlockedDrivers(),
                completedDrivers,

                succinctBytes(pipelineMemoryContext.getUserMemory()),
                succinctBytes(pipelineMemoryContext.getRevocableMemory()),

                queuedTime.snapshot(),
                elapsedTime.snapshot(),

                new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                fullyBlocked,
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

                ImmutableList.copyOf(operatorSummaries.values()),
                drivers.build());
    }

    public <C, R> R accept(QueryContextVisitor<C, R> visitor, C context)
    {
        return visitor.visitPipelineContext(this, context);
    }

    public <C, R> List<R> acceptChildren(QueryContextVisitor<C, R> visitor, C context)
    {
        return drivers.stream()
                .map(driver -> driver.accept(visitor, context))
                .collect(toList());
    }

    @VisibleForTesting
    public MemoryTrackingContext getPipelineMemoryContext()
    {
        return pipelineMemoryContext;
    }

    private static PipelineStatus getPipelineStatus(Iterator<DriverContext> driverContextsIterator, int totalSplits, int completedDrivers, long activePartitionedSplitsWeight, boolean partitioned)
    {
        PipelineStatusBuilder builder = new PipelineStatusBuilder(totalSplits, completedDrivers, activePartitionedSplitsWeight, partitioned);
        while (driverContextsIterator.hasNext()) {
            builder.accumulate(driverContextsIterator.next());
        }
        return builder.build();
    }

    /**
     * Allows building a {@link PipelineStatus} either from a series of {@link DriverContext} instances or
     * {@link DriverStats} instances. In {@link PipelineContext#getPipelineStats()} where {@link DriverStats}
     * instances are already created as a state snapshot of {@link DriverContext}, using those instead of
     * re-checking the fields on {@link DriverContext} is cheaper since it avoids extra volatile reads and
     * reduces the opportunities to read inconsistent values
     */
    private static final class PipelineStatusBuilder
    {
        private final int totalSplits;
        private final int completedDrivers;
        private final long activePartitionedSplitsWeight;
        private final boolean partitioned;
        private int runningDrivers;
        private int blockedDrivers;
        private long runningSplitsWeight;
        private long blockedSplitsWeight;
        // When a split for a partitioned pipeline is delivered to a worker,
        // conceptually, the worker would have an additional driver.
        // The queuedDrivers field in PipelineStatus is supposed to represent this.
        // However, due to implementation details of SqlTaskExecution, it may defer instantiation of drivers.
        //
        // physically queued drivers: actual number of instantiated drivers whose execution hasn't started
        // conceptually queued drivers: includes assigned splits that haven't been turned into a driver
        private int physicallyQueuedDrivers;

        private PipelineStatusBuilder(int totalSplits, int completedDrivers, long activePartitionedSplitsWeight, boolean partitioned)
        {
            this.totalSplits = totalSplits;
            this.completedDrivers = completedDrivers;
            this.activePartitionedSplitsWeight = activePartitionedSplitsWeight;
            this.partitioned = partitioned;
        }

        public void accumulate(DriverContext driverContext)
        {
            if (!driverContext.isExecutionStarted()) {
                physicallyQueuedDrivers++;
            }
            else if (driverContext.isFullyBlocked()) {
                blockedDrivers++;
                blockedSplitsWeight += driverContext.getSplitWeight();
            }
            else {
                runningDrivers++;
                runningSplitsWeight += driverContext.getSplitWeight();
            }
        }

        public void accumulate(DriverStats driverStats, long splitWeight)
        {
            if (driverStats.getStartTime() == null) {
                // driver has not started running
                physicallyQueuedDrivers++;
            }
            else if (driverStats.isFullyBlocked()) {
                blockedDrivers++;
                blockedSplitsWeight += splitWeight;
            }
            else {
                runningDrivers++;
                runningSplitsWeight += splitWeight;
            }
        }

        public PipelineStatus build()
        {
            int queuedDrivers;
            int queuedPartitionedSplits;
            int runningPartitionedSplits;
            long queuedPartitionedSplitsWeight;
            long runningPartitionedSplitsWeight;
            if (partitioned) {
                queuedDrivers = totalSplits - runningDrivers - blockedDrivers - completedDrivers;
                if (queuedDrivers < 0) {
                    // It is possible to observe negative here because inputs to the above expression was not taken in a snapshot.
                    queuedDrivers = 0;
                }
                queuedPartitionedSplitsWeight = activePartitionedSplitsWeight - runningSplitsWeight - blockedSplitsWeight;
                if (queuedDrivers == 0 || queuedPartitionedSplitsWeight < 0) {
                    // negative or inconsistent count vs weight inputs might occur
                    queuedPartitionedSplitsWeight = 0;
                }
                queuedPartitionedSplits = queuedDrivers;
                runningPartitionedSplits = runningDrivers;
                runningPartitionedSplitsWeight = runningSplitsWeight;
            }
            else {
                queuedDrivers = physicallyQueuedDrivers;
                queuedPartitionedSplits = 0;
                queuedPartitionedSplitsWeight = 0;
                runningPartitionedSplits = 0;
                runningPartitionedSplitsWeight = 0;
            }
            return new PipelineStatus(queuedDrivers, runningDrivers, blockedDrivers, queuedPartitionedSplits, queuedPartitionedSplitsWeight, runningPartitionedSplits, runningPartitionedSplitsWeight);
        }
    }

    private record AlternativeOperatorId(int operatorId, int alternativeId)
            implements Comparable<AlternativeOperatorId>
    {
        @Override
        public int compareTo(AlternativeOperatorId o)
        {
            if (alternativeId != o.alternativeId) {
                return alternativeId - o.alternativeId;
            }

            return operatorId - o.operatorId;
        }
    }
}
