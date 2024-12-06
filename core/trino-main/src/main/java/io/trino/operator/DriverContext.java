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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.cache.CacheDriverContext;
import io.trino.execution.TaskId;
import io.trino.memory.QueryContextVisitor;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.operator.OperationTimer.OperationTiming;
import io.trino.sql.planner.plan.PlanNodeId;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.getLast;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Only calling getDriverStats is ThreadSafe
 */
public class DriverContext
{
    private final PipelineContext pipelineContext;
    private final AtomicReference<Integer> alternativeId = new AtomicReference<>();
    private final Executor notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    private final AtomicBoolean finished = new AtomicBoolean();

    private final DateTime createdTime = DateTime.now();
    private final long createNanos = System.nanoTime();

    private final AtomicLong startNanos = new AtomicLong();
    private final AtomicLong endNanos = new AtomicLong();

    private final OperationTiming overallTiming = new OperationTiming();

    private final AtomicReference<BlockedMonitor> blockedMonitor = new AtomicReference<>();
    private final AtomicLong blockedWallNanos = new AtomicLong();

    private final AtomicReference<DateTime> executionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> executionEndTime = new AtomicReference<>();
    private final AtomicReference<Optional<Duration>> blockedTimeout = new AtomicReference<>(Optional.empty());

    private final MemoryTrackingContext driverMemoryContext;

    private final DriverYieldSignal yieldSignal;

    private final List<OperatorContext> operatorContexts = new CopyOnWriteArrayList<>();
    private final long splitWeight;
    private final AtomicReference<Optional<CacheDriverContext>> cacheDriverContext = new AtomicReference<>(Optional.empty());

    public DriverContext(
            PipelineContext pipelineContext,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            ScheduledExecutorService timeoutExecutor,
            MemoryTrackingContext driverMemoryContext,
            long splitWeight)
    {
        this.pipelineContext = requireNonNull(pipelineContext, "pipelineContext is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.yieldExecutor = requireNonNull(yieldExecutor, "yieldExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.driverMemoryContext = requireNonNull(driverMemoryContext, "driverMemoryContext is null");
        this.yieldSignal = new DriverYieldSignal();
        this.splitWeight = splitWeight;
        checkArgument(splitWeight >= 0, "splitWeight must be >= 0, found: %s", splitWeight);
    }

    public TaskId getTaskId()
    {
        return pipelineContext.getTaskId();
    }

    public long getSplitWeight()
    {
        return splitWeight;
    }

    public OperatorContext addOperatorContext(int operatorId, PlanNodeId planNodeId, String operatorType)
    {
        checkArgument(operatorId >= 0, "operatorId is negative");

        for (OperatorContext operatorContext : operatorContexts) {
            checkArgument(operatorId != operatorContext.getOperatorId(), "A context already exists for operatorId %s", operatorId);
        }

        OperatorContext operatorContext = new OperatorContext(
                operatorId,
                planNodeId,
                operatorType,
                this,
                notificationExecutor,
                driverMemoryContext.newMemoryTrackingContext());
        operatorContexts.add(operatorContext);
        return operatorContext;
    }

    public List<OperatorContext> getOperatorContexts()
    {
        return ImmutableList.copyOf(operatorContexts);
    }

    public PipelineContext getPipelineContext()
    {
        return pipelineContext;
    }

    public void setAlternativeId(int alternativeId)
    {
        checkState(this.alternativeId.get() == null, "alternativeId is already set");
        this.alternativeId.set(alternativeId);
    }

    public int getAlternativeId()
    {
        return Optional.ofNullable(alternativeId.get()).orElse(0);
    }

    public Session getSession()
    {
        return pipelineContext.getSession();
    }

    public void startProcessTimer()
    {
        // Must update startNanos first so that the value is valid once executionStartTime is not null
        if (executionStartTime.get() == null && startNanos.compareAndSet(0, System.nanoTime())) {
            executionStartTime.set(DateTime.now());
            pipelineContext.start();
        }
    }

    public void recordProcessed(OperationTimer operationTimer)
    {
        operationTimer.end(overallTiming);
    }

    public void recordBlocked(ListenableFuture<Void> blocked)
    {
        requireNonNull(blocked, "blocked is null");

        BlockedMonitor monitor = new BlockedMonitor();

        BlockedMonitor oldMonitor = blockedMonitor.getAndSet(monitor);
        if (oldMonitor != null) {
            oldMonitor.run();
        }

        blocked.addListener(monitor, notificationExecutor);
    }

    public void finished()
    {
        if (!finished.compareAndSet(false, true)) {
            // already finished
            return;
        }
        // Must update endNanos first, so that the value is valid after executionEndTime is not null
        endNanos.set(System.nanoTime());
        executionEndTime.set(DateTime.now());

        pipelineContext.driverFinished(this);
    }

    public void failed(Throwable cause)
    {
        if (finished.compareAndSet(false, true)) {
            pipelineContext.driverFailed(cause);
        }
    }

    public boolean isTerminatingOrDone()
    {
        return finished.get() || pipelineContext.isTerminatingOrDone();
    }

    public ListenableFuture<Void> reserveSpill(long bytes)
    {
        return pipelineContext.reserveSpill(bytes);
    }

    public void freeSpill(long bytes)
    {
        if (bytes == 0) {
            return;
        }
        checkArgument(bytes > 0, "bytes is negative");
        pipelineContext.freeSpill(bytes);
    }

    public DriverYieldSignal getYieldSignal()
    {
        return yieldSignal;
    }

    public long getMemoryUsage()
    {
        return driverMemoryContext.getUserMemory();
    }

    public long getRevocableMemoryUsage()
    {
        return driverMemoryContext.getRevocableMemory();
    }

    public boolean isPerOperatorCpuTimerEnabled()
    {
        return pipelineContext.isPerOperatorCpuTimerEnabled();
    }

    public boolean isCpuTimerEnabled()
    {
        return pipelineContext.isCpuTimerEnabled();
    }

    public CounterStat getInputDataSize()
    {
        OperatorContext inputOperator = getFirst(operatorContexts, null);
        if (inputOperator != null) {
            return inputOperator.getInputDataSize();
        }
        return new CounterStat();
    }

    public CounterStat getInputPositions()
    {
        OperatorContext inputOperator = getFirst(operatorContexts, null);
        if (inputOperator != null) {
            return inputOperator.getInputPositions();
        }
        return new CounterStat();
    }

    public CounterStat getOutputDataSize()
    {
        OperatorContext inputOperator = getLast(operatorContexts, null);
        if (inputOperator != null) {
            return inputOperator.getOutputDataSize();
        }
        return new CounterStat();
    }

    public CounterStat getOutputPositions()
    {
        OperatorContext inputOperator = getLast(operatorContexts, null);
        if (inputOperator != null) {
            return inputOperator.getOutputPositions();
        }
        return new CounterStat();
    }

    public long getWriterInputDataSize()
    {
        // Avoid using stream api for performance reasons
        long writerInputDataSize = 0;
        for (OperatorContext context : operatorContexts) {
            writerInputDataSize += context.getWriterInputDataSize();
        }
        return writerInputDataSize;
    }

    public long getPhysicalWrittenDataSize()
    {
        // Avoid using stream api for performance reasons
        long physicalWrittenBytes = 0;
        for (OperatorContext context : operatorContexts) {
            physicalWrittenBytes += context.getPhysicalWrittenDataSize();
        }
        return physicalWrittenBytes;
    }

    public boolean isExecutionStarted()
    {
        return executionStartTime.get() != null;
    }

    public boolean isFullyBlocked()
    {
        return blockedMonitor.get() != null;
    }

    public List<OperatorStats> getOperatorStats()
    {
        return operatorContexts.stream()
                .map(OperatorContext::getOperatorStats)
                .collect(toImmutableList());
    }

    public DriverStats getDriverStats()
    {
        long totalScheduledTime = overallTiming.getWallNanos();
        long totalCpuTime = overallTiming.getCpuNanos();

        long totalBlockedTime = blockedWallNanos.get();
        BlockedMonitor blockedMonitor = this.blockedMonitor.get();
        if (blockedMonitor != null) {
            totalBlockedTime += blockedMonitor.getBlockedTime();
        }

        // startNanos is always valid once executionStartTime is not null
        DateTime executionStartTime = this.executionStartTime.get();
        Duration queuedTime = new Duration(nanosBetween(createNanos, executionStartTime == null ? System.nanoTime() : startNanos.get()), NANOSECONDS);

        // endNanos is always valid once executionStartTime is not null
        DateTime executionEndTime = this.executionEndTime.get();
        Duration elapsedTime = new Duration(nanosBetween(createNanos, executionEndTime == null ? System.nanoTime() : endNanos.get()), NANOSECONDS);

        List<OperatorStats> operators = getOperatorStats();
        OperatorStats inputOperator = getFirst(operators, null);

        DataSize physicalInputDataSize;
        long physicalInputPositions;
        Duration physicalInputReadTime;

        DataSize internalNetworkInputDataSize;
        long internalNetworkInputPositions;

        DataSize rawInputDataSize;
        long rawInputPositions;
        Duration rawInputReadTime;

        DataSize processedInputDataSize;
        long processedInputPositions;
        Duration inputBlockedTime;
        DataSize outputDataSize;
        long outputPositions;
        Duration outputBlockedTime;
        if (inputOperator != null) {
            physicalInputDataSize = inputOperator.getPhysicalInputDataSize();
            physicalInputPositions = inputOperator.getPhysicalInputPositions();
            physicalInputReadTime = inputOperator.getPhysicalInputReadTime();

            internalNetworkInputDataSize = inputOperator.getInternalNetworkInputDataSize();
            internalNetworkInputPositions = inputOperator.getInternalNetworkInputPositions();

            rawInputDataSize = inputOperator.getRawInputDataSize();
            rawInputPositions = inputOperator.getInputPositions();
            rawInputReadTime = inputOperator.getAddInputWall();

            processedInputDataSize = inputOperator.getInputDataSize();
            processedInputPositions = inputOperator.getInputPositions();

            inputBlockedTime = inputOperator.getBlockedWall();

            OperatorStats outputOperator = requireNonNull(getLast(operators, null));
            outputDataSize = outputOperator.getOutputDataSize();
            outputPositions = outputOperator.getOutputPositions();

            outputBlockedTime = outputOperator.getBlockedWall();
        }
        else {
            physicalInputDataSize = DataSize.ofBytes(0);
            physicalInputPositions = 0;
            physicalInputReadTime = new Duration(0, MILLISECONDS);

            internalNetworkInputDataSize = DataSize.ofBytes(0);
            internalNetworkInputPositions = 0;

            rawInputDataSize = DataSize.ofBytes(0);
            rawInputPositions = 0;
            rawInputReadTime = new Duration(0, MILLISECONDS);

            processedInputDataSize = DataSize.ofBytes(0);
            processedInputPositions = 0;

            inputBlockedTime = new Duration(0, MILLISECONDS);

            outputDataSize = DataSize.ofBytes(0);
            outputPositions = 0;

            outputBlockedTime = new Duration(0, MILLISECONDS);
        }

        ImmutableSet.Builder<BlockedReason> builder = ImmutableSet.builder();
        long physicalWrittenDataSize = 0;
        for (OperatorStats operator : operators) {
            physicalWrittenDataSize += operator.getPhysicalWrittenDataSize().toBytes();
            if (operator.getBlockedReason().isPresent()) {
                builder.add(operator.getBlockedReason().get());
            }
        }

        return new DriverStats(
                createdTime,
                executionStartTime,
                executionEndTime,
                queuedTime.convertToMostSuccinctTimeUnit(),
                elapsedTime.convertToMostSuccinctTimeUnit(),
                succinctBytes(driverMemoryContext.getUserMemory()),
                succinctBytes(driverMemoryContext.getRevocableMemory()),
                new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                blockedMonitor != null,
                builder.build(),
                physicalInputDataSize.succinct(),
                physicalInputPositions,
                physicalInputReadTime,
                internalNetworkInputDataSize.succinct(),
                internalNetworkInputPositions,
                rawInputDataSize.succinct(),
                rawInputPositions,
                rawInputReadTime,
                processedInputDataSize.succinct(),
                processedInputPositions,
                inputBlockedTime,
                outputDataSize.succinct(),
                outputPositions,
                outputBlockedTime,
                succinctBytes(physicalWrittenDataSize),
                operators);
    }

    public <C, R> R accept(QueryContextVisitor<C, R> visitor, C context)
    {
        return visitor.visitDriverContext(this, context);
    }

    public <C, R> List<R> acceptChildren(QueryContextVisitor<C, R> visitor, C context)
    {
        return operatorContexts.stream()
                .map(operatorContext -> operatorContext.accept(visitor, context))
                .collect(toList());
    }

    public Optional<CacheDriverContext> getCacheDriverContext()
    {
        return cacheDriverContext.get();
    }

    public void setCacheDriverContext(CacheDriverContext cacheDriverContext)
    {
        if (!this.cacheDriverContext.compareAndSet(Optional.empty(), Optional.of(cacheDriverContext))) {
            throw new IllegalStateException("CacheDriverContext is already set");
        }
    }

    public ScheduledExecutorService getYieldExecutor()
    {
        return yieldExecutor;
    }

    public ScheduledExecutorService getTimeoutExecutor()
    {
        return timeoutExecutor;
    }

    public void setBlockedTimeout(Duration duration)
    {
        this.blockedTimeout.set(Optional.of(duration));
    }

    public Optional<Duration> getBlockedTimeout()
    {
        return blockedTimeout.get();
    }

    private static long nanosBetween(long start, long end)
    {
        return max(0, end - start);
    }

    private class BlockedMonitor
            implements Runnable
    {
        private final long start = System.nanoTime();
        private boolean finished;

        @Override
        public void run()
        {
            synchronized (this) {
                if (finished) {
                    return;
                }
                finished = true;
                blockedMonitor.compareAndSet(this, null);
                blockedWallNanos.getAndAdd(getBlockedTime());
            }
        }

        public long getBlockedTime()
        {
            return nanosBetween(start, System.nanoTime());
        }
    }
}
