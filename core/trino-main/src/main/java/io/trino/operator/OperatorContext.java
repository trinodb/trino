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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.memory.QueryContextVisitor;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.operator.OperationTimer.OperationTiming;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.operator.BlockedReason.WAITING_FOR_MEMORY;
import static io.trino.operator.Operator.NOT_BLOCKED;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains information about {@link Operator} execution.
 * <p>
 * Not thread-safe. Only revocable-memory-related operations are thread-safe.
 */
public class OperatorContext
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final String operatorType;
    private final DriverContext driverContext;
    private final Executor executor;

    private final CounterStat physicalInputDataSize = new CounterStat();
    private final CounterStat physicalInputPositions = new CounterStat();
    private final AtomicLong physicalInputReadTimeNanos = new AtomicLong();

    private final CounterStat internalNetworkInputDataSize = new CounterStat();
    private final CounterStat internalNetworkPositions = new CounterStat();

    private final OperationTiming addInputTiming = new OperationTiming();
    private final CounterStat inputDataSize = new CounterStat();
    private final CounterStat inputPositions = new CounterStat();

    private final OperationTiming getOutputTiming = new OperationTiming();
    private final CounterStat outputDataSize = new CounterStat();
    private final CounterStat outputPositions = new CounterStat();

    private final AtomicLong dynamicFilterSplitsProcessed = new AtomicLong();
    private final AtomicReference<Metrics> metrics = new AtomicReference<>(Metrics.EMPTY);  // this is not incremental, but gets overwritten by the latest value.
    private final AtomicReference<Metrics> connectorMetrics = new AtomicReference<>(Metrics.EMPTY); // this is not incremental, but gets overwritten by the latest value.

    private final AtomicLong writerInputDataSize = new AtomicLong();
    private final AtomicLong physicalWrittenDataSize = new AtomicLong();

    private final AtomicReference<SettableFuture<Void>> memoryFuture;
    private final AtomicReference<SettableFuture<Void>> revocableMemoryFuture;
    private final AtomicReference<BlockedMonitor> blockedMonitor = new AtomicReference<>();
    private final AtomicReference<ListenableFuture<Void>> finishedFuture = new AtomicReference<>();
    private final AtomicLong blockedWallNanos = new AtomicLong();

    private final OperationTiming finishTiming = new OperationTiming();

    private final OperatorSpillContext spillContext;
    private final AtomicReference<Supplier<? extends OperatorInfo>> infoSupplier = new AtomicReference<>();

    private final AtomicLong peakUserMemoryReservation = new AtomicLong();
    private final AtomicLong peakRevocableMemoryReservation = new AtomicLong();
    private final AtomicLong peakTotalMemoryReservation = new AtomicLong();

    @GuardedBy("this")
    private boolean memoryRevokingRequested;

    @Nullable
    @GuardedBy("this")
    private Runnable memoryRevocationRequestListener;

    private final MemoryTrackingContext operatorMemoryContext;

    public OperatorContext(
            int operatorId,
            PlanNodeId planNodeId,
            String operatorType,
            DriverContext driverContext,
            Executor executor,
            MemoryTrackingContext operatorMemoryContext)
    {
        checkArgument(operatorId >= 0, "operatorId is negative");
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.operatorType = requireNonNull(operatorType, "operatorType is null");
        this.driverContext = requireNonNull(driverContext, "driverContext is null");
        this.spillContext = new OperatorSpillContext(this.driverContext);
        this.executor = requireNonNull(executor, "executor is null");
        this.memoryFuture = new AtomicReference<>(SettableFuture.create());
        this.memoryFuture.get().set(null);
        this.revocableMemoryFuture = new AtomicReference<>(SettableFuture.create());
        this.revocableMemoryFuture.get().set(null);
        this.operatorMemoryContext = requireNonNull(operatorMemoryContext, "operatorMemoryContext is null");
        operatorMemoryContext.initializeLocalMemoryContexts(operatorType);
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    public String getOperatorType()
    {
        return operatorType;
    }

    public DriverContext getDriverContext()
    {
        return driverContext;
    }

    public Session getSession()
    {
        return driverContext.getSession();
    }

    void recordAddInput(OperationTimer operationTimer, Page page)
    {
        operationTimer.recordOperationComplete(addInputTiming);
        if (page != null) {
            inputDataSize.update(page.getSizeInBytes());
            inputPositions.update(page.getPositionCount());
        }
    }

    /**
     * Record the amount of physical bytes that were read by an operator and
     * the time it took to read the data. This metric is valid only for source operators.
     */
    public void recordPhysicalInputWithTiming(long sizeInBytes, long positions, long readNanos)
    {
        checkArgument(sizeInBytes >= 0, "sizeInBytes is negative (%s)", sizeInBytes);
        checkArgument(positions >= 0, "positions is negative (%s)", positions);
        checkArgument(readNanos >= 0, "readNanos is negative (%s)", readNanos);
        physicalInputDataSize.update(sizeInBytes);
        physicalInputPositions.update(positions);
        physicalInputReadTimeNanos.getAndAdd(readNanos);
    }

    /**
     * Record the amount of network bytes that were read by an operator.
     * This metric is valid only for source operators.
     */
    public void recordNetworkInput(long sizeInBytes, long positions)
    {
        checkArgument(sizeInBytes >= 0, "sizeInBytes is negative (%s)", sizeInBytes);
        checkArgument(positions >= 0, "positions is negative (%s)", positions);
        internalNetworkInputDataSize.update(sizeInBytes);
        internalNetworkPositions.update(positions);
    }

    /**
     * Record the size in bytes of input blocks that were processed by an operator.
     * This metric is valid only for source operators.
     */
    public void recordProcessedInput(long sizeInBytes, long positions)
    {
        checkArgument(sizeInBytes >= 0, "sizeInBytes is negative (%s)", sizeInBytes);
        checkArgument(positions >= 0, "positions is negative (%s)", positions);
        inputDataSize.update(sizeInBytes);
        inputPositions.update(positions);
    }

    void recordGetOutput(OperationTimer operationTimer, Page page)
    {
        operationTimer.recordOperationComplete(getOutputTiming);
        if (page != null) {
            outputDataSize.update(page.getSizeInBytes());
            outputPositions.update(page.getPositionCount());
        }
    }

    public void recordOutput(long sizeInBytes, long positions)
    {
        checkArgument(sizeInBytes >= 0, "sizeInBytes is negative (%s)", sizeInBytes);
        checkArgument(positions >= 0, "positions is negative (%s)", positions);
        outputDataSize.update(sizeInBytes);
        outputPositions.update(positions);
    }

    public void recordDynamicFilterSplitProcessed(long dynamicFilterSplits)
    {
        dynamicFilterSplitsProcessed.getAndAdd(dynamicFilterSplits);
    }

    /**
     * Overwrites the metrics with the latest one.
     *
     * @param metrics Latest operator's metrics.
     */
    public void setLatestMetrics(Metrics metrics)
    {
        this.metrics.set(metrics);
    }

    public void setLatestConnectorMetrics(Metrics metrics)
    {
        this.connectorMetrics.set(metrics);
    }

    public void setPipelineOperatorMetrics(Metrics metrics)
    {
        getDriverContext().getPipelineContext().setPipelineOperatorMetrics(operatorId, driverContext.getAlternativeId(), metrics);
    }

    Optional<ListenableFuture<Void>> getFinishedFuture()
    {
        return Optional.ofNullable(finishedFuture.get());
    }

    public void setFinishedFuture(ListenableFuture<Void> finishedFuture)
    {
        checkState(this.finishedFuture.getAndSet(requireNonNull(finishedFuture, "finishedFuture is null")) == null, "finishedFuture already set");
    }

    public void recordWriterInputDataSize(long sizeInBytes)
    {
        writerInputDataSize.getAndAdd(sizeInBytes);
    }

    public void recordPhysicalWrittenData(long sizeInBytes)
    {
        physicalWrittenDataSize.getAndAdd(sizeInBytes);
    }

    public void recordBlocked(ListenableFuture<Void> blocked)
    {
        requireNonNull(blocked, "blocked is null");

        BlockedMonitor monitor = new BlockedMonitor();

        BlockedMonitor oldMonitor = blockedMonitor.getAndSet(monitor);
        if (oldMonitor != null) {
            oldMonitor.run();
        }

        blocked.addListener(monitor, executor);
        // Do not register blocked with driver context.  The driver handles this directly.
    }

    void recordFinish(OperationTimer operationTimer)
    {
        operationTimer.recordOperationComplete(finishTiming);
    }

    public ListenableFuture<Void> isWaitingForMemory()
    {
        return memoryFuture.get();
    }

    public ListenableFuture<Void> isWaitingForRevocableMemory()
    {
        return revocableMemoryFuture.get();
    }

    // caller should close this context as it's a new context
    public LocalMemoryContext newLocalUserMemoryContext(String allocationTag)
    {
        return new InternalLocalMemoryContext(operatorMemoryContext.newUserMemoryContext(allocationTag), memoryFuture, this::updatePeakMemoryReservations, true);
    }

    // caller shouldn't close this context as it's managed by the OperatorContext
    public LocalMemoryContext localUserMemoryContext()
    {
        return new InternalLocalMemoryContext(operatorMemoryContext.localUserMemoryContext(), memoryFuture, this::updatePeakMemoryReservations, false);
    }

    // caller shouldn't close this context as it's managed by the OperatorContext
    public LocalMemoryContext localRevocableMemoryContext()
    {
        return new InternalLocalMemoryContext(operatorMemoryContext.localRevocableMemoryContext(), revocableMemoryFuture, this::updatePeakMemoryReservations, false);
    }

    // caller shouldn't close this context as it's managed by the OperatorContext
    public AggregatedMemoryContext aggregateUserMemoryContext()
    {
        return new InternalAggregatedMemoryContext(operatorMemoryContext.aggregateUserMemoryContext(), memoryFuture, this::updatePeakMemoryReservations, false);
    }

    // caller shouldn't close this context as it's managed by the OperatorContext
    public AggregatedMemoryContext aggregateRevocableMemoryContext()
    {
        return new InternalAggregatedMemoryContext(operatorMemoryContext.aggregateRevocableMemoryContext(), revocableMemoryFuture, this::updatePeakMemoryReservations, false);
    }

    // caller should close this context as it's a new context
    public AggregatedMemoryContext newAggregateUserMemoryContext()
    {
        return new InternalAggregatedMemoryContext(operatorMemoryContext.newAggregateUserMemoryContext(), memoryFuture, this::updatePeakMemoryReservations, true);
    }

    // caller should close this context as it's a new context
    public AggregatedMemoryContext newAggregateRevocableMemoryContext()
    {
        return new InternalAggregatedMemoryContext(operatorMemoryContext.newAggregateRevocableMemoryContext(), revocableMemoryFuture, this::updatePeakMemoryReservations, true);
    }

    // listen to all memory allocations and update the peak memory reservations accordingly
    private void updatePeakMemoryReservations()
    {
        long userMemory = operatorMemoryContext.getUserMemory();
        long revocableMemory = operatorMemoryContext.getRevocableMemory();
        // TODO on cluster level, "total memory" means "user memory + revocable", and used to include the system memory.
        //   Here, the total memory used to be user+system, and sans revocable. This apparent inconsistency should be removed.
        //   Perhaps, we don't need to track "total memory" here.
        long totalMemory = userMemory;
        peakUserMemoryReservation.accumulateAndGet(userMemory, Math::max);
        peakRevocableMemoryReservation.accumulateAndGet(revocableMemory, Math::max);
        peakTotalMemoryReservation.accumulateAndGet(totalMemory, Math::max);
    }

    public long getReservedRevocableBytes()
    {
        return operatorMemoryContext.getRevocableMemory();
    }

    private static void updateMemoryFuture(ListenableFuture<Void> memoryPoolFuture, AtomicReference<SettableFuture<Void>> targetFutureReference)
    {
        if (!memoryPoolFuture.isDone()) {
            SettableFuture<Void> currentMemoryFuture = targetFutureReference.get();
            while (currentMemoryFuture.isDone()) {
                SettableFuture<Void> settableFuture = SettableFuture.create();
                // We can't replace one that's not done, because the task may be blocked on that future
                if (targetFutureReference.compareAndSet(currentMemoryFuture, settableFuture)) {
                    currentMemoryFuture = settableFuture;
                }
                else {
                    currentMemoryFuture = targetFutureReference.get();
                }
            }

            SettableFuture<Void> finalMemoryFuture = currentMemoryFuture;
            // Create a new future, so that this operator can un-block before the pool does, if it's moved to a new pool
            memoryPoolFuture.addListener(() -> finalMemoryFuture.set(null), directExecutor());
        }
    }

    public void destroy()
    {
        // reset memory revocation listener so that OperatorContext doesn't hold any references to Driver instance
        synchronized (this) {
            memoryRevocationRequestListener = null;
        }
        // memoize the result of and then clear any reference to the original suppliers (which might otherwise retain operators or other large objects)
        Supplier<? extends OperatorInfo> infoSupplier = this.infoSupplier.get();
        if (infoSupplier != null) {
            OperatorInfo info = infoSupplier.get();
            this.infoSupplier.set(info == null ? null : Suppliers.ofInstance(info));
        }

        operatorMemoryContext.close();

        if (operatorMemoryContext.getUserMemory() != 0) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Operator %s has non-zero user memory (%d bytes) after destroy()", this, operatorMemoryContext.getUserMemory()));
        }

        if (operatorMemoryContext.getRevocableMemory() != 0) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Operator %s has non-zero revocable memory (%d bytes) after destroy()", this, operatorMemoryContext.getRevocableMemory()));
        }
    }

    public SpillContext getSpillContext()
    {
        return spillContext;
    }

    public synchronized boolean isMemoryRevokingRequested()
    {
        return memoryRevokingRequested;
    }

    /**
     * Returns how much revocable memory will be revoked by the operator
     */
    public long requestMemoryRevoking()
    {
        long revokedMemory = 0L;
        Runnable listener = null;
        synchronized (this) {
            if (!isMemoryRevokingRequested() && operatorMemoryContext.getRevocableMemory() > 0) {
                memoryRevokingRequested = true;
                revokedMemory = operatorMemoryContext.getRevocableMemory();
                listener = memoryRevocationRequestListener;
            }
        }
        if (listener != null) {
            runListener(listener);
        }
        return revokedMemory;
    }

    public synchronized void resetMemoryRevokingRequested()
    {
        memoryRevokingRequested = false;
    }

    public void setMemoryRevocationRequestListener(Runnable listener)
    {
        requireNonNull(listener, "listener is null");

        boolean shouldNotify;
        synchronized (this) {
            checkState(memoryRevocationRequestListener == null, "listener already set");
            memoryRevocationRequestListener = listener;
            shouldNotify = memoryRevokingRequested;
        }
        // if memory revoking is requested immediately run the listener
        if (shouldNotify) {
            runListener(listener);
        }
    }

    private static void runListener(Runnable listener)
    {
        requireNonNull(listener, "listener is null");
        try {
            listener.run();
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Exception while running the listener", e);
        }
    }

    public void setInfoSupplier(Supplier<? extends OperatorInfo> infoSupplier)
    {
        requireNonNull(infoSupplier, "infoSupplier is null");
        this.infoSupplier.set(infoSupplier);
    }

    public CounterStat getInputDataSize()
    {
        return inputDataSize;
    }

    public CounterStat getInputPositions()
    {
        return inputPositions;
    }

    public CounterStat getOutputDataSize()
    {
        return outputDataSize;
    }

    public CounterStat getOutputPositions()
    {
        return outputPositions;
    }

    public long getWriterInputDataSize()
    {
        return writerInputDataSize.get();
    }

    public long getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize.get();
    }

    @Override
    public String toString()
    {
        return format("%s-%s", operatorType, planNodeId);
    }

    public static Metrics getOperatorMetrics(Metrics operatorMetrics, long inputPositions, double cpuTimeSeconds, double wallTimeSeconds, double blockedWallSeconds)
    {
        return operatorMetrics.mergeWith(new Metrics(ImmutableMap.of(
                "Input rows distribution", TDigestHistogram.fromValue(inputPositions),
                "CPU time distribution (s)", TDigestHistogram.fromValue(cpuTimeSeconds),
                "Scheduled time distribution (s)", TDigestHistogram.fromValue(wallTimeSeconds),
                "Blocked time distribution (s)", TDigestHistogram.fromValue(blockedWallSeconds))));
    }

    public <C, R> R accept(QueryContextVisitor<C, R> visitor, C context)
    {
        return visitor.visitOperatorContext(this, context);
    }

    public OperatorStats getOperatorStats()
    {
        Supplier<? extends OperatorInfo> infoSupplier = this.infoSupplier.get();
        OperatorInfo info = Optional.ofNullable(infoSupplier).map(Supplier::get).orElse(null);

        long inputPositionsCount = inputPositions.getTotalCount();

        return new OperatorStats(
                driverContext.getTaskId().getStageId().getId(),
                driverContext.getPipelineContext().getPipelineId(),
                driverContext.getAlternativeId(),
                operatorId,
                planNodeId,
                operatorType,

                1,

                addInputTiming.getCalls(),
                new Duration(addInputTiming.getWallNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(addInputTiming.getCpuNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                DataSize.ofBytes(physicalInputDataSize.getTotalCount()),
                physicalInputPositions.getTotalCount(),
                new Duration(physicalInputReadTimeNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                DataSize.ofBytes(internalNetworkInputDataSize.getTotalCount()),
                internalNetworkPositions.getTotalCount(),
                DataSize.ofBytes(physicalInputDataSize.getTotalCount() + internalNetworkInputDataSize.getTotalCount()),
                DataSize.ofBytes(inputDataSize.getTotalCount()),
                inputPositionsCount,
                (double) inputPositionsCount * inputPositionsCount,

                getOutputTiming.getCalls(),
                new Duration(getOutputTiming.getWallNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(getOutputTiming.getCpuNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                DataSize.ofBytes(outputDataSize.getTotalCount()),
                outputPositions.getTotalCount(),

                dynamicFilterSplitsProcessed.get(),
                getOperatorMetrics(
                        metrics.get(),
                        inputPositionsCount,
                        new Duration(addInputTiming.getCpuNanos() + getOutputTiming.getCpuNanos() + finishTiming.getCpuNanos(), NANOSECONDS).convertTo(SECONDS).getValue(),
                        new Duration(addInputTiming.getWallNanos() + getOutputTiming.getWallNanos() + finishTiming.getWallNanos(), NANOSECONDS).convertTo(SECONDS).getValue(),
                        new Duration(blockedWallNanos.get(), NANOSECONDS).convertTo(SECONDS).getValue()),
                connectorMetrics.get(),
                Metrics.EMPTY, // will be filled in when aggregating at pipeline level

                DataSize.ofBytes(physicalWrittenDataSize.get()),

                new Duration(blockedWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),

                finishTiming.getCalls(),
                new Duration(finishTiming.getWallNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(finishTiming.getCpuNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),

                DataSize.ofBytes(operatorMemoryContext.getUserMemory()),
                DataSize.ofBytes(getReservedRevocableBytes()),

                DataSize.ofBytes(peakUserMemoryReservation.get()),
                DataSize.ofBytes(peakRevocableMemoryReservation.get()),
                DataSize.ofBytes(peakTotalMemoryReservation.get()),

                DataSize.ofBytes(spillContext.getSpilledBytes()),

                memoryFuture.get().isDone() ? Optional.empty() : Optional.of(WAITING_FOR_MEMORY),
                info);
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
        public synchronized void run()
        {
            if (finished) {
                return;
            }
            finished = true;
            blockedMonitor.compareAndSet(this, null);
            blockedWallNanos.getAndAdd(getBlockedTime());
        }

        public long getBlockedTime()
        {
            return nanosBetween(start, System.nanoTime());
        }
    }

    @ThreadSafe
    private static class OperatorSpillContext
            implements SpillContext
    {
        private final DriverContext driverContext;
        private final AtomicLong reservedBytes = new AtomicLong();
        private final AtomicLong spilledBytes = new AtomicLong();

        public OperatorSpillContext(DriverContext driverContext)
        {
            this.driverContext = driverContext;
        }

        @Override
        public void updateBytes(long bytes)
        {
            if (bytes >= 0) {
                reservedBytes.addAndGet(bytes);
                driverContext.reserveSpill(bytes);
                spilledBytes.addAndGet(bytes);
            }
            else {
                reservedBytes.accumulateAndGet(-bytes, this::decrementSpilledReservation);
                driverContext.freeSpill(-bytes);
            }
        }

        public long getSpilledBytes()
        {
            return spilledBytes.longValue();
        }

        private long decrementSpilledReservation(long reservedBytes, long bytesBeingFreed)
        {
            checkArgument(bytesBeingFreed >= 0);
            checkArgument(bytesBeingFreed <= reservedBytes, "tried to free %s spilled bytes from %s bytes reserved", bytesBeingFreed, reservedBytes);
            return reservedBytes - bytesBeingFreed;
        }

        @Override
        public void close()
        {
            // Only products of SpillContext.newLocalSpillContext() should be closed.
            throw new UnsupportedOperationException(format("%s should not be closed directly", getClass()));
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("usedBytes", reservedBytes.get())
                    .toString();
        }
    }

    private static class InternalLocalMemoryContext
            implements LocalMemoryContext
    {
        private final LocalMemoryContext delegate;
        private final AtomicReference<SettableFuture<Void>> memoryFuture;
        private final Runnable allocationListener;
        private final boolean closeable;

        InternalLocalMemoryContext(LocalMemoryContext delegate, AtomicReference<SettableFuture<Void>> memoryFuture, Runnable allocationListener, boolean closeable)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.memoryFuture = requireNonNull(memoryFuture, "memoryFuture is null");
            this.allocationListener = requireNonNull(allocationListener, "allocationListener is null");
            this.closeable = closeable;
        }

        @Override
        public long getBytes()
        {
            return delegate.getBytes();
        }

        @Override
        public ListenableFuture<Void> setBytes(long bytes)
        {
            if (bytes == delegate.getBytes()) {
                return NOT_BLOCKED;
            }

            ListenableFuture<Void> blocked = delegate.setBytes(bytes);
            updateMemoryFuture(blocked, memoryFuture);
            allocationListener.run();
            return blocked;
        }

        @Override
        public boolean trySetBytes(long bytes)
        {
            if (delegate.trySetBytes(bytes)) {
                allocationListener.run();
                return true;
            }

            return false;
        }

        @Override
        public void close()
        {
            if (!closeable) {
                throw new UnsupportedOperationException("Called close on unclosable local memory context");
            }
            delegate.close();
            allocationListener.run();
        }
    }

    private static class InternalAggregatedMemoryContext
            implements AggregatedMemoryContext
    {
        private final AggregatedMemoryContext delegate;
        private final AtomicReference<SettableFuture<Void>> memoryFuture;
        private final Runnable allocationListener;
        private final boolean closeable;

        InternalAggregatedMemoryContext(AggregatedMemoryContext delegate, AtomicReference<SettableFuture<Void>> memoryFuture, Runnable allocationListener, boolean closeable)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.memoryFuture = requireNonNull(memoryFuture, "memoryFuture is null");
            this.allocationListener = requireNonNull(allocationListener, "allocationListener is null");
            this.closeable = closeable;
        }

        @Override
        public AggregatedMemoryContext newAggregatedMemoryContext()
        {
            return new InternalAggregatedMemoryContext(delegate.newAggregatedMemoryContext(), memoryFuture, allocationListener, true);
        }

        @Override
        public LocalMemoryContext newLocalMemoryContext(String allocationTag)
        {
            return new InternalLocalMemoryContext(delegate.newLocalMemoryContext(allocationTag), memoryFuture, allocationListener, true);
        }

        @Override
        public long getBytes()
        {
            return delegate.getBytes();
        }

        @Override
        public void close()
        {
            if (!closeable) {
                throw new UnsupportedOperationException("Called close on unclosable aggregated memory context");
            }
            delegate.close();
        }
    }

    @VisibleForTesting
    public MemoryTrackingContext getOperatorMemoryContext()
    {
        return operatorMemoryContext;
    }
}
