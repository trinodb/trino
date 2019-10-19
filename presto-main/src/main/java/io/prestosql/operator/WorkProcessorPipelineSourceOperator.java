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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.metadata.Split;
import io.prestosql.operator.OperationTimer.OperationTiming;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.sql.planner.plan.PlanNodeId;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.operator.BlockedReason.WAITING_FOR_MEMORY;
import static io.prestosql.operator.PageUtils.recordMaterializedBytes;
import static io.prestosql.operator.WorkProcessor.ProcessState.Type.BLOCKED;
import static io.prestosql.operator.WorkProcessor.ProcessState.Type.FINISHED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class WorkProcessorPipelineSourceOperator
        implements SourceOperator
{
    private static final Logger log = Logger.get(WorkProcessorPipelineSourceOperator.class);
    private static final Duration ZERO_DURATION = new Duration(0, NANOSECONDS);

    private final int stageId;
    private final int pipelineId;
    private final PlanNodeId sourceId;
    private final OperatorContext operatorContext;
    private final WorkProcessor<Page> pages;
    private final OperationTimer timer;
    // operator instances including source operator
    private final List<WorkProcessorOperatorContext> workProcessorOperatorContexts = new ArrayList<>();
    private final List<Split> pendingSplits = new ArrayList<>();

    private ListenableFuture<?> blockedFuture;
    private WorkProcessorSourceOperator sourceOperator;
    private SettableFuture<?> blockedOnSplits = SettableFuture.create();
    private boolean operatorFinishing;

    public static List<OperatorFactory> convertOperators(int operatorId, List<OperatorFactory> operatorFactories)
    {
        if (operatorFactories.isEmpty() || !(operatorFactories.get(0) instanceof WorkProcessorSourceOperatorFactory)) {
            return operatorFactories;
        }

        WorkProcessorSourceOperatorFactory sourceOperatorFactory = (WorkProcessorSourceOperatorFactory) operatorFactories.get(0);
        ImmutableList.Builder<WorkProcessorOperatorFactory> workProcessorOperatorFactoriesBuilder = ImmutableList.builder();
        int operatorIndex = 1;
        for (; operatorIndex < operatorFactories.size(); ++operatorIndex) {
            if (!(operatorFactories.get(operatorIndex) instanceof WorkProcessorOperatorFactory)) {
                break;
            }
            workProcessorOperatorFactoriesBuilder.add((WorkProcessorOperatorFactory) operatorFactories.get(operatorIndex));
        }

        List<WorkProcessorOperatorFactory> workProcessorOperatorFactories = workProcessorOperatorFactoriesBuilder.build();
        if (workProcessorOperatorFactories.isEmpty()) {
            return operatorFactories;
        }

        return ImmutableList.<OperatorFactory>builder()
                .add(new WorkProcessorPipelineSourceOperatorFactory(operatorId, sourceOperatorFactory, workProcessorOperatorFactories))
                .addAll(operatorFactories.subList(operatorIndex, operatorFactories.size()))
                .build();
    }

    private WorkProcessorPipelineSourceOperator(
            int operatorId,
            DriverContext driverContext,
            WorkProcessorSourceOperatorFactory sourceOperatorFactory,
            List<WorkProcessorOperatorFactory> operatorFactories)
    {
        requireNonNull(driverContext, "driverContext is null");
        requireNonNull(sourceOperatorFactory, "sourceOperatorFactory is null");
        requireNonNull(operatorFactories, "operatorFactories is null");
        this.stageId = driverContext.getTaskId().getStageId().getId();
        this.pipelineId = driverContext.getPipelineContext().getPipelineId();
        this.sourceId = requireNonNull(sourceOperatorFactory.getSourceId(), "sourceId is null");
        this.operatorContext = driverContext.addOperatorContext(operatorId, sourceId, WorkProcessorPipelineSourceOperator.class.getSimpleName());
        this.timer = new OperationTimer(
                operatorContext.getDriverContext().isCpuTimerEnabled(),
                operatorContext.getDriverContext().isCpuTimerEnabled() && operatorContext.getDriverContext().isPerOperatorCpuTimerEnabled());

        MemoryTrackingContext sourceOperatorMemoryTrackingContext = createMemoryTrackingContext(operatorContext, 0);
        sourceOperatorMemoryTrackingContext.initializeLocalMemoryContexts(sourceOperatorFactory.getOperatorType());
        WorkProcessor<Split> splits = WorkProcessor.create(new Splits());

        sourceOperator = sourceOperatorFactory.create(
                operatorContext.getSession(),
                sourceOperatorMemoryTrackingContext,
                operatorContext.getDriverContext().getYieldSignal(),
                splits);
        workProcessorOperatorContexts.add(new WorkProcessorOperatorContext(
                sourceOperator,
                sourceOperatorFactory.getOperatorId(),
                sourceOperatorFactory.getPlanNodeId(),
                sourceOperatorFactory.getOperatorType(),
                sourceOperatorMemoryTrackingContext));
        WorkProcessor<Page> pages = sourceOperator.getOutputPages();
        pages = pages
                .yielding(() -> operatorContext.getDriverContext().getYieldSignal().isSet())
                .withProcessEntryMonitor(() -> workProcessorOperatorEntryMonitor(0))
                .withProcessStateMonitor(state -> workProcessorOperatorStateMonitor(state, 0))
                .map(page -> recordProcessedOutput(page, 0));

        for (int i = 0; i < operatorFactories.size(); ++i) {
            int operatorIndex = i + 1;
            WorkProcessorOperatorFactory operatorFactory = operatorFactories.get(i);
            MemoryTrackingContext operatorMemoryTrackingContext = createMemoryTrackingContext(operatorContext, operatorIndex);
            operatorMemoryTrackingContext.initializeLocalMemoryContexts(operatorFactory.getOperatorType());
            WorkProcessorOperator operator = operatorFactory.create(
                    operatorContext.getSession(),
                    operatorMemoryTrackingContext,
                    operatorContext.getDriverContext().getYieldSignal(),
                    pages);
            workProcessorOperatorContexts.add(new WorkProcessorOperatorContext(
                    operator,
                    operatorFactory.getOperatorId(),
                    operatorFactory.getPlanNodeId(),
                    operatorFactory.getOperatorType(),
                    operatorMemoryTrackingContext));
            pages = operator.getOutputPages();
            pages = pages
                    .yielding(() -> operatorContext.getDriverContext().getYieldSignal().isSet())
                    .withProcessEntryMonitor(() -> workProcessorOperatorEntryMonitor(operatorIndex))
                    .withProcessStateMonitor(state -> workProcessorOperatorStateMonitor(state, operatorIndex));
            pages = pages.map(page -> recordProcessedOutput(page, operatorIndex));
        }

        // materialize output pages as there are no semantics guarantees for non WorkProcessor operators
        pages = pages.map(Page::getLoadedPage);

        // finish early when entire pipeline is closed
        this.pages = pages.finishWhen(() -> operatorFinishing);

        operatorContext.setNestedOperatorStatsSupplier(this::getNestedOperatorStats);
    }

    private void workProcessorOperatorEntryMonitor(int operatorIndex)
    {
        if (isLastOperator(operatorIndex)) {
            // this is the top operator so timer needs to be reset
            timer.resetInterval();
        }
        else {
            // report timing for downstream operator
            timer.recordOperationComplete(workProcessorOperatorContexts.get(operatorIndex + 1).operatorTiming);
        }
    }

    private void workProcessorOperatorStateMonitor(WorkProcessor.ProcessState<Page> state, int operatorIndex)
    {
        // report timing for current operator
        WorkProcessorOperatorContext context = workProcessorOperatorContexts.get(operatorIndex);
        timer.recordOperationComplete(context.operatorTiming);

        if (operatorIndex == 0) {
            // update input stats for source operator
            WorkProcessorSourceOperator sourceOperator = (WorkProcessorSourceOperator) context.operator;

            long deltaPhysicalInputDataSize = deltaAndSet(context.physicalInputDataSize, sourceOperator.getPhysicalInputDataSize().toBytes());
            long deltaPhysicalInputPositions = deltaAndSet(context.physicalInputPositions, sourceOperator.getPhysicalInputPositions());

            long deltaInternalNetworkInputDataSize = deltaAndSet(context.internalNetworkInputDataSize, sourceOperator.getInternalNetworkInputDataSize().toBytes());
            long deltaInternalNetworkInputPositions = deltaAndSet(context.internalNetworkInputPositions, sourceOperator.getInternalNetworkPositions());

            long deltaInputDataSize = deltaAndSet(context.inputDataSize, sourceOperator.getInputDataSize().toBytes());
            long deltaInputPositions = deltaAndSet(context.inputPositions, sourceOperator.getInputPositions());

            long deltaReadTimeNanos = deltaAndSet(context.readTimeNanos, sourceOperator.getReadTime().roundTo(NANOSECONDS));

            operatorContext.recordPhysicalInputWithTiming(deltaPhysicalInputDataSize, deltaPhysicalInputPositions, deltaReadTimeNanos);
            operatorContext.recordNetworkInput(deltaInternalNetworkInputDataSize, deltaInternalNetworkInputPositions);
            operatorContext.recordProcessedInput(deltaInputDataSize, deltaInputPositions);
        }

        if (state.getType() == FINISHED) {
            // immediately close all upstream operators (including finished operator)
            closeOperators(operatorIndex);
        }
        else if (state.getType() == BLOCKED && blockedFuture != state.getBlocked()) {
            blockedFuture = state.getBlocked();
            long start = System.nanoTime();
            blockedFuture.addListener(
                    () -> context.blockedWallNanos.getAndAdd(System.nanoTime() - start),
                    directExecutor());
        }
    }

    private static long deltaAndSet(AtomicLong currentValue, long newValue)
    {
        return newValue - currentValue.getAndSet(newValue);
    }

    private Page recordProcessedOutput(Page page, int operatorIndex)
    {
        WorkProcessorOperatorContext operatorContext = workProcessorOperatorContexts.get(operatorIndex);
        operatorContext.outputPositions.getAndAdd(page.getPositionCount());

        WorkProcessorOperatorContext downstreamOperatorContext;
        if (!isLastOperator(operatorIndex)) {
            downstreamOperatorContext = workProcessorOperatorContexts.get(operatorIndex + 1);
            downstreamOperatorContext.inputPositions.getAndAdd(page.getPositionCount());
        }
        else {
            downstreamOperatorContext = null;
        }

        // account processed bytes from lazy blocks only when they are loaded
        recordMaterializedBytes(page, sizeInBytes -> {
            operatorContext.outputDataSize.getAndAdd(sizeInBytes);
            if (downstreamOperatorContext != null) {
                downstreamOperatorContext.inputDataSize.getAndAdd(sizeInBytes);
            }
        });

        return page;
    }

    private boolean isLastOperator(int operatorIndex)
    {
        return operatorIndex + 1 == workProcessorOperatorContexts.size();
    }

    private MemoryTrackingContext createMemoryTrackingContext(OperatorContext operatorContext, int operatorIndex)
    {
        return new MemoryTrackingContext(
                new InternalAggregatedMemoryContext(operatorContext.newAggregateUserMemoryContext(), () -> updatePeakMemoryReservations(operatorIndex)),
                new InternalAggregatedMemoryContext(operatorContext.newAggregateRevocableMemoryContext(), () -> updatePeakMemoryReservations(operatorIndex)),
                new InternalAggregatedMemoryContext(operatorContext.newAggregateSystemMemoryContext(), () -> updatePeakMemoryReservations(operatorIndex)));
    }

    private void updatePeakMemoryReservations(int operatorIndex)
    {
        workProcessorOperatorContexts.get(operatorIndex).updatePeakMemoryReservations();
    }

    private List<OperatorStats> getNestedOperatorStats()
    {
        return workProcessorOperatorContexts.stream()
                .map(context -> new OperatorStats(
                        stageId,
                        pipelineId,
                        context.operatorId,
                        context.planNodeId,
                        context.operatorType,
                        1,

                        // WorkProcessorOperator doesn't have addInput call
                        0,
                        // source operators report read time though
                        new Duration(context.readTimeNanos.get(), NANOSECONDS),
                        ZERO_DURATION,

                        succinctBytes(context.physicalInputDataSize.get()),
                        context.physicalInputPositions.get(),

                        succinctBytes(context.internalNetworkInputDataSize.get()),
                        context.internalNetworkInputPositions.get(),

                        succinctBytes(context.physicalInputDataSize.get() + context.internalNetworkInputDataSize.get()),

                        succinctBytes(context.inputDataSize.get()),
                        context.inputPositions.get(),
                        context.inputPositions.get() * context.inputPositions.get(),

                        context.operatorTiming.getCalls(),
                        new Duration(context.operatorTiming.getWallNanos(), NANOSECONDS),
                        new Duration(context.operatorTiming.getCpuNanos(), NANOSECONDS),

                        succinctBytes(context.outputDataSize.get()),
                        context.outputPositions.get(),

                        new DataSize(0, BYTE),

                        new Duration(context.blockedWallNanos.get(), NANOSECONDS),

                        // WorkProcessorOperator doesn't have finish call
                        0,
                        ZERO_DURATION,
                        ZERO_DURATION,

                        succinctBytes(context.memoryTrackingContext.getUserMemory()),
                        succinctBytes(context.memoryTrackingContext.getRevocableMemory()),
                        succinctBytes(context.memoryTrackingContext.getSystemMemory()),
                        succinctBytes(context.peakUserMemoryReservation.get()),
                        succinctBytes(context.peakSystemMemoryReservation.get()),
                        succinctBytes(context.peakRevocableMemoryReservation.get()),
                        succinctBytes(context.peakTotalMemoryReservation.get()),
                        new DataSize(0, BYTE),
                        operatorContext.isWaitingForMemory().isDone() ? Optional.empty() : Optional.of(WAITING_FOR_MEMORY),
                        getOperatorInfo(context)))
                .collect(toImmutableList());
    }

    @Nullable
    private OperatorInfo getOperatorInfo(WorkProcessorOperatorContext context)
    {
        WorkProcessorOperator operator = context.operator;
        if (operator != null) {
            return operator.getOperatorInfo().orElse(null);
        }

        return context.finalOperatorInfo;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        if (sourceOperator == null) {
            return Optional::empty;
        }

        Object splitInfo = split.getInfo();
        if (splitInfo != null) {
            operatorContext.setInfoSupplier(() -> new SplitOperatorInfo(splitInfo));
        }

        pendingSplits.add(split);
        blockedOnSplits.set(null);

        return sourceOperator.getUpdatablePageSourceSupplier();
    }

    @Override
    public void noMoreSplits()
    {
        blockedOnSplits.set(null);
        sourceOperator = null;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (!pages.process()) {
            return null;
        }

        if (pages.isFinished()) {
            return null;
        }

        return pages.getResult();
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        // TODO: support spill
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishMemoryRevoke()
    {
        // TODO: support spill
        throw new UnsupportedOperationException();
    }

    @Override
    public void finish()
    {
        // operator is finished early without waiting for all pages to process
        operatorFinishing = true;
        noMoreSplits();
        closeOperators(workProcessorOperatorContexts.size() - 1);
    }

    @Override
    public boolean isFinished()
    {
        return pages.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!pages.isBlocked()) {
            return NOT_BLOCKED;
        }

        return pages.getBlockedFuture();
    }

    @Override
    public void close()
    {
        finish();
    }

    private class Splits
            implements WorkProcessor.Process<Split>
    {
        @Override
        public ProcessState<Split> process()
        {
            boolean noMoreSplits = sourceOperator == null;

            if (pendingSplits.isEmpty()) {
                if (noMoreSplits) {
                    return ProcessState.finished();
                }

                blockedOnSplits = SettableFuture.create();
                blockedFuture = blockedOnSplits;
                return ProcessState.blocked(blockedOnSplits);
            }

            return ProcessState.ofResult(pendingSplits.remove(0));
        }
    }

    private void closeOperators(int lastOperatorIndex)
    {
        // record the current interrupted status (and clear the flag); we'll reset it later
        boolean wasInterrupted = Thread.interrupted();
        Throwable inFlightException = null;
        try {
            for (int i = 0; i <= lastOperatorIndex; ++i) {
                WorkProcessorOperatorContext workProcessorOperatorContext = workProcessorOperatorContexts.get(i);
                WorkProcessorOperator operator = workProcessorOperatorContext.operator;
                if (operator == null) {
                    // operator is already closed
                    continue;
                }

                try {
                    operator.close();
                }
                catch (InterruptedException t) {
                    // don't record the stack
                    wasInterrupted = true;
                }
                catch (Throwable t) {
                    inFlightException = handleOperatorCloseError(
                            inFlightException,
                            t,
                            "Error closing WorkProcessor operator %s for task %s",
                            workProcessorOperatorContext.operatorId,
                            operatorContext.getDriverContext().getTaskId());
                }
                finally {
                    workProcessorOperatorContext.memoryTrackingContext.close();
                    workProcessorOperatorContext.finalOperatorInfo = operator.getOperatorInfo().orElse(null);
                    workProcessorOperatorContext.operator = null;
                }
            }
        }
        finally {
            // reset the interrupted flag
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
        if (inFlightException != null) {
            throwIfUnchecked(inFlightException);
            throw new RuntimeException(inFlightException);
        }
    }

    private static Throwable handleOperatorCloseError(Throwable inFlightException, Throwable newException, String message, Object... args)
    {
        if (newException instanceof Error) {
            if (inFlightException == null) {
                inFlightException = newException;
            }
            else {
                // Self-suppression not permitted
                if (inFlightException != newException) {
                    inFlightException.addSuppressed(newException);
                }
            }
        }
        else {
            // log normal exceptions instead of rethrowing them
            log.error(newException, message, args);
        }
        return inFlightException;
    }

    private static class InternalLocalMemoryContext
            implements LocalMemoryContext
    {
        final LocalMemoryContext delegate;
        final Runnable allocationListener;

        InternalLocalMemoryContext(LocalMemoryContext delegate, Runnable allocationListener)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.allocationListener = requireNonNull(allocationListener, "allocationListener is null");
        }

        @Override
        public long getBytes()
        {
            return delegate.getBytes();
        }

        @Override
        public ListenableFuture<?> setBytes(long bytes)
        {
            ListenableFuture<?> blocked = delegate.setBytes(bytes);
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
            delegate.close();
            allocationListener.run();
        }
    }

    private static class InternalAggregatedMemoryContext
            implements AggregatedMemoryContext
    {
        final AggregatedMemoryContext delegate;
        final Runnable allocationListener;

        InternalAggregatedMemoryContext(AggregatedMemoryContext delegate, Runnable allocationListener)

        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.allocationListener = requireNonNull(allocationListener, "allocationListener is null");
        }

        @Override
        public AggregatedMemoryContext newAggregatedMemoryContext()
        {
            return new InternalAggregatedMemoryContext(delegate.newAggregatedMemoryContext(), allocationListener);
        }

        @Override
        public LocalMemoryContext newLocalMemoryContext(String allocationTag)
        {
            return new InternalLocalMemoryContext(delegate.newLocalMemoryContext(allocationTag), allocationListener);
        }

        @Override
        public long getBytes()
        {
            return delegate.getBytes();
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }

    private static class WorkProcessorOperatorContext
    {
        final int operatorId;
        final PlanNodeId planNodeId;
        final String operatorType;
        final MemoryTrackingContext memoryTrackingContext;

        final OperationTiming operatorTiming = new OperationTiming();
        final AtomicLong blockedWallNanos = new AtomicLong();

        final AtomicLong physicalInputDataSize = new AtomicLong();
        final AtomicLong physicalInputPositions = new AtomicLong();

        final AtomicLong internalNetworkInputDataSize = new AtomicLong();
        final AtomicLong internalNetworkInputPositions = new AtomicLong();

        final AtomicLong inputDataSize = new AtomicLong();
        final AtomicLong inputPositions = new AtomicLong();

        final AtomicLong readTimeNanos = new AtomicLong();

        final AtomicLong outputDataSize = new AtomicLong();
        final AtomicLong outputPositions = new AtomicLong();

        final AtomicLong peakUserMemoryReservation = new AtomicLong();
        final AtomicLong peakSystemMemoryReservation = new AtomicLong();
        final AtomicLong peakRevocableMemoryReservation = new AtomicLong();
        final AtomicLong peakTotalMemoryReservation = new AtomicLong();

        @Nullable
        volatile WorkProcessorOperator operator;
        @Nullable
        volatile OperatorInfo finalOperatorInfo;

        private WorkProcessorOperatorContext(
                WorkProcessorOperator operator,
                int operatorId,
                PlanNodeId planNodeId,
                String operatorType,
                MemoryTrackingContext memoryTrackingContext)
        {
            this.operator = operator;
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.operatorType = operatorType;
            this.memoryTrackingContext = memoryTrackingContext;
        }

        void updatePeakMemoryReservations()
        {
            long userMemory = memoryTrackingContext.getUserMemory();
            long systemMemory = memoryTrackingContext.getSystemMemory();
            long revocableMemory = memoryTrackingContext.getRevocableMemory();
            long totalMemory = userMemory + systemMemory;
            peakUserMemoryReservation.accumulateAndGet(userMemory, Math::max);
            peakSystemMemoryReservation.accumulateAndGet(systemMemory, Math::max);
            peakRevocableMemoryReservation.accumulateAndGet(revocableMemory, Math::max);
            peakTotalMemoryReservation.accumulateAndGet(totalMemory, Math::max);
        }
    }

    public static class WorkProcessorPipelineSourceOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final WorkProcessorSourceOperatorFactory sourceOperatorFactory;
        private final List<WorkProcessorOperatorFactory> operatorFactories;
        private boolean closed;

        private WorkProcessorPipelineSourceOperatorFactory(
                int operatorId,
                WorkProcessorSourceOperatorFactory sourceOperatorFactory,
                List<WorkProcessorOperatorFactory> operatorFactories)
        {
            this.operatorId = operatorId;
            this.sourceOperatorFactory = requireNonNull(sourceOperatorFactory, "sourceOperatorFactory is null");
            this.operatorFactories = requireNonNull(operatorFactories, "operatorFactories is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceOperatorFactory.getSourceId();
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            return new WorkProcessorPipelineSourceOperator(operatorId, driverContext, sourceOperatorFactory, operatorFactories);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }
}
