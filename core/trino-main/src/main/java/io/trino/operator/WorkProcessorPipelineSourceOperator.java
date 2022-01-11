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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.FormatMethod;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.Lifespan;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.metadata.Split;
import io.trino.operator.OperationTimer.OperationTiming;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.spi.Page;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;
import io.trino.sql.planner.LocalExecutionPlanner.OperatorFactoryWithTypes;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.operator.BlockedReason.WAITING_FOR_MEMORY;
import static io.trino.operator.OperatorContext.getConnectorMetrics;
import static io.trino.operator.OperatorContext.getOperatorMetrics;
import static io.trino.operator.PageUtils.recordMaterializedBytes;
import static io.trino.operator.WorkProcessor.ProcessState.Type.BLOCKED;
import static io.trino.operator.WorkProcessor.ProcessState.Type.FINISHED;
import static io.trino.operator.project.MergePages.mergePages;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class WorkProcessorPipelineSourceOperator
        implements SourceOperator
{
    private static final Logger log = Logger.get(WorkProcessorPipelineSourceOperator.class);
    private static final Duration ZERO_DURATION = new Duration(0, NANOSECONDS);
    private static final int OPERATOR_ID = Integer.MAX_VALUE;

    private final int stageId;
    private final int pipelineId;
    private final PlanNodeId sourceId;
    private final OperatorContext operatorContext;
    private final WorkProcessor<Page> pages;
    private final OperationTimer timer;
    // operator instances including source operator
    private final List<WorkProcessorOperatorContext> workProcessorOperatorContexts = new ArrayList<>();
    private final List<Split> pendingSplits = new ArrayList<>();

    private ListenableFuture<Void> blockedFuture;
    private WorkProcessorSourceOperator sourceOperator;
    private SettableFuture<Void> blockedOnSplits = SettableFuture.create();
    private boolean operatorFinishing;

    public static List<OperatorFactory> convertOperators(
            List<OperatorFactoryWithTypes> operatorFactoriesWithTypes,
            DataSize minOutputPageSize,
            int minOutputPageRowCount)
    {
        if (operatorFactoriesWithTypes.isEmpty() || !(operatorFactoriesWithTypes.get(0).getOperatorFactory() instanceof WorkProcessorSourceOperatorFactory)) {
            return toOperatorFactories(operatorFactoriesWithTypes);
        }

        WorkProcessorSourceOperatorFactory sourceOperatorFactory = (WorkProcessorSourceOperatorFactory) operatorFactoriesWithTypes.get(0).getOperatorFactory();
        ImmutableList.Builder<WorkProcessorOperatorFactory> workProcessorOperatorFactoriesBuilder = ImmutableList.builder();
        int operatorIndex = 1;
        for (; operatorIndex < operatorFactoriesWithTypes.size(); ++operatorIndex) {
            OperatorFactory operatorFactory = operatorFactoriesWithTypes.get(operatorIndex).getOperatorFactory();
            if (!(operatorFactory instanceof WorkProcessorOperatorFactory)) {
                break;
            }
            workProcessorOperatorFactoriesBuilder.add((WorkProcessorOperatorFactory) operatorFactory);
        }

        List<WorkProcessorOperatorFactory> workProcessorOperatorFactories = workProcessorOperatorFactoriesBuilder.build();
        if (workProcessorOperatorFactories.isEmpty()) {
            return toOperatorFactories(operatorFactoriesWithTypes);
        }

        return ImmutableList.<OperatorFactory>builder()
                .add(new WorkProcessorPipelineSourceOperatorFactory(
                        sourceOperatorFactory,
                        workProcessorOperatorFactories,
                        operatorFactoriesWithTypes.get(operatorIndex - 1).getTypes(),
                        minOutputPageSize,
                        minOutputPageRowCount))
                .addAll(toOperatorFactories(operatorFactoriesWithTypes.subList(operatorIndex, operatorFactoriesWithTypes.size())))
                .build();
    }

    public static List<OperatorFactory> toOperatorFactories(List<OperatorFactoryWithTypes> operatorFactoriesWithTypes)
    {
        return operatorFactoriesWithTypes.stream()
                .map(OperatorFactoryWithTypes::getOperatorFactory)
                .collect(toImmutableList());
    }

    private WorkProcessorPipelineSourceOperator(
            DriverContext driverContext,
            WorkProcessorSourceOperatorFactory sourceOperatorFactory,
            List<WorkProcessorOperatorFactory> operatorFactories,
            List<Type> outputTypes,
            DataSize minOutputPageSize,
            int minOutputPageRowCount)
    {
        requireNonNull(driverContext, "driverContext is null");
        requireNonNull(sourceOperatorFactory, "sourceOperatorFactory is null");
        requireNonNull(operatorFactories, "operatorFactories is null");
        this.stageId = driverContext.getTaskId().getStageId().getId();
        this.pipelineId = driverContext.getPipelineContext().getPipelineId();
        this.sourceId = requireNonNull(sourceOperatorFactory.getSourceId(), "sourceId is null");
        this.operatorContext = driverContext.addOperatorContext(OPERATOR_ID, sourceId, WorkProcessorPipelineSourceOperator.class.getSimpleName());
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
                    new ProcessorContext(operatorContext.getSession(), operatorMemoryTrackingContext, operatorContext),
                    pages);
            workProcessorOperatorContexts.add(new WorkProcessorOperatorContext(
                    operator,
                    operatorFactory.getOperatorId(),
                    operatorFactory.getPlanNodeId(),
                    operatorFactory.getOperatorType(),
                    operatorMemoryTrackingContext));
            pages = operator.getOutputPages();
            if (i == operatorFactories.size() - 1) {
                // materialize output pages as there are no semantics guarantees for non WorkProcessor operators
                pages = pages.map(Page::getLoadedPage);
                pages = pages.transformProcessor(processor -> mergePages(
                        outputTypes,
                        minOutputPageSize.toBytes(),
                        minOutputPageRowCount,
                        processor,
                        operatorContext.aggregateUserMemoryContext()));
            }
            pages = pages
                    .yielding(() -> operatorContext.getDriverContext().getYieldSignal().isSet())
                    .withProcessEntryMonitor(() -> workProcessorOperatorEntryMonitor(operatorIndex))
                    .withProcessStateMonitor(state -> workProcessorOperatorStateMonitor(state, operatorIndex));
            pages = pages.map(page -> recordProcessedOutput(page, operatorIndex));
        }

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

        context.metrics.set(context.operator.getMetrics());

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

            context.dynamicFilterSplitsProcessed.set(sourceOperator.getDynamicFilterSplitsProcessed());
            context.connectorMetrics.set(sourceOperator.getConnectorMetrics());

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
                        new Duration(0, NANOSECONDS),
                        ZERO_DURATION,

                        succinctBytes(context.physicalInputDataSize.get()),
                        context.physicalInputPositions.get(),

                        succinctBytes(context.internalNetworkInputDataSize.get()),
                        context.internalNetworkInputPositions.get(),

                        succinctBytes(context.physicalInputDataSize.get() + context.internalNetworkInputDataSize.get()),

                        succinctBytes(context.inputDataSize.get()),
                        context.inputPositions.get(),
                        (double) context.inputPositions.get() * context.inputPositions.get(),

                        context.operatorTiming.getCalls(),
                        new Duration(context.operatorTiming.getWallNanos(), NANOSECONDS),
                        new Duration(context.operatorTiming.getCpuNanos(), NANOSECONDS),

                        succinctBytes(context.outputDataSize.get()),
                        context.outputPositions.get(),

                        context.dynamicFilterSplitsProcessed.get(),
                        getOperatorMetrics(context.metrics.get(), context.inputPositions.get()),
                        getConnectorMetrics(context.connectorMetrics.get(), context.readTimeNanos.get()),

                        DataSize.ofBytes(0),

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
                        DataSize.ofBytes(0),
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
            operatorContext.setInfoSupplier(Suppliers.ofInstance(new SplitOperatorInfo(split.getCatalogName(), splitInfo)));
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
    public ListenableFuture<Void> startMemoryRevoke()
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
    public ListenableFuture<Void> isBlocked()
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
                    workProcessorOperatorContext.metrics.set(operator.getMetrics());
                    if (operator instanceof WorkProcessorSourceOperator) {
                        WorkProcessorSourceOperator sourceOperator = (WorkProcessorSourceOperator) operator;
                        workProcessorOperatorContext.connectorMetrics.set(sourceOperator.getConnectorMetrics());
                    }
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

    @FormatMethod
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
        public ListenableFuture<Void> setBytes(long bytes)
        {
            ListenableFuture<Void> blocked = delegate.setBytes(bytes);
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

        final AtomicLong dynamicFilterSplitsProcessed = new AtomicLong();
        final AtomicReference<Metrics> metrics = new AtomicReference<>(Metrics.EMPTY);
        final AtomicReference<Metrics> connectorMetrics = new AtomicReference<>(Metrics.EMPTY);

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
        private final WorkProcessorSourceOperatorFactory sourceOperatorFactory;
        private final List<WorkProcessorOperatorFactory> operatorFactories;
        private final List<Type> outputTypes;
        private final DataSize minOutputPageSize;
        private final int minOutputPageRowCount;
        private boolean closed;

        private WorkProcessorPipelineSourceOperatorFactory(
                WorkProcessorSourceOperatorFactory sourceOperatorFactory,
                List<WorkProcessorOperatorFactory> operatorFactories,
                List<Type> outputTypes,
                DataSize minOutputPageSize,
                int minOutputPageRowCount)
        {
            this.sourceOperatorFactory = requireNonNull(sourceOperatorFactory, "sourceOperatorFactory is null");
            this.operatorFactories = requireNonNull(operatorFactories, "operatorFactories is null");
            this.outputTypes = requireNonNull(outputTypes, "outputTypes is null");
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
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
            return new WorkProcessorPipelineSourceOperator(
                    driverContext,
                    sourceOperatorFactory,
                    operatorFactories,
                    outputTypes,
                    minOutputPageSize,
                    minOutputPageRowCount);
        }

        @Override
        public void noMoreOperators()
        {
            this.operatorFactories.forEach(WorkProcessorOperatorFactory::close);
            closed = true;
        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {
            this.operatorFactories.forEach(operatorFactory -> operatorFactory.lifespanFinished(lifespan));
        }
    }
}
