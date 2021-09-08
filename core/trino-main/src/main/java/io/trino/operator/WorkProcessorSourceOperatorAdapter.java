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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.Session;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.metadata.Split;
import io.trino.spi.Page;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.operator.WorkProcessor.ProcessState.blocked;
import static io.trino.operator.WorkProcessor.ProcessState.finished;
import static io.trino.operator.WorkProcessor.ProcessState.ofResult;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class WorkProcessorSourceOperatorAdapter
        implements SourceOperator
{
    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final WorkProcessorSourceOperator sourceOperator;
    private final WorkProcessor<Page> pages;
    private final SplitBuffer splitBuffer;

    private boolean operatorFinishing;

    private long previousPhysicalInputBytes;
    private long previousPhysicalInputPositions;
    private long previousInternalNetworkInputBytes;
    private long previousInternalNetworkPositions;
    private long previousInputBytes;
    private long previousInputPositions;
    private long previousReadTimeNanos;
    private long previousDynamicFilterSplitsProcessed;

    public interface AdapterWorkProcessorSourceOperatorFactory
            extends WorkProcessorSourceOperatorFactory
    {
        default WorkProcessorSourceOperator createAdapterOperator(
                Session session,
                MemoryTrackingContext memoryTrackingContext,
                DriverYieldSignal yieldSignal,
                WorkProcessor<Split> splits)
        {
            return create(session, memoryTrackingContext, yieldSignal, splits);
        }
    }

    public WorkProcessorSourceOperatorAdapter(OperatorContext operatorContext, AdapterWorkProcessorSourceOperatorFactory sourceOperatorFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceOperatorFactory, "sourceOperatorFactory is null").getSourceId();
        this.splitBuffer = new SplitBuffer();
        this.sourceOperator = sourceOperatorFactory
                .createAdapterOperator(
                        operatorContext.getSession(),
                        new MemoryTrackingContext(
                                operatorContext.aggregateUserMemoryContext(),
                                operatorContext.aggregateRevocableMemoryContext(),
                                operatorContext.aggregateSystemMemoryContext()),
                        operatorContext.getDriverContext().getYieldSignal(),
                        WorkProcessor.create(splitBuffer));
        this.pages = sourceOperator.getOutputPages()
                .map(Page::getLoadedPage)
                .withProcessStateMonitor(state -> updateOperatorStats())
                .finishWhen(() -> operatorFinishing);
        operatorContext.setInfoSupplier(() -> sourceOperator.getOperatorInfo().orElse(null));
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        if (operatorFinishing) {
            return Optional::empty;
        }

        Object splitInfo = split.getInfo();
        if (splitInfo != null) {
            operatorContext.setInfoSupplier(Suppliers.ofInstance(new SplitOperatorInfo(split.getCatalogName(), splitInfo)));
        }

        splitBuffer.add(split);
        return sourceOperator.getUpdatablePageSourceSupplier();
    }

    @Override
    public void noMoreSplits()
    {
        splitBuffer.noMoreSplits();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
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
    public void finish()
    {
        operatorFinishing = true;
        noMoreSplits();
    }

    @Override
    public boolean isFinished()
    {
        return pages.isFinished();
    }

    @Override
    public void close()
            throws Exception
    {
        operatorContext.setLatestMetrics(sourceOperator.getMetrics());
        operatorContext.setLatestConnectorMetrics(sourceOperator.getConnectorMetrics());
        sourceOperator.close();
    }

    private void updateOperatorStats()
    {
        long currentPhysicalInputBytes = sourceOperator.getPhysicalInputDataSize().toBytes();
        long currentPhysicalInputPositions = sourceOperator.getPhysicalInputPositions();
        long currentReadTimeNanos = sourceOperator.getReadTime().roundTo(NANOSECONDS);

        long currentInternalNetworkInputBytes = sourceOperator.getInternalNetworkInputDataSize().toBytes();
        long currentInternalNetworkPositions = sourceOperator.getInternalNetworkPositions();

        long currentInputBytes = sourceOperator.getInputDataSize().toBytes();
        long currentInputPositions = sourceOperator.getInputPositions();

        long currentDynamicFilterSplitsProcessed = sourceOperator.getDynamicFilterSplitsProcessed();
        Metrics currentMetrics = sourceOperator.getMetrics();
        Metrics currentConnectorMetrics = sourceOperator.getConnectorMetrics();

        if (currentPhysicalInputBytes != previousPhysicalInputBytes
                || currentPhysicalInputPositions != previousPhysicalInputPositions
                || currentReadTimeNanos != previousReadTimeNanos) {
            operatorContext.recordPhysicalInputWithTiming(
                    currentPhysicalInputBytes - previousPhysicalInputBytes,
                    currentPhysicalInputPositions - previousPhysicalInputPositions,
                    currentReadTimeNanos - previousReadTimeNanos);

            previousPhysicalInputBytes = currentPhysicalInputBytes;
            previousPhysicalInputPositions = currentPhysicalInputPositions;
            previousReadTimeNanos = currentReadTimeNanos;
        }

        if (currentInternalNetworkInputBytes != previousInternalNetworkInputBytes
                || currentInternalNetworkPositions != previousInternalNetworkPositions) {
            operatorContext.recordNetworkInput(
                    currentInternalNetworkInputBytes - previousInternalNetworkInputBytes,
                    currentInternalNetworkPositions - previousInternalNetworkPositions);

            previousInternalNetworkInputBytes = currentInternalNetworkInputBytes;
            previousInternalNetworkPositions = currentInternalNetworkPositions;
        }

        if (currentInputBytes != previousInputBytes
                || currentInputPositions != previousInputPositions) {
            operatorContext.recordProcessedInput(
                    currentInputBytes - previousInputBytes,
                    currentInputPositions - previousInputPositions);

            previousInputBytes = currentInputBytes;
            previousInputPositions = currentInputPositions;
        }

        if (currentDynamicFilterSplitsProcessed != previousDynamicFilterSplitsProcessed) {
            operatorContext.recordDynamicFilterSplitProcessed(currentDynamicFilterSplitsProcessed - previousDynamicFilterSplitsProcessed);
            previousDynamicFilterSplitsProcessed = currentDynamicFilterSplitsProcessed;
        }

        operatorContext.setLatestMetrics(currentMetrics);
        operatorContext.setLatestConnectorMetrics(currentConnectorMetrics);
    }

    private static class SplitBuffer
            implements WorkProcessor.Process<Split>
    {
        private final List<Split> pendingSplits = new ArrayList<>();

        private SettableFuture<Void> blockedOnSplits = SettableFuture.create();
        private boolean noMoreSplits;

        @Override
        public WorkProcessor.ProcessState<Split> process()
        {
            if (pendingSplits.isEmpty()) {
                if (noMoreSplits) {
                    return finished();
                }

                blockedOnSplits = SettableFuture.create();
                return blocked(blockedOnSplits);
            }

            return ofResult(pendingSplits.remove(0));
        }

        void add(Split split)
        {
            pendingSplits.add(split);
            blockedOnSplits.set(null);
        }

        void noMoreSplits()
        {
            noMoreSplits = true;
            blockedOnSplits.set(null);
        }
    }
}
