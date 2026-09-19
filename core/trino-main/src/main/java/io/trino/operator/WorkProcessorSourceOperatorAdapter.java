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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.metadata.Split;
import io.trino.spi.Page;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;

import static io.trino.SystemSessionProperties.isSourcePagesValidationEnabled;
import static io.trino.operator.PageValidations.validateOutputPageTypes;
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
    private final boolean validateOutputPages;
    private final List<Type> outputTypes;
    private final Supplier<String> debugContext;
    private final SplitBuffer splitBuffer;

    // committed to the regular or masked stream on first output; both share one source, so only one is ever built
    private WorkProcessor<?> pages;

    private boolean operatorFinishing;

    private long previousPhysicalInputBytes;
    private long previousPhysicalInputPositions;
    private long previousInternalNetworkInputBytes;
    private long previousInternalNetworkPositions;
    private long previousInputBytes;
    private long previousInputPositions;
    private long previousReadTimeNanos;
    private long previousDynamicFilterSplitsProcessed;

    public WorkProcessorSourceOperatorAdapter(OperatorContext operatorContext, WorkProcessorSourceOperatorFactory sourceOperatorFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = sourceOperatorFactory.getSourceId();
        this.splitBuffer = new SplitBuffer();
        this.sourceOperator = sourceOperatorFactory
                .create(
                        operatorContext,
                        operatorContext.getDriverContext().getYieldSignal(),
                        WorkProcessor.create(splitBuffer));
        this.validateOutputPages = isSourcePagesValidationEnabled(operatorContext.getSession());
        this.outputTypes = sourceOperatorFactory.getOutputTypes();
        this.debugContext = () -> "%s; taskId=%s; operatorId=%s".formatted(
                operatorContext.getOperatorType(),
                operatorContext.getDriverContext().getTaskId(),
                operatorContext.getOperatorId());
        operatorContext.setInfoSupplier(() -> sourceOperator.getOperatorInfo().orElse(null));
    }

    private WorkProcessor<Page> regularOutput()
    {
        WorkProcessor<Page> outputPages = sourceOperator.getOutputPages()
                .withProcessStateMonitor(_ -> updateOperatorStats())
                .finishWhen(() -> operatorFinishing);
        if (validateOutputPages) {
            outputPages = outputPages.map(page -> {
                validateOutputPageTypes(page, outputTypes, debugContext);
                return page;
            });
        }
        return outputPages;
    }

    private WorkProcessor<MaskedPage> maskedOutput()
    {
        WorkProcessor<MaskedPage> maskedPages = sourceOperator.getMaskedOutputPages()
                .withProcessStateMonitor(_ -> updateOperatorStats())
                .finishWhen(() -> operatorFinishing);
        if (validateOutputPages) {
            // masked output validates lazily inside materialize(), so deferred channels stay undecoded
            maskedPages = maskedPages.map(maskedPage -> {
                maskedPage.setOutputValidator(page -> validateOutputPageTypes(page, outputTypes, debugContext));
                return maskedPage;
            });
        }
        return maskedPages;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public void addSplit(Split split)
    {
        if (operatorFinishing) {
            return;
        }

        splitBuffer.add(split);
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
        if (pages == null || !pages.isBlocked()) {
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
        if (pages == null) {
            pages = regularOutput();
        }
        if (!pages.process()) {
            return null;
        }

        if (pages.isFinished()) {
            return null;
        }

        return (Page) pages.getResult();
    }

    @Override
    public boolean producesMaskedOutput()
    {
        return sourceOperator.producesMaskedOutput();
    }

    @Override
    public MaskedPage getMaskedOutput()
    {
        if (pages == null) {
            pages = maskedOutput();
        }
        if (!pages.process()) {
            return null;
        }

        if (pages.isFinished()) {
            return null;
        }

        return (MaskedPage) pages.getResult();
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
        return pages != null && pages.isFinished();
    }

    @Override
    public void close()
            throws Exception
    {
        sourceOperator.close();
        operatorContext.setLatestMetrics(sourceOperator.getMetrics());
        operatorContext.setLatestConnectorMetrics(sourceOperator.getConnectorMetrics());
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
        private final Deque<Split> pendingSplits = new ArrayDeque<>();

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

            return ofResult(pendingSplits.remove());
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
