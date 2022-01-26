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
import io.airlift.units.DataSize;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.operator.BasicWorkProcessorOperatorAdapter.BasicAdapterWorkProcessorOperatorFactory;
import io.trino.operator.WorkProcessor.TransformationState;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.BasicWorkProcessorOperatorAdapter.createAdapterOperatorFactory;
import static java.util.Objects.requireNonNull;

/**
 * Returns the top N rows from the source sorted according to the specified ordering in the keyChannelIndex channel.
 */
public class TopNOperator
        implements WorkProcessorOperator
{
    public static OperatorFactory createOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            List<? extends Type> types,
            int n,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            TypeOperators typeOperators,
            Optional<DataSize> maxPartialMemory)
    {
        return createAdapterOperatorFactory(new Factory(operatorId, planNodeId, types, n, sortChannels, sortOrders, typeOperators, maxPartialMemory));
    }

    private static class Factory
            implements BasicAdapterWorkProcessorOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final int n;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;
        private final TypeOperators typeOperators;
        private final Optional<DataSize> maxPartialMemory;
        private boolean closed;

        private Factory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> types,
                int n,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders,
                TypeOperators typeOperators,
                Optional<DataSize> maxPartialMemory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.n = n;
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
            this.typeOperators = typeOperators;
            this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");
        }

        @Override
        public WorkProcessorOperator create(
                ProcessorContext processorContext,
                WorkProcessor<Page> sourcePages)
        {
            checkState(!closed, "Factory is already closed");
            return new TopNOperator(
                    processorContext.getMemoryTrackingContext(),
                    sourcePages,
                    sourceTypes,
                    n,
                    sortChannels,
                    sortOrders,
                    typeOperators,
                    maxPartialMemory);
        }

        @Override
        public int getOperatorId()
        {
            return operatorId;
        }

        @Override
        public PlanNodeId getPlanNodeId()
        {
            return planNodeId;
        }

        @Override
        public String getOperatorType()
        {
            return TopNOperator.class.getSimpleName();
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public Factory duplicate()
        {
            return new Factory(operatorId, planNodeId, sourceTypes, n, sortChannels, sortOrders, typeOperators, maxPartialMemory);
        }
    }

    private final WorkProcessor<Page> pages;

    private TopNOperator(
            MemoryTrackingContext memoryTrackingContext,
            WorkProcessor<Page> sourcePages,
            List<Type> types,
            int n,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            TypeOperators typeOperators,
            Optional<DataSize> maxPartialMemory)
    {
        requireNonNull(memoryTrackingContext, "memoryTrackingContext is null");

        if (n == 0) {
            pages = WorkProcessor.of();
        }
        else {
            TopNProcessor topNProcessor = new TopNProcessor(
                    memoryTrackingContext.aggregateUserMemoryContext(),
                    types,
                    n,
                    sortChannels,
                    sortOrders,
                    typeOperators);
            long maxPartialMemoryWithDefaultValueIfAbsent = requireNonNull(maxPartialMemory, "maxPartialMemory is null")
                    .map(DataSize::toBytes).orElse(Long.MAX_VALUE);
            pages = sourcePages.transform(new TopNPages(topNProcessor, maxPartialMemoryWithDefaultValueIfAbsent));
        }
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    private static class TopNPages
            implements WorkProcessor.Transformation<Page, Page>
    {
        private final TopNProcessor topNProcessor;
        private final long maxPartialMemory;

        private boolean isPartialFlushing;

        private TopNPages(TopNProcessor topNProcessor, long maxPartialMemory)
        {
            this.topNProcessor = topNProcessor;
            this.maxPartialMemory = maxPartialMemory;
        }

        private boolean isBuilderFull()
        {
            return topNProcessor.getEstimatedSizeInBytes() >= maxPartialMemory;
        }

        private void addPage(Page page)
        {
            checkState(!isPartialFlushing, "TopN buffer is already full");
            topNProcessor.addInput(page);
            if (isBuilderFull()) {
                isPartialFlushing = true;
            }
        }

        @Override
        public TransformationState<Page> process(Page inputPage)
        {
            if (!isPartialFlushing && inputPage != null) {
                addPage(inputPage);
                if (!isPartialFlushing) {
                    return TransformationState.needsMoreData();
                }
            }

            Page page = null;
            while (page == null && !topNProcessor.noMoreOutput()) {
                page = topNProcessor.getOutput();
            }

            if (page != null) {
                return TransformationState.ofResult(page, false);
            }

            if (isPartialFlushing) {
                checkState(inputPage != null, "inputPage that triggered partial flushing is null");
                isPartialFlushing = false;
                // resume receiving pages
                return TransformationState.needsMoreData();
            }

            // all input pages are consumed
            return TransformationState.finished();
        }
    }
}
