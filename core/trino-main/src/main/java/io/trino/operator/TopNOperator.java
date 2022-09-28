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
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.operator.BasicWorkProcessorOperatorAdapter.BasicAdapterWorkProcessorOperatorFactory;
import io.trino.operator.WorkProcessor.TransformationState;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;

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
            TypeOperators typeOperators)
    {
        return createAdapterOperatorFactory(new Factory(operatorId, planNodeId, types, n, sortChannels, sortOrders, typeOperators));
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
        private boolean closed;

        private Factory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> types,
                int n,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders,
                TypeOperators typeOperators)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.n = n;
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
            this.typeOperators = typeOperators;
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
                    typeOperators);
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
            return new Factory(operatorId, planNodeId, sourceTypes, n, sortChannels, sortOrders, typeOperators);
        }
    }

    private final TopNProcessor topNProcessor;
    private final WorkProcessor<Page> pages;

    private TopNOperator(
            MemoryTrackingContext memoryTrackingContext,
            WorkProcessor<Page> sourcePages,
            List<Type> types,
            int n,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            TypeOperators typeOperators)
    {
        this.topNProcessor = new TopNProcessor(
                memoryTrackingContext.aggregateUserMemoryContext(),
                types,
                n,
                sortChannels,
                sortOrders,
                typeOperators);

        if (n == 0) {
            pages = WorkProcessor.of();
        }
        else {
            pages = sourcePages.transform(new TopNPages());
        }
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    private class TopNPages
            implements WorkProcessor.Transformation<Page, Page>
    {
        @Override
        public TransformationState<Page> process(Page inputPage)
        {
            if (inputPage != null) {
                topNProcessor.addInput(inputPage);
                return TransformationState.needsMoreData();
            }

            // no more input, return results
            Page page = null;
            while (page == null && !topNProcessor.noMoreOutput()) {
                page = topNProcessor.getOutput();
            }

            if (page != null) {
                return TransformationState.ofResult(page, false);
            }

            return TransformationState.finished();
        }
    }
}
