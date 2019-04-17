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
import io.prestosql.Session;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Returns the top N rows from the source sorted according to the specified ordering in the keyChannelIndex channel.
 */
public class TopNOperator
        implements Operator
{
    public static class TopNOperatorFactory
            implements OperatorFactory, WorkProcessorOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final int n;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;
        private boolean closed;

        public TopNOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> types,
                int n,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.n = n;
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
        }

        @Override
        public int getOperatorId()
        {
            return operatorId;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TopNOperator.class.getSimpleName());
            return new TopNOperator(
                    operatorContext,
                    sourceTypes,
                    n,
                    sortChannels,
                    sortOrders);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TopNOperatorFactory(operatorId, planNodeId, sourceTypes, n, sortChannels, sortOrders);
        }

        @Override
        public WorkProcessorOperator create(
                Session session,
                MemoryTrackingContext memoryTrackingContext,
                DriverYieldSignal yieldSignal,
                WorkProcessor<Page> sourcePages)
        {
            return new TopNWorkProcessorOperator(memoryTrackingContext, sourcePages, sourceTypes, n, sortChannels, sortOrders);
        }
    }

    private final OperatorContext operatorContext;
    private final TopNProcessor topNProcessor;
    private boolean finishing;

    public TopNOperator(
            OperatorContext operatorContext,
            List<Type> types,
            int n,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders)
    {
        this.operatorContext = operatorContext;
        this.topNProcessor = new TopNProcessor(
                requireNonNull(operatorContext, "operatorContext is null").aggregateUserMemoryContext(),
                types,
                n,
                sortChannels,
                sortOrders);

        if (n == 0) {
            finishing = true;
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && topNProcessor.noMoreOutput();
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && !topNProcessor.noMoreOutput();
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        topNProcessor.addInput(page);
    }

    @Override
    public Page getOutput()
    {
        if (!finishing || topNProcessor.noMoreOutput()) {
            return null;
        }

        return topNProcessor.getOutput();
    }
}
