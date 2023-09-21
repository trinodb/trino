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
package io.trino.operator.exchange;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PageWithPositionComparator;
import io.trino.operator.WorkProcessor;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.util.MergeSortedPages.mergeSortedPages;
import static java.util.Objects.requireNonNull;

public class LocalMergeSourceOperator
        implements Operator
{
    public static class LocalMergeSourceOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final LocalExchange localExchange;
        private final List<Type> types;
        private final OrderingCompiler orderingCompiler;
        private final List<Integer> sortChannels;
        private final List<SortOrder> orderings;
        private boolean closed;

        public LocalMergeSourceOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                LocalExchange localExchange,
                List<Type> types,
                OrderingCompiler orderingCompiler,
                List<Integer> sortChannels,
                List<SortOrder> orderings)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.localExchange = requireNonNull(localExchange, "localExchange is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LocalMergeSourceOperator.class.getSimpleName());
            PageWithPositionComparator comparator = orderingCompiler.compilePageWithPositionComparator(types, sortChannels, orderings);
            List<LocalExchangeSource> sources = IntStream.range(0, localExchange.getBufferCount())
                    .boxed()
                    .map(index -> localExchange.getNextSource())
                    .collect(toImmutableList());
            return new LocalMergeSourceOperator(operatorContext, sources, types, comparator);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Source operator factories cannot be duplicated");
        }
    }

    private final OperatorContext operatorContext;
    private final List<LocalExchangeSource> sources;
    private final WorkProcessor<Page> mergedPages;

    public LocalMergeSourceOperator(OperatorContext operatorContext, List<LocalExchangeSource> sources, List<Type> types, PageWithPositionComparator comparator)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sources = requireNonNull(sources, "sources is null");
        List<WorkProcessor<Page>> pageProducers = sources.stream()
                .map(LocalExchangeSource::pages)
                .collect(toImmutableList());
        mergedPages = mergeSortedPages(
                pageProducers,
                requireNonNull(comparator, "comparator is null"),
                types,
                operatorContext.aggregateUserMemoryContext(),
                operatorContext.getDriverContext().getYieldSignal());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        sources.forEach(LocalExchangeSource::finish);
    }

    @Override
    public boolean isFinished()
    {
        return mergedPages.isFinished();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        if (mergedPages.isBlocked()) {
            return mergedPages.getBlockedFuture();
        }

        return NOT_BLOCKED;
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
        if (!mergedPages.process() || mergedPages.isFinished()) {
            return null;
        }

        Page page = mergedPages.getResult();
        operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
        return page;
    }

    @Override
    public void close()
    {
        sources.forEach(LocalExchangeSource::close);
    }
}
