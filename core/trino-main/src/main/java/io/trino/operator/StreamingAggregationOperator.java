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
import com.google.common.primitives.Ints;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.BasicWorkProcessorOperatorAdapter.BasicAdapterWorkProcessorOperatorFactory;
import io.trino.operator.WorkProcessor.Transformation;
import io.trino.operator.WorkProcessor.TransformationState;
import io.trino.operator.aggregation.Aggregator;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.Nullable;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.BasicWorkProcessorOperatorAdapter.createAdapterOperatorFactory;
import static io.trino.operator.WorkProcessor.TransformationState.finished;
import static io.trino.operator.WorkProcessor.TransformationState.needsMoreData;
import static io.trino.operator.WorkProcessor.TransformationState.ofResult;
import static java.util.Objects.requireNonNull;

public class StreamingAggregationOperator
        implements WorkProcessorOperator
{
    public static OperatorFactory createOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            List<Type> sourceTypes,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            List<AggregatorFactory> aggregatorFactories,
            JoinCompiler joinCompiler)
    {
        return createAdapterOperatorFactory(new Factory(
                operatorId,
                planNodeId,
                sourceTypes,
                groupByTypes,
                groupByChannels,
                aggregatorFactories,
                joinCompiler));
    }

    private static class Factory
            implements BasicAdapterWorkProcessorOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Type> groupByTypes;
        private final List<Integer> groupByChannels;
        private final List<AggregatorFactory> aggregatorFactories;
        private final JoinCompiler joinCompiler;
        private boolean closed;

        private Factory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> sourceTypes,
                List<Type> groupByTypes,
                List<Integer> groupByChannels,
                List<AggregatorFactory> aggregatorFactories,
                JoinCompiler joinCompiler)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.groupByTypes = ImmutableList.copyOf(requireNonNull(groupByTypes, "groupByTypes is null"));
            this.groupByChannels = ImmutableList.copyOf(requireNonNull(groupByChannels, "groupByChannels is null"));
            this.aggregatorFactories = ImmutableList.copyOf(requireNonNull(aggregatorFactories, "aggregatorFactories is null"));
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
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
            return StreamingAggregationOperator.class.getSimpleName();
        }

        @Override
        public WorkProcessorOperator create(ProcessorContext processorContext, WorkProcessor<Page> sourcePages)
        {
            checkState(!closed, "Factory is already closed");
            return new StreamingAggregationOperator(processorContext, sourcePages, sourceTypes, groupByTypes, groupByChannels, aggregatorFactories, joinCompiler);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public Factory duplicate()
        {
            return new Factory(operatorId, planNodeId, sourceTypes, groupByTypes, groupByChannels, aggregatorFactories, joinCompiler);
        }
    }

    private final WorkProcessor<Page> pages;

    private StreamingAggregationOperator(
            ProcessorContext processorContext,
            WorkProcessor<Page> sourcePages,
            List<Type> sourceTypes,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            List<AggregatorFactory> aggregatorFactories,
            JoinCompiler joinCompiler)
    {
        pages = sourcePages
                .transform(new StreamingAggregation(
                        processorContext,
                        sourceTypes,
                        groupByTypes,
                        groupByChannels,
                        aggregatorFactories,
                        joinCompiler));
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    private static class StreamingAggregation
            implements Transformation<Page, Page>
    {
        private final LocalMemoryContext userMemoryContext;
        private final List<Type> groupByTypes;
        private final int[] groupByChannels;
        private final List<AggregatorFactory> aggregatorFactories;
        private final PagesHashStrategy pagesHashStrategy;

        private List<Aggregator> aggregates;
        private final PageBuilder pageBuilder;
        private final Deque<Page> outputPages = new LinkedList<>();
        private Page currentGroup;

        private StreamingAggregation(
                ProcessorContext processorContext,
                List<Type> sourceTypes,
                List<Type> groupByTypes,
                List<Integer> groupByChannels,
                List<AggregatorFactory> aggregatorFactories,
                JoinCompiler joinCompiler)
        {
            requireNonNull(processorContext, "processorContext is null");
            this.userMemoryContext = processorContext.getMemoryTrackingContext().localUserMemoryContext();
            this.groupByTypes = ImmutableList.copyOf(requireNonNull(groupByTypes, "groupByTypes is null"));
            this.groupByChannels = Ints.toArray(requireNonNull(groupByChannels, "groupByChannels is null"));
            this.aggregatorFactories = requireNonNull(aggregatorFactories, "aggregatorFactories is null");

            this.aggregates = aggregatorFactories.stream()
                    .map(AggregatorFactory::createAggregator)
                    .collect(toImmutableList());
            this.pageBuilder = new PageBuilder(toTypes(groupByTypes, aggregates));
            requireNonNull(joinCompiler, "joinCompiler is null");

            requireNonNull(sourceTypes, "sourceTypes is null");
            pagesHashStrategy = joinCompiler.compilePagesHashStrategyFactory(sourceTypes, groupByChannels, Optional.empty())
                    .createPagesHashStrategy(
                            sourceTypes.stream()
                                    .map(type -> ImmutableList.<Block>of())
                                    .collect(toImmutableList()), OptionalInt.empty());
        }

        @Override
        public TransformationState<Page> process(@Nullable Page inputPage)
        {
            if (inputPage == null) {
                if (currentGroup != null) {
                    evaluateAndFlushGroup(currentGroup, 0);
                    currentGroup = null;
                }

                if (!pageBuilder.isEmpty()) {
                    outputPages.add(pageBuilder.build());
                    pageBuilder.reset();
                }

                if (outputPages.isEmpty()) {
                    return finished();
                }

                return ofResult(outputPages.removeFirst(), false);
            }

            // flush remaining output pages before requesting next input page
            // invariant: queued output pages are produced from argument `inputPage`
            if (!outputPages.isEmpty()) {
                Page outputPage = outputPages.removeFirst();
                return ofResult(outputPage, outputPages.isEmpty());
            }

            processInput(inputPage);
            updateMemoryUsage();

            if (outputPages.isEmpty()) {
                return needsMoreData();
            }

            Page outputPage = outputPages.removeFirst();
            return ofResult(outputPage, outputPages.isEmpty());
        }

        private void updateMemoryUsage()
        {
            long memorySize = pageBuilder.getRetainedSizeInBytes();
            for (Page output : outputPages) {
                memorySize += output.getRetainedSizeInBytes();
            }
            for (Aggregator aggregator : aggregates) {
                memorySize += aggregator.getEstimatedSize();
            }

            if (currentGroup != null) {
                memorySize += currentGroup.getRetainedSizeInBytes();
            }

            userMemoryContext.setBytes(memorySize);
        }

        private void processInput(Page page)
        {
            requireNonNull(page, "page is null");

            Page groupByPage = page.getColumns(groupByChannels);
            if (currentGroup != null) {
                if (!pagesHashStrategy.rowNotDistinctFromRow(0, currentGroup.getColumns(groupByChannels), 0, groupByPage)) {
                    // page starts with new group, so flush it
                    evaluateAndFlushGroup(currentGroup, 0);
                }
                currentGroup = null;
            }

            int startPosition = 0;
            while (true) {
                // may be equal to page.getPositionCount() if the end is not found in this page
                int nextGroupStart = findNextGroupStart(startPosition, groupByPage);
                addRowsToAggregates(page, startPosition, nextGroupStart - 1);

                if (nextGroupStart < page.getPositionCount()) {
                    // current group stops somewhere in the middle of the page, so flush it
                    evaluateAndFlushGroup(page, startPosition);
                    startPosition = nextGroupStart;
                }
                else {
                    // late materialization requires that page being locally stored is materialized before the next one is fetched
                    currentGroup = page.getRegion(page.getPositionCount() - 1, 1).getLoadedPage();
                    return;
                }
            }
        }

        private void addRowsToAggregates(Page page, int startPosition, int endPosition)
        {
            Page region = page.getRegion(startPosition, endPosition - startPosition + 1);
            for (Aggregator aggregator : aggregates) {
                aggregator.processPage(region);
            }
        }

        private void evaluateAndFlushGroup(Page page, int position)
        {
            pageBuilder.declarePosition();
            for (int i = 0; i < groupByTypes.size(); i++) {
                Block block = page.getBlock(groupByChannels[i]);
                Type type = groupByTypes.get(i);
                type.appendTo(block, position, pageBuilder.getBlockBuilder(i));
            }
            int offset = groupByTypes.size();
            for (int i = 0; i < aggregates.size(); i++) {
                aggregates.get(i).evaluate(pageBuilder.getBlockBuilder(offset + i));
            }

            if (pageBuilder.isFull()) {
                outputPages.add(pageBuilder.build());
                pageBuilder.reset();
            }

            aggregates = aggregatorFactories.stream()
                    .map(AggregatorFactory::createAggregator)
                    .collect(toImmutableList());
        }

        private int findNextGroupStart(int startPosition, Page page)
        {
            for (int i = startPosition + 1; i < page.getPositionCount(); i++) {
                if (!pagesHashStrategy.rowNotDistinctFromRow(startPosition, page, i, page)) {
                    return i;
                }
            }

            return page.getPositionCount();
        }

        private static List<Type> toTypes(List<Type> groupByTypes, List<Aggregator> aggregates)
        {
            ImmutableList.Builder<Type> builder = ImmutableList.builder();
            builder.addAll(groupByTypes);
            aggregates.stream()
                    .map(Aggregator::getType)
                    .forEach(builder::add);
            return builder.build();
        }
    }
}
