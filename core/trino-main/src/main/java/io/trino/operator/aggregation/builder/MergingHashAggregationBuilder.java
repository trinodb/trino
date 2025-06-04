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
package io.trino.operator.aggregation.builder;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.AggregationMetrics;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.OperatorContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessor.Transformation;
import io.trino.operator.WorkProcessor.TransformationState;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.AggregationNode;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class MergingHashAggregationBuilder
        implements Closeable
{
    private final List<AggregatorFactory> aggregatorFactories;
    private final AggregationNode.Step step;
    private final int expectedGroups;
    private final List<Integer> groupByPartialChannels;
    private final Optional<Integer> hashChannel;
    private final OperatorContext operatorContext;
    private final WorkProcessor<Page> sortedPages;
    private InMemoryHashAggregationBuilder hashAggregationBuilder;
    private final List<Type> groupByTypes;
    private final LocalMemoryContext memoryContext;
    private final long memoryLimitForMerge;
    private final int overwriteIntermediateChannelOffset;
    private final FlatHashStrategyCompiler hashStrategyCompiler;
    private final AggregationMetrics aggregationMetrics;

    public MergingHashAggregationBuilder(
            List<AggregatorFactory> aggregatorFactories,
            AggregationNode.Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            WorkProcessor<Page> sortedPages,
            AggregatedMemoryContext aggregatedMemoryContext,
            long memoryLimitForMerge,
            int overwriteIntermediateChannelOffset,
            FlatHashStrategyCompiler hashStrategyCompiler,
            AggregationMetrics aggregationMetrics)
    {
        ImmutableList.Builder<Integer> groupByPartialChannels = ImmutableList.builderWithExpectedSize(groupByTypes.size());
        for (int i = 0; i < groupByTypes.size(); i++) {
            groupByPartialChannels.add(i);
        }

        this.aggregatorFactories = aggregatorFactories;
        this.step = AggregationNode.Step.partialInput(step);
        this.expectedGroups = expectedGroups;
        this.groupByPartialChannels = groupByPartialChannels.build();
        this.hashChannel = hashChannel.isPresent() ? Optional.of(groupByTypes.size()) : hashChannel;
        this.operatorContext = operatorContext;
        this.sortedPages = sortedPages;
        this.groupByTypes = groupByTypes;
        this.memoryContext = aggregatedMemoryContext.newLocalMemoryContext(MergingHashAggregationBuilder.class.getSimpleName());
        this.memoryLimitForMerge = memoryLimitForMerge;
        this.overwriteIntermediateChannelOffset = overwriteIntermediateChannelOffset;
        this.hashStrategyCompiler = hashStrategyCompiler;
        this.aggregationMetrics = requireNonNull(aggregationMetrics, "aggregationMetrics is null");

        rebuildHashAggregationBuilder();
    }

    public WorkProcessor<Page> buildResult()
    {
        return sortedPages.flatTransform(new Transformation<>()
        {
            private boolean reset = true;
            private long memorySize;

            @Override
            public TransformationState<WorkProcessor<Page>> process(Page inputPage)
            {
                if (reset) {
                    rebuildHashAggregationBuilder();
                    memorySize = 0;
                    reset = false;
                }

                boolean inputFinished = inputPage == null;
                if (inputFinished && memorySize == 0) {
                    // no more pages and aggregation builder is empty
                    return TransformationState.finished();
                }

                if (!inputFinished) {
                    boolean done = hashAggregationBuilder.processPage(inputPage).process();
                    // TODO: this class does not yield wrt memory limit; enable it
                    verify(done);
                    memorySize = hashAggregationBuilder.getSizeInMemory();
                    memoryContext.setBytes(memorySize);

                    if (!shouldProduceOutput(memorySize)) {
                        return TransformationState.needsMoreData();
                    }
                }

                reset = true;
                // we can produce output after every input page, because input pages do not have
                // hash values that span multiple pages (guaranteed by MergeHashSort)
                return TransformationState.ofResult(hashAggregationBuilder.buildResult(), !inputFinished);
            }
        });
    }

    @Override
    public void close()
    {
        hashAggregationBuilder.close();
    }

    private boolean shouldProduceOutput(long memorySize)
    {
        return (memoryLimitForMerge > 0 && memorySize > memoryLimitForMerge);
    }

    private void rebuildHashAggregationBuilder()
    {
        this.hashAggregationBuilder = new InMemoryHashAggregationBuilder(
                aggregatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                groupByPartialChannels,
                hashChannel,
                false, // spillable
                operatorContext,
                Optional.of(DataSize.succinctBytes(0)),
                Optional.of(overwriteIntermediateChannelOffset),
                hashStrategyCompiler,
                // TODO: merging should also yield on memory reservations
                () -> true,
                aggregationMetrics);
    }
}
