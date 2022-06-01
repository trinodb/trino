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
package io.trino.operator.aggregation.partial;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.CompletedWork;
import io.trino.operator.GroupByIdBlock;
import io.trino.operator.HashCollisionsCounter;
import io.trino.operator.Work;
import io.trino.operator.WorkProcessor;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.GroupedAggregator;
import io.trino.operator.aggregation.builder.HashAggregationBuilder;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LongArrayBlock;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * {@link HashAggregationBuilder} that does not aggregate input rows at all.
 * It passes the input pages, augmented with initial accumulator state to the output.
 * It can only be used at the partial aggregation step as it relies on rows be aggregated at the final step.
 */
public class SkipAggregationBuilder
        implements HashAggregationBuilder
{
    private final LocalMemoryContext memoryContext;
    private final List<GroupedAggregator> groupedAggregators;
    @Nullable
    private Page currentPage;
    private final int[] hashChannels;

    public SkipAggregationBuilder(
            List<Integer> groupByChannels,
            Optional<Integer> inputHashChannel,
            List<AggregatorFactory> aggregatorFactories,
            LocalMemoryContext memoryContext)
    {
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.groupedAggregators = requireNonNull(aggregatorFactories, "aggregatorFactories is null")
                .stream()
                .map(AggregatorFactory::createGroupedAggregator)
                .collect(toImmutableList());
        this.hashChannels = new int[groupByChannels.size() + (inputHashChannel.isPresent() ? 1 : 0)];
        for (int i = 0; i < groupByChannels.size(); i++) {
            hashChannels[i] = groupByChannels.get(i);
        }
        inputHashChannel.ifPresent(channelIndex -> hashChannels[groupByChannels.size()] = channelIndex);
    }

    @Override
    public Work<?> processPage(Page page)
    {
        checkArgument(currentPage == null);
        currentPage = page;
        return new CompletedWork<>();
    }

    @Override
    public WorkProcessor<Page> buildResult()
    {
        if (currentPage == null) {
            return WorkProcessor.of();
        }

        Page result = buildOutputPage(currentPage);
        currentPage = null;
        return WorkProcessor.of(result);
    }

    @Override
    public boolean isFull()
    {
        return currentPage != null;
    }

    @Override
    public void updateMemory()
    {
        if (currentPage != null) {
            memoryContext.setBytes(currentPage.getSizeInBytes());
        }
    }

    @Override
    public void recordHashCollisions(HashCollisionsCounter hashCollisionsCounter)
    {
        // no op
    }

    @Override
    public void close()
    {
    }

    @Override
    public ListenableFuture<Void> startMemoryRevoke()
    {
        throw new UnsupportedOperationException("startMemoryRevoke not supported for SkipAggregationBuilder");
    }

    @Override
    public void finishMemoryRevoke()
    {
        throw new UnsupportedOperationException("finishMemoryRevoke not supported for SkipAggregationBuilder");
    }

    private Page buildOutputPage(Page page)
    {
        populateInitialAccumulatorState(page);

        BlockBuilder[] outputBuilders = serializeAccumulatorState(page.getPositionCount());

        return constructOutputPage(page, outputBuilders);
    }

    private void populateInitialAccumulatorState(Page page)
    {
        GroupByIdBlock groupByIdBlock = getGroupByIdBlock(page.getPositionCount());
        for (GroupedAggregator groupedAggregator : groupedAggregators) {
            groupedAggregator.processPage(groupByIdBlock, page);
        }
    }

    private GroupByIdBlock getGroupByIdBlock(int positionCount)
    {
        return new GroupByIdBlock(
                positionCount,
                new LongArrayBlock(positionCount, Optional.empty(), consecutive(positionCount)));
    }

    private BlockBuilder[] serializeAccumulatorState(int positionCount)
    {
        BlockBuilder[] outputBuilders = new BlockBuilder[groupedAggregators.size()];
        for (int i = 0; i < outputBuilders.length; i++) {
            outputBuilders[i] = groupedAggregators.get(i).getType().createBlockBuilder(null, positionCount);
        }

        for (int position = 0; position < positionCount; position++) {
            for (int i = 0; i < groupedAggregators.size(); i++) {
                GroupedAggregator groupedAggregator = groupedAggregators.get(i);
                BlockBuilder output = outputBuilders[i];
                groupedAggregator.evaluate(position, output);
            }
        }
        return outputBuilders;
    }

    private Page constructOutputPage(Page page, BlockBuilder[] outputBuilders)
    {
        Block[] outputBlocks = new Block[hashChannels.length + outputBuilders.length];
        for (int i = 0; i < hashChannels.length; i++) {
            outputBlocks[i] = page.getBlock(hashChannels[i]);
        }
        for (int i = 0; i < outputBuilders.length; i++) {
            outputBlocks[hashChannels.length + i] = outputBuilders[i].build();
        }
        return new Page(page.getPositionCount(), outputBlocks);
    }

    private static long[] consecutive(int positionCount)
    {
        long[] longs = new long[positionCount];
        for (int i = 0; i < positionCount; i++) {
            longs[i] = i;
        }
        return longs;
    }
}
