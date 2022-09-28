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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.operator.BasicWorkProcessorOperatorAdapter.BasicAdapterWorkProcessorOperatorFactory;
import io.trino.operator.SetBuilderOperator.SetSupplier;
import io.trino.operator.WorkProcessor.TransformationState;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.checkSuccess;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.operator.BasicWorkProcessorOperatorAdapter.createAdapterOperatorFactory;
import static io.trino.operator.WorkProcessor.TransformationState.blocked;
import static io.trino.operator.WorkProcessor.TransformationState.finished;
import static io.trino.operator.WorkProcessor.TransformationState.ofResult;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public class HashSemiJoinOperator
        implements WorkProcessorOperator
{
    public static OperatorFactory createOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            SetSupplier setSupplier,
            List<? extends Type> probeTypes,
            int probeJoinChannel,
            Optional<Integer> probeJoinHashChannel)
    {
        return createAdapterOperatorFactory(new Factory(operatorId, planNodeId, setSupplier, probeTypes, probeJoinChannel, probeJoinHashChannel));
    }

    private static class Factory
            implements BasicAdapterWorkProcessorOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final SetSupplier setSupplier;
        private final List<Type> probeTypes;
        private final int probeJoinChannel;
        private final Optional<Integer> probeJoinHashChannel;
        private boolean closed;

        private Factory(int operatorId, PlanNodeId planNodeId, SetSupplier setSupplier, List<? extends Type> probeTypes, int probeJoinChannel, Optional<Integer> probeJoinHashChannel)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.setSupplier = setSupplier;
            this.probeTypes = ImmutableList.copyOf(probeTypes);
            checkArgument(probeJoinChannel >= 0, "probeJoinChannel is negative");
            this.probeJoinChannel = probeJoinChannel;
            this.probeJoinHashChannel = probeJoinHashChannel;
        }

        @Override
        public WorkProcessorOperator create(ProcessorContext processorContext, WorkProcessor<Page> sourcePages)
        {
            checkState(!closed, "Factory is already closed");
            return new HashSemiJoinOperator(sourcePages, setSupplier, probeJoinChannel, probeJoinHashChannel, processorContext.getMemoryTrackingContext());
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
            return HashSemiJoinOperator.class.getSimpleName();
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public Factory duplicate()
        {
            return new Factory(operatorId, planNodeId, setSupplier, probeTypes, probeJoinChannel, probeJoinHashChannel);
        }
    }

    private final WorkProcessor<Page> pages;

    private HashSemiJoinOperator(
            WorkProcessor<Page> sourcePages,
            SetSupplier channelSetFuture,
            int probeJoinChannel,
            Optional<Integer> probeHashChannel,
            MemoryTrackingContext memoryTrackingContext)
    {
        pages = sourcePages
                .transform(new SemiJoinPages(
                        channelSetFuture,
                        probeJoinChannel,
                        probeHashChannel,
                        memoryTrackingContext.aggregateUserMemoryContext()));
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    private static class SemiJoinPages
            implements WorkProcessor.Transformation<Page, Page>
    {
        private static final int NO_PRECOMPUTED_HASH_CHANNEL = -1;

        private final int probeJoinChannel;
        private final int probeHashChannel; // when >= 0, this is the precomputed hash channel
        private final ListenableFuture<ChannelSet> channelSetFuture;
        private final LocalMemoryContext localMemoryContext;

        @Nullable
        private ChannelSet channelSet;

        public SemiJoinPages(SetSupplier channelSetFuture, int probeJoinChannel, Optional<Integer> probeHashChannel, AggregatedMemoryContext aggregatedMemoryContext)
        {
            checkArgument(probeJoinChannel >= 0, "probeJoinChannel is negative");

            this.channelSetFuture = channelSetFuture.getChannelSet();
            this.probeJoinChannel = probeJoinChannel;
            this.probeHashChannel = probeHashChannel.orElse(NO_PRECOMPUTED_HASH_CHANNEL);
            this.localMemoryContext = aggregatedMemoryContext.newLocalMemoryContext(SemiJoinPages.class.getSimpleName());
        }

        @Override
        public TransformationState<Page> process(Page inputPage)
        {
            if (inputPage == null) {
                return finished();
            }

            if (channelSet == null) {
                if (!channelSetFuture.isDone()) {
                    // This will materialize page but it shouldn't matter for the first page
                    localMemoryContext.setBytes(inputPage.getSizeInBytes());
                    return blocked(asVoid(channelSetFuture));
                }
                checkSuccess(channelSetFuture, "ChannelSet building failed");
                channelSet = getFutureValue(channelSetFuture);
                localMemoryContext.setBytes(0);
            }
            // use an effectively-final local variable instead of the non-final instance field inside of the loop
            ChannelSet channelSet = requireNonNull(this.channelSet, "channelSet is null");

            // create the block builder for the new boolean column
            // we know the exact size required for the block
            BlockBuilder blockBuilder = BOOLEAN.createFixedSizeBlockBuilder(inputPage.getPositionCount());

            Page probeJoinPage = inputPage.getLoadedPage(probeJoinChannel);
            Block probeJoinNulls = probeJoinPage.getBlock(0).mayHaveNull() ? probeJoinPage.getBlock(0) : null;
            Block hashBlock = probeHashChannel >= 0 ? inputPage.getBlock(probeHashChannel) : null;

            // update hashing strategy to use probe cursor
            for (int position = 0; position < inputPage.getPositionCount(); position++) {
                if (probeJoinNulls != null && probeJoinNulls.isNull(position)) {
                    if (channelSet.isEmpty()) {
                        BOOLEAN.writeBoolean(blockBuilder, false);
                    }
                    else {
                        blockBuilder.appendNull();
                    }
                }
                else {
                    boolean contains;
                    if (hashBlock != null) {
                        long rawHash = BIGINT.getLong(hashBlock, position);
                        contains = channelSet.contains(position, probeJoinPage, rawHash);
                    }
                    else {
                        contains = channelSet.contains(position, probeJoinPage);
                    }
                    if (!contains && channelSet.containsNull()) {
                        blockBuilder.appendNull();
                    }
                    else {
                        BOOLEAN.writeBoolean(blockBuilder, contains);
                    }
                }
            }
            // add the new boolean column to the page
            return ofResult(inputPage.appendColumn(blockBuilder.build()));
        }
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }
}
