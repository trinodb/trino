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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.aggregation.TypedSet;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.sql.planner.DynamicFilterSourceConsumer;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionComparison;

import javax.annotation.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.operator.aggregation.TypedSet.createUnboundedEqualityTypedSet;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * This operator acts as a simple "pass-through" pipe, while saving its input pages.
 * The collected pages' value are used for creating a run-time filtering constraint (for probe-side table scan in an inner join).
 * We record all values for the run-time filter only for small build-side pages (which should be the case when using "broadcast" join).
 * For large inputs on build side, we can optionally record the min and max values per channel for orderable types (except Double and Real).
 */
public class DynamicFilterSourceOperator
        implements Operator
{
    private static final int EXPECTED_BLOCK_BUILDER_SIZE = 8;

    public static class Channel
    {
        private final DynamicFilterId filterId;
        private final Type type;
        private final int index;

        public Channel(DynamicFilterId filterId, Type type, int index)
        {
            this.filterId = filterId;
            this.type = type;
            this.index = index;
        }
    }

    public static class DynamicFilterSourceOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final DynamicFilterSourceConsumer dynamicPredicateConsumer;
        private final List<Channel> channels;
        private final int maxDistinctValues;
        private final DataSize maxFilterSize;
        private final int minMaxCollectionLimit;
        private final BlockTypeOperators blockTypeOperators;

        private boolean closed;
        private int createdOperatorsCount;

        public DynamicFilterSourceOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                DynamicFilterSourceConsumer dynamicPredicateConsumer,
                List<Channel> channels,
                int maxDistinctValues,
                DataSize maxFilterSize,
                int minMaxCollectionLimit,
                BlockTypeOperators blockTypeOperators)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.dynamicPredicateConsumer = requireNonNull(dynamicPredicateConsumer, "dynamicPredicateConsumer is null");
            this.channels = requireNonNull(channels, "channels is null");
            verify(channels.stream().map(channel -> channel.filterId).collect(toSet()).size() == channels.size(),
                    "duplicate dynamic filters are not allowed");
            verify(channels.stream().map(channel -> channel.index).collect(toSet()).size() == channels.size(),
                    "duplicate channel indices are not allowed");
            this.maxDistinctValues = maxDistinctValues;
            this.maxFilterSize = maxFilterSize;
            this.minMaxCollectionLimit = minMaxCollectionLimit;
            this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            createdOperatorsCount++;
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, DynamicFilterSourceOperator.class.getSimpleName());
            if (!dynamicPredicateConsumer.isDomainCollectionComplete()) {
                return new DynamicFilterSourceOperator(
                        operatorContext,
                        dynamicPredicateConsumer,
                        channels,
                        planNodeId,
                        maxDistinctValues,
                        maxFilterSize,
                        minMaxCollectionLimit,
                        blockTypeOperators);
            }
            // Return a pass-through operator which adds little overhead
            return new PassthroughDynamicFilterSourceOperator(operatorContext);
        }

        @Override
        public void noMoreOperators()
        {
            checkState(!closed, "Factory is already closed");
            closed = true;
            dynamicPredicateConsumer.setPartitionCount(createdOperatorsCount);
        }

        @Override
        public OperatorFactory duplicate()
        {
            // A duplicate factory may be required for DynamicFilterSourceOperatorFactory in fault tolerant execution mode
            // by LocalExecutionPlanner#addLookupOuterDrivers to add a new driver to output the unmatched rows in an outer join.
            // Since the logic for tracking partitions count for dynamicPredicateConsumer requires there to be only one DynamicFilterSourceOperatorFactory,
            // we turn off dynamic filtering and provide a duplicate factory which will act as pass through to allow the query to succeed.
            dynamicPredicateConsumer.addPartition(TupleDomain.all());
            return new DynamicFilterSourceOperatorFactory(
                    operatorId,
                    planNodeId,
                    new DynamicFilterSourceConsumer() {
                        @Override
                        public void addPartition(TupleDomain<DynamicFilterId> tupleDomain)
                        {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public void setPartitionCount(int partitionCount) {}

                        @Override
                        public boolean isDomainCollectionComplete()
                        {
                            return true;
                        }
                    },
                    channels,
                    maxDistinctValues,
                    maxFilterSize,
                    minMaxCollectionLimit,
                    blockTypeOperators);
        }
    }

    private final OperatorContext context;
    private final LocalMemoryContext userMemoryContext;
    private boolean finished;
    private Page current;
    private final DynamicFilterSourceConsumer dynamicPredicateConsumer;

    private final List<Channel> channels;
    private final ChannelFilter[] channelFilters;

    private int minMaxCollectionLimit;
    private boolean isDomainCollectionComplete;

    private DynamicFilterSourceOperator(
            OperatorContext context,
            DynamicFilterSourceConsumer dynamicPredicateConsumer,
            List<Channel> channels,
            PlanNodeId planNodeId,
            int maxDistinctValues,
            DataSize maxFilterSize,
            int minMaxCollectionLimit,
            BlockTypeOperators blockTypeOperators)
    {
        this.context = requireNonNull(context, "context is null");
        this.userMemoryContext = context.localUserMemoryContext();
        this.minMaxCollectionLimit = minMaxCollectionLimit;
        this.dynamicPredicateConsumer = requireNonNull(dynamicPredicateConsumer, "dynamicPredicateConsumer is null");
        this.channels = requireNonNull(channels, "channels is null");
        this.channelFilters = new ChannelFilter[channels.size()];

        for (int channelIndex = 0; channelIndex < channels.size(); ++channelIndex) {
            channelFilters[channelIndex] = new ChannelFilter(
                    blockTypeOperators,
                    minMaxCollectionLimit > 0,
                    planNodeId,
                    maxDistinctValues,
                    maxFilterSize.toBytes(),
                    this::finishDomainCollectionIfNecessary,
                    channels.get(channelIndex));
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return context;
    }

    @Override
    public boolean needsInput()
    {
        return current == null && !finished;
    }

    @Override
    public void addInput(Page page)
    {
        verify(!finished, "DynamicFilterSourceOperator: addInput() may not be called after finish()");
        current = page;

        if (isDomainCollectionComplete) {
            return;
        }

        if (minMaxCollectionLimit >= 0) {
            minMaxCollectionLimit -= page.getPositionCount();
            if (minMaxCollectionLimit < 0) {
                for (int channelIndex = 0; channelIndex < channels.size(); channelIndex++) {
                    channelFilters[channelIndex].disableMinMax();
                }
                finishDomainCollectionIfNecessary();
            }
        }

        // Collect only the columns which are relevant for the JOIN.
        long filterSizeInBytes = 0;
        for (int channelIndex = 0; channelIndex < channels.size(); ++channelIndex) {
            Block block = page.getBlock(channels.get(channelIndex).index);
            filterSizeInBytes += channelFilters[channelIndex].process(block);
        }
        userMemoryContext.setBytes(filterSizeInBytes);
    }

    @Override
    public Page getOutput()
    {
        Page result = current;
        current = null;
        return result;
    }

    @Override
    public void finish()
    {
        if (finished) {
            // NOTE: finish() may be called multiple times (see comment at Driver::processInternal).
            return;
        }
        finished = true;
        if (isDomainCollectionComplete) {
            // dynamicPredicateConsumer was already notified with 'all'
            return;
        }

        ImmutableMap.Builder<DynamicFilterId, Domain> domainsBuilder = ImmutableMap.builder();
        for (int channelIndex = 0; channelIndex < channels.size(); ++channelIndex) {
            DynamicFilterId filterId = channels.get(channelIndex).filterId;
            domainsBuilder.put(filterId, channelFilters[channelIndex].getDomain());
        }
        dynamicPredicateConsumer.addPartition(TupleDomain.withColumnDomains(domainsBuilder.buildOrThrow()));
        userMemoryContext.setBytes(0);
    }

    @Override
    public boolean isFinished()
    {
        return current == null && finished;
    }

    @Override
    public void close()
            throws Exception
    {
        userMemoryContext.setBytes(0);
    }

    private void finishDomainCollectionIfNecessary()
    {
        if (!isDomainCollectionComplete && stream(channelFilters).allMatch(channel -> channel.state == ChannelState.NONE)) {
            // allow all probe-side values to be read.
            dynamicPredicateConsumer.addPartition(TupleDomain.all());
            isDomainCollectionComplete = true;
        }
    }

    private static boolean isMinMaxPossible(Type type)
    {
        // Skipping DOUBLE and REAL in collectMinMaxValues to avoid dealing with NaN values
        return type.isOrderable() && type != DOUBLE && type != REAL;
    }

    private static class PassthroughDynamicFilterSourceOperator
            implements Operator
    {
        private final OperatorContext operatorContext;
        private boolean finished;
        private Page current;

        private PassthroughDynamicFilterSourceOperator(OperatorContext operatorContext)
        {
            this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public boolean needsInput()
        {
            return current == null && !finished;
        }

        @Override
        public void addInput(Page page)
        {
            verify(!finished, "DynamicFilterSourceOperator: addInput() may not be called after finish()");
            current = page;
        }

        @Override
        public Page getOutput()
        {
            Page result = current;
            current = null;
            return result;
        }

        @Override
        public void finish()
        {
            if (finished) {
                // NOTE: finish() may be called multiple times (see comment at Driver::processInternal).
                return;
            }
            finished = true;
        }

        @Override
        public boolean isFinished()
        {
            return current == null && finished;
        }
    }

    private static class ChannelFilter
    {
        private final Type type;
        private final int maxDistinctValues;
        private final long maxFilterSizeInBytes;
        private final Runnable notifyStateChange;

        private ChannelState state;
        private boolean collectMinMax;
        // May be dropped if the predicate becomes too large.
        @Nullable
        private BlockBuilder blockBuilder;
        @Nullable
        private TypedSet valueSet;
        @Nullable
        private Block minValues;
        @Nullable
        private Block maxValues;
        @Nullable
        private BlockPositionComparison minMaxComparison;

        private ChannelFilter(
                BlockTypeOperators blockTypeOperators,
                boolean minMaxEnabled,
                PlanNodeId planNodeId,
                int maxDistinctValues,
                long maxFilterSizeInBytes,
                Runnable notifyStateChange,
                Channel channel)
        {
            this.maxDistinctValues = maxDistinctValues;
            this.maxFilterSizeInBytes = maxFilterSizeInBytes;
            this.notifyStateChange = requireNonNull(notifyStateChange, "notifyStateChange is null");
            type = channel.type;
            state = ChannelState.SET;
            collectMinMax = minMaxEnabled && isMinMaxPossible(type);
            if (collectMinMax) {
                minMaxComparison = blockTypeOperators.getComparisonUnorderedLastOperator(type);
            }
            blockBuilder = type.createBlockBuilder(null, EXPECTED_BLOCK_BUILDER_SIZE);
            valueSet = createUnboundedEqualityTypedSet(
                    type,
                    blockTypeOperators.getEqualOperator(type),
                    blockTypeOperators.getHashCodeOperator(type),
                    blockBuilder,
                    EXPECTED_BLOCK_BUILDER_SIZE,
                    format("DynamicFilterSourceOperator_%s_%d", planNodeId, channel.index));
        }

        private long process(Block block)
        {
            long retainedSizeInBytes = 0;
            switch (state) {
                case SET:
                    for (int position = 0; position < block.getPositionCount(); ++position) {
                        valueSet.add(block, position);
                    }
                    if (valueSet.size() > maxDistinctValues || valueSet.getRetainedSizeInBytes() > maxFilterSizeInBytes) {
                        if (collectMinMax) {
                            state = ChannelState.MIN_MAX;
                            updateMinMaxValues(blockBuilder.build(), minMaxComparison);
                        }
                        else {
                            state = ChannelState.NONE;
                            notifyStateChange.run();
                        }
                        valueSet = null;
                        blockBuilder = null;
                    }
                    else {
                        retainedSizeInBytes = valueSet.getRetainedSizeInBytes();
                    }
                    break;
                case MIN_MAX:
                    updateMinMaxValues(block, minMaxComparison);
                    break;
                case NONE:
                    break;
            }
            return retainedSizeInBytes;
        }

        private Domain getDomain()
        {
            return switch (state) {
                case SET -> {
                    Block block = blockBuilder.build();
                    ImmutableList.Builder<Object> values = ImmutableList.builder();
                    for (int position = 0; position < block.getPositionCount(); ++position) {
                        Object value = readNativeValue(type, block, position);
                        if (value != null) {
                            // join doesn't match rows with NaN values.
                            if (!isFloatingPointNaN(type, value)) {
                                values.add(value);
                            }
                        }
                    }
                    // Drop references to collected values
                    valueSet = null;
                    blockBuilder = null;
                    // Inner and right join doesn't match rows with null key column values.
                    yield Domain.create(ValueSet.copyOf(type, values.build()), false);
                }
                case MIN_MAX -> {
                    if (minValues == null) {
                        // all values were null
                        yield Domain.none(type);
                    }
                    Object min = blockToNativeValue(type, minValues);
                    Object max = blockToNativeValue(type, maxValues);
                    // Drop references to collected values
                    minValues = null;
                    maxValues = null;
                    yield Domain.create(ValueSet.ofRanges(range(type, min, true, max, true)), false);
                }
                case NONE -> Domain.all(type);
            };
        }

        private void disableMinMax()
        {
            collectMinMax = false;
            if (state == ChannelState.MIN_MAX) {
                state = ChannelState.NONE;
            }
            // Drop references to collected values.
            minValues = null;
            maxValues = null;
        }

        private void updateMinMaxValues(Block block, BlockPositionComparison comparison)
        {
            int minValuePosition = -1;
            int maxValuePosition = -1;

            for (int position = 0; position < block.getPositionCount(); ++position) {
                if (block.isNull(position)) {
                    continue;
                }
                if (minValuePosition == -1) {
                    // First non-null value
                    minValuePosition = position;
                    maxValuePosition = position;
                    continue;
                }
                if (comparison.compare(block, position, block, minValuePosition) < 0) {
                    minValuePosition = position;
                }
                else if (comparison.compare(block, position, block, maxValuePosition) > 0) {
                    maxValuePosition = position;
                }
            }

            if (minValuePosition == -1) {
                // all block values are nulls
                return;
            }
            if (minValues == null) {
                // First Page with non-null value for this block
                minValues = block.getSingleValueBlock(minValuePosition);
                maxValues = block.getSingleValueBlock(maxValuePosition);
                return;
            }
            // Compare with min/max values from previous Pages
            Block currentMin = minValues;
            Block currentMax = maxValues;

            if (comparison.compare(block, minValuePosition, currentMin, 0) < 0) {
                minValues = block.getSingleValueBlock(minValuePosition);
            }
            if (comparison.compare(block, maxValuePosition, currentMax, 0) > 0) {
                maxValues = block.getSingleValueBlock(maxValuePosition);
            }
        }
    }

    private enum ChannelState
    {
        SET,
        MIN_MAX,
        NONE,
    }
}
