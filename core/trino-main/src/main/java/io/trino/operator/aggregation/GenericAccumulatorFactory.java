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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import io.trino.Session;
import io.trino.array.IntBigArray;
import io.trino.array.ObjectBigArray;
import io.trino.operator.GroupByIdBlock;
import io.trino.operator.MarkDistinctHash;
import io.trino.operator.PagesIndex;
import io.trino.operator.UpdateMemory;
import io.trino.operator.Work;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.Page.wrapBlocksWithoutCopy;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.Long.max;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class GenericAccumulatorFactory
        implements AccumulatorFactory
{
    private final List<AccumulatorStateDescriptor> stateDescriptors;
    private final Constructor<? extends Accumulator> accumulatorConstructor;
    private final Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor;
    private final List<LambdaProvider> lambdaProviders;
    private final Optional<Integer> maskChannel;
    private final List<Integer> inputChannels;
    private final List<Type> sourceTypes;
    private final List<Integer> orderByChannels;
    private final List<SortOrder> orderings;
    private final boolean accumulatorHasRemoveInput;

    @Nullable
    private final JoinCompiler joinCompiler;
    @Nullable
    private final BlockTypeOperators blockTypeOperators;

    @Nullable
    private final Session session;
    private final boolean distinct;
    private final boolean spillEnabled;
    private final PagesIndex.Factory pagesIndexFactory;

    public GenericAccumulatorFactory(
            List<AccumulatorStateDescriptor> stateDescriptors,
            Constructor<? extends Accumulator> accumulatorConstructor,
            boolean accumulatorHasRemoveInput,
            Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor,
            List<LambdaProvider> lambdaProviders,
            List<Integer> inputChannels,
            Optional<Integer> maskChannel,
            List<Type> sourceTypes,
            List<Integer> orderByChannels,
            List<SortOrder> orderings,
            PagesIndex.Factory pagesIndexFactory,
            @Nullable JoinCompiler joinCompiler,
            @Nullable BlockTypeOperators blockTypeOperators,
            @Nullable Session session,
            boolean distinct,
            boolean spillEnabled)
    {
        this.stateDescriptors = requireNonNull(stateDescriptors, "stateDescriptors is null");
        this.accumulatorConstructor = requireNonNull(accumulatorConstructor, "accumulatorConstructor is null");
        this.accumulatorHasRemoveInput = accumulatorHasRemoveInput;
        this.groupedAccumulatorConstructor = requireNonNull(groupedAccumulatorConstructor, "groupedAccumulatorConstructor is null");
        this.lambdaProviders = ImmutableList.copyOf(requireNonNull(lambdaProviders, "lambdaProviders is null"));
        this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
        this.inputChannels = ImmutableList.copyOf(requireNonNull(inputChannels, "inputChannels is null"));
        this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
        this.orderByChannels = ImmutableList.copyOf(requireNonNull(orderByChannels, "orderByChannels is null"));
        this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
        checkArgument(orderByChannels.isEmpty() || !isNull(pagesIndexFactory), "No pagesIndexFactory to process ordering");
        this.pagesIndexFactory = pagesIndexFactory;

        checkArgument(!distinct || !isNull(session) && !isNull(joinCompiler) && !isNull(blockTypeOperators), "joinCompiler, blockTypeOperators, and session needed when distinct is true");
        this.joinCompiler = joinCompiler;
        this.blockTypeOperators = blockTypeOperators;
        this.session = session;
        this.distinct = distinct;
        this.spillEnabled = spillEnabled;
    }

    @Override
    public List<Integer> getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public boolean hasRemoveInput()
    {
        return accumulatorHasRemoveInput;
    }

    @Override
    public Accumulator createAccumulator()
    {
        Accumulator accumulator;

        if (distinct) {
            // channel 0 will contain the distinct mask
            accumulator = instantiateAccumulator(
                    inputChannels.stream()
                            .map(value -> value + 1)
                            .collect(Collectors.toList()),
                    Optional.of(0));

            List<Type> argumentTypes = inputChannels.stream()
                    .map(sourceTypes::get)
                    .collect(Collectors.toList());

            accumulator = new DistinctingAccumulator(accumulator, argumentTypes, inputChannels, maskChannel, session, joinCompiler, blockTypeOperators);
        }
        else {
            accumulator = instantiateAccumulator(inputChannels, maskChannel);
        }

        if (orderByChannels.isEmpty()) {
            return accumulator;
        }

        return new OrderingAccumulator(accumulator, sourceTypes, orderByChannels, orderings, pagesIndexFactory);
    }

    @Override
    public Accumulator createIntermediateAccumulator()
    {
        try {
            return accumulatorConstructor.newInstance(stateDescriptors, ImmutableList.of(), Optional.empty(), lambdaProviders);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public GroupedAccumulator createGroupedAccumulator()
    {
        GroupedAccumulator accumulator;

        if (distinct) {
            // channel 0 will contain the distinct mask
            accumulator = instantiateGroupedAccumulator(
                    inputChannels.stream()
                            .map(value -> value + 1)
                            .collect(Collectors.toList()),
                    Optional.of(0));

            List<Type> argumentTypes = new ArrayList<>();
            for (int input : inputChannels) {
                argumentTypes.add(sourceTypes.get(input));
            }

            Optional<SpillableStateHolder> spillableStateHolder = Optional.empty();
            if (spillEnabled && orderByChannels.isEmpty()) {
                spillableStateHolder = Optional.of(getSpillableStateHolder());
            }
            accumulator = new DistinctingGroupedAccumulator(accumulator, argumentTypes, inputChannels, maskChannel, session, joinCompiler, blockTypeOperators, spillableStateHolder);
        }
        else {
            accumulator = instantiateGroupedAccumulator(inputChannels, maskChannel);
        }

        if (!orderByChannels.isEmpty()) {
            Optional<SpillableStateHolder> spillableStateHolder = Optional.empty();
            if (spillEnabled) {
                spillableStateHolder = Optional.of(getSpillableStateHolder());
            }
            accumulator = new OrderingGroupedAccumulator(accumulator, sourceTypes, orderByChannels, orderings, pagesIndexFactory, spillableStateHolder);
        }

        return accumulator;
    }

    private SpillableStateHolder getSpillableStateHolder()
    {
        ImmutableSet.Builder<Integer> accumulatorInputChannels = ImmutableSet.builder();
        accumulatorInputChannels.addAll(inputChannels);
        maskChannel.ifPresent(accumulatorInputChannels::add);
        accumulatorInputChannels.addAll(orderByChannels);
        return new SpillableStateHolder(sourceTypes, accumulatorInputChannels.build().asList());
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAccumulator()
    {
        if (!orderByChannels.isEmpty() || distinct) {
            // This is called only when unspill. This is needed because Distincting/Ordering GroupedAccumulator needs to be returned.
            // No matter which underlying accumulator working on intermediate values is used in SpillableFinalOnlyGroupedAccumulator under Distincting/Ordering GroupedAccumulator.
            // Because it will be rebuilt during processing MergingHashAggregationBuilder for unspilled data.
            return createGroupedAccumulator();
        }

        try {
            return groupedAccumulatorConstructor.newInstance(stateDescriptors, ImmutableList.of(), Optional.empty(), lambdaProviders);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private Accumulator instantiateAccumulator(List<Integer> inputs, Optional<Integer> mask)
    {
        try {
            return accumulatorConstructor.newInstance(stateDescriptors, inputs, mask, lambdaProviders);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private GroupedAccumulator instantiateGroupedAccumulator(List<Integer> inputs, Optional<Integer> mask)
    {
        try {
            return groupedAccumulatorConstructor.newInstance(stateDescriptors, inputs, mask, lambdaProviders);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private static class DistinctingAccumulator
            implements Accumulator
    {
        private final Accumulator accumulator;
        private final MarkDistinctHash hash;
        private final int maskChannel;

        private DistinctingAccumulator(
                Accumulator accumulator,
                List<Type> inputTypes,
                List<Integer> inputs,
                Optional<Integer> maskChannel,
                Session session,
                JoinCompiler joinCompiler,
                BlockTypeOperators blockTypeOperators)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.maskChannel = requireNonNull(maskChannel, "maskChannel is null").orElse(-1);

            hash = new MarkDistinctHash(session, inputTypes, Ints.toArray(inputs), Optional.empty(), joinCompiler, blockTypeOperators, UpdateMemory.NOOP);
        }

        @Override
        public long getEstimatedSize()
        {
            return hash.getEstimatedSize() + accumulator.getEstimatedSize();
        }

        @Override
        public Type getFinalType()
        {
            return accumulator.getFinalType();
        }

        @Override
        public Type getIntermediateType()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Accumulator copy()
        {
            return accumulator.copy();
        }

        @Override
        public void addInput(Page page)
        {
            // 1. filter out positions based on mask, if present
            Page filtered;
            if (maskChannel >= 0) {
                filtered = filter(page, page.getBlock(maskChannel));
            }
            else {
                filtered = page;
            }

            if (filtered.getPositionCount() == 0) {
                return;
            }

            // 2. compute a distinct mask
            Work<Block> work = hash.markDistinctRows(filtered);
            checkState(work.process());
            Block distinctMask = work.getResult();

            // 3. feed a Page with a new mask to the underlying aggregation
            accumulator.addInput(filtered.prependColumn(distinctMask));
        }

        @Override
        public void addInput(WindowIndex index, List<Integer> channels, int startPosition, int endPosition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeInput(WindowIndex index, List<Integer> channels, int startPosition, int endPosition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addIntermediate(Block block)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateIntermediate(BlockBuilder blockBuilder)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateFinal(BlockBuilder blockBuilder)
        {
            accumulator.evaluateFinal(blockBuilder);
        }
    }

    private static Page filter(Page page, Block mask)
    {
        int positions = mask.getPositionCount();
        if (positions > 0 && mask instanceof RunLengthEncodedBlock) {
            // must have at least 1 position to be able to check the value at position 0
            if (!mask.isNull(0) && BOOLEAN.getBoolean(mask, 0)) {
                return page;
            }
            else {
                return page.getPositions(new int[0], 0, 0);
            }
        }
        boolean mayHaveNull = mask.mayHaveNull();
        int[] ids = new int[positions];
        int next = 0;
        for (int i = 0; i < ids.length; ++i) {
            boolean isNull = mayHaveNull && mask.isNull(i);
            if (!isNull && BOOLEAN.getBoolean(mask, i)) {
                ids[next++] = i;
            }
        }

        if (next == ids.length) {
            return page; // no rows were eliminated by the filter
        }
        return page.getPositions(ids, 0, next);
    }

    private static class DistinctingGroupedAccumulator
            implements GroupedAccumulator
    {
        private final GroupedAccumulator accumulator;
        private final MarkDistinctHash hash;
        private final int maskChannel;
        private final Optional<SpillableStateHolder> spillableStateHolder;

        private DistinctingGroupedAccumulator(
                GroupedAccumulator accumulator,
                List<Type> inputTypes,
                List<Integer> inputChannels,
                Optional<Integer> maskChannel,
                Session session,
                JoinCompiler joinCompiler,
                BlockTypeOperators blockTypeOperators,
                Optional<SpillableStateHolder> spillableStateHolder)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.maskChannel = requireNonNull(maskChannel, "maskChannel is null").orElse(-1);

            List<Type> types = ImmutableList.<Type>builder()
                    .add(BIGINT) // group id column
                    .addAll(inputTypes)
                    .build();

            int[] inputs = new int[inputChannels.size() + 1];
            inputs[0] = 0; // we'll use the first channel for group id column
            for (int i = 0; i < inputChannels.size(); i++) {
                inputs[i + 1] = inputChannels.get(i) + 1;
            }

            hash = new MarkDistinctHash(session, types, inputs, Optional.empty(), joinCompiler, blockTypeOperators, UpdateMemory.NOOP);
            this.spillableStateHolder = requireNonNull(spillableStateHolder, "spillableStateHolder is null");
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = hash.getEstimatedSize() + accumulator.getEstimatedSize();
            if (spillableStateHolder.isPresent()) {
                estimatedSize += spillableStateHolder.get().getEstimatedSize();
            }
            return estimatedSize;
        }

        @Override
        public Type getFinalType()
        {
            return accumulator.getFinalType();
        }

        @Override
        public Type getIntermediateType()
        {
            return spillableStateHolder.orElseThrow(UnsupportedOperationException::new).getIntermediateType();
        }

        @Override
        public void addInput(GroupByIdBlock groupIdsBlock, Page page)
        {
            if (spillableStateHolder.isPresent()) {
                spillableStateHolder.get().addInput(groupIdsBlock, page);
            }
            else {
                addInputToUnderlyingAccumulator(groupIdsBlock, page);
            }
        }

        private void addInputToUnderlyingAccumulator(GroupByIdBlock groupIdsBlock, Page page)
        {
            Page withGroup = page.prependColumn(groupIdsBlock);

            // 1. filter out positions based on mask, if present
            Page filtered;
            if (maskChannel >= 0) {
                filtered = filter(withGroup, withGroup.getBlock(maskChannel + 1)); // offset by one because of group id in column 0
            }
            else {
                filtered = withGroup;
            }

            // 2. compute a mask for the distinct rows (including the group id)
            Work<Block> work = hash.markDistinctRows(filtered);
            checkState(work.process());
            Block distinctMask = work.getResult();

            // 3. feed a Page with a new mask to the underlying aggregation
            GroupByIdBlock groupIds = new GroupByIdBlock(groupIdsBlock.getGroupCount(), filtered.getBlock(0));

            // drop the group id column and prepend the distinct mask column
            Block[] columns = new Block[filtered.getChannelCount()];
            columns[0] = distinctMask;
            for (int i = 1; i < filtered.getChannelCount(); i++) {
                columns[i] = filtered.getBlock(i);
            }

            accumulator.addInput(groupIds, new Page(filtered.getPositionCount(), columns));
        }

        @Override
        public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            spillableStateHolder.orElseThrow(UnsupportedOperationException::new).addIntermediate(groupIdsBlock, block);
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            spillableStateHolder.orElseThrow(UnsupportedOperationException::new).evaluateIntermediate(groupId, output);
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            spillableStateHolder.ifPresent(holder -> holder.verifyEvaluateFinal());
            accumulator.evaluateFinal(groupId, output);
        }

        @Override
        public void prepareFinal()
        {
            if (spillableStateHolder.isPresent()) {
                spillableStateHolder.get().prepareFinal((groupIdBlock, page) -> addInputToUnderlyingAccumulator(groupIdBlock, page));
            }
        }
    }

    private static class OrderingAccumulator
            implements Accumulator
    {
        private final Accumulator accumulator;
        private final List<Integer> orderByChannels;
        private final List<SortOrder> orderings;
        private final PagesIndex pagesIndex;

        private OrderingAccumulator(
                Accumulator accumulator,
                List<Type> aggregationSourceTypes,
                List<Integer> orderByChannels,
                List<SortOrder> orderings,
                PagesIndex.Factory pagesIndexFactory)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.orderByChannels = ImmutableList.copyOf(requireNonNull(orderByChannels, "orderByChannels is null"));
            this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
            this.pagesIndex = pagesIndexFactory.newPagesIndex(aggregationSourceTypes, 10_000);
        }

        @Override
        public long getEstimatedSize()
        {
            return pagesIndex.getEstimatedSize().toBytes() + accumulator.getEstimatedSize();
        }

        @Override
        public Type getFinalType()
        {
            return accumulator.getFinalType();
        }

        @Override
        public Type getIntermediateType()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Accumulator copy()
        {
            return accumulator.copy();
        }

        @Override
        public void addInput(Page page)
        {
            pagesIndex.addPage(page);
        }

        @Override
        public void addInput(WindowIndex index, List<Integer> channels, int startPosition, int endPosition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeInput(WindowIndex index, List<Integer> channels, int startPosition, int endPosition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addIntermediate(Block block)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateIntermediate(BlockBuilder blockBuilder)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateFinal(BlockBuilder blockBuilder)
        {
            pagesIndex.sort(orderByChannels, orderings);
            Iterator<Page> pagesIterator = pagesIndex.getSortedPages();
            pagesIterator.forEachRemaining(accumulator::addInput);
            accumulator.evaluateFinal(blockBuilder);
        }
    }

    private static class OrderingGroupedAccumulator
            implements GroupedAccumulator
    {
        private final GroupedAccumulator accumulator;
        private final List<Integer> orderByChannels;
        private final List<SortOrder> orderings;
        private final PagesIndex pagesIndex;
        private final Optional<SpillableStateHolder> spillableStateHolder;

        private long groupCount;

        private OrderingGroupedAccumulator(
                GroupedAccumulator accumulator,
                List<Type> aggregationSourceTypes,
                List<Integer> orderByChannels,
                List<SortOrder> orderings,
                PagesIndex.Factory pagesIndexFactory,
                Optional<SpillableStateHolder> spillableStateHolder)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            requireNonNull(aggregationSourceTypes, "aggregationSourceTypes is null");
            this.orderByChannels = ImmutableList.copyOf(requireNonNull(orderByChannels, "orderByChannels is null"));
            this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
            List<Type> pageIndexTypes = new ArrayList<>(aggregationSourceTypes);
            // Add group id column
            pageIndexTypes.add(BIGINT);
            this.pagesIndex = pagesIndexFactory.newPagesIndex(pageIndexTypes, 10_000);
            this.groupCount = 0;
            this.spillableStateHolder = requireNonNull(spillableStateHolder, "spillableStateHolder is null");
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = pagesIndex.getEstimatedSize().toBytes() + accumulator.getEstimatedSize();
            if (spillableStateHolder.isPresent()) {
                estimatedSize += spillableStateHolder.get().getEstimatedSize();
            }
            return estimatedSize;
        }

        @Override
        public Type getFinalType()
        {
            return accumulator.getFinalType();
        }

        @Override
        public Type getIntermediateType()
        {
            return spillableStateHolder.orElseThrow(UnsupportedOperationException::new).getIntermediateType();
        }

        @Override
        public void addInput(GroupByIdBlock groupIdsBlock, Page page)
        {
            if (spillableStateHolder.isPresent()) {
                spillableStateHolder.get().addInput(groupIdsBlock, page);
            }
            else {
                addInputToUnderlyingAccumulator(groupIdsBlock, page);
            }
        }

        private void addInputToUnderlyingAccumulator(GroupByIdBlock groupIdsBlock, Page page)
        {
            groupCount = max(groupCount, groupIdsBlock.getGroupCount());
            // Add group id block
            pagesIndex.addPage(page.appendColumn(groupIdsBlock));
        }

        @Override
        public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            spillableStateHolder.orElseThrow(UnsupportedOperationException::new).addIntermediate(groupIdsBlock, block);
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            spillableStateHolder.orElseThrow(UnsupportedOperationException::new).evaluateIntermediate(groupId, output);
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            spillableStateHolder.ifPresent(holder -> holder.verifyEvaluateFinal());
            accumulator.evaluateFinal(groupId, output);
        }

        @Override
        public void prepareFinal()
        {
            if (spillableStateHolder.isPresent()) {
                spillableStateHolder.get().prepareFinal((groupByIdBlock, page) -> addInputToUnderlyingAccumulator(groupByIdBlock, page));
            }
            pagesIndex.sort(orderByChannels, orderings);
            Iterator<Page> pagesIterator = pagesIndex.getSortedPages();
            pagesIterator.forEachRemaining(page -> {
                // The last channel of the page is the group id
                GroupByIdBlock groupIds = new GroupByIdBlock(groupCount, page.getBlock(page.getChannelCount() - 1));
                // We pass group id together with the other input channels to accumulator. Accumulator knows which input channels
                // to use. Since we did not change the order of original input channels, passing the group id is safe.
                accumulator.addInput(groupIds, page);
            });
        }
    }

    /**
     * {@link SpillableStateHolder} enables spilling for an accumulator that does not support partial aggregation
     */
    private static class SpillableStateHolder
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SpillableStateHolder.class).instanceSize();

        private final ImmutableList<Type> sourceTypes;
        private final ImmutableList<Type> accumulatorInputChannelTypes;
        private final ImmutableList<Integer> accumulatorInputChannels;

        private ObjectBigArray<AccumulatorInput> rawInputs = new ObjectBigArray<>();
        private IntBigArray groupIdCount = new IntBigArray();
        private ObjectBigArray<RowBlockBuilder> blockBuilders;
        private long rawInputsSizeInBytes;
        private long blockBuildersSizeInBytes;
        private long rawInputsLength;

        public SpillableStateHolder(List<Type> sourceTypes, List<Integer> accumulatorInputChannels)
        {
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.accumulatorInputChannels = ImmutableList.copyOf(requireNonNull(accumulatorInputChannels, "accumulatorInputChannels is null"));
            this.accumulatorInputChannelTypes = accumulatorInputChannels.stream()
                    .map(sourceTypes::get)
                    .collect(toImmutableList());
        }

        public long getEstimatedSize()
        {
            return INSTANCE_SIZE +
                    (rawInputs == null ? 0 : rawInputsSizeInBytes + rawInputs.sizeOf()) +
                    (groupIdCount == null ? 0 : groupIdCount.sizeOf()) +
                    (blockBuilders == null ? 0 : blockBuildersSizeInBytes + blockBuilders.sizeOf());
        }

        public Type getIntermediateType()
        {
            return new ArrayType(RowType.anonymous(accumulatorInputChannelTypes));
        }

        public void addInput(GroupByIdBlock groupIdsBlock, Page page)
        {
            verify(rawInputs != null, "rawInputs is expected to be not null");
            verify(blockBuilders == null, "blockBuilders is expected to be null");
            rawInputs.ensureCapacity(rawInputsLength);

            // Create a new Page that only have channels which will be consumed by the aggregate
            Block[] blocks = new Block[accumulatorInputChannels.size()];
            for (int i = 0; i < accumulatorInputChannels.size(); i++) {
                blocks[i] = page.getBlock(accumulatorInputChannels.get(i));
            }
            Page accumulatorInputPage = wrapBlocksWithoutCopy(page.getPositionCount(), blocks);
            AccumulatorInput accumulatorInput = new AccumulatorInput(groupIdsBlock, accumulatorInputPage);
            rawInputsSizeInBytes += accumulatorInput.getRetainedSizeInBytes();
            rawInputs.set(rawInputsLength, accumulatorInput);
            // TODO(junhyung) deduplicate inputs for DISTINCT accumulator case by doing page compaction
            rawInputsLength++;

            // Keep track of number of elements for each groupId. This will later help us know the size of each
            // RowBlock we spill to disk. E.g. Let's say groupIdsBlock = [0, 1, 0]. In a subsequent addInput call,
            // groupIdsBlock = [2, 1, 0]. The resultant groupIdCount would be [3, 2, 1]. This is because there are
            // 3 values for groupId 0, 2 values for groupId 1, and 1 value for groupId 2. The index into groupIdCount
            // represents the groupId while the value is the total number of values for that groupId.
            for (int i = 0; i < groupIdsBlock.getPositionCount(); i++) {
                long currentGroupId = groupIdsBlock.getGroupId(i);
                groupIdCount.ensureCapacity(currentGroupId);
                groupIdCount.increment(currentGroupId);
            }
        }

        public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            verify(rawInputs != null, "rawInputs is expected to be not null");
            verify(blockBuilders == null, "blockBuilders is expected to be null");
            ArrayBlock arrayBlock = (ArrayBlock) block;

            // expand array block back into page
            // flattens the squashed arrays; so there is no need to flatten block again.
            ColumnarArray columnarArray = toColumnarArray(block);
            // contains the flattened array
            ColumnarRow columnarRow = toColumnarRow(columnarArray.getElementsBlock());
            // number of positions in expanded array (since columnarRow is already flattened)
            int newPositionCount = columnarRow.getPositionCount();
            long[] newGroupIds = new long[newPositionCount];
            boolean[] nulls = new boolean[newPositionCount];
            int currentRowBlockIndex = 0;
            for (int groupIdPosition = 0; groupIdPosition < groupIdsBlock.getPositionCount(); groupIdPosition++) {
                for (int unused = 0; unused < arrayBlock.getObject(groupIdPosition, Block.class).getPositionCount(); unused++) {
                    // unused because we are expanding all the squashed values for the same group id
                    newGroupIds[currentRowBlockIndex] = groupIdsBlock.getGroupId(groupIdPosition);
                    nulls[currentRowBlockIndex] = groupIdsBlock.isNull(groupIdPosition);
                    currentRowBlockIndex++;
                }
            }

            Block[] blocks = new Block[accumulatorInputChannelTypes.size()];
            for (int channel = 0; channel < accumulatorInputChannelTypes.size(); channel++) {
                blocks[channel] = columnarRow.getField(channel);
            }
            Page page = new Page(blocks);
            GroupByIdBlock squashedGroupIds = new GroupByIdBlock(groupIdsBlock.getGroupCount(), new LongArrayBlock(newPositionCount, Optional.of(nulls), newGroupIds));

            rawInputs.ensureCapacity(rawInputsLength);
            AccumulatorInput accumulatorInput = new AccumulatorInput(squashedGroupIds, page);
            rawInputsSizeInBytes += accumulatorInput.getRetainedSizeInBytes();
            rawInputs.set(rawInputsLength, accumulatorInput);
            rawInputsLength++;
        }

        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            // output needs to be ArrayBlockBuilder
            ArrayBlockBuilder arrayOutput = (ArrayBlockBuilder) output;
            if (blockBuilders == null) {
                verify(rawInputs != null, "rawInputs is null");

                blockBuilders = new ObjectBigArray<>();
                for (int i = 0; i < rawInputsLength; i++) {
                    AccumulatorInput accumulatorInput = rawInputs.get(i);
                    Page page = accumulatorInput.getPage();
                    GroupByIdBlock groupIdsBlock = accumulatorInput.getGroupByIdBlock();
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        long currentGroupId = groupIdsBlock.getGroupId(position);
                        blockBuilders.ensureCapacity(currentGroupId);
                        RowBlockBuilder rowBlockBuilder = blockBuilders.get(currentGroupId);
                        long currentRowBlockSizeInBytes = 0;
                        if (rowBlockBuilder == null) {
                            rowBlockBuilder = new RowBlockBuilder(accumulatorInputChannelTypes, null, groupIdCount.get(currentGroupId));
                        }
                        else {
                            currentRowBlockSizeInBytes = rowBlockBuilder.getRetainedSizeInBytes();
                        }

                        BlockBuilder currentOutput = rowBlockBuilder.beginBlockEntry();
                        for (int channel = 0; channel < accumulatorInputChannelTypes.size(); channel++) {
                            accumulatorInputChannelTypes.get(channel).appendTo(page.getBlock(channel), position, currentOutput);
                        }
                        rowBlockBuilder.closeEntry();

                        blockBuildersSizeInBytes += (rowBlockBuilder.getRetainedSizeInBytes() - currentRowBlockSizeInBytes);
                        blockBuilders.set(currentGroupId, rowBlockBuilder);
                    }
                    rawInputs.set(i, null);
                }
                groupIdCount = null;
                rawInputs = null;
                rawInputsSizeInBytes = 0;
                rawInputsLength = 0;
            }

            BlockBuilder singleArrayBlockWriter = arrayOutput.beginBlockEntry();
            verify(rawInputs == null, "rawInputs is expected to be null");
            verify(blockBuilders != null, "blockBuilders is expected to be not null");

            // We need to squash the entire page into one array block since we can't spill multiple values for a single group ID during evaluateIntermediate.
            RowBlock rowBlock = (RowBlock) blockBuilders.get(groupId).build();
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                singleArrayBlockWriter.appendStructure(rowBlock.getObject(i, Block.class));
            }
            arrayOutput.closeEntry();

            // We only call evaluateIntermediate when it is time to spill. We never call evaluate intermediate twice for the same groupId.
            // This means we can null our reference to the groupId's corresponding blockBuilder to reduce memory usage
            blockBuilders.set(groupId, null);
        }

        public void verifyEvaluateFinal()
        {
            verify(rawInputs == null, "rawInputs is expected to be null");
            verify(blockBuilders == null, "blockBuilders is expected to be null");
        }

        public void prepareFinal(BiConsumer<GroupByIdBlock, Page> addInputToUnderlyingAccumulator)
        {
            verify(rawInputs != null, "rawInputs is expected to be not null");
            verify(blockBuilders == null, "blockBuilders is expected to be null");
            for (int i = 0; i < rawInputsLength; i++) {
                AccumulatorInput accumulatorInput = rawInputs.get(i);
                // Before pushing the page to delegate, restore it back to it original structure
                // in terms of number of channels. Channels which are not consumed by the accumulator
                // will be replaced with null block
                Page page = accumulatorInput.getPage();
                Block[] blocks = new Block[sourceTypes.size()];
                for (int channel = 0; channel < sourceTypes.size(); channel++) {
                    if (accumulatorInputChannels.contains(channel)) {
                        blocks[channel] = page.getBlock(accumulatorInputChannels.indexOf(channel));
                    }
                    else {
                        blocks[channel] = RunLengthEncodedBlock.create(sourceTypes.get(channel), null, page.getPositionCount());
                    }
                }
                addInputToUnderlyingAccumulator.accept(accumulatorInput.getGroupByIdBlock(), wrapBlocksWithoutCopy(page.getPositionCount(), blocks));
            }

            rawInputs = null;
            rawInputsSizeInBytes = 0;
            rawInputsLength = 0;
        }

        // AccumulatorInput contains an original page and a corresponding groupByIdBlock
        private static class AccumulatorInput
        {
            private static final int INSTANCE_SIZE = ClassLayout.parseClass(AccumulatorInput.class).instanceSize();

            private final GroupByIdBlock groupByIdBlock;
            private final Page page;

            public AccumulatorInput(GroupByIdBlock groupByIdBlock, Page page)
            {
                this.groupByIdBlock = requireNonNull(groupByIdBlock, "groupByIdBlock is null");
                this.page = requireNonNull(page, "page is null");
            }

            public Page getPage()
            {
                return page;
            }

            public GroupByIdBlock getGroupByIdBlock()
            {
                return groupByIdBlock;
            }

            public long getRetainedSizeInBytes()
            {
                return INSTANCE_SIZE + groupByIdBlock.getRetainedSizeInBytes() + page.getRetainedSizeInBytes();
            }
        }
    }
}
