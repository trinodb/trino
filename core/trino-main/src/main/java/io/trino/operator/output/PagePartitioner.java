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
package io.trino.operator.output;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.PagesSerde;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.operator.OperatorContext;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.execution.buffer.PageSplitterUtil.splitPage;
import static io.trino.operator.output.PartitionedOutputOperator.PartitionedOutputInfo;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PagePartitioner
{
    private static final int COLUMNAR_STRATEGY_COEFFICIENT = 4;
    private final OutputBuffer outputBuffer;
    private final Type[] sourceTypes;
    private final PartitionFunction partitionFunction;
    private final int[] partitionChannels;
    @Nullable
    private final Block[] partitionConstantBlocks; // when null, no constants are present. Only non-null elements are constants
    private final PagesSerde serde;
    private final PageBuilder[] pageBuilders;
    private final boolean replicatesAnyRow;
    private final int nullChannel; // when >= 0, send the position to every partition if this channel is null
    private final AtomicLong rowsAdded = new AtomicLong();
    private final AtomicLong pagesAdded = new AtomicLong();
    private final OperatorContext operatorContext;
    private final PositionsAppenderFactory positionsAppenderFactory;
    private final PositionsAppender[] positionsAppenders;

    private boolean hasAnyRowBeenReplicated;

    public PagePartitioner(
            PartitionFunction partitionFunction,
            List<Integer> partitionChannels,
            List<Optional<NullableValue>> partitionConstants,
            boolean replicatesAnyRow,
            OptionalInt nullChannel,
            OutputBuffer outputBuffer,
            PagesSerdeFactory serdeFactory,
            List<Type> sourceTypes,
            DataSize maxMemory,
            OperatorContext operatorContext,
            PositionsAppenderFactory positionsAppenderFactory)
    {
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.partitionChannels = Ints.toArray(requireNonNull(partitionChannels, "partitionChannels is null"));
        this.positionsAppenderFactory = requireNonNull(positionsAppenderFactory, "positionsAppenderFactory is null");
        Block[] partitionConstantBlocks = requireNonNull(partitionConstants, "partitionConstants is null").stream()
                .map(constant -> constant.map(NullableValue::asBlock).orElse(null))
                .toArray(Block[]::new);
        if (Arrays.stream(partitionConstantBlocks).anyMatch(Objects::nonNull)) {
            this.partitionConstantBlocks = partitionConstantBlocks;
        }
        else {
            this.partitionConstantBlocks = null;
        }
        this.replicatesAnyRow = replicatesAnyRow;
        this.nullChannel = requireNonNull(nullChannel, "nullChannel is null").orElse(-1);
        this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null").toArray(new Type[0]);
        this.serde = requireNonNull(serdeFactory, "serdeFactory is null").createPagesSerde();
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        //  Ensure partition channels align with constant arguments provided
        for (int i = 0; i < this.partitionChannels.length; i++) {
            if (this.partitionChannels[i] < 0) {
                checkArgument(this.partitionConstantBlocks != null && this.partitionConstantBlocks[i] != null,
                        "Expected constant for partitioning channel %s, but none was found", i);
            }
        }

        int partitionCount = partitionFunction.getPartitionCount();
        int pageSize = toIntExact(min(DEFAULT_MAX_PAGE_SIZE_IN_BYTES, maxMemory.toBytes() / partitionCount));
        pageSize = max(1, pageSize);

        this.pageBuilders = new PageBuilder[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            pageBuilders[i] = PageBuilder.withMaxPageSize(pageSize, sourceTypes);
        }
        positionsAppenders = new PositionsAppender[sourceTypes.size()];
    }

    public ListenableFuture<Void> isFull()
    {
        return outputBuffer.isFull();
    }

    public long getSizeInBytes()
    {
        // We use a foreach loop instead of streams
        // as it has much better performance.
        long sizeInBytes = 0;
        for (PageBuilder pageBuilder : pageBuilders) {
            sizeInBytes += pageBuilder.getSizeInBytes();
        }
        return sizeInBytes;
    }

    /**
     * This method can be expensive for complex types.
     */
    public long getRetainedSizeInBytes()
    {
        long sizeInBytes = 0;
        for (PageBuilder pageBuilder : pageBuilders) {
            sizeInBytes += pageBuilder.getRetainedSizeInBytes();
        }
        return sizeInBytes;
    }

    public Supplier<PartitionedOutputInfo> getOperatorInfoSupplier()
    {
        return createPartitionedOutputOperatorInfoSupplier(rowsAdded, pagesAdded, outputBuffer);
    }

    private static Supplier<PartitionedOutputInfo> createPartitionedOutputOperatorInfoSupplier(AtomicLong rowsAdded, AtomicLong pagesAdded, OutputBuffer outputBuffer)
    {
        // Must be a separate static method to avoid embedding references to "this" in the supplier
        requireNonNull(rowsAdded, "rowsAdded is null");
        requireNonNull(pagesAdded, "pagesAdded is null");
        requireNonNull(outputBuffer, "outputBuffer is null");
        return () -> new PartitionedOutputInfo(rowsAdded.get(), pagesAdded.get(), outputBuffer.getPeakMemoryUsage());
    }

    public void partitionPage(Page page)
    {
        if (page.getPositionCount() == 0) {
            return;
        }

        if (page.getPositionCount() < partitionFunction.getPartitionCount() * COLUMNAR_STRATEGY_COEFFICIENT) {
            // Partition will have on average less than COLUMNAR_STRATEGY_COEFFICIENT rows.
            // Doing it column-wise would degrade performance, so we fall back to row-wise approach.
            // Performance degradation is the worst in case of skewed hash distribution when only small subset
            // of partitions is selected.
            partitionPageByRow(page);
        }
        else {
            partitionPageByColumn(page);
        }
    }

    public void partitionPageByRow(Page page)
    {
        requireNonNull(page, "page is null");
        if (page.getPositionCount() == 0) {
            return;
        }

        int position;
        // Handle "any row" replication outside of the inner loop processing
        if (replicatesAnyRow && !hasAnyRowBeenReplicated) {
            for (PageBuilder pageBuilder : pageBuilders) {
                appendRow(pageBuilder, page, 0);
            }
            hasAnyRowBeenReplicated = true;
            position = 1;
        }
        else {
            position = 0;
        }

        Page partitionFunctionArgs = getPartitionFunctionArguments(page);
        // Skip null block checks if mayHaveNull reports that no positions will be null
        if (nullChannel >= 0 && page.getBlock(nullChannel).mayHaveNull()) {
            Block nullsBlock = page.getBlock(nullChannel);
            for (; position < page.getPositionCount(); position++) {
                if (nullsBlock.isNull(position)) {
                    for (PageBuilder pageBuilder : pageBuilders) {
                        appendRow(pageBuilder, page, position);
                    }
                }
                else {
                    int partition = partitionFunction.getPartition(partitionFunctionArgs, position);
                    appendRow(pageBuilders[partition], page, position);
                }
            }
        }
        else {
            for (; position < page.getPositionCount(); position++) {
                int partition = partitionFunction.getPartition(partitionFunctionArgs, position);
                appendRow(pageBuilders[partition], page, position);
            }
        }

        flush(false);
    }

    private void appendRow(PageBuilder pageBuilder, Page page, int position)
    {
        pageBuilder.declarePosition();

        for (int channel = 0; channel < sourceTypes.length; channel++) {
            Type type = sourceTypes[channel];
            type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
        }
    }

    public void partitionPageByColumn(Page page)
    {
        IntArrayList[] partitionedPositions = partitionPositions(page);

        PositionsAppender[] positionsAppenders = getAppenders(page);

        for (int i = 0; i < partitionFunction.getPartitionCount(); i++) {
            IntArrayList partitionPositions = partitionedPositions[i];
            if (!partitionPositions.isEmpty()) {
                appendToOutputPartition(pageBuilders[i], page, partitionPositions, positionsAppenders);
                partitionPositions.clear();
            }
        }

        flush(false);
    }

    private PositionsAppender[] getAppenders(Page page)
    {
        for (int i = 0; i < positionsAppenders.length; i++) {
            positionsAppenders[i] = positionsAppenderFactory.create(sourceTypes[i], page.getBlock(i).getClass());
        }
        return positionsAppenders;
    }

    private IntArrayList[] partitionPositions(Page page)
    {
        verify(page.getPositionCount() > 0, "position count is 0");
        IntArrayList[] partitionPositions = initPositions(page);
        int position;
        // Handle "any row" replication outside the inner loop processing
        if (replicatesAnyRow && !hasAnyRowBeenReplicated) {
            for (IntList partitionPosition : partitionPositions) {
                partitionPosition.add(0);
            }
            hasAnyRowBeenReplicated = true;
            position = 1;
        }
        else {
            position = 0;
        }

        Page partitionFunctionArgs = getPartitionFunctionArguments(page);

        if (partitionFunctionArgs.getChannelCount() > 0 && onlyRleBlocks(partitionFunctionArgs)) {
            // we need at least one Rle block since with no blocks partition function
            // can return a different value per invocation (e.g. RoundRobinBucketFunction)
            partitionBySingleRleValue(page, position, partitionFunctionArgs, partitionPositions);
        }
        else if (partitionFunctionArgs.getChannelCount() == 1 && isDictionaryProcessingFaster(partitionFunctionArgs.getBlock(0))) {
            partitionBySingleDictionary(page, position, partitionFunctionArgs, partitionPositions);
        }
        else {
            partitionGeneric(page, position, aPosition -> partitionFunction.getPartition(partitionFunctionArgs, aPosition), partitionPositions);
        }
        return partitionPositions;
    }

    private void appendToOutputPartition(PageBuilder outputPartition, Page page, IntArrayList positions, PositionsAppender[] positionsAppenders)
    {
        outputPartition.declarePositions(positions.size());

        for (int channel = 0; channel < positionsAppenders.length; channel++) {
            Block partitionBlock = page.getBlock(channel);
            BlockBuilder target = outputPartition.getBlockBuilder(channel);
            positionsAppenders[channel].appendTo(positions, partitionBlock, target);
        }
    }

    private IntArrayList[] initPositions(Page page)
    {
        // We allocate new arrays for every page (instead of caching them) because we don't
        // want memory to explode in case there are input pages with many positions, where each page
        // is assigned to a single partition entirely.
        // For example this can happen for partition columns if they are represented by RLE blocks.
        IntArrayList[] partitionPositions = new IntArrayList[partitionFunction.getPartitionCount()];
        for (int i = 0; i < partitionPositions.length; i++) {
            partitionPositions[i] = new IntArrayList(initialPartitionSize(page.getPositionCount() / partitionFunction.getPartitionCount()));
        }
        return partitionPositions;
    }

    private static int initialPartitionSize(int averagePositionsPerPartition)
    {
        // 1.1 coefficient compensates for the not perfect hash distribution.
        // 32 compensates for the case when averagePositionsPerPartition is small,
        // and we would see more variance in the hash distribution.
        return (int) (averagePositionsPerPartition * 1.1) + 32;
    }

    private boolean onlyRleBlocks(Page page)
    {
        for (int i = 0; i < page.getChannelCount(); i++) {
            if (!(page.getBlock(i) instanceof RunLengthEncodedBlock)) {
                return false;
            }
        }
        return true;
    }

    private void partitionBySingleRleValue(Page page, int position, Page partitionFunctionArgs, IntArrayList[] partitionPositions)
    {
        // copy all positions because all hash function args are the same for every position
        if (nullChannel != -1 && page.getBlock(nullChannel).isNull(0)) {
            verify(page.getBlock(nullChannel) instanceof RunLengthEncodedBlock, "null channel is not RunLengthEncodedBlock", page.getBlock(nullChannel));
            // all positions are null
            int[] allPositions = integersInRange(position, page.getPositionCount());
            for (IntList partitionPosition : partitionPositions) {
                partitionPosition.addElements(position, allPositions);
            }
        }
        else {
            // extract rle page to prevent JIT profile pollution
            Page rlePage = extractRlePage(partitionFunctionArgs);

            int partition = partitionFunction.getPartition(rlePage, 0);
            IntArrayList positions = partitionPositions[partition];
            for (int i = position; i < page.getPositionCount(); i++) {
                positions.add(i);
            }
        }
    }

    private Page extractRlePage(Page page)
    {
        Block[] valueBlocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < valueBlocks.length; ++channel) {
            valueBlocks[channel] = ((RunLengthEncodedBlock) page.getBlock(channel)).getValue();
        }
        return new Page(valueBlocks);
    }

    private int[] integersInRange(int start, int endExclusive)
    {
        int[] array = new int[endExclusive - start];
        int current = start;
        for (int i = 0; i < array.length; i++) {
            array[i] = current++;
        }
        return array;
    }

    private boolean isDictionaryProcessingFaster(Block block)
    {
        if (!(block instanceof DictionaryBlock)) {
            return false;
        }
        DictionaryBlock dictionaryBlock = (DictionaryBlock) block;
        // if dictionary block positionCount is greater than number of elements in the dictionary
        // it will be faster to compute hash for the dictionary values only once and re-use it
        // instead of recalculating it.
        return dictionaryBlock.getPositionCount() > dictionaryBlock.getDictionary().getPositionCount();
    }

    private void partitionBySingleDictionary(Page page, int position, Page partitionFunctionArgs, IntArrayList[] partitionPositions)
    {
        DictionaryBlock dictionaryBlock = (DictionaryBlock) partitionFunctionArgs.getBlock(0);
        Block dictionary = dictionaryBlock.getDictionary();
        int[] dictionaryPartitions = new int[dictionary.getPositionCount()];
        Page dictionaryPage = new Page(dictionary);
        for (int i = 0; i < dictionary.getPositionCount(); i++) {
            dictionaryPartitions[i] = partitionFunction.getPartition(dictionaryPage, i);
        }

        partitionGeneric(page, position, aPosition -> dictionaryPartitions[dictionaryBlock.getId(aPosition)], partitionPositions);
    }

    private void partitionGeneric(Page page, int position, IntUnaryOperator partitionFunction, IntArrayList[] partitionPositions)
    {
        // Skip null block checks if mayHaveNull reports that no positions will be null
        if (nullChannel != -1 && page.getBlock(nullChannel).mayHaveNull()) {
            partitionNullablePositions(page, position, partitionPositions, partitionFunction);
        }
        else {
            partitionNotNullPositions(page, position, partitionPositions, partitionFunction);
        }
    }

    private IntArrayList[] partitionNullablePositions(Page page, int position, IntArrayList[] partitionPositions, IntUnaryOperator partitionFunction)
    {
        Block nullsBlock = page.getBlock(nullChannel);
        int[] nullPositions = new int[page.getPositionCount()];
        int[] nonNullPositions = new int[page.getPositionCount()];
        int nullCount = 0;
        int nonNullCount = 0;
        for (int i = position; i < page.getPositionCount(); i++) {
            nullPositions[nullCount] = i;
            nonNullPositions[nonNullCount] = i;
            int isNull = nullsBlock.isNull(i) ? 1 : 0;
            nullCount += isNull;
            nonNullCount += isNull ^ 1;
        }
        for (IntArrayList positions : partitionPositions) {
            positions.addElements(position, nullPositions, 0, nullCount);
        }
        for (int i = 0; i < nonNullCount; i++) {
            int nonNullPosition = nonNullPositions[i];
            int partition = partitionFunction.applyAsInt(nonNullPosition);
            partitionPositions[partition].add(nonNullPosition);
        }
        return partitionPositions;
    }

    private IntArrayList[] partitionNotNullPositions(Page page, int startingPosition, IntArrayList[] partitionPositions, IntUnaryOperator partitionFunction)
    {
        for (int position = startingPosition; position < page.getPositionCount(); position++) {
            int partition = partitionFunction.applyAsInt(position);
            partitionPositions[partition].add(position);
        }

        return partitionPositions;
    }

    private Page getPartitionFunctionArguments(Page page)
    {
        // Fast path for no constants
        if (partitionConstantBlocks == null) {
            return page.getColumns(partitionChannels);
        }

        Block[] blocks = new Block[partitionChannels.length];
        for (int i = 0; i < blocks.length; i++) {
            int channel = partitionChannels[i];
            if (channel < 0) {
                blocks[i] = new RunLengthEncodedBlock(partitionConstantBlocks[i], page.getPositionCount());
            }
            else {
                blocks[i] = page.getBlock(channel);
            }
        }
        return new Page(page.getPositionCount(), blocks);
    }

    public void flush(boolean force)
    {
        try (PagesSerde.PagesSerdeContext context = serde.newContext()) {
            // add all full pages to output buffer
            for (int partition = 0; partition < pageBuilders.length; partition++) {
                PageBuilder partitionPageBuilder = pageBuilders[partition];
                if (!partitionPageBuilder.isEmpty() && (force || partitionPageBuilder.isFull())) {
                    Page pagePartition = partitionPageBuilder.build();
                    partitionPageBuilder.reset();

                    operatorContext.recordOutput(pagePartition.getSizeInBytes(), pagePartition.getPositionCount());

                    outputBuffer.enqueue(partition, splitAndSerializePage(context, pagePartition));
                    pagesAdded.incrementAndGet();
                    rowsAdded.addAndGet(pagePartition.getPositionCount());
                }
            }
        }
    }

    private List<Slice> splitAndSerializePage(PagesSerde.PagesSerdeContext context, Page page)
    {
        List<Page> split = splitPage(page, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        ImmutableList.Builder<Slice> builder = ImmutableList.builderWithExpectedSize(split.size());
        for (Page chunk : split) {
            builder.add(serde.serialize(context, chunk));
        }
        return builder.build();
    }
}
