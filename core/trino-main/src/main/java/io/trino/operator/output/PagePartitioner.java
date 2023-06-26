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
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.PageSerializer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.OperatorContext;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;
import io.trino.util.Ciphers;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import jakarta.annotation.Nullable;

import java.io.Closeable;
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
        implements Closeable
{
    private static final int COLUMNAR_STRATEGY_COEFFICIENT = 2;
    private final OutputBuffer outputBuffer;
    private final PartitionFunction partitionFunction;
    private final int[] partitionChannels;
    private final LocalMemoryContext memoryContext;
    @Nullable
    private final Block[] partitionConstantBlocks; // when null, no constants are present. Only non-null elements are constants
    private final PageSerializer serializer;
    private final PositionsAppenderPageBuilder[] positionsAppenders;
    private final boolean replicatesAnyRow;
    private final boolean partitionProcessRleAndDictionaryBlocks;
    private final int nullChannel; // when >= 0, send the position to every partition if this channel is null
    private PartitionedOutputInfoSupplier partitionedOutputInfoSupplier;

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
            PositionsAppenderFactory positionsAppenderFactory,
            Optional<Slice> exchangeEncryptionKey,
            AggregatedMemoryContext aggregatedMemoryContext,
            boolean partitionProcessRleAndDictionaryBlocks)
    {
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.partitionChannels = Ints.toArray(requireNonNull(partitionChannels, "partitionChannels is null"));
        requireNonNull(positionsAppenderFactory, "positionsAppenderFactory is null");
        Block[] partitionConstantBlocks = partitionConstants.stream()
                .map(constant -> constant.map(NullableValue::asBlock).orElse(null))
                .toArray(Block[]::new);
        if (Arrays.stream(partitionConstantBlocks).anyMatch(Objects::nonNull)) {
            this.partitionConstantBlocks = partitionConstantBlocks;
        }
        else {
            this.partitionConstantBlocks = null;
        }
        this.replicatesAnyRow = replicatesAnyRow;
        this.nullChannel = nullChannel.orElse(-1);
        this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        this.serializer = serdeFactory.createSerializer(exchangeEncryptionKey.map(Ciphers::deserializeAesEncryptionKey));
        this.partitionProcessRleAndDictionaryBlocks = partitionProcessRleAndDictionaryBlocks;

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

        this.positionsAppenders = new PositionsAppenderPageBuilder[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            positionsAppenders[i] = PositionsAppenderPageBuilder.withMaxPageSize(pageSize, requireNonNull(sourceTypes, "sourceTypes is null"), positionsAppenderFactory);
        }
        this.memoryContext = aggregatedMemoryContext.newLocalMemoryContext(PagePartitioner.class.getSimpleName());
        updateMemoryUsage();
    }

    public PartitionFunction getPartitionFunction()
    {
        return partitionFunction;
    }

    // sets up this partitioner for the new operator
    public void setupOperator(OperatorContext operatorContext)
    {
        // for new operator we need to reset the stats gathered by this PagePartitioner
        partitionedOutputInfoSupplier = new PartitionedOutputInfoSupplier(outputBuffer, operatorContext);
        operatorContext.setInfoSupplier(partitionedOutputInfoSupplier);
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
        updateMemoryUsage();
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
            for (PositionsAppenderPageBuilder pageBuilder : positionsAppenders) {
                pageBuilder.appendToOutputPartition(page, 0);
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
                    for (PositionsAppenderPageBuilder pageBuilder : positionsAppenders) {
                        pageBuilder.appendToOutputPartition(page, position);
                    }
                }
                else {
                    int partition = partitionFunction.getPartition(partitionFunctionArgs, position);
                    positionsAppenders[partition].appendToOutputPartition(page, position);
                }
            }
        }
        else {
            for (; position < page.getPositionCount(); position++) {
                int partition = partitionFunction.getPartition(partitionFunctionArgs, position);
                positionsAppenders[partition].appendToOutputPartition(page, position);
            }
        }

        flushPositionsAppenders(false);
    }

    public void partitionPageByColumn(Page page)
    {
        IntArrayList[] partitionedPositions = partitionPositions(page);

        for (int i = 0; i < partitionFunction.getPartitionCount(); i++) {
            IntArrayList partitionPositions = partitionedPositions[i];
            if (!partitionPositions.isEmpty()) {
                positionsAppenders[i].appendToOutputPartition(page, partitionPositions);
                partitionPositions.clear();
            }
        }

        flushPositionsAppenders(false);
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

        if (partitionProcessRleAndDictionaryBlocks && partitionFunctionArgs.getChannelCount() > 0 && onlyRleBlocks(partitionFunctionArgs)) {
            // we need at least one Rle block since with no blocks partition function
            // can return a different value per invocation (e.g. RoundRobinBucketFunction)
            partitionBySingleRleValue(page, position, partitionFunctionArgs, partitionPositions);
        }
        else if (partitionProcessRleAndDictionaryBlocks && partitionFunctionArgs.getChannelCount() == 1 && isDictionaryProcessingFaster(partitionFunctionArgs.getBlock(0))) {
            partitionBySingleDictionary(page, position, partitionFunctionArgs, partitionPositions);
        }
        else {
            partitionGeneric(page, position, aPosition -> partitionFunction.getPartition(partitionFunctionArgs, aPosition), partitionPositions);
        }
        return partitionPositions;
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

    @SuppressWarnings("NumericCastThatLosesPrecision")
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
            verify(page.getBlock(nullChannel) instanceof RunLengthEncodedBlock, "null channel is not RunLengthEncodedBlock, found instead: %s", page.getBlock(nullChannel));
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
        if (!(block instanceof DictionaryBlock dictionaryBlock)) {
            return false;
        }
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

    private void partitionNullablePositions(Page page, int position, IntArrayList[] partitionPositions, IntUnaryOperator partitionFunction)
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
    }

    private void partitionNotNullPositions(Page page, int startingPosition, IntArrayList[] partitionPositions, IntUnaryOperator partitionFunction)
    {
        int positionCount = page.getPositionCount();
        int[] partitionPerPosition = new int[positionCount];
        for (int position = startingPosition; position < positionCount; position++) {
            int partition = partitionFunction.applyAsInt(position);
            partitionPerPosition[position] = partition;
        }

        for (int position = startingPosition; position < positionCount; position++) {
            partitionPositions[partitionPerPosition[position]].add(position);
        }
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
                blocks[i] = RunLengthEncodedBlock.create(partitionConstantBlocks[i], page.getPositionCount());
            }
            else {
                blocks[i] = page.getBlock(channel);
            }
        }
        return new Page(page.getPositionCount(), blocks);
    }

    @Override
    public void close()
    {
        try {
            flushPositionsAppenders(true);
        }
        finally {
            // clear buffers before memory release
            Arrays.fill(positionsAppenders, null);
            memoryContext.close();
        }
    }

    private void flushPositionsAppenders(boolean force)
    {
        // add all full pages to output buffer
        for (int partition = 0; partition < positionsAppenders.length; partition++) {
            PositionsAppenderPageBuilder partitionPageBuilder = positionsAppenders[partition];
            if (!partitionPageBuilder.isEmpty() && (force || partitionPageBuilder.isFull())) {
                Page pagePartition = partitionPageBuilder.build();
                enqueuePage(pagePartition, partition);
            }
        }
    }

    private void enqueuePage(Page pagePartition, int partition)
    {
        outputBuffer.enqueue(partition, splitAndSerializePage(pagePartition));
        partitionedOutputInfoSupplier.recordPage(pagePartition);
    }

    private List<Slice> splitAndSerializePage(Page page)
    {
        List<Page> split = splitPage(page, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        ImmutableList.Builder<Slice> builder = ImmutableList.builderWithExpectedSize(split.size());
        for (Page chunk : split) {
            builder.add(serializer.serialize(chunk));
        }
        return builder.build();
    }

    private void updateMemoryUsage()
    {
        long retainedSizeInBytes = 0;
        for (PositionsAppenderPageBuilder pageBuilder : positionsAppenders) {
            retainedSizeInBytes += pageBuilder.getRetainedSizeInBytes();
        }
        retainedSizeInBytes += serializer.getRetainedSizeInBytes();
        memoryContext.setBytes(retainedSizeInBytes);
    }

    /**
     * Keeps statistics about output pages produced by the partitioner + updates the stats in the operatorContext.
     */
    private static class PartitionedOutputInfoSupplier
            implements Supplier<PartitionedOutputInfo>
    {
        private final AtomicLong rowsAdded = new AtomicLong();
        private final AtomicLong pagesAdded = new AtomicLong();
        private final OutputBuffer outputBuffer;
        private final OperatorContext operatorContext;

        private PartitionedOutputInfoSupplier(OutputBuffer outputBuffer, OperatorContext operatorContext)
        {
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        }

        @Override
        public PartitionedOutputInfo get()
        {
            // note that outputBuffer.getPeakMemoryUsage() will produce peak across many operators
            // this is suboptimal but hard to fix properly
            return new PartitionedOutputInfo(rowsAdded.get(), pagesAdded.get(), outputBuffer.getPeakMemoryUsage());
        }

        public void recordPage(Page pagePartition)
        {
            operatorContext.recordOutput(pagePartition.getSizeInBytes(), pagePartition.getPositionCount());
            pagesAdded.incrementAndGet();
            rowsAdded.addAndGet(pagePartition.getPositionCount());
        }
    }
}
