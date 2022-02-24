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
import io.trino.operator.output.PartitionedOutputOperator.PartitionedOutputInfo;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.execution.buffer.PageSplitterUtil.splitPage;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PagePartitioner
{
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
    private boolean hasAnyRowBeenReplicated;
    private final OperatorContext operatorContext;

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
            OperatorContext operatorContext)
    {
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.partitionChannels = Ints.toArray(requireNonNull(partitionChannels, "partitionChannels is null"));
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

    private void appendRow(PageBuilder pageBuilder, Page page, int position)
    {
        pageBuilder.declarePosition();

        for (int channel = 0; channel < sourceTypes.length; channel++) {
            Type type = sourceTypes[channel];
            type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
        }
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
        for (Page p : split) {
            builder.add(serde.serialize(context, p));
        }
        return builder.build();
    }
}
