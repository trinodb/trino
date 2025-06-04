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
package io.trino.orc.writer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.array.IntBigArray;
import io.trino.orc.DictionaryCompressionOptimizer.DictionaryColumn;
import io.trino.orc.checkpoint.BooleanStreamCheckpoint;
import io.trino.orc.checkpoint.LongStreamCheckpoint;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.CompressedMetadataWriter;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.RowGroupIndex;
import io.trino.orc.metadata.Stream;
import io.trino.orc.metadata.Stream.StreamKind;
import io.trino.orc.metadata.statistics.BloomFilter;
import io.trino.orc.metadata.statistics.ColumnStatistics;
import io.trino.orc.metadata.statistics.SliceColumnStatisticsBuilder;
import io.trino.orc.stream.ByteArrayOutputStream;
import io.trino.orc.stream.LongOutputStream;
import io.trino.orc.stream.LongOutputStreamV2;
import io.trino.orc.stream.PresentOutputStream;
import io.trino.orc.stream.StreamDataOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrays;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.orc.DictionaryCompressionOptimizer.estimateIndexBytesPerValue;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.orc.metadata.Stream.StreamKind.DATA;
import static io.trino.orc.stream.LongOutputStream.createLengthOutputStream;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class SliceDictionaryColumnWriter
        implements ColumnWriter, DictionaryColumn
{
    private static final int INSTANCE_SIZE = instanceSize(SliceDictionaryColumnWriter.class);
    private static final int DIRECT_CONVERSION_CHUNK_MAX_BYTES = toIntExact(DataSize.of(32, MEGABYTE).toBytes());

    private final OrcColumnId columnId;
    private final Type type;
    private final CompressionKind compression;
    private final int bufferSize;
    private final LongOutputStream dataStream;
    private final PresentOutputStream presentStream;
    private final ByteArrayOutputStream dictionaryDataStream;
    private final LongOutputStream dictionaryLengthStream;

    private final DictionaryBuilder dictionary = new DictionaryBuilder(10000);

    private final List<DictionaryRowGroup> rowGroups = new ArrayList<>();

    private final Supplier<SliceColumnStatisticsBuilder> statisticsBuilderSupplier;

    private IntBigArray values;
    private int rowGroupValueCount;
    private SliceColumnStatisticsBuilder statisticsBuilder;

    private long rawBytes;
    private long totalValueCount;
    private long totalNonNullValueCount;

    private boolean closed;
    private boolean inRowGroup;
    private ColumnEncoding columnEncoding;

    private boolean directEncoded;
    private SliceDirectColumnWriter directColumnWriter;

    public SliceDictionaryColumnWriter(OrcColumnId columnId, Type type, CompressionKind compression, int bufferSize, Supplier<SliceColumnStatisticsBuilder> statisticsBuilderSupplier)
    {
        this.columnId = requireNonNull(columnId, "columnId is null");
        this.type = requireNonNull(type, "type is null");
        this.compression = requireNonNull(compression, "compression is null");
        this.bufferSize = bufferSize;
        this.dataStream = new LongOutputStreamV2(compression, bufferSize, false, DATA);
        this.presentStream = new PresentOutputStream(compression, bufferSize);
        this.dictionaryDataStream = new ByteArrayOutputStream(compression, bufferSize, StreamKind.DICTIONARY_DATA);
        this.dictionaryLengthStream = createLengthOutputStream(compression, bufferSize);
        values = new IntBigArray();
        this.statisticsBuilderSupplier = requireNonNull(statisticsBuilderSupplier, "statisticsBuilderSupplier is null");
        this.statisticsBuilder = statisticsBuilderSupplier.get();
    }

    @Override
    public long getRawBytes()
    {
        checkState(!directEncoded);
        return rawBytes;
    }

    @Override
    public int getDictionaryBytes()
    {
        checkState(!directEncoded);
        return toIntExact(dictionary.getSizeInBytes());
    }

    @Override
    public int getIndexBytes()
    {
        checkState(!directEncoded);
        return toIntExact(estimateIndexBytesPerValue(dictionary.getEntryCount()) * getNonNullValueCount());
    }

    @Override
    public long getValueCount()
    {
        checkState(!directEncoded);
        return totalValueCount;
    }

    @Override
    public long getNonNullValueCount()
    {
        checkState(!directEncoded);
        return totalNonNullValueCount;
    }

    @Override
    public int getDictionaryEntries()
    {
        checkState(!directEncoded);
        return dictionary.getEntryCount();
    }

    @Override
    public OptionalInt tryConvertToDirect(int maxDirectBytes)
    {
        checkState(!closed);
        checkState(!directEncoded);
        if (directColumnWriter == null) {
            directColumnWriter = new SliceDirectColumnWriter(columnId, type, compression, bufferSize, statisticsBuilderSupplier);
        }
        checkState(directColumnWriter.getBufferedBytes() == 0);

        Block dictionaryValues = dictionary.getElementBlock();
        for (DictionaryRowGroup rowGroup : rowGroups) {
            directColumnWriter.beginRowGroup();
            // todo we should be able to pass the stats down to avoid recalculating min and max
            boolean success = writeDictionaryRowGroup(dictionaryValues, rowGroup.getValueCount(), rowGroup.getDictionaryIndexes(), maxDirectBytes);
            directColumnWriter.finishRowGroup();

            if (!success) {
                directColumnWriter.close();
                directColumnWriter.reset();
                return OptionalInt.empty();
            }
        }

        if (inRowGroup) {
            directColumnWriter.beginRowGroup();
            if (!writeDictionaryRowGroup(dictionaryValues, rowGroupValueCount, values, maxDirectBytes)) {
                directColumnWriter.close();
                directColumnWriter.reset();
                return OptionalInt.empty();
            }
        }
        else {
            checkState(rowGroupValueCount == 0);
        }

        rowGroups.clear();

        // free the dictionary
        dictionary.clear();

        rawBytes = 0;
        totalValueCount = 0;
        totalNonNullValueCount = 0;

        rowGroupValueCount = 0;
        statisticsBuilder = statisticsBuilderSupplier.get();

        directEncoded = true;

        return OptionalInt.of(toIntExact(directColumnWriter.getBufferedBytes()));
    }

    private boolean writeDictionaryRowGroup(Block dictionary, int valueCount, IntBigArray dictionaryIndexes, int maxDirectBytes)
    {
        int[][] segments = dictionaryIndexes.getSegments();
        for (int i = 0; valueCount > 0 && i < segments.length; i++) {
            int[] segment = segments[i];
            int positionCount = Math.min(valueCount, segment.length);
            Block block = DictionaryBlock.create(positionCount, dictionary, segment);

            while (block != null) {
                int chunkPositionCount = block.getPositionCount();
                Block chunk = block.getRegion(0, chunkPositionCount);

                // avoid chunk with huge logical size
                while (chunkPositionCount > 1 && chunk.getSizeInBytes() > DIRECT_CONVERSION_CHUNK_MAX_BYTES) {
                    chunkPositionCount /= 2;
                    chunk = chunk.getRegion(0, chunkPositionCount);
                }

                directColumnWriter.writeBlock(chunk);
                if (directColumnWriter.getBufferedBytes() > maxDirectBytes) {
                    return false;
                }

                // slice block to only unconverted rows
                if (chunkPositionCount < block.getPositionCount()) {
                    block = block.getRegion(chunkPositionCount, block.getPositionCount() - chunkPositionCount);
                }
                else {
                    block = null;
                }
            }

            valueCount -= positionCount;
        }
        checkState(valueCount == 0);
        return true;
    }

    @Override
    public Map<OrcColumnId, ColumnEncoding> getColumnEncodings()
    {
        checkState(closed);
        if (directEncoded) {
            return directColumnWriter.getColumnEncodings();
        }
        return ImmutableMap.of(columnId, columnEncoding);
    }

    @Override
    public void beginRowGroup()
    {
        checkState(!inRowGroup);
        inRowGroup = true;

        if (directEncoded) {
            directColumnWriter.beginRowGroup();
        }
    }

    @Override
    public void writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        if (directEncoded) {
            directColumnWriter.writeBlock(block);
            return;
        }

        // record values
        values.ensureCapacity(rowGroupValueCount + block.getPositionCount());
        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        for (int i = 0; i < block.getPositionCount(); i++) {
            int position = block.getUnderlyingValuePosition(i);
            int index = dictionary.putIfAbsent(valueBlock, position);
            values.set(rowGroupValueCount, index);
            rowGroupValueCount++;
            totalValueCount++;

            if (!valueBlock.isNull(position)) {
                // todo min/max statistics only need to be updated if value was not already in the dictionary, but non-null count does
                statisticsBuilder.addValue(type.getSlice(valueBlock, position));

                rawBytes += valueBlock.getSliceLength(position);
                totalNonNullValueCount++;
            }
        }
    }

    @Override
    public Map<OrcColumnId, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);
        checkState(inRowGroup);
        inRowGroup = false;

        if (directEncoded) {
            return directColumnWriter.finishRowGroup();
        }

        ColumnStatistics statistics = statisticsBuilder.buildColumnStatistics();
        rowGroups.add(new DictionaryRowGroup(values, rowGroupValueCount, statistics));
        rowGroupValueCount = 0;
        statisticsBuilder = statisticsBuilderSupplier.get();
        values = new IntBigArray();
        return ImmutableMap.of(columnId, statistics);
    }

    @Override
    public void close()
    {
        checkState(!closed);
        checkState(!inRowGroup);
        closed = true;
        if (directEncoded) {
            directColumnWriter.close();
        }
        else {
            bufferOutputData();
        }
    }

    @Override
    public Map<OrcColumnId, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        if (directEncoded) {
            return directColumnWriter.getColumnStripeStatistics();
        }

        return ImmutableMap.of(columnId, ColumnStatistics.mergeColumnStatistics(rowGroups.stream()
                .map(DictionaryRowGroup::getColumnStatistics)
                .collect(toList())));
    }

    private void bufferOutputData()
    {
        checkState(closed);
        checkState(!directEncoded);

        VariableWidthBlock dictionaryElements = dictionary.getElementBlock();

        // write dictionary in sorted order
        int[] sortedDictionaryIndexes = getSortedDictionaryNullsLast(dictionaryElements);
        for (int sortedDictionaryIndex : sortedDictionaryIndexes) {
            if (!dictionaryElements.isNull(sortedDictionaryIndex)) {
                Slice value = dictionaryElements.getSlice(sortedDictionaryIndex);
                dictionaryLengthStream.writeLong(value.length());
                dictionaryDataStream.writeSlice(value);
            }
        }
        columnEncoding = new ColumnEncoding(DICTIONARY_V2, dictionaryElements.getPositionCount() - 1);

        // build index from original dictionary index to new sorted position
        int[] originalDictionaryToSortedIndex = new int[sortedDictionaryIndexes.length];
        for (int sortOrdinal = 0; sortOrdinal < sortedDictionaryIndexes.length; sortOrdinal++) {
            int dictionaryIndex = sortedDictionaryIndexes[sortOrdinal];
            originalDictionaryToSortedIndex[dictionaryIndex] = sortOrdinal;
        }

        if (!rowGroups.isEmpty()) {
            presentStream.recordCheckpoint();
            dataStream.recordCheckpoint();
        }
        for (DictionaryRowGroup rowGroup : rowGroups) {
            IntBigArray dictionaryIndexes = rowGroup.getDictionaryIndexes();
            for (int position = 0; position < rowGroup.getValueCount(); position++) {
                presentStream.writeBoolean(dictionaryIndexes.get(position) != 0);
            }
            for (int position = 0; position < rowGroup.getValueCount(); position++) {
                int originalDictionaryIndex = dictionaryIndexes.get(position);
                // index zero in original dictionary is reserved for null
                if (originalDictionaryIndex != 0) {
                    int sortedIndex = originalDictionaryToSortedIndex[originalDictionaryIndex];
                    if (sortedIndex < 0) {
                        throw new IllegalArgumentException();
                    }
                    dataStream.writeLong(sortedIndex);
                }
            }
            presentStream.recordCheckpoint();
            dataStream.recordCheckpoint();
        }

        // free the dictionary memory
        dictionary.clear();

        dictionaryDataStream.close();
        dictionaryLengthStream.close();

        dataStream.close();
        presentStream.close();
    }

    private static int[] getSortedDictionaryNullsLast(VariableWidthBlock elementBlock)
    {
        int[] sortedPositions = new int[elementBlock.getPositionCount()];
        for (int i = 0; i < sortedPositions.length; i++) {
            sortedPositions[i] = i;
        }

        Slice rawSlice = elementBlock.getRawSlice();
        IntArrays.quickSort(sortedPositions, 0, sortedPositions.length, (int left, int right) -> {
            boolean nullLeft = elementBlock.isNull(left);
            boolean nullRight = elementBlock.isNull(right);
            if (nullLeft && nullRight) {
                return 0;
            }
            if (nullLeft) {
                return 1;
            }
            if (nullRight) {
                return -1;
            }
            return rawSlice.compareTo(
                    elementBlock.getRawSliceOffset(left),
                    elementBlock.getSliceLength(left),
                    rawSlice,
                    elementBlock.getRawSliceOffset(right),
                    elementBlock.getSliceLength(right));
        });

        return sortedPositions;
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(CompressedMetadataWriter metadataWriter)
            throws IOException
    {
        checkState(closed);

        if (directEncoded) {
            return directColumnWriter.getIndexStreams(metadataWriter);
        }

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        List<LongStreamCheckpoint> dataCheckpoints = dataStream.getCheckpoints();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroups.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroups.get(groupId).getColumnStatistics();
            LongStreamCheckpoint dataCheckpoint = dataCheckpoints.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createSliceColumnPositionList(compression != NONE, dataCheckpoint, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(columnId, StreamKind.ROW_INDEX, slice.length(), false);
        return ImmutableList.of(new StreamDataOutput(slice, stream));
    }

    private static List<Integer> createSliceColumnPositionList(
            boolean compressed,
            LongStreamCheckpoint dataCheckpoint,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        positionList.addAll(dataCheckpoint.toPositionList(compressed));
        return positionList.build();
    }

    @Override
    public List<StreamDataOutput> getBloomFilters(CompressedMetadataWriter metadataWriter)
            throws IOException
    {
        if (directEncoded) {
            return directColumnWriter.getBloomFilters(metadataWriter);
        }

        List<BloomFilter> bloomFilters = rowGroups.stream()
                .map(rowGroup -> rowGroup.getColumnStatistics().getBloomFilter())
                .filter(Objects::nonNull)
                .collect(toImmutableList());

        if (!bloomFilters.isEmpty()) {
            Slice slice = metadataWriter.writeBloomFilters(bloomFilters);
            Stream stream = new Stream(columnId, StreamKind.BLOOM_FILTER_UTF8, slice.length(), false);
            return ImmutableList.of(new StreamDataOutput(slice, stream));
        }

        return ImmutableList.of();
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);

        if (directEncoded) {
            return directColumnWriter.getDataStreams();
        }

        // actually write data
        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(columnId).ifPresent(outputDataStreams::add);
        outputDataStreams.add(dataStream.getStreamDataOutput(columnId));
        outputDataStreams.add(dictionaryLengthStream.getStreamDataOutput(columnId));
        outputDataStreams.add(dictionaryDataStream.getStreamDataOutput(columnId));
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        checkState(!closed);
        if (directEncoded) {
            return directColumnWriter.getBufferedBytes();
        }
        // for dictionary columns we report the data we expect to write to the output stream
        return getIndexBytes() + getDictionaryBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        long retainedBytes = INSTANCE_SIZE +
                values.sizeOf() +
                dataStream.getRetainedBytes() +
                presentStream.getRetainedBytes() +
                dictionaryDataStream.getRetainedBytes() +
                dictionaryLengthStream.getRetainedBytes() +
                dictionary.getRetainedSizeInBytes() +
                (directColumnWriter == null ? 0 : directColumnWriter.getRetainedBytes());

        for (DictionaryRowGroup rowGroup : rowGroups) {
            retainedBytes += rowGroup.getColumnStatistics().getRetainedSizeInBytes();
        }
        return retainedBytes;
    }

    @Override
    public void reset()
    {
        checkState(closed);
        closed = false;
        dataStream.reset();
        presentStream.reset();
        dictionaryDataStream.reset();
        dictionaryLengthStream.reset();
        rowGroups.clear();
        rowGroupValueCount = 0;
        statisticsBuilder = statisticsBuilderSupplier.get();
        columnEncoding = null;

        dictionary.clear();
        rawBytes = 0;
        totalValueCount = 0;
        totalNonNullValueCount = 0;

        if (directEncoded) {
            directEncoded = false;
            directColumnWriter.reset();
        }
    }

    private static class DictionaryRowGroup
    {
        private final IntBigArray dictionaryIndexes;
        private final int valueCount;
        private final ColumnStatistics columnStatistics;

        public DictionaryRowGroup(IntBigArray dictionaryIndexes, int valueCount, ColumnStatistics columnStatistics)
        {
            requireNonNull(dictionaryIndexes, "dictionaryIndexes is null");
            checkArgument(valueCount >= 0, "valueCount is negative");
            requireNonNull(columnStatistics, "columnStatistics is null");

            this.dictionaryIndexes = dictionaryIndexes;
            this.valueCount = valueCount;
            this.columnStatistics = columnStatistics;
        }

        public IntBigArray getDictionaryIndexes()
        {
            return dictionaryIndexes;
        }

        public int getValueCount()
        {
            return valueCount;
        }

        public ColumnStatistics getColumnStatistics()
        {
            return columnStatistics;
        }
    }
}
