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
import io.trino.orc.checkpoint.BooleanStreamCheckpoint;
import io.trino.orc.checkpoint.ByteArrayStreamCheckpoint;
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
import io.trino.orc.stream.PresentOutputStream;
import io.trino.orc.stream.StreamDataOutput;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.orc.stream.LongOutputStream.createLengthOutputStream;
import static java.util.Objects.requireNonNull;

public class SliceDirectColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = instanceSize(SliceDirectColumnWriter.class);
    private final OrcColumnId columnId;
    private final Type type;
    private final boolean compressed;
    private final ColumnEncoding columnEncoding;
    private final LongOutputStream lengthStream;
    private final ByteArrayOutputStream dataStream;
    private final PresentOutputStream presentStream;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();

    private final Supplier<SliceColumnStatisticsBuilder> statisticsBuilderSupplier;
    private SliceColumnStatisticsBuilder statisticsBuilder;

    private boolean closed;

    public SliceDirectColumnWriter(OrcColumnId columnId, Type type, CompressionKind compression, int bufferSize, Supplier<SliceColumnStatisticsBuilder> statisticsBuilderSupplier)
    {
        this.columnId = requireNonNull(columnId, "columnId is null");
        this.type = requireNonNull(type, "type is null");
        this.compressed = requireNonNull(compression, "compression is null") != NONE;
        this.columnEncoding = new ColumnEncoding(DIRECT_V2, 0);
        this.lengthStream = createLengthOutputStream(compression, bufferSize);
        this.dataStream = new ByteArrayOutputStream(compression, bufferSize);
        this.presentStream = new PresentOutputStream(compression, bufferSize);
        this.statisticsBuilderSupplier = statisticsBuilderSupplier;
        statisticsBuilder = statisticsBuilderSupplier.get();
    }

    @Override
    public Map<OrcColumnId, ColumnEncoding> getColumnEncodings()
    {
        return ImmutableMap.of(columnId, columnEncoding);
    }

    @Override
    public void beginRowGroup()
    {
        checkState(!closed);
        presentStream.recordCheckpoint();
        lengthStream.recordCheckpoint();
        dataStream.recordCheckpoint();
    }

    @Override
    public void writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        // record nulls
        for (int position = 0; position < block.getPositionCount(); position++) {
            presentStream.writeBoolean(!block.isNull(position));
        }

        // record values
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                Slice value = type.getSlice(block, position);
                lengthStream.writeLong(value.length());
                dataStream.writeSlice(value);
                statisticsBuilder.addValue(value);
            }
        }
    }

    @Override
    public Map<OrcColumnId, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);

        ColumnStatistics statistics = statisticsBuilder.buildColumnStatistics();
        rowGroupColumnStatistics.add(statistics);

        statisticsBuilder = statisticsBuilderSupplier.get();
        return ImmutableMap.of(columnId, statistics);
    }

    @Override
    public void close()
    {
        checkState(!closed);
        closed = true;
        lengthStream.close();
        dataStream.close();
        presentStream.close();
    }

    @Override
    public Map<OrcColumnId, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        return ImmutableMap.of(columnId, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics));
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(CompressedMetadataWriter metadataWriter)
            throws IOException
    {
        checkState(closed);

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        List<LongStreamCheckpoint> lengthCheckpoints = lengthStream.getCheckpoints();
        List<ByteArrayStreamCheckpoint> dataCheckpoints = dataStream.getCheckpoints();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroupColumnStatistics.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroupColumnStatistics.get(groupId);
            LongStreamCheckpoint lengthCheckpoint = lengthCheckpoints.get(groupId);
            ByteArrayStreamCheckpoint dataCheckpoint = dataCheckpoints.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createSliceColumnPositionList(compressed, lengthCheckpoint, dataCheckpoint, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(columnId, StreamKind.ROW_INDEX, slice.length(), false);
        return ImmutableList.of(new StreamDataOutput(slice, stream));
    }

    private static List<Integer> createSliceColumnPositionList(
            boolean compressed,
            LongStreamCheckpoint lengthCheckpoint,
            ByteArrayStreamCheckpoint dataCheckpoint,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        positionList.addAll(dataCheckpoint.toPositionList(compressed));
        positionList.addAll(lengthCheckpoint.toPositionList(compressed));
        return positionList.build();
    }

    @Override
    public List<StreamDataOutput> getBloomFilters(CompressedMetadataWriter metadataWriter)
            throws IOException
    {
        List<BloomFilter> bloomFilters = rowGroupColumnStatistics.stream()
                .map(ColumnStatistics::getBloomFilter)
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

        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(columnId).ifPresent(outputDataStreams::add);
        outputDataStreams.add(lengthStream.getStreamDataOutput(columnId));
        outputDataStreams.add(dataStream.getStreamDataOutput(columnId));
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return lengthStream.getBufferedBytes() + dataStream.getBufferedBytes() + presentStream.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        long retainedBytes = INSTANCE_SIZE + lengthStream.getRetainedBytes() + dataStream.getRetainedBytes() + presentStream.getRetainedBytes();
        for (ColumnStatistics statistics : rowGroupColumnStatistics) {
            retainedBytes += statistics.getRetainedSizeInBytes();
        }
        return retainedBytes;
    }

    @Override
    public void reset()
    {
        checkState(closed);
        closed = false;
        lengthStream.reset();
        dataStream.reset();
        presentStream.reset();
        rowGroupColumnStatistics.clear();
        statisticsBuilder = statisticsBuilderSupplier.get();
    }
}
