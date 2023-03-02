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
import io.trino.orc.checkpoint.DecimalStreamCheckpoint;
import io.trino.orc.checkpoint.LongStreamCheckpoint;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.CompressedMetadataWriter;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.RowGroupIndex;
import io.trino.orc.metadata.Stream;
import io.trino.orc.metadata.Stream.StreamKind;
import io.trino.orc.metadata.statistics.ColumnStatistics;
import io.trino.orc.metadata.statistics.LongDecimalStatisticsBuilder;
import io.trino.orc.metadata.statistics.ShortDecimalStatisticsBuilder;
import io.trino.orc.stream.DecimalOutputStream;
import io.trino.orc.stream.LongOutputStream;
import io.trino.orc.stream.LongOutputStreamV2;
import io.trino.orc.stream.PresentOutputStream;
import io.trino.orc.stream.StreamDataOutput;
import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.orc.metadata.Stream.StreamKind.SECONDARY;
import static java.util.Objects.requireNonNull;

public class DecimalColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = instanceSize(DecimalColumnWriter.class);
    private final OrcColumnId columnId;
    private final DecimalType type;
    private final ColumnEncoding columnEncoding;
    private final boolean compressed;
    private final DecimalOutputStream dataStream;
    private final LongOutputStream scaleStream;
    private final PresentOutputStream presentStream;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();

    private ShortDecimalStatisticsBuilder shortDecimalStatisticsBuilder;
    private LongDecimalStatisticsBuilder longDecimalStatisticsBuilder;

    private boolean closed;

    public DecimalColumnWriter(OrcColumnId columnId, Type type, CompressionKind compression, int bufferSize)
    {
        this.columnId = requireNonNull(columnId, "columnId is null");
        this.type = (DecimalType) requireNonNull(type, "type is null");
        this.compressed = requireNonNull(compression, "compression is null") != NONE;
        this.columnEncoding = new ColumnEncoding(DIRECT_V2, 0);
        this.dataStream = new DecimalOutputStream(compression, bufferSize);
        this.scaleStream = new LongOutputStreamV2(compression, bufferSize, true, SECONDARY);
        this.presentStream = new PresentOutputStream(compression, bufferSize);
        if (this.type.isShort()) {
            shortDecimalStatisticsBuilder = new ShortDecimalStatisticsBuilder(this.type.getScale());
        }
        else {
            longDecimalStatisticsBuilder = new LongDecimalStatisticsBuilder();
        }
    }

    @Override
    public Map<OrcColumnId, ColumnEncoding> getColumnEncodings()
    {
        return ImmutableMap.of(columnId, columnEncoding);
    }

    @Override
    public void beginRowGroup()
    {
        presentStream.recordCheckpoint();
        dataStream.recordCheckpoint();
        scaleStream.recordCheckpoint();
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
        if (type.isShort()) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (!block.isNull(position)) {
                    long value = type.getLong(block, position);
                    dataStream.writeUnscaledValue(value);
                    shortDecimalStatisticsBuilder.addValue(value);
                }
            }
        }
        else {
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (!block.isNull(position)) {
                    Int128 value = (Int128) type.getObject(block, position);
                    dataStream.writeUnscaledValue(value);

                    longDecimalStatisticsBuilder.addValue(new BigDecimal(((Int128) value).toBigInteger(), type.getScale()));
                }
            }
        }
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                scaleStream.writeLong(type.getScale());
            }
        }
    }

    @Override
    public Map<OrcColumnId, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);
        ColumnStatistics statistics;
        if (type.isShort()) {
            statistics = shortDecimalStatisticsBuilder.buildColumnStatistics();
            shortDecimalStatisticsBuilder = new ShortDecimalStatisticsBuilder(type.getScale());
        }
        else {
            statistics = longDecimalStatisticsBuilder.buildColumnStatistics();
            longDecimalStatisticsBuilder = new LongDecimalStatisticsBuilder();
        }
        rowGroupColumnStatistics.add(statistics);

        return ImmutableMap.of(columnId, statistics);
    }

    @Override
    public void close()
    {
        closed = true;
        dataStream.close();
        scaleStream.close();
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

        List<DecimalStreamCheckpoint> dataCheckpoints = dataStream.getCheckpoints();
        List<LongStreamCheckpoint> scaleCheckpoints = scaleStream.getCheckpoints();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroupColumnStatistics.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroupColumnStatistics.get(groupId);
            DecimalStreamCheckpoint dataCheckpoint = dataCheckpoints.get(groupId);
            LongStreamCheckpoint scaleCheckpoint = scaleCheckpoints.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createDecimalColumnPositionList(compressed, dataCheckpoint, scaleCheckpoint, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(columnId, StreamKind.ROW_INDEX, slice.length(), false);
        return ImmutableList.of(new StreamDataOutput(slice, stream));
    }

    @Override
    public List<StreamDataOutput> getBloomFilters(CompressedMetadataWriter metadataWriter)
    {
        return ImmutableList.of();
    }

    private static List<Integer> createDecimalColumnPositionList(
            boolean compressed,
            DecimalStreamCheckpoint dataCheckpoint,
            LongStreamCheckpoint scaleCheckpoint,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        positionList.addAll(dataCheckpoint.toPositionList(compressed));
        positionList.addAll(scaleCheckpoint.toPositionList(compressed));
        return positionList.build();
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);

        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(columnId).ifPresent(outputDataStreams::add);
        outputDataStreams.add(dataStream.getStreamDataOutput(columnId));
        outputDataStreams.add(scaleStream.getStreamDataOutput(columnId));
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return dataStream.getBufferedBytes() + scaleStream.getBufferedBytes() + presentStream.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        long retainedBytes = INSTANCE_SIZE + dataStream.getRetainedBytes() + scaleStream.getRetainedBytes() + presentStream.getRetainedBytes();
        for (ColumnStatistics statistics : rowGroupColumnStatistics) {
            retainedBytes += statistics.getRetainedSizeInBytes();
        }
        return retainedBytes;
    }

    @Override
    public void reset()
    {
        closed = false;
        dataStream.reset();
        scaleStream.reset();
        presentStream.reset();
        rowGroupColumnStatistics.clear();
        shortDecimalStatisticsBuilder = new ShortDecimalStatisticsBuilder(this.type.getScale());
        longDecimalStatisticsBuilder = new LongDecimalStatisticsBuilder();
    }
}
