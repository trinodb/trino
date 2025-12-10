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
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.CompressedMetadataWriter;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.RowGroupIndex;
import io.trino.orc.metadata.Stream;
import io.trino.orc.metadata.Stream.StreamKind;
import io.trino.orc.metadata.statistics.ColumnStatistics;
import io.trino.orc.stream.PresentOutputStream;
import io.trino.orc.stream.StreamDataOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariantBlock;
import io.trino.spi.block.VariantBlock.VariantNestedBlocks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static java.util.Objects.requireNonNull;

/**
 * Writes a variant column to ORC.
 * Variant is stored as a struct with two binary fields: metadata and value.
 */
public class VariantColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = instanceSize(VariantColumnWriter.class);
    private static final ColumnEncoding COLUMN_ENCODING = new ColumnEncoding(DIRECT, 0);

    private final OrcColumnId columnId;
    private final boolean compressed;
    private final PresentOutputStream presentStream;
    private final ColumnWriter metadataWriter;
    private final ColumnWriter valueWriter;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();

    private int nonNullValueCount;

    private boolean closed;

    public VariantColumnWriter(OrcColumnId columnId, CompressionKind compression, int bufferSize, ColumnWriter metadataWriter, ColumnWriter valueWriter)
    {
        this.columnId = columnId;
        this.compressed = requireNonNull(compression, "compression is null") != NONE;
        this.metadataWriter = requireNonNull(metadataWriter, "metadataWriter is null");
        this.valueWriter = requireNonNull(valueWriter, "valueWriter is null");
        this.presentStream = new PresentOutputStream(compression, bufferSize);
    }

    @Override
    public List<ColumnWriter> getNestedColumnWriters()
    {
        return ImmutableList.<ColumnWriter>builder()
                .add(metadataWriter)
                .addAll(metadataWriter.getNestedColumnWriters())
                .add(valueWriter)
                .addAll(valueWriter.getNestedColumnWriters())
                .build();
    }

    @Override
    public Map<OrcColumnId, ColumnEncoding> getColumnEncodings()
    {
        ImmutableMap.Builder<OrcColumnId, ColumnEncoding> encodings = ImmutableMap.builder();
        encodings.put(columnId, COLUMN_ENCODING);
        encodings.putAll(metadataWriter.getColumnEncodings());
        encodings.putAll(valueWriter.getColumnEncodings());
        return encodings.buildOrThrow();
    }

    @Override
    public void beginRowGroup()
    {
        presentStream.recordCheckpoint();
        metadataWriter.beginRowGroup();
        valueWriter.beginRowGroup();
    }

    @Override
    public void writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        // record nulls
        for (int position = 0; position < block.getPositionCount(); position++) {
            boolean present = !block.isNull(position);
            presentStream.writeBoolean(present);
            if (present) {
                nonNullValueCount++;
            }
        }

        // write null-suppressed field values
        VariantNestedBlocks nested = VariantBlock.getNullSuppressedNestedFields(block);
        Block metadataBlock = nested.metadataBlock();
        Block valueBlock = nested.valueBlock();

        if (metadataBlock.getPositionCount() > 0) {
            metadataWriter.writeBlock(metadataBlock);
            valueWriter.writeBlock(valueBlock);
        }
    }

    @Override
    public Map<OrcColumnId, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);
        ColumnStatistics statistics = new ColumnStatistics((long) nonNullValueCount, 0, null, null, null, null, null, null, null, null, null, null);
        rowGroupColumnStatistics.add(statistics);
        nonNullValueCount = 0;

        ImmutableMap.Builder<OrcColumnId, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        columnStatistics.put(columnId, statistics);
        columnStatistics.putAll(metadataWriter.finishRowGroup());
        columnStatistics.putAll(valueWriter.finishRowGroup());
        return columnStatistics.buildOrThrow();
    }

    @Override
    public void close()
    {
        closed = true;
        metadataWriter.close();
        valueWriter.close();
        presentStream.close();
    }

    @Override
    public Map<OrcColumnId, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        ImmutableMap.Builder<OrcColumnId, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        columnStatistics.put(columnId, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics));
        columnStatistics.putAll(metadataWriter.getColumnStripeStatistics());
        columnStatistics.putAll(valueWriter.getColumnStripeStatistics());
        return columnStatistics.buildOrThrow();
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(CompressedMetadataWriter metadataWriter)
            throws IOException
    {
        checkState(closed);

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroupColumnStatistics.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroupColumnStatistics.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createVariantColumnPositionList(compressed, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(columnId, StreamKind.ROW_INDEX, slice.length(), false);

        ImmutableList.Builder<StreamDataOutput> indexStreams = ImmutableList.builder();
        indexStreams.add(new StreamDataOutput(slice, stream));
        indexStreams.addAll(this.metadataWriter.getIndexStreams(metadataWriter));
        indexStreams.addAll(this.metadataWriter.getBloomFilters(metadataWriter));
        indexStreams.addAll(this.valueWriter.getIndexStreams(metadataWriter));
        indexStreams.addAll(this.valueWriter.getBloomFilters(metadataWriter));
        return indexStreams.build();
    }

    @Override
    public List<StreamDataOutput> getBloomFilters(CompressedMetadataWriter metadataWriter)
    {
        return ImmutableList.of();
    }

    private static List<Integer> createVariantColumnPositionList(
            boolean compressed,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        return positionList.build();
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);

        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(columnId).ifPresent(outputDataStreams::add);
        outputDataStreams.addAll(metadataWriter.getDataStreams());
        outputDataStreams.addAll(valueWriter.getDataStreams());
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return presentStream.getBufferedBytes() + metadataWriter.getBufferedBytes() + valueWriter.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        long retainedBytes = INSTANCE_SIZE + presentStream.getRetainedBytes();
        retainedBytes += metadataWriter.getRetainedBytes();
        retainedBytes += valueWriter.getRetainedBytes();
        for (ColumnStatistics statistics : rowGroupColumnStatistics) {
            retainedBytes += statistics.getRetainedSizeInBytes();
        }
        return retainedBytes;
    }

    @Override
    public void reset()
    {
        closed = false;
        presentStream.reset();
        metadataWriter.reset();
        valueWriter.reset();
        rowGroupColumnStatistics.clear();
        nonNullValueCount = 0;
    }
}
