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
import io.trino.orc.checkpoint.LongStreamCheckpoint;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.CompressedMetadataWriter;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.RowGroupIndex;
import io.trino.orc.metadata.Stream;
import io.trino.orc.metadata.Stream.StreamKind;
import io.trino.orc.metadata.statistics.ColumnStatistics;
import io.trino.orc.stream.LongOutputStream;
import io.trino.orc.stream.PresentOutputStream;
import io.trino.orc.stream.StreamDataOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.orc.stream.LongOutputStream.createLengthOutputStream;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static java.util.Objects.requireNonNull;

public class ListColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = instanceSize(ListColumnWriter.class);
    private final OrcColumnId columnId;
    private final boolean compressed;
    private final ColumnEncoding columnEncoding;
    private final LongOutputStream lengthStream;
    private final PresentOutputStream presentStream;
    private final ColumnWriter elementWriter;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();

    private int nonNullValueCount;

    private boolean closed;

    public ListColumnWriter(OrcColumnId columnId, CompressionKind compression, int bufferSize, ColumnWriter elementWriter)
    {
        this.columnId = requireNonNull(columnId, "columnId is null");
        this.compressed = requireNonNull(compression, "compression is null") != NONE;
        this.columnEncoding = new ColumnEncoding(DIRECT_V2, 0);
        this.elementWriter = requireNonNull(elementWriter, "elementWriter is null");
        this.lengthStream = createLengthOutputStream(compression, bufferSize);
        this.presentStream = new PresentOutputStream(compression, bufferSize);
    }

    @Override
    public List<ColumnWriter> getNestedColumnWriters()
    {
        return ImmutableList.<ColumnWriter>builder()
                .add(elementWriter)
                .addAll(elementWriter.getNestedColumnWriters())
                .build();
    }

    @Override
    public Map<OrcColumnId, ColumnEncoding> getColumnEncodings()
    {
        ImmutableMap.Builder<OrcColumnId, ColumnEncoding> encodings = ImmutableMap.builder();
        encodings.put(columnId, columnEncoding);
        encodings.putAll(elementWriter.getColumnEncodings());
        return encodings.buildOrThrow();
    }

    @Override
    public void beginRowGroup()
    {
        lengthStream.recordCheckpoint();
        presentStream.recordCheckpoint();

        elementWriter.beginRowGroup();
    }

    @Override
    public void writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        ColumnarArray columnarArray = toColumnarArray(block);
        writeColumnarArray(columnarArray);
    }

    private void writeColumnarArray(ColumnarArray columnarArray)
    {
        // write nulls and lengths
        for (int position = 0; position < columnarArray.getPositionCount(); position++) {
            boolean present = !columnarArray.isNull(position);
            presentStream.writeBoolean(present);
            if (present) {
                nonNullValueCount++;
                lengthStream.writeLong(columnarArray.getLength(position));
            }
        }

        // write element values
        Block elementsBlock = columnarArray.getElementsBlock();
        if (elementsBlock.getPositionCount() > 0) {
            elementWriter.writeBlock(elementsBlock);
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
        columnStatistics.putAll(elementWriter.finishRowGroup());
        return columnStatistics.buildOrThrow();
    }

    @Override
    public void close()
    {
        closed = true;
        elementWriter.close();
        lengthStream.close();
        presentStream.close();
    }

    @Override
    public Map<OrcColumnId, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        ImmutableMap.Builder<OrcColumnId, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        columnStatistics.put(columnId, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics));
        columnStatistics.putAll(elementWriter.getColumnStripeStatistics());
        return columnStatistics.buildOrThrow();
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(CompressedMetadataWriter metadataWriter)
            throws IOException
    {
        checkState(closed);

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        List<LongStreamCheckpoint> lengthCheckpoints = lengthStream.getCheckpoints();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroupColumnStatistics.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroupColumnStatistics.get(groupId);
            LongStreamCheckpoint lengthCheckpoint = lengthCheckpoints.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createArrayColumnPositionList(compressed, lengthCheckpoint, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(columnId, StreamKind.ROW_INDEX, slice.length(), false);

        ImmutableList.Builder<StreamDataOutput> indexStreams = ImmutableList.builder();
        indexStreams.add(new StreamDataOutput(slice, stream));
        indexStreams.addAll(elementWriter.getIndexStreams(metadataWriter));
        indexStreams.addAll(elementWriter.getBloomFilters(metadataWriter));
        return indexStreams.build();
    }

    private static List<Integer> createArrayColumnPositionList(
            boolean compressed,
            LongStreamCheckpoint lengthCheckpoint,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        positionList.addAll(lengthCheckpoint.toPositionList(compressed));
        return positionList.build();
    }

    @Override
    public List<StreamDataOutput> getBloomFilters(CompressedMetadataWriter metadataWriter)
            throws IOException
    {
        return ImmutableList.of();
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);

        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(columnId).ifPresent(outputDataStreams::add);
        outputDataStreams.add(lengthStream.getStreamDataOutput(columnId));
        outputDataStreams.addAll(elementWriter.getDataStreams());
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return lengthStream.getBufferedBytes() + presentStream.getBufferedBytes() + elementWriter.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        long retainedBytes = INSTANCE_SIZE + lengthStream.getRetainedBytes() + presentStream.getRetainedBytes() + elementWriter.getRetainedBytes();
        for (ColumnStatistics statistics : rowGroupColumnStatistics) {
            retainedBytes += statistics.getRetainedSizeInBytes();
        }
        return retainedBytes;
    }

    @Override
    public void reset()
    {
        closed = false;
        lengthStream.reset();
        presentStream.reset();
        elementWriter.reset();
        rowGroupColumnStatistics.clear();
        nonNullValueCount = 0;
    }
}
