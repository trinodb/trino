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
package io.trino.lance.file;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.CheckReturnValue;
import io.airlift.slice.Slice;
import io.trino.lance.file.v2.metadata.ColumnMetadata;
import io.trino.lance.file.v2.metadata.DiskRange;
import io.trino.lance.file.v2.metadata.Field;
import io.trino.lance.file.v2.metadata.FileVersion;
import io.trino.lance.file.v2.metadata.Footer;
import io.trino.lance.file.v2.metadata.LanceTypeUtil;
import io.trino.lance.file.v2.reader.ColumnReader;
import io.trino.lance.file.v2.reader.Range;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.SourcePage;
import jakarta.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ObjLongConsumer;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.lance.file.v2.metadata.DiskRange.BUFFER_DESCRIPTOR_SIZE;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkIndex;

public class LanceReader
        implements Closeable
{
    public static final int FOOTER_LEN = 40;
    public static final int MAX_BATCH_SIZE = 8 * 1024;

    private final Footer footer;
    private final FileVersion fileVersion;
    private final Map<Integer, ColumnMetadata> columnMetadata;
    private final List<Field> fields;
    private final ColumnReader[] columnReaders;
    private final long numRows;

    private int currentPageId;
    private long currentRowId;
    private long currentBatchSize;

    public LanceReader(LanceDataSource dataSource,
            List<Integer> columnIds,
            Optional<List<Range>> requestRanges,
            AggregatedMemoryContext memoryUsage)
            throws IOException
    {
        // read footer
        Slice data = dataSource.readTail(FOOTER_LEN);
        this.footer = Footer.from(data);
        this.fileVersion = FileVersion.fromMajorMinor(footer.getMajorVersion(), footer.getMinorVersion());

        // read Global Buffer Offset Table
        Slice bufferOffsetTableSlice = dataSource.readFully(footer.getGlobalBuffOffsetStart(), footer.getNumGlobalBuffers() * BUFFER_DESCRIPTOR_SIZE);
        List<DiskRange> bufferOffsets = IntStream.range(0, footer.getNumGlobalBuffers()).boxed()
                .map(i -> new DiskRange(bufferOffsetTableSlice.getLong(BUFFER_DESCRIPTOR_SIZE * i), bufferOffsetTableSlice.getLong(BUFFER_DESCRIPTOR_SIZE * i + 8)))
                .collect(toImmutableList());
        if (bufferOffsets.size() == 0) {
            throw new RuntimeException("File did not contain any buffers");
        }

        // read global schema
        DiskRange schemaBufferLocation = bufferOffsets.get(0);
        // prefetch all metadata
        Slice metadataSlice = dataSource.readTail(toIntExact(dataSource.getEstimatedSize() - schemaBufferLocation.position()));
        // read file descriptor
        Slice schemaSlice = metadataSlice.slice(0, toIntExact(schemaBufferLocation.length()));
        build.buf.gen.lance.file.FileDescriptor fileDescriptor = build.buf.gen.lance.file.FileDescriptor.parseFrom(schemaSlice.toByteBuffer());
        checkArgument(fileDescriptor.hasSchema(), "FileDescriptor does not contain a schema");
        this.fields = toFields(fileDescriptor.getSchema());
        List<Range> ranges = requestRanges.orElse(ImmutableList.of(new Range(0, fileDescriptor.getLength())));
        this.numRows = ranges.stream()
                .mapToLong(Range::length)
                .sum();
        // read Column Metadata Offset Table
        Slice columnMetadataOffsetsSlice = metadataSlice.slice(toIntExact(footer.getColumnMetadataOffsetsStart() - schemaBufferLocation.position()), toIntExact(footer.getGlobalBuffOffsetStart() - footer.getColumnMetadataOffsetsStart()));
        List<DiskRange> columnMetadataOffsets = IntStream.range(0, footer.getNumColumns()).boxed()
                .map(i -> {
                    long position = columnMetadataOffsetsSlice.getLong(i * BUFFER_DESCRIPTOR_SIZE);
                    return new DiskRange(position, columnMetadataOffsetsSlice.getLong(i * BUFFER_DESCRIPTOR_SIZE + 8));
                })
                .collect(toImmutableList());

        // read Column Metadata
        Slice columnMetadataSlice = metadataSlice.slice(toIntExact(footer.getColumnMetadataStart() - schemaBufferLocation.position()), toIntExact(footer.getColumnMetadataOffsetsStart() - footer.getColumnMetadataStart()));
        List<ColumnMetadata> metadata = IntStream.range(0, footer.getNumColumns()).boxed()
                .map(i -> {
                    DiskRange offset = columnMetadataOffsets.get(i);
                    Slice message = columnMetadataSlice.slice(toIntExact(offset.position() - footer.getColumnMetadataStart()), toIntExact(offset.length()));
                    return ColumnMetadata.from(i, message);
                })
                .collect(toImmutableList());

        Map<Integer, Integer> fieldIdMap = LanceTypeUtil.visit(fields, new LanceTypeUtil.FieldIdToColumnIndexVisitor());
        this.columnMetadata = fieldIdMap.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> metadata.get(entry.getValue())));

        this.columnReaders = fields.stream()
                .filter(field -> columnIds.contains(field.id()))
                .map(field ->
                        ColumnReader.createColumnReader(
                                dataSource,
                                field,
                                columnMetadata,
                                ranges,
                                memoryUsage))
                .collect(toImmutableList())
                .toArray(ColumnReader[]::new);
    }

    public static List<Field> toFields(build.buf.gen.lance.file.Schema schema)
    {
        return toFields(schema.getFieldsList());
    }

    public static List<Field> toFields(List<build.buf.gen.lance.file.Field> fieldsProto)
    {
        Map<Integer, Field> fieldMap = Maps.newHashMapWithExpectedSize(fieldsProto.size());
        for (build.buf.gen.lance.file.Field proto : fieldsProto) {
            fieldMap.put(proto.getId(), Field.fromProto(proto));
        }
        List<Field> fields = new ArrayList<>();
        for (Map.Entry<Integer, Field> entry : fieldMap.entrySet()) {
            int parentId = fieldMap.get(entry.getKey()).parentId();
            Field field = entry.getValue();
            if (parentId == -1) {
                fields.add(field);
            }
            else {
                fieldMap.get(parentId).addChild(field);
            }
        }
        return fields;
    }

    public SourcePage nextSourcePage()
    {
        currentRowId += currentBatchSize;
        currentBatchSize = 0;

        // return null if no more rows
        if (currentRowId >= numRows) {
            return null;
        }

        // TODO: add exponential growth of batch size to unblock consumer faster
        currentBatchSize = min(MAX_BATCH_SIZE, numRows - currentRowId);
        for (ColumnReader reader : columnReaders) {
            if (reader != null) {
                reader.prepareNextRead(toIntExact(currentBatchSize));
            }
        }

        currentPageId++;
        return new LanceSourcePage(toIntExact(currentBatchSize));
    }

    public List<Field> getFields()
    {
        return fields;
    }

    public Footer getFooter()
    {
        return footer;
    }

    @Override
    public void close()
            throws IOException
    {
    }

    public FileVersion getFileVersion()
    {
        return fileVersion;
    }

    private record SelectedPositions(int positionCount, @Nullable int[] positions)
    {
        @CheckReturnValue
        public Block apply(Block block)
        {
            if (positions == null) {
                return block;
            }
            return block.getPositions(positions, 0, positionCount);
        }

        public Block createRowNumberBlock(long filePosition)
        {
            long[] rowNumbers = new long[positionCount];
            for (int i = 0; i < positionCount; i++) {
                int position = positions == null ? i : positions[i];
                rowNumbers[i] = filePosition + position;
            }
            return new LongArrayBlock(positionCount, Optional.empty(), rowNumbers);
        }

        @CheckReturnValue
        public SelectedPositions selectPositions(int[] positions, int offset, int size)
        {
            if (this.positions == null) {
                for (int i = 0; i < size; i++) {
                    checkIndex(offset + i, positionCount);
                }
                return new SelectedPositions(size, Arrays.copyOfRange(positions, offset, offset + size));
            }

            int[] newPositions = new int[size];
            for (int i = 0; i < size; i++) {
                newPositions[i] = this.positions[positions[offset + i]];
            }
            return new SelectedPositions(size, newPositions);
        }
    }

    private class LanceSourcePage
            implements SourcePage
    {
        private final int expectedPageId = currentPageId;
        private final Block[] blocks = new Block[columnReaders.length];
        private SelectedPositions selectedPositions;
        private long sizeInBytes;
        private long retainedSizeInBytes;

        public LanceSourcePage(int positionCount)
        {
            selectedPositions = new SelectedPositions(positionCount, null);
        }

        @Override
        public int getPositionCount()
        {
            return selectedPositions.positionCount();
        }

        @Override
        public long getSizeInBytes()
        {
            return sizeInBytes;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return retainedSizeInBytes;
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            for (Block block : blocks) {
                if (block != null) {
                    block.retainedBytesForEachPart(consumer);
                }
            }
        }

        @Override
        public int getChannelCount()
        {
            return blocks.length;
        }

        @Override
        public Block getBlock(int channel)
        {
            checkState(currentPageId == expectedPageId);
            checkIndex(channel, blocks.length);

            Block block = blocks[channel];
            if (block == null) {
                block = columnReaders[channel].read().block();
                block = selectedPositions.apply(block);
            }
            blocks[channel] = block;
            sizeInBytes += block.getSizeInBytes();
            retainedSizeInBytes += block.getRetainedSizeInBytes();
            return block;
        }

        @Override
        public Page getPage()
        {
            for (int i = 0; i < blocks.length; i++) {
                getBlock(i);
            }
            return new Page(selectedPositions.positionCount(), blocks);
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            selectedPositions = selectedPositions.selectPositions(positions, offset, size);
            retainedSizeInBytes = 0;
            for (int i = 0; i < blocks.length; i++) {
                Block block = blocks[i];
                if (block != null) {
                    block = selectedPositions.apply(block);
                    retainedSizeInBytes += block.getRetainedSizeInBytes();
                    blocks[i] = block;
                }
            }
        }
    }
}
