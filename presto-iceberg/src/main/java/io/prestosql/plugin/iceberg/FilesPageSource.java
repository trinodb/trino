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
package io.prestosql.plugin.iceberg;

import io.airlift.slice.Slices;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Conversions;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.prestosql.plugin.iceberg.FilesTable.FILE_FORMAT;
import static io.prestosql.plugin.iceberg.FilesTable.FILE_PATH;
import static io.prestosql.plugin.iceberg.FilesTable.FILE_SIZE_IN_BYTES;
import static io.prestosql.plugin.iceberg.FilesTable.KEY_METADATA;
import static io.prestosql.plugin.iceberg.FilesTable.RECORD_COUNT;
import static io.prestosql.plugin.iceberg.FilesTable.SPLIT_OFFSETS;
import static io.prestosql.plugin.iceberg.PartitionTable.convert;
import static io.prestosql.plugin.iceberg.TypeConverter.toIcebergType;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static java.util.stream.Collectors.toList;

public class FilesPageSource
        implements ConnectorPageSource
{
    private FileIO fileIO;
    private List<IcebergColumnHandle> columnHandles;
    private boolean isFinished;
    private final List<Type> columnTypes;
    private final String path;
    private long readTimeNanos;

    public FilesPageSource(FileIO fileIO, List<IcebergColumnHandle> columnHandles, String path)
    {
        this.fileIO = fileIO;
        this.columnHandles = columnHandles;
        this.columnTypes = columnHandles.stream().map(ch -> ch.getType()).collect(toList());
        this.path = path;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return this.isFinished;
    }

    @Override
    public Page getNextPage()
    {
        long start = System.nanoTime();
        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        GenericManifestFile genericManifestFile = new GenericManifestFile(path,
                0, //length is lazily loaded
                -1, //specId
                ManifestContent.DATA, // ManifestContent
                0, //sequenceNumber
                0, //minSequenceNumber
                0L, //snapshotId
                0, //addedFilesCount
                0, //addedRowsCount
                0, //existingFilesCount
                0, //existingRowsCount
                0, //deletedFilesCount
                0, //deletedRowsCount
                null); //partitions
        Iterator<DataFile> dataFileIterator = ManifestFiles.read(genericManifestFile, fileIO).iterator();
        Map<Integer, Object> valueMap = new HashMap<>();
        while (dataFileIterator.hasNext()) {
            DataFile dataFile = dataFileIterator.next();
            valueMap.put(FILE_PATH.getId(), dataFile.path());
            valueMap.put(FILE_FORMAT.getId(), dataFile.format().name());
            valueMap.put(RECORD_COUNT.getId(), dataFile.recordCount());
            valueMap.put(FILE_SIZE_IN_BYTES.getId(), dataFile.fileSizeInBytes());
            valueMap.put(KEY_METADATA.getId(), dataFile.keyMetadata() == null ? null : Slices.wrappedBuffer(dataFile.keyMetadata()));
            valueMap.put(SPLIT_OFFSETS.getId(), dataFile.splitOffsets());
            Map<Integer, Long> columnSizes = dataFile.columnSizes();
            Map<Integer, Long> valueCounts = dataFile.valueCounts();
            Map<Integer, Long> nullValueCounts = dataFile.nullValueCounts();
            Map<Integer, ByteBuffer> lowerBounds = dataFile.lowerBounds();
            Map<Integer, ByteBuffer> upperBounds = dataFile.upperBounds();

            columnHandles.forEach(columnHandle -> {
                Integer id = columnHandle.getId();
                if (id > 0) {
                    Long columnSize = columnSizes != null ? columnSizes.get(id) : null;
                    Long valueCount = valueCounts != null ? valueCounts.get(id) : null;
                    Long nullValueCount = nullValueCounts != null ? nullValueCounts.get(id) : null;
                    ByteBuffer lowerBound = lowerBounds != null ? lowerBounds.get(id) : null;
                    ByteBuffer upperBound = upperBounds != null ? upperBounds.get(id) : null;

                    valueMap.put(id, new ColumnStats(columnSize, valueCount, nullValueCount, lowerBound, upperBound));
                }
            });

            for (int i = 0; i < columnHandles.size(); i++) {
                Object value = valueMap.get(columnHandles.get(i).getId());
                if (value instanceof ColumnStats) {
                    ColumnStats val = (ColumnStats) value;
                    writeRow(val.size, val.valueCount, val.nullCount, val.lowerBound, val.upperBound, (RowType) columnTypes.get(i), pageBuilder.getBlockBuilder(i));
                }
                else if (value instanceof Collection) {
                    writeArray((List) value, columnTypes.get(i), pageBuilder.getBlockBuilder(i));
                }
                else {
                    writeNativeValue(columnTypes.get(i), pageBuilder.getBlockBuilder(i), value);
                }
            }
            pageBuilder.declarePosition();
            valueMap.clear();
        }
        this.readTimeNanos += System.nanoTime() - start;
        this.isFinished = true;
        return pageBuilder.build();
    }

    private void writeRow(Long columnSize, Long valueCount, Long nullValueCount,
            ByteBuffer lowerBound, ByteBuffer upperBound, RowType rowType, BlockBuilder blockBuilder)
    {
        BlockBuilder builder = blockBuilder.beginBlockEntry();
        writeNativeValue(BIGINT, builder, columnSize);
        writeNativeValue(BIGINT, builder, valueCount);
        writeNativeValue(BIGINT, builder, nullValueCount);
        Type columnType = rowType.getFields().get(rowType.getFields().size() - 1).getType();
        org.apache.iceberg.types.Type icebergType = toIcebergType(columnType);
        writeNativeValue(columnType, builder,
                convert(Conversions.fromByteBuffer(icebergType, lowerBound), icebergType));
        writeNativeValue(columnType, builder,
                convert(Conversions.fromByteBuffer(icebergType, upperBound), icebergType));
        blockBuilder.closeEntry();
    }

    private void writeArray(List<?> values, Type type, BlockBuilder blockBuilder)
    {
        if (values == null) {
            blockBuilder.appendNull();
        }
        else {
            BlockBuilder array = blockBuilder.beginBlockEntry();
            for (Object value : values) {
                writeNativeValue(type, array, value);
            }
            blockBuilder.closeEntry();
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
    }

    private class ColumnStats
    {
        private final Long size;
        private final Long valueCount;
        private final Long nullCount;
        private final ByteBuffer lowerBound;
        private final ByteBuffer upperBound;

        private ColumnStats(Long size, Long valueCount, Long nullCount, ByteBuffer lowerBound, ByteBuffer upperBound)
        {
            this.size = size;
            this.valueCount = valueCount;
            this.nullCount = nullCount;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }
    }
}
