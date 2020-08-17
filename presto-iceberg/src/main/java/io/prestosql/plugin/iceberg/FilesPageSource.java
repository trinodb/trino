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
import io.prestosql.plugin.iceberg.util.PageListBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.io.FileIO;

import java.util.Iterator;
import java.util.List;

import static io.prestosql.plugin.iceberg.PartitionTable.convert;
import static io.prestosql.plugin.iceberg.TypeConverter.toIcebergType;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static java.util.stream.Collectors.toList;
import static org.apache.iceberg.types.Conversions.fromByteBuffer;

public class FilesPageSource
        implements ConnectorPageSource
{
    private FileIO fileIO;
    private List<IcebergColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final String path;
    private long readTimeNanos;
    private Iterator<Page> pages;

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
        return pages != null && !pages.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        if (pages != null && pages.hasNext()) {
            return pages.next();
        }

        long start = System.nanoTime();
        PageListBuilder pageListBuilder = new PageListBuilder(columnTypes);

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

        while (dataFileIterator.hasNext()) {
            DataFile dataFile = dataFileIterator.next();

            pageListBuilder.beginRow();
            pageListBuilder.appendVarchar(dataFile.path().toString());
            pageListBuilder.appendVarchar(dataFile.format().name());
            pageListBuilder.appendBigint(dataFile.recordCount());
            pageListBuilder.appendBigint(dataFile.fileSizeInBytes());
            pageListBuilder.appendNull();
            pageListBuilder.appendNull();
            pageListBuilder.appendVarbinary(dataFile.keyMetadata() == null ? null : Slices.wrappedBuffer(dataFile.keyMetadata()));
            pageListBuilder.appendBigintArray(dataFile.splitOffsets());

            columnHandles.forEach(columnHandle -> {
                Integer id = columnHandle.getId();
                if (id > 0) {
                    final BlockBuilder rowBuilder = pageListBuilder.nextColumn();
                    final BlockBuilder singleRowBlockBuilder = rowBuilder.beginBlockEntry();
                    writeNativeValue(BIGINT, singleRowBlockBuilder, dataFile.columnSizes() != null ? dataFile.columnSizes().get(id) : null);
                    writeNativeValue(BIGINT, singleRowBlockBuilder, dataFile.valueCounts() != null ? dataFile.valueCounts().get(id) : null);
                    writeNativeValue(BIGINT, singleRowBlockBuilder, dataFile.nullValueCounts() != null ? dataFile.nullValueCounts().get(id) : null);

                    // actual primitive Type for bounds
                    final Type type = ((RowType) columnHandle.getType()).getFields().get(4).getType();
                    org.apache.iceberg.types.Type icebergType = toIcebergType(type);
                    if (icebergType.isPrimitiveType() && dataFile.lowerBounds() != null && dataFile.upperBounds() != null) {
                        final Object lowerBound = convert(fromByteBuffer(icebergType, dataFile.lowerBounds().get(id)), icebergType);
                        writeNativeValue(type, singleRowBlockBuilder, lowerBound);
                        final Object upperBound = convert(fromByteBuffer(icebergType, dataFile.upperBounds().get(id)), icebergType);
                        writeNativeValue(type, singleRowBlockBuilder, upperBound);
                    }
                    else {
                        singleRowBlockBuilder.appendNull();
                        singleRowBlockBuilder.appendNull();
                    }
                    rowBuilder.closeEntry();
                }
            });
            pageListBuilder.endRow();
        }

        this.readTimeNanos += System.nanoTime() - start;
        this.pages = pageListBuilder.build().iterator();
        return pages.next();
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
}
