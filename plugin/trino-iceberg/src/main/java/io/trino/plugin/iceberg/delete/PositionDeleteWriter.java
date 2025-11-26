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
package io.trino.plugin.iceberg.delete;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergFileWriter;
import io.trino.plugin.iceberg.IcebergFileWriterFactory;
import io.trino.plugin.iceberg.MetricsWrapper;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.io.LocationProvider;
import org.roaringbitmap.longlong.ImmutableLongBitmapDataProvider;

import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class PositionDeleteWriter
{
    private final String dataFilePath;
    private final Block dataFilePathBlock;
    private final PartitionSpec partitionSpec;
    private final Optional<PartitionData> partition;
    private final String outputPath;
    private final IcebergFileWriter writer;
    private final IcebergFileFormat fileFormat;

    public PositionDeleteWriter(
            String dataFilePath,
            PartitionSpec partitionSpec,
            Optional<PartitionData> partition,
            LocationProvider locationProvider,
            IcebergFileWriterFactory fileWriterFactory,
            TrinoFileSystem fileSystem,
            ConnectorSession session,
            IcebergFileFormat fileFormat,
            Map<String, String> storageProperties)
    {
        this.dataFilePath = requireNonNull(dataFilePath, "dataFilePath is null");
        this.dataFilePathBlock = nativeValueToBlock(VARCHAR, utf8Slice(dataFilePath));
        this.partitionSpec = requireNonNull(partitionSpec, "partitionSpec is null");
        this.partition = requireNonNull(partition, "partition is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        // Prepend query ID to the file name, allowing us to determine the files written by a query.
        // This is necessary for opportunistic cleanup of extra files, which may be present for
        // successfully completed queries in the presence of failure recovery mechanisms.
        String fileName = fileFormat.toIceberg().addExtension(session.getQueryId() + "-" + randomUUID());
        this.outputPath = partition
                .map(partitionData -> locationProvider.newDataLocation(partitionSpec, partitionData, fileName))
                .orElseGet(() -> locationProvider.newDataLocation(fileName));
        this.writer = fileWriterFactory.createPositionDeleteWriter(fileSystem, Location.of(outputPath), session, fileFormat, storageProperties);
    }

    public CommitTaskData write(ImmutableLongBitmapDataProvider rowsToDelete)
    {
        writeDeletes(rowsToDelete);
        writer.commit();

        return new CommitTaskData(
                outputPath,
                fileFormat,
                writer.getWrittenBytes(),
                new MetricsWrapper(writer.getFileMetrics().metrics()),
                PartitionSpecParser.toJson(partitionSpec),
                partition.map(PartitionData::toJson),
                FileContent.POSITION_DELETES,
                Optional.of(dataFilePath),
                writer.getFileMetrics().splitOffsets());
    }

    public void abort()
    {
        writer.rollback();
    }

    private void writeDeletes(ImmutableLongBitmapDataProvider rowsToDelete)
    {
        PositionsList deletedPositions = new PositionsList(4 * 1024);
        rowsToDelete.forEach(rowPosition -> {
            deletedPositions.add(rowPosition);
            if (deletedPositions.isFull()) {
                writePage(deletedPositions);
                deletedPositions.reset();
            }
        });

        if (!deletedPositions.isEmpty()) {
            writePage(deletedPositions);
        }
    }

    private void writePage(PositionsList deletedPositions)
    {
        writer.appendRows(new Page(
                deletedPositions.size(),
                RunLengthEncodedBlock.create(dataFilePathBlock, deletedPositions.size()),
                new LongArrayBlock(deletedPositions.size(), Optional.empty(), deletedPositions.elements())));
    }

    // Wrapper around a long[] to provide an effectively final variable for lambda use
    private static class PositionsList
    {
        private long[] positions;
        private int size;

        PositionsList(int initialCapacity)
        {
            this.positions = new long[initialCapacity];
            this.size = 0;
        }

        void add(long position)
        {
            positions[size++] = position;
        }

        boolean isEmpty()
        {
            return size == 0;
        }

        boolean isFull()
        {
            return size == positions.length;
        }

        long[] elements()
        {
            return positions;
        }

        int size()
        {
            return size;
        }

        void reset()
        {
            size = 0;
            positions = new long[positions.length];
        }
    }
}
