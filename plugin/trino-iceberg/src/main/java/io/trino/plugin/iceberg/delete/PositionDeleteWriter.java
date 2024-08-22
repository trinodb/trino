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

import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergFileWriter;
import io.trino.plugin.iceberg.IcebergFileWriterFactory;
import io.trino.plugin.iceberg.MetricsWrapper;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.io.LocationProvider;
import org.roaringbitmap.longlong.ImmutableLongBitmapDataProvider;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
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
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final IcebergFileWriter writer;
    private final IcebergFileFormat fileFormat;

    public PositionDeleteWriter(
            String dataFilePath,
            PartitionSpec partitionSpec,
            Optional<PartitionData> partition,
            LocationProvider locationProvider,
            IcebergFileWriterFactory fileWriterFactory,
            TrinoFileSystem fileSystem,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            IcebergFileFormat fileFormat,
            Map<String, String> storageProperties)
    {
        this.dataFilePath = requireNonNull(dataFilePath, "dataFilePath is null");
        this.dataFilePathBlock = nativeValueToBlock(VARCHAR, utf8Slice(dataFilePath));
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
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

    public Collection<Slice> write(ImmutableLongBitmapDataProvider rowsToDelete)
    {
        writeDeletes(rowsToDelete);
        writer.commit();

        CommitTaskData task = new CommitTaskData(
                outputPath,
                fileFormat,
                writer.getWrittenBytes(),
                new MetricsWrapper(writer.getFileMetrics().metrics()),
                PartitionSpecParser.toJson(partitionSpec),
                partition.map(PartitionData::toJson),
                FileContent.POSITION_DELETES,
                Optional.of(dataFilePath),
                writer.getFileMetrics().splitOffsets());

        return List.of(wrappedBuffer(jsonCodec.toJsonBytes(task)));
    }

    public void abort()
    {
        writer.rollback();
    }

    private void writeDeletes(ImmutableLongBitmapDataProvider rowsToDelete)
    {
        PageBuilder pageBuilder = new PageBuilder(List.of(BIGINT));

        rowsToDelete.forEach(rowPosition -> {
            pageBuilder.declarePosition();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), rowPosition);
            if (pageBuilder.isFull()) {
                writePage(pageBuilder.build());
                pageBuilder.reset();
            }
        });

        if (!pageBuilder.isEmpty()) {
            writePage(pageBuilder.build());
        }
    }

    private void writePage(Page page)
    {
        writer.appendRows(new Page(
                RunLengthEncodedBlock.create(dataFilePathBlock, page.getPositionCount()),
                page.getBlock(0)));
    }
}
