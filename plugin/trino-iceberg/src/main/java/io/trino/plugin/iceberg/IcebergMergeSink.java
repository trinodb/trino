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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.delete.PositionDeleteWriter;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MergePage;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Type;
import org.roaringbitmap.longlong.ImmutableLongBitmapDataProvider;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.spi.connector.MergePage.createDeleteAndInsertPages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class IcebergMergeSink
        implements ConnectorMergeSink
{
    private final LocationProvider locationProvider;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final TrinoFileSystem fileSystem;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final ConnectorSession session;
    private final IcebergFileFormat fileFormat;
    private final Map<String, String> storageProperties;
    private final Schema schema;
    private final Map<Integer, PartitionSpec> partitionsSpecs;
    private final ConnectorPageSink insertPageSink;
    private final int columnCount;
    private final Map<Slice, FileDeletion> fileDeletions = new HashMap<>();

    public IcebergMergeSink(
            LocationProvider locationProvider,
            IcebergFileWriterFactory fileWriterFactory,
            TrinoFileSystem fileSystem,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            IcebergFileFormat fileFormat,
            Map<String, String> storageProperties,
            Schema schema,
            Map<Integer, PartitionSpec> partitionsSpecs,
            ConnectorPageSink insertPageSink,
            int columnCount)
    {
        this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.session = requireNonNull(session, "session is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.storageProperties = ImmutableMap.copyOf(requireNonNull(storageProperties, "storageProperties is null"));
        this.schema = requireNonNull(schema, "schema is null");
        this.partitionsSpecs = ImmutableMap.copyOf(requireNonNull(partitionsSpecs, "partitionsSpecs is null"));
        this.insertPageSink = requireNonNull(insertPageSink, "insertPageSink is null");
        this.columnCount = columnCount;
    }

    @Override
    public void storeMergedRows(Page page)
    {
        MergePage mergePage = createDeleteAndInsertPages(page, columnCount);

        mergePage.getInsertionsPage().ifPresent(insertPageSink::appendPage);

        mergePage.getDeletionsPage().ifPresent(deletions -> {
            List<Block> fields = RowBlock.getRowFieldsFromBlock(deletions.getBlock(deletions.getChannelCount() - 1));
            Block fieldPathBlock = fields.get(0);
            Block rowPositionBlock = fields.get(1);
            Block partitionSpecIdBlock = fields.get(2);
            Block partitionDataBlock = fields.get(3);
            for (int position = 0; position < fieldPathBlock.getPositionCount(); position++) {
                Slice filePath = VarcharType.VARCHAR.getSlice(fieldPathBlock, position);
                long rowPosition = BIGINT.getLong(rowPositionBlock, position);

                int index = position;
                FileDeletion deletion = fileDeletions.computeIfAbsent(filePath, _ -> {
                    int partitionSpecId = INTEGER.getInt(partitionSpecIdBlock, index);
                    String partitionData = VarcharType.VARCHAR.getSlice(partitionDataBlock, index).toStringUtf8();
                    return new FileDeletion(partitionSpecId, partitionData);
                });

                deletion.rowsToDelete().addLong(rowPosition);
            }
        });
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        List<Slice> fragments = new ArrayList<>(insertPageSink.finish().join());

        fileDeletions.forEach((dataFilePath, deletion) -> {
            PositionDeleteWriter writer = createPositionDeleteWriter(
                    dataFilePath.toStringUtf8(),
                    partitionsSpecs.get(deletion.partitionSpecId()),
                    deletion.partitionDataJson());

            fragments.addAll(writePositionDeletes(writer, deletion.rowsToDelete()));
        });

        return completedFuture(fragments);
    }

    @Override
    public void abort()
    {
        insertPageSink.abort();
    }

    private PositionDeleteWriter createPositionDeleteWriter(String dataFilePath, PartitionSpec partitionSpec, String partitionDataJson)
    {
        Optional<PartitionData> partitionData = Optional.empty();
        if (partitionSpec.isPartitioned()) {
            Type[] columnTypes = partitionSpec.fields().stream()
                    .map(field -> field.transform().getResultType(schema.findType(field.sourceId())))
                    .toArray(Type[]::new);
            partitionData = Optional.of(PartitionData.fromJson(partitionDataJson, columnTypes));
        }

        return new PositionDeleteWriter(
                dataFilePath,
                partitionSpec,
                partitionData,
                locationProvider,
                fileWriterFactory,
                fileSystem,
                jsonCodec,
                session,
                fileFormat,
                storageProperties);
    }

    private static Collection<Slice> writePositionDeletes(PositionDeleteWriter writer, ImmutableLongBitmapDataProvider rowsToDelete)
    {
        try {
            return writer.write(rowsToDelete);
        }
        catch (Throwable t) {
            closeAllSuppress(t, writer::abort);
            throw t;
        }
    }

    private static class FileDeletion
    {
        private final int partitionSpecId;
        private final String partitionDataJson;
        private final LongBitmapDataProvider rowsToDelete = new Roaring64Bitmap();

        public FileDeletion(int partitionSpecId, String partitionDataJson)
        {
            this.partitionSpecId = partitionSpecId;
            this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
        }

        public int partitionSpecId()
        {
            return partitionSpecId;
        }

        public String partitionDataJson()
        {
            return partitionDataJson;
        }

        public LongBitmapDataProvider rowsToDelete()
        {
            return rowsToDelete;
        }
    }
}
