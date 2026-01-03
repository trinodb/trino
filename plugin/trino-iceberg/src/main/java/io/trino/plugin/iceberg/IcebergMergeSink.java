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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.delete.DeletionVector;
import io.trino.plugin.iceberg.delete.PositionDeleteWriter;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class IcebergMergeSink
        implements ConnectorMergeSink
{
    private final int formatVersion;
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
    private long writtenBytes;

    public IcebergMergeSink(
            int formatVersion,
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
        this.formatVersion = formatVersion;
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
        MergePage mergePage = MergePage.createMergePage(page, columnCount, formatVersion >= 3);

        mergePage.removals().ifPresent(this::processRemovals);
        mergePage.additions().ifPresent(insertPageSink::appendPage);

        writtenBytes = insertPageSink.getCompletedBytes();
    }

    private void processRemovals(Page removals)
    {
        List<Block> fields = RowBlock.getRowFieldsFromBlock(removals.getBlock(removals.getChannelCount() - 1));
        Block filePathBlock = fields.get(0);
        Block rowPositionBlock = fields.get(1);
        Block partitionSpecIdBlock = fields.get(2);
        Block partitionDataBlock = fields.get(3);
        for (int position = 0; position < filePathBlock.getPositionCount(); position++) {
            Slice filePath = VarcharType.VARCHAR.getSlice(filePathBlock, position);
            long rowPosition = BIGINT.getLong(rowPositionBlock, position);

            int index = position;
            FileDeletion deletion = fileDeletions.computeIfAbsent(filePath, _ -> {
                int partitionSpecId = INTEGER.getInt(partitionSpecIdBlock, index);
                String partitionData = VarcharType.VARCHAR.getSlice(partitionDataBlock, index).toStringUtf8();
                return new FileDeletion(partitionSpecId, partitionData);
            });

            deletion.rowsToDelete().add(rowPosition);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        List<Slice> fragments = new ArrayList<>(insertPageSink.finish().join());
        writtenBytes = insertPageSink.getCompletedBytes();

        if (formatVersion < 2) {
            // position deletes are only supported in Iceberg format v2 and above
            verify(fileDeletions.isEmpty(), "Position deletes are not supported in Iceberg format version %s", formatVersion);
        }
        else if (formatVersion == 2) {
            fileDeletions.forEach((dataFilePath, deletion) -> deletion.rowsToDelete().build().ifPresent(deletionVector -> {
                PositionDeleteWriter writer = createPositionDeleteWriter(
                        dataFilePath.toStringUtf8(),
                        partitionsSpecs.get(deletion.partitionSpecId()),
                        deletion.partitionDataJson());
                fragments.add(writePositionDeletes(writer, deletionVector));
            }));
        }
        else if (formatVersion == 3) {
            fileDeletions.forEach((dataFilePath, deletion) -> deletion.rowsToDelete().build().ifPresent(deletionVector -> {
                PartitionSpec partitionSpec = partitionsSpecs.get(deletion.partitionSpecId());
                Optional<PartitionData> partitionData = createPartitionData(partitionSpec, deletion.partitionDataJson());
                CommitTaskData task = new CommitTaskData(
                        "", // path of the v2 delete file
                        fileFormat,
                        0, // size of the v2 delete file
                        new MetricsWrapper(new Metrics(deletionVector.cardinality())),
                        PartitionSpecParser.toJson(partitionSpec),
                        partitionData.map(PartitionData::toJson),
                        FileContent.POSITION_DELETES,
                        Optional.of(dataFilePath.toStringUtf8()),
                        Optional.empty(), // unused for v3
                        Optional.of(deletionVector.serialize().getBytes()));
                fragments.add(wrappedBuffer(jsonCodec.toJsonBytes(task)));
            }));
        }
        else {
            throw new VerifyException("Unsupported Iceberg format version: " + formatVersion);
        }

        return completedFuture(fragments);
    }

    @Override
    public void abort()
    {
        insertPageSink.abort();
    }

    private PositionDeleteWriter createPositionDeleteWriter(String dataFilePath, PartitionSpec partitionSpec, String partitionDataJson)
    {
        return new PositionDeleteWriter(
                dataFilePath,
                partitionSpec,
                createPartitionData(partitionSpec, partitionDataJson),
                locationProvider,
                fileWriterFactory,
                fileSystem,
                session,
                fileFormat,
                storageProperties);
    }

    private Slice writePositionDeletes(PositionDeleteWriter writer, DeletionVector rowsToDelete)
    {
        try {
            CommitTaskData task = writer.write(rowsToDelete);
            writtenBytes += task.fileSizeInBytes();
            return wrappedBuffer(jsonCodec.toJsonBytes(task));
        }
        catch (Throwable t) {
            closeAllSuppress(t, writer::abort);
            throw t;
        }
    }

    private Optional<PartitionData> createPartitionData(PartitionSpec partitionSpec, String partitionDataAsJson)
    {
        if (!partitionSpec.isPartitioned()) {
            return Optional.empty();
        }

        Type[] columnTypes = partitionSpec.fields().stream()
                .map(field -> field.transform().getResultType(schema.findType(field.sourceId())))
                .toArray(Type[]::new);
        return Optional.of(PartitionData.fromJson(partitionDataAsJson, columnTypes));
    }

    private record MergePage(Optional<Page> removals, Optional<Page> additions)
    {
        MergePage
        {
            requireNonNull(removals, "removals is null");
            requireNonNull(additions, "additions is null");
        }

        static MergePage createMergePage(Page inputPage, int dataColumnCount, boolean includeRowId)
        {
            int positionCount = inputPage.getPositionCount();
            if (positionCount <= 0) {
                throw new IllegalArgumentException("positionCount should be > 0, but is " + positionCount);
            }

            Block operationBlock = inputPage.getBlock(dataColumnCount);

            int[] removalPositions = new int[positionCount];
            int[] additionPositions = new int[positionCount];
            int removalCount = 0;
            int additionCount = 0;

            for (int position = 0; position < positionCount; position++) {
                byte operation = TINYINT.getByte(operationBlock, position);
                switch (operation) {
                    case DELETE_OPERATION_NUMBER, UPDATE_DELETE_OPERATION_NUMBER -> {
                        removalPositions[removalCount] = position;
                        removalCount++;
                    }
                    case INSERT_OPERATION_NUMBER, UPDATE_INSERT_OPERATION_NUMBER -> {
                        additionPositions[additionCount] = position;
                        additionCount++;
                    }
                    default -> throw new IllegalArgumentException("Invalid merge operation: " + operation);
                }
            }

            Optional<Page> removalsPage = Optional.empty();
            if (removalCount > 0) {
                // Removals page: data columns + merge row ID column
                int[] columns = new int[dataColumnCount + 1];
                for (int i = 0; i < dataColumnCount; i++) {
                    columns[i] = i;
                }
                columns[dataColumnCount] = dataColumnCount + 2; // merge row ID column
                removalsPage = Optional.of(inputPage
                        .getColumns(columns)
                        .getPositions(removalPositions, 0, removalCount));
            }

            Optional<Page> additionsPage = Optional.empty();
            if (additionCount > 0) {
                if (includeRowId) {
                    // V3: data columns + row_id (extracted from merge row ID's source_row_id field)
                    Block[] blocks = new Block[dataColumnCount + 1];
                    for (int i = 0; i < dataColumnCount; i++) {
                        blocks[i] = inputPage.getBlock(i).getPositions(additionPositions, 0, additionCount);
                    }
                    blocks[dataColumnCount] = createRowIdBlock(inputPage, dataColumnCount, additionPositions, additionCount);

                    additionsPage = Optional.of(new Page(additionCount, blocks));
                }
                else {
                    // V2: data columns only
                    int[] columns = new int[dataColumnCount];
                    for (int i = 0; i < dataColumnCount; i++) {
                        columns[i] = i;
                    }
                    additionsPage = Optional.of(inputPage
                            .getColumns(columns)
                            .getPositions(additionPositions, 0, additionCount));
                }
            }

            return new MergePage(removalsPage, additionsPage);
        }

        private static Block createRowIdBlock(Page inputPage, int dataColumnCount, int[] additionPositions, int additionCount)
        {
            // For V3, we need to extract source_row_id from the merge row ID for UPDATE_INSERT rows.
            // UPDATE_DELETE is immediately followed by UPDATE_INSERT, so we track pending source row IDs.
            Block operationBlock = inputPage.getBlock(dataColumnCount);
            Block mergeRowIdBlock = inputPage.getBlock(dataColumnCount + 2);
            List<Block> mergeRowIdFields = RowBlock.getRowFieldsFromBlock(mergeRowIdBlock);
            Block sourceRowIdBlock = mergeRowIdFields.get(4);

            long[] rowIdValues = new long[additionCount];
            boolean[] rowIdNulls = new boolean[additionCount];

            Long pendingSourceRowId = null;
            int additionIndex = 0;
            int nextAdditionPosition = additionPositions[0];

            for (int position = 0; position < inputPage.getPositionCount(); position++) {
                byte operation = TINYINT.getByte(operationBlock, position);

                if (operation == UPDATE_DELETE_OPERATION_NUMBER) {
                    // Extract source row ID for the next UPDATE_INSERT
                    if (sourceRowIdBlock.isNull(position)) {
                        pendingSourceRowId = null;
                    }
                    else {
                        pendingSourceRowId = BIGINT.getLong(sourceRowIdBlock, position);
                    }
                }

                if (position == nextAdditionPosition) {
                    if (operation == UPDATE_INSERT_OPERATION_NUMBER && pendingSourceRowId != null) {
                        rowIdValues[additionIndex] = pendingSourceRowId;
                        rowIdNulls[additionIndex] = false;
                        pendingSourceRowId = null;
                    }
                    else {
                        // Pure INSERT or UPDATE with null source row ID
                        rowIdNulls[additionIndex] = true;
                    }
                    additionIndex++;
                    if (additionIndex < additionCount) {
                        nextAdditionPosition = additionPositions[additionIndex];
                    }
                }
            }

            Block rowIdBlock = new LongArrayBlock(additionCount, Optional.of(rowIdNulls), rowIdValues);
            return rowIdBlock;
        }
    }

    private static class FileDeletion
    {
        private final int partitionSpecId;
        private final String partitionDataJson;
        private final DeletionVector.Builder rowsToDelete = DeletionVector.builder();

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

        public DeletionVector.Builder rowsToDelete()
        {
            return rowsToDelete;
        }
    }
}
