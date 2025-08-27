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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.plugin.iceberg.delete.PositionDeleteWriter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MergePage;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.connector.UpdateKind;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Type;
import org.roaringbitmap.longlong.ImmutableLongBitmapDataProvider;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_CLOSE_ERROR;
import static io.trino.plugin.iceberg.IcebergUtil.getWriteChangeMode;
import static io.trino.spi.connector.MergePage.createDeleteAndInsertPages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Predicate.not;
import static org.apache.iceberg.FileContent.DATA;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;

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
    private final List<IcebergColumnHandle> regularColumns;
    private final Optional<NameMapping> nameMapping;
    private final Map<Slice, FileDeletion> fileDeletions = new HashMap<>();
    private final IcebergPageSourceProvider icebergPageSourceProvider;
    private final UpdateKind updateKind;

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
            int columnCount,
            List<IcebergColumnHandle> regularColumns,
            Optional<NameMapping> nameMapping,
            IcebergPageSourceProvider icebergPageSourceProvider,
            UpdateKind updateKind)
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
        this.regularColumns = requireNonNull(regularColumns, "regularColumns is null");
        this.nameMapping = requireNonNull(nameMapping, "nameMapping is null");
        this.icebergPageSourceProvider = requireNonNull(icebergPageSourceProvider, "icebergPageSourceProvider is null");
        this.updateKind = requireNonNull(updateKind, "writeOperation is null");
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
                final OptionalLong dataSequenceNumber;

                final List<DeleteFile> deleteFiles;
                if (getWriteChangeMode(storageProperties, updateKind).equals(WriteChangeMode.COW)) {
                    dataSequenceNumber = OptionalLong.of(BIGINT.getLong(fields.get(4), position));
                    deleteFiles = extractDeleteFilesFromFields(fields, position, 5);
                }
                else {
                    dataSequenceNumber = OptionalLong.empty();
                    deleteFiles = List.of();
                }

                int index = position;
                FileDeletion deletion = fileDeletions.computeIfAbsent(filePath, _ -> {
                    int partitionSpecId = INTEGER.getInt(partitionSpecIdBlock, index);
                    String partitionData = VarcharType.VARCHAR.getSlice(partitionDataBlock, index).toStringUtf8();
                    return new FileDeletion(partitionSpecId, partitionData, deleteFiles, dataSequenceNumber);
                });

                deletion.rowsToDelete().addLong(rowPosition);
            }
        });
    }

    private static List<DeleteFile> extractDeleteFilesFromFields(List<Block> fields, int position, int startIdx)
    {
        ArrayType integerArrayType = new ArrayType(INTEGER);
        ArrayType bigintArrayType = new ArrayType(BIGINT);
        ArrayType varcharArrayType = new ArrayType(VarcharType.VARCHAR);
        ArrayType integerArrayArrayType = new ArrayType(new ArrayType(INTEGER));

        Block fileContentBlock = integerArrayType.getObject(fields.get(startIdx), position);
        Block pathBlock = varcharArrayType.getObject(fields.get(startIdx + 1), position);
        Block formatBlock = varcharArrayType.getObject(fields.get(startIdx + 2), position);
        Block recordCountBlock = bigintArrayType.getObject(fields.get(startIdx + 3), position);
        Block fileSizeBlock = bigintArrayType.getObject(fields.get(startIdx + 4), position);
        Block equalityFieldIdsBlock = integerArrayArrayType.getObject(fields.get(startIdx + 5), position);
        Block lowerBoundBlock = bigintArrayType.getObject(fields.get(startIdx + 6), position);
        Block upperBoundBlock = bigintArrayType.getObject(fields.get(startIdx + 7), position);
        Block sequenceBlock = bigintArrayType.getObject(fields.get(startIdx + 8), position);

        int count = fileContentBlock.getPositionCount();
        List<DeleteFile> result = new ArrayList<>(count);

        Function<Integer, FileContent> id2FileContent = id -> switch (id) {
            case 0 -> FileContent.DATA;
            case 1 -> FileContent.POSITION_DELETES;
            case 2 -> FileContent.EQUALITY_DELETES;
            default -> throw new IllegalArgumentException("Unknown FileContent id: " + id);
        };

        for (int i = 0; i < count; i++) {
            FileContent content = id2FileContent.apply(INTEGER.getInt(fileContentBlock, i));
            String path = VarcharType.VARCHAR.getSlice(pathBlock, i).toStringUtf8();
            FileFormat format = FileFormat.fromString(VarcharType.VARCHAR.getSlice(formatBlock, i).toStringUtf8());
            long recordCount = BIGINT.getLong(recordCountBlock, i);
            long fileSizeInBytes = BIGINT.getLong(fileSizeBlock, i);

            Block equalityFieldIdsSingleFileBlock = integerArrayType.getObject(equalityFieldIdsBlock, i);
            List<Integer> equalityFieldIds = new ArrayList<>();
            IntStream.range(0, equalityFieldIdsSingleFileBlock.getPositionCount())
                    .mapToObj(j -> INTEGER.getInt(equalityFieldIdsSingleFileBlock, j))
                    .forEach(equalityFieldIds::add);

            Optional<Long> rowPositionLowerBound = lowerBoundBlock.isNull(i) ? Optional.empty() : Optional.of(BIGINT.getLong(lowerBoundBlock, i));
            Optional<Long> rowPositionUpperBound = upperBoundBlock.isNull(i) ? Optional.empty() : Optional.of(BIGINT.getLong(upperBoundBlock, i));
            long sequenceNum = BIGINT.getLong(sequenceBlock, i);

            result.add(new DeleteFile(
                    content,
                    path,
                    format,
                    recordCount,
                    fileSizeInBytes,
                    equalityFieldIds,
                    rowPositionLowerBound,
                    rowPositionUpperBound,
                    sequenceNum));
        }

        return result;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        List<Slice> fragments = new ArrayList<>();

        if (getWriteChangeMode(storageProperties, updateKind).equals(WriteChangeMode.COW)) {
            fileDeletions.forEach((dataFilePath, deletion) ->
                    fragments.addAll(rewriteFile(dataFilePath.toStringUtf8(), deletion)));
        }
        else {
            fileDeletions.forEach((dataFilePath, deletion) -> {
                PositionDeleteWriter writer = createPositionDeleteWriter(
                        dataFilePath.toStringUtf8(),
                        partitionsSpecs.get(deletion.partitionSpecId()),
                        deletion.partitionDataJson());

                fragments.addAll(writePositionDeletes(writer, deletion.rowsToDelete()));
            });
        }

        fragments.addAll(insertPageSink.finish().join());

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

    public static class FileDeletion
    {
        private final int partitionSpecId;
        private final String partitionDataJson;
        private final LongBitmapDataProvider rowsToDelete = new Roaring64Bitmap();
        private final List<DeleteFile> deleteFiles;
        private final OptionalLong dataSequenceNumber;

        public FileDeletion(int partitionSpecId, String partitionDataJson, List<DeleteFile> deleteFiles, OptionalLong dataSequenceNumber)
        {
            this.partitionSpecId = partitionSpecId;
            this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
            this.deleteFiles = requireNonNull(deleteFiles, "deleteFilePaths is null");
            this.dataSequenceNumber = requireNonNull(dataSequenceNumber, "dataSequenceNumber is null");
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

        public List<DeleteFile> getDeleteFiles()
        {
            return deleteFiles;
        }

        public OptionalLong getDataSequenceNumber()
        {
            return dataSequenceNumber;
        }
    }

    private List<Slice> rewriteFile(String dataFilePath, FileDeletion deletion)
    {
        try {
            TrinoInputFile dataFile = fileSystem.newInputFile(Location.of(dataFilePath));
            // Fetch size early to use in task data + createParquetPageSource
            long dataFileSize = dataFile.length();
            copyOnWriteRewriteFile(dataFile, dataFileSize, deletion);
            CommitTaskData task = new CommitTaskData(
                    dataFilePath,
                    fileFormat,
                    dataFileSize,
                    // metrics is not used for OverwriteFiles.deleteFile, just pass a dummy one
                    new MetricsWrapper(0L, null, null, null, null, null, null),
                    PartitionSpecParser.toJson(partitionsSpecs.get(deletion.partitionSpecId)),
                    Optional.of(deletion.partitionDataJson),
                    DATA,
                    Optional.empty(),
                    Optional.empty(),
                    true);
            return ImmutableList.of(utf8Slice(jsonCodec.toJson(task)));
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_WRITER_CLOSE_ERROR, "Unable to rewrite Parquet file", e);
        }
    }

    private void copyOnWriteRewriteFile(TrinoInputFile sourceDataFile, long sourceDataFileSize, FileDeletion deletion)
            throws IOException
    {
        LongBitmapDataProvider rowsDeletedByDelete = deletion.rowsToDelete();
        try (PageSourceWithPosition pageSourceWithPosition =
                     createCopyOnWritePageSource(sourceDataFile, sourceDataFileSize, deletion)) {
            // grab the inner source once, or inline the method call
            ConnectorPageSource pageSource = pageSourceWithPosition.pageSource();

            while (!pageSource.isFinished()) {
                SourcePage sourcePage = pageSource.getNextSourcePage();
                if (sourcePage == null) {
                    continue;
                }
                // fully load page
                Page page = sourcePage.getPage();

                int positionCount = page.getPositionCount();
                int[] retained = new int[positionCount];
                int retainedCount = 0;
                Block rowPositionBlock = page.getBlock(pageSourceWithPosition.posColumnIdx());
                for (int position = 0; position < positionCount; position++) {
                    long rowId = BIGINT.getLong(rowPositionBlock, position);
                    if (!rowsDeletedByDelete.contains(rowId)) {
                        retained[retainedCount++] = position;
                    }
                }

                if (retainedCount != positionCount) {
                    page = page.getPositions(retained, 0, retainedCount);
                }
                page = page.getColumns(IntStream.range(0, page.getChannelCount()).filter(col -> col != pageSourceWithPosition.posColumnIdx).toArray());
                insertPageSink.appendPage(page);
            }
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_WRITER_CLOSE_ERROR,
                    "Unable to rewrite parquet file", e);
        }
    }

    private PageSourceWithPosition createCopyOnWritePageSource(TrinoInputFile inputFile, long fileSize, FileDeletion deletion)
    {
        PartitionSpec partitionSpec = partitionsSpecs.get(deletion.partitionSpecId);
        Type[] columnTypes = partitionSpec.fields().stream()
                .map(field -> field.transform().getResultType(schema.findType(field.sourceId())))
                .toArray(Type[]::new);
        PartitionData partitionData = PartitionData.fromJson(deletion.partitionDataJson, columnTypes);

        List<IcebergColumnHandle> requiredColumns = new ArrayList<>(regularColumns);

        List<DeleteFile> deletes = deletion.getDeleteFiles();

        Set<IcebergColumnHandle> deleteFilterRequiredColumns = icebergPageSourceProvider.requiredColumnsForDeletes(schema, deletes);

        deleteFilterRequiredColumns.stream()
                .filter(not(regularColumns::contains))
                .forEach(requiredColumns::add);

        int posColumnIdx = -1;

        for (int i = 0; i < requiredColumns.size(); i++) {
            if (requiredColumns.get(i).isRowPositionColumn()) {
                posColumnIdx = i;
                break;
            }
        }

        if (posColumnIdx == -1) {
            requiredColumns.add(icebergPageSourceProvider.getColumnHandle(ROW_POSITION));
            posColumnIdx = requiredColumns.size() - 1;
        }

        ConnectorPageSource pageSource = icebergPageSourceProvider.createPageSource(
                session,
                inputFile,
                fileSystem,
                0,
                fileSize,
                fileSize,
                partitionSpec,
                partitionData,
                deletion.partitionDataJson,
                fileFormat,
                schema,
                requiredColumns,
                requiredColumns,
                TupleDomain.all(),
                nameMapping,
                partitionSpec.partitionToPath(partitionData),
                IcebergUtil.getPartitionKeys(partitionData, partitionSpec),
                deletion.getDeleteFiles(),
                deletion.getDataSequenceNumber(),
                Optional.empty());
        return new PageSourceWithPosition(pageSource, posColumnIdx);
    }

    private record PageSourceWithPosition(
            ConnectorPageSource pageSource,
            int posColumnIdx
    ) implements AutoCloseable
    {
        @Override
        public void close()
                throws IOException
        {
            pageSource.close();
        }
    }
}
