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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import io.airlift.concurrent.MoreFutures;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.CompressionCodec;
import org.joda.time.DateTimeZone;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.DataFileInfo.DataFileType.DATA;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.SYNTHESIZED;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_WRITE;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getCompressionCodec;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetWriterBlockSize;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetWriterPageSize;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.deserializePartitionValue;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class DeltaLakeMergeSink
        implements ConnectorMergeSink
{
    private static final JsonCodec<List<String>> PARTITIONS_CODEC = listJsonCodec(String.class);
    public static final String INSERT_CDF_LABEL = "insert";
    public static final String DELETE_CDF_LABEL = "delete";
    public static final String UPDATE_PREIMAGE_CDF_LABEL = "update_preimage";
    public static final String UPDATE_POSTIMAGE_CDF_LABEL = "update_postimage";

    private final TrinoFileSystem fileSystem;
    private final ConnectorSession session;
    private final DateTimeZone parquetDateTimeZone;
    private final String trinoVersion;
    private final JsonCodec<DataFileInfo> dataFileInfoCodec;
    private final JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec;
    private final DeltaLakeWriterStats writerStats;
    private final String rootTableLocation;
    private final ConnectorPageSink insertPageSink;
    private final List<DeltaLakeColumnHandle> dataColumns;
    private final List<DeltaLakeColumnHandle> nonSynthesizedColumns;
    private final int tableColumnCount;
    private final int domainCompactionThreshold;
    private final Supplier<DeltaLakeCdfPageSink> cdfPageSinkSupplier;
    private final boolean cdfEnabled;
    private final Map<Slice, FileDeletion> fileDeletions = new HashMap<>();
    private final int[] dataColumnsIndices;
    private final int[] dataAndRowIdColumnsIndices;

    @Nullable
    private DeltaLakeCdfPageSink cdfPageSink;

    public DeltaLakeMergeSink(
            TrinoFileSystemFactory fileSystemFactory,
            ConnectorSession session,
            DateTimeZone parquetDateTimeZone,
            String trinoVersion,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec,
            DeltaLakeWriterStats writerStats,
            String rootTableLocation,
            ConnectorPageSink insertPageSink,
            List<DeltaLakeColumnHandle> tableColumns,
            int domainCompactionThreshold,
            Supplier<DeltaLakeCdfPageSink> cdfPageSinkSupplier,
            boolean cdfEnabled)
    {
        this.session = requireNonNull(session, "session is null");
        this.fileSystem = fileSystemFactory.create(session);
        this.parquetDateTimeZone = requireNonNull(parquetDateTimeZone, "parquetDateTimeZone is null");
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.dataFileInfoCodec = requireNonNull(dataFileInfoCodec, "dataFileInfoCodec is null");
        this.mergeResultJsonCodec = requireNonNull(mergeResultJsonCodec, "mergeResultJsonCodec is null");
        this.writerStats = requireNonNull(writerStats, "writerStats is null");
        this.rootTableLocation = requireNonNull(rootTableLocation, "rootTableLocation is null");
        this.insertPageSink = requireNonNull(insertPageSink, "insertPageSink is null");
        requireNonNull(tableColumns, "tableColumns is null");
        this.tableColumnCount = tableColumns.size();
        this.dataColumns = tableColumns.stream()
                .filter(column -> column.getColumnType() == REGULAR)
                .collect(toImmutableList());
        this.domainCompactionThreshold = domainCompactionThreshold;
        this.nonSynthesizedColumns = tableColumns.stream()
                .filter(column -> column.getColumnType() != SYNTHESIZED)
                .collect(toImmutableList());
        this.cdfPageSinkSupplier = requireNonNull(cdfPageSinkSupplier);
        this.cdfEnabled = cdfEnabled;
        dataColumnsIndices = new int[tableColumnCount];
        dataAndRowIdColumnsIndices = new int[tableColumnCount + 1];
        for (int i = 0; i < tableColumnCount; i++) {
            dataColumnsIndices[i] = i;
            dataAndRowIdColumnsIndices[i] = i;
        }
        dataAndRowIdColumnsIndices[tableColumnCount] = tableColumnCount + 1; // row ID channel
    }

    @Override
    public void storeMergedRows(Page page)
    {
        DeltaLakeMergePage mergePage = createPages(page, tableColumnCount);

        mergePage.insertionsPage().ifPresent(insertPageSink::appendPage);
        mergePage.updateInsertionsPage().ifPresent(insertPageSink::appendPage);

        processInsertions(mergePage.insertionsPage(), INSERT_CDF_LABEL);
        processInsertions(mergePage.updateInsertionsPage(), UPDATE_POSTIMAGE_CDF_LABEL);

        mergePage.deletionsPage().ifPresent(deletions -> processDeletion(deletions, DELETE_CDF_LABEL));
        mergePage.updateDeletionsPage().ifPresent(deletions -> processDeletion(deletions, UPDATE_PREIMAGE_CDF_LABEL));
    }

    private void processInsertions(Optional<Page> optionalInsertionPage, String cdfOperation)
    {
        if (cdfEnabled && optionalInsertionPage.isPresent()) {
            if (cdfPageSink == null) {
                cdfPageSink = cdfPageSinkSupplier.get();
            }
            Page updateInsertionsPage = optionalInsertionPage.get();
            Block[] cdfPostUpdateBlocks = new Block[nonSynthesizedColumns.size() + 1];
            for (int i = 0; i < nonSynthesizedColumns.size(); i++) {
                cdfPostUpdateBlocks[i] = updateInsertionsPage.getBlock(i);
            }
            cdfPostUpdateBlocks[nonSynthesizedColumns.size()] = RunLengthEncodedBlock.create(
                    nativeValueToBlock(VARCHAR, utf8Slice(cdfOperation)), updateInsertionsPage.getPositionCount());
            cdfPageSink.appendPage(new Page(updateInsertionsPage.getPositionCount(), cdfPostUpdateBlocks));
        }
    }

    private void processDeletion(Page deletions, String cdfOperation)
    {
        ColumnarRow rowIdRow = toColumnarRow(deletions.getBlock(deletions.getChannelCount() - 1));

        for (int position = 0; position < rowIdRow.getPositionCount(); position++) {
            Slice filePath = VARCHAR.getSlice(rowIdRow.getField(0), position);
            long rowPosition = BIGINT.getLong(rowIdRow.getField(1), position);
            Slice partitions = VARCHAR.getSlice(rowIdRow.getField(2), position);

            List<String> partitionValues = PARTITIONS_CODEC.fromJson(partitions.toStringUtf8());

            FileDeletion deletion = fileDeletions.computeIfAbsent(filePath, x -> new FileDeletion(partitionValues));

            if (cdfOperation.equals(UPDATE_PREIMAGE_CDF_LABEL)) {
                deletion.rowsDeletedByUpdate().addLong(rowPosition);
            }
            else {
                deletion.rowsDeletedByDelete().addLong(rowPosition);
            }
        }
    }

    private DeltaLakeMergePage createPages(Page inputPage, int dataColumnCount)
    {
        int inputChannelCount = inputPage.getChannelCount();
        if (inputChannelCount != dataColumnCount + 2) {
            throw new IllegalArgumentException(format("inputPage channelCount (%s) == dataColumns size (%s) + 2", inputChannelCount, dataColumnCount));
        }

        int positionCount = inputPage.getPositionCount();
        if (positionCount <= 0) {
            throw new IllegalArgumentException("positionCount should be > 0, but is " + positionCount);
        }
        Block operationBlock = inputPage.getBlock(inputChannelCount - 2);
        int[] deletePositions = new int[positionCount];
        int[] insertPositions = new int[positionCount];
        int[] updateInsertPositions = new int[positionCount];
        int[] updateDeletePositions = new int[positionCount];
        int deletePositionCount = 0;
        int insertPositionCount = 0;
        int updateInsertPositionCount = 0;
        int updateDeletePositionCount = 0;

        for (int position = 0; position < positionCount; position++) {
            int operation = toIntExact(TINYINT.getLong(operationBlock, position));
            switch (operation) {
                case DELETE_OPERATION_NUMBER:
                    deletePositions[deletePositionCount] = position;
                    deletePositionCount++;
                    break;
                case INSERT_OPERATION_NUMBER:
                    insertPositions[insertPositionCount] = position;
                    insertPositionCount++;
                    break;
                case UPDATE_INSERT_OPERATION_NUMBER:
                    updateInsertPositions[updateInsertPositionCount] = position;
                    updateInsertPositionCount++;
                    break;
                case UPDATE_DELETE_OPERATION_NUMBER:
                    updateDeletePositions[updateDeletePositionCount] = position;
                    updateDeletePositionCount++;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid merge operation: " + operation);
            }
        }
        Optional<Page> deletePage = Optional.empty();
        if (deletePositionCount > 0) {
            deletePage = Optional.of(inputPage
                    .getColumns(dataAndRowIdColumnsIndices)
                    .getPositions(deletePositions, 0, deletePositionCount));
        }

        Optional<Page> insertPage = Optional.empty();
        if (insertPositionCount > 0) {
            insertPage = Optional.of(inputPage
                    .getColumns(dataColumnsIndices)
                    .getPositions(insertPositions, 0, insertPositionCount));
        }

        Optional<Page> updateInsertPage = Optional.empty();
        if (updateInsertPositionCount > 0) {
            updateInsertPage = Optional.of(inputPage
                    .getColumns(dataColumnsIndices)
                    .getPositions(updateInsertPositions, 0, updateInsertPositionCount));
        }

        Optional<Page> updateDeletePage = Optional.empty();
        if (updateDeletePositionCount > 0) {
            updateDeletePage = Optional.of(inputPage
                    .getColumns(dataAndRowIdColumnsIndices)
                    .getPositions(updateDeletePositions, 0, updateDeletePositionCount));
        }
        return new DeltaLakeMergePage(deletePage, insertPage, updateInsertPage, updateDeletePage);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        List<Slice> fragments = new ArrayList<>();

        insertPageSink.finish().join().stream()
                .map(Slice::getBytes)
                .map(dataFileInfoCodec::fromJson)
                .map(info -> new DeltaLakeMergeResult(Optional.empty(), Optional.of(info)))
                .map(mergeResultJsonCodec::toJsonBytes)
                .map(Slices::wrappedBuffer)
                .forEach(fragments::add);

        fileDeletions.forEach((path, deletion) ->
                fragments.addAll(rewriteFile(new Path(path.toStringUtf8()), deletion)));

        if (cdfEnabled && cdfPageSink != null) { // cdf may be enabled but there may be no update/deletion so sink was not instantiated
            MoreFutures.getDone(cdfPageSink.finish()).stream()
                    .map(Slice::getBytes)
                    .map(dataFileInfoCodec::fromJson)
                    .map(info -> new DeltaLakeMergeResult(Optional.empty(), Optional.of(info)))
                    .map(mergeResultJsonCodec::toJsonBytes)
                    .map(Slices::wrappedBuffer)
                    .forEach(fragments::add);
        }

        return completedFuture(fragments);
    }

    // In spite of the name "Delta" Lake, we must rewrite the entire file to delete rows.
    private List<Slice> rewriteFile(Path sourcePath, FileDeletion deletion)
    {
        try {
            Path rootTablePath = new Path(rootTableLocation);
            String sourceRelativePath = rootTablePath.toUri().relativize(sourcePath.toUri()).toString();

            Path targetPath = new Path(sourcePath.getParent(), session.getQueryId() + "_" + randomUUID());
            String targetRelativePath = rootTablePath.toUri().relativize(targetPath.toUri()).toString();
            FileWriter fileWriter = createParquetFileWriter(targetPath.toString(), dataColumns);

            DeltaLakeWriter writer = new DeltaLakeWriter(
                    fileSystem,
                    fileWriter,
                    rootTableLocation,
                    targetRelativePath,
                    deletion.partitionValues(),
                    writerStats,
                    dataColumns,
                    DATA);

            Optional<DataFileInfo> newFileInfo = rewriteParquetFile(sourcePath, deletion, writer);

            DeltaLakeMergeResult result = new DeltaLakeMergeResult(Optional.of(sourceRelativePath), newFileInfo);
            return ImmutableList.of(utf8Slice(mergeResultJsonCodec.toJson(result)));
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Unable to rewrite Parquet file", e);
        }
    }

    private FileWriter createParquetFileWriter(String path, List<DeltaLakeColumnHandle> dataColumns)
    {
        ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                .setMaxBlockSize(getParquetWriterBlockSize(session))
                .setMaxPageSize(getParquetWriterPageSize(session))
                .build();
        CompressionCodec compressionCodec = getCompressionCodec(session).getParquetCompressionCodec();

        try {
            Closeable rollbackAction = () -> fileSystem.deleteFile(path);

            List<Type> parquetTypes = dataColumns.stream()
                    .map(column -> {
                        Type type = column.getType();
                        if (type instanceof TimestampWithTimeZoneType timestamp) {
                            verify(timestamp.getPrecision() == 3, "Unsupported type: %s", type);
                            return TIMESTAMP_MILLIS;
                        }
                        return type;
                    })
                    .collect(toImmutableList());

            List<String> dataColumnNames = dataColumns.stream()
                    .map(DeltaLakeColumnHandle::getName)
                    .collect(toImmutableList());
            ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                    parquetTypes,
                    dataColumnNames,
                    false,
                    false);

            return new ParquetFileWriter(
                    fileSystem.newOutputFile(path),
                    rollbackAction,
                    parquetTypes,
                    dataColumnNames,
                    schemaConverter.getMessageType(),
                    schemaConverter.getPrimitiveTypes(),
                    parquetWriterOptions,
                    IntStream.range(0, dataColumns.size()).toArray(),
                    compressionCodec,
                    trinoVersion,
                    false,
                    Optional.empty(),
                    Optional.empty());
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Error creating Parquet file", e);
        }
    }

    private Optional<DataFileInfo> rewriteParquetFile(Path path, FileDeletion deletion, DeltaLakeWriter fileWriter)
            throws IOException
    {
        LongBitmapDataProvider rowsDeletedByDelete = deletion.rowsDeletedByDelete();
        LongBitmapDataProvider rowsDeletedByUpdate = deletion.rowsDeletedByUpdate();
        try (ConnectorPageSource connectorPageSource = createParquetPageSource(path).get()) {
            long filePosition = 0;
            while (!connectorPageSource.isFinished()) {
                Page page = connectorPageSource.getNextPage();
                if (page == null) {
                    continue;
                }

                int positionCount = page.getPositionCount();
                int[] retained = new int[positionCount];
                int[] deletedByDelete = new int[(int) rowsDeletedByDelete.getLongCardinality()];
                int[] deletedByUpdate = new int[(int) rowsDeletedByUpdate.getLongCardinality()];
                int retainedCount = 0;
                int deletedByUpdateCount = 0;
                int deletedByDeleteCount = 0;
                for (int position = 0; position < positionCount; position++) {
                    if (rowsDeletedByDelete.contains(filePosition)) {
                        deletedByDelete[deletedByDeleteCount] = position;
                        deletedByDeleteCount++;
                    }
                    else if (rowsDeletedByUpdate.contains(filePosition)) {
                        deletedByUpdate[deletedByUpdateCount] = position;
                        deletedByUpdateCount++;
                    }
                    else {
                        retained[retainedCount] = position;
                        retainedCount++;
                    }
                    filePosition++;
                }

                storeCdfEntries(page, deletedByDelete, deletedByDeleteCount, deletion, DELETE_CDF_LABEL);
                storeCdfEntries(page, deletedByUpdate, deletedByUpdateCount, deletion, UPDATE_PREIMAGE_CDF_LABEL);

                if (retainedCount != positionCount) {
                    page = page.getPositions(retained, 0, retainedCount);
                }

                if (page.getPositionCount() > 0) {
                    fileWriter.appendRows(page);
                }
            }
            if (fileWriter.getRowCount() == 0) {
                fileWriter.rollback();
                return Optional.empty();
            }
            fileWriter.commit();
        }
        catch (Throwable t) {
            try {
                fileWriter.rollback();
            }
            catch (RuntimeException e) {
                if (!t.equals(e)) {
                    t.addSuppressed(e);
                }
            }
            throw t;
        }

        return Optional.of(fileWriter.getDataFileInfo());
    }

    private void storeCdfEntries(Page page, int[] deleted, int deletedCount, FileDeletion deletion, String operation)
    {
        if (cdfEnabled && page.getPositionCount() > 0) {
            if (cdfPageSink == null) {
                cdfPageSink = cdfPageSinkSupplier.get();
            }
            Page cdfPage = page.getPositions(deleted, 0, deletedCount);
            Block[] outputBlocks = new Block[nonSynthesizedColumns.size() + 1];
            int cdfPageIndex = 0;
            int partitionIndex = 0;
            List<String> partitionValues = deletion.partitionValues;
            for (int i = 0; i < nonSynthesizedColumns.size(); i++) {
                if (nonSynthesizedColumns.get(i).getColumnType() == REGULAR) {
                    outputBlocks[i] = cdfPage.getBlock(cdfPageIndex);
                    cdfPageIndex++;
                }
                else {
                    outputBlocks[i] = RunLengthEncodedBlock.create(nativeValueToBlock(
                                    nonSynthesizedColumns.get(i).getType(),
                                    deserializePartitionValue(
                                            nonSynthesizedColumns.get(i),
                                            Optional.ofNullable(partitionValues.get(partitionIndex)))),
                            cdfPage.getPositionCount());
                    partitionIndex++;
                }
            }
            Block cdfOperationBlock = RunLengthEncodedBlock.create(
                    nativeValueToBlock(VARCHAR, utf8Slice(operation)), cdfPage.getPositionCount());
            outputBlocks[nonSynthesizedColumns.size()] = cdfOperationBlock;
            cdfPageSink.appendPage(new Page(cdfPage.getPositionCount(), outputBlocks));
        }
    }

    private ReaderPageSource createParquetPageSource(Path path)
            throws IOException
    {
        TrinoInputFile inputFile = fileSystem.newInputFile(path.toString());

        return ParquetPageSourceFactory.createPageSource(
                inputFile,
                0,
                inputFile.length(),
                dataColumns.stream()
                        .map(DeltaLakeColumnHandle::toHiveColumnHandle)
                        .collect(toImmutableList()),
                TupleDomain.all(),
                true,
                parquetDateTimeZone,
                new FileFormatDataSourceStats(),
                new ParquetReaderOptions().withBloomFilter(false),
                Optional.empty(),
                domainCompactionThreshold);
    }

    @Override
    public void abort()
    {
        if (cdfPageSink != null) {
            cdfPageSink.abort();
        }
    }

    private static class FileDeletion
    {
        private final List<String> partitionValues;
        private final LongBitmapDataProvider rowsDeletedByDelete = new Roaring64Bitmap();
        private final LongBitmapDataProvider rowsDeletedByUpdate = new Roaring64Bitmap();

        private FileDeletion(List<String> partitionValues)
        {
            // Use ArrayList since Delta Lake allows NULL partition values, and wrap it in
            // an unmodifiableList.
            this.partitionValues = unmodifiableList(new ArrayList<>(requireNonNull(partitionValues, "partitionValues is null")));
        }

        public List<String> partitionValues()
        {
            return partitionValues;
        }

        public LongBitmapDataProvider rowsDeletedByDelete()
        {
            return rowsDeletedByDelete;
        }

        public LongBitmapDataProvider rowsDeletedByUpdate()
        {
            return rowsDeletedByUpdate;
        }
    }

    private record DeltaLakeMergePage(
            Optional<Page> deletionsPage,
            Optional<Page> insertionsPage,
            Optional<Page> updateInsertionsPage,
            Optional<Page> updateDeletionsPage)
    {
        public DeltaLakeMergePage
        {
            requireNonNull(deletionsPage, "deletionsPage is null");
            requireNonNull(insertionsPage, "insertionsPage is null");
            requireNonNull(updateInsertionsPage, "updateInsertionsPage is null");
            requireNonNull(updateDeletionsPage, "updateDeletionsPage is null");
        }
    }
}
