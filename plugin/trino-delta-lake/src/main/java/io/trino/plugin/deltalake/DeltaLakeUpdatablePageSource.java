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
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.RecordFileWriter;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.ROW_ID_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_DATA;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_WRITE;
import static io.trino.plugin.deltalake.DeltaLakePageSink.createPartitionValues;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetMaxReadBlockSize;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isParquetUseColumnIndex;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.deserializePartitionValue;
import static io.trino.plugin.hive.HiveCompressionCodec.SNAPPY;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.util.CompressionConfigUtil.configureCompression;
import static io.trino.plugin.hive.util.ConfigurationUtils.toJobConf;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class DeltaLakeUpdatablePageSource
        implements UpdatablePageSource
{
    /**
     * Column index of the connector's row ID for updates
     *
     * @see DeltaLakeColumnHandle#ROW_ID_COLUMN_NAME
     */
    private final int rowIdColumnIndex;
    private final List<DeltaLakeColumnHandle> queryColumns;
    private final DeltaLakeTableHandle tableHandle;
    private final Map<String, Optional<String>> partitionKeys;
    private final String path;
    private final long fileSize;
    private final ConnectorSession session;
    private final ExecutorService executorService;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsEnvironment.HdfsContext hdfsContext;
    private final DateTimeZone parquetDateTimeZone;
    private final ParquetReaderOptions parquetReaderOptions;
    private final TypeManager typeManager;
    private final JsonCodec<DeltaLakeUpdateResult> updateResultJsonCodec;
    private final BitSet rowsToDelete;
    private final DeltaLakePageSource pageSourceDelegate;
    private final int totalRecordCount;
    // Handles for all columns stored in the Parquet file. Does not contain partition keys or any synthetic columns.
    private final List<DeltaLakeColumnHandle> allDataColumns;
    private final DeltaLakeTableHandle.WriteType writeType;
    private final DeltaLakeWriter updatedFileWriter;

    // UPDATE only: Maps the column index from `queryColumns` to the index in the underlying page
    private final Optional<int[]> queryColumnMapping;
    // UPDATE only: Maps the column index from DeltaLakeTableHandle#getRowIdColumns to the index in the underlying page
    private final Optional<int[]> rowIdColumnMapping;
    // UPDATE only: The set of columns which are being updated
    private final Set<DeltaLakeColumnHandle> updatedColumns;

    public DeltaLakeUpdatablePageSource(
            DeltaLakeTableHandle tableHandle,
            List<DeltaLakeColumnHandle> queryColumns,
            Map<String, Optional<String>> partitionKeys,
            String path,
            long fileSize,
            long fileModifiedTime,
            ConnectorSession session,
            ExecutorService executorService,
            HdfsEnvironment hdfsEnvironment,
            HdfsEnvironment.HdfsContext hdfsContext,
            DateTimeZone parquetDateTimeZone,
            ParquetReaderOptions parquetReaderOptions,
            TupleDomain<HiveColumnHandle> parquetPredicate,
            TypeManager typeManager,
            JsonCodec<DeltaLakeUpdateResult> updateResultJsonCodec)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.queryColumns = requireNonNull(queryColumns, "queryColumns is null");
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
        this.path = requireNonNull(path, "path is null");
        this.fileSize = fileSize;
        this.session = requireNonNull(session, "session is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
        this.parquetDateTimeZone = requireNonNull(parquetDateTimeZone, "parquetDateTimeZone is null");
        this.parquetReaderOptions = requireNonNull(parquetReaderOptions, "parquetReaderOptions is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.updateResultJsonCodec = requireNonNull(updateResultJsonCodec, "deleteResultJsonCodec is null");

        List<ColumnMetadata> columnMetadata = extractSchema(tableHandle.getMetadataEntry(), typeManager);
        List<DeltaLakeColumnHandle> allColumns = columnMetadata.stream()
                .map(metadata -> new DeltaLakeColumnHandle(metadata.getName(), metadata.getType(), partitionKeys.containsKey(metadata.getName()) ? PARTITION_KEY : REGULAR))
                .collect(toImmutableList());
        this.allDataColumns = allColumns.stream()
                .filter(columnHandle -> columnHandle.getColumnType() == REGULAR)
                .collect(toImmutableList());

        this.writeType = tableHandle.getWriteType().orElseThrow();
        List<DeltaLakeColumnHandle> nonRowIdQueryColumns = queryColumns.stream()
                .filter(column -> !column.getName().equals(ROW_ID_COLUMN_NAME))
                .collect(toImmutableList());
        List<DeltaLakeColumnHandle> delegatedColumns;
        switch (writeType) {
            case UPDATE:
                delegatedColumns = allColumns;

                int queryColumnIndex = 0;
                Set<DeltaLakeColumnHandle> queryColumnSet = ImmutableSet.copyOf(nonRowIdQueryColumns);
                int[] queryColumnMapping = new int[nonRowIdQueryColumns.size()];

                int rowIdColumnIndex = 0;
                Set<DeltaLakeColumnHandle> rowIdUnmodifiedColumns = ImmutableSet.copyOf(tableHandle.getUpdateRowIdColumns().orElseThrow());
                int[] rowIdColumnMapping = new int[rowIdUnmodifiedColumns.size()];

                for (int delegatedColumnIndex = 0; delegatedColumnIndex < delegatedColumns.size(); delegatedColumnIndex++) {
                    DeltaLakeColumnHandle delegatedColumn = delegatedColumns.get(delegatedColumnIndex);
                    if (rowIdUnmodifiedColumns.contains(delegatedColumn)) {
                        rowIdColumnMapping[rowIdColumnIndex] = delegatedColumnIndex;
                        rowIdColumnIndex++;
                    }
                    if (queryColumnSet.contains(delegatedColumn)) {
                        queryColumnMapping[queryColumnIndex] = delegatedColumnIndex;
                        queryColumnIndex++;
                    }
                }

                this.queryColumnMapping = Optional.of(queryColumnMapping);
                this.rowIdColumnMapping = Optional.of(rowIdColumnMapping);
                this.updatedColumns = ImmutableSet.copyOf(tableHandle.getUpdatedColumns().orElseThrow());
                break;
            case DELETE:
                delegatedColumns = nonRowIdQueryColumns;
                this.queryColumnMapping = Optional.empty();
                this.rowIdColumnMapping = Optional.empty();
                this.updatedColumns = ImmutableSet.of();
                break;
            default:
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported write type: " + writeType);
        }

        DeltaLakeColumnHandle rowIndexColumn = rowIndexColumn();
        // Add the row index column
        delegatedColumns = ImmutableList.<DeltaLakeColumnHandle>builder()
                .addAll(delegatedColumns)
                .add(rowIndexColumn)
                .build();

        // Create page source with predicate pushdown and index column
        ReaderPageSource parquetPageSource = createParquetPageSource(
                parquetPredicate,
                delegatedColumns.stream()
                        .filter(column -> column.getColumnType() == REGULAR || column == rowIndexColumn)
                        .map(DeltaLakeColumnHandle::toHiveColumnHandle)
                        .collect(toImmutableList()));

        this.pageSourceDelegate = new DeltaLakePageSource(
                delegatedColumns,
                partitionKeys,
                parquetPageSource.get(),
                path,
                fileSize,
                fileModifiedTime);

        Path updatedFileLocation = getPathForNewFile();
        try {
            this.updatedFileWriter = createWriter(
                    updatedFileLocation,
                    columnMetadata,
                    allDataColumns);
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Unable to create writer for location: " + updatedFileLocation, e);
        }

        OptionalInt rowIdColumnIndex = IntStream.range(0, queryColumns.size())
                .filter(columnIndex -> queryColumns.get(columnIndex).getName().equals(ROW_ID_COLUMN_NAME))
                .findFirst();
        checkArgument(rowIdColumnIndex.isPresent(), "RowId column was not provided during delete operation");
        this.rowIdColumnIndex = rowIdColumnIndex.getAsInt();

        Path fsPath = new Path(path);
        try (ParquetFileReader parquetReader = ParquetFileReader.open(HadoopInputFile.fromPath(fsPath, hdfsEnvironment.getConfiguration(hdfsContext, fsPath)))) {
            checkArgument(parquetReader.getRecordCount() <= Integer.MAX_VALUE, "Deletes from files with more than Integer.MAX_VALUE rows is not supported");
            this.totalRecordCount = toIntExact(parquetReader.getRecordCount());
            this.rowsToDelete = new BitSet(totalRecordCount);
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_BAD_DATA, "Unable to read parquet metadata for file: " + path, e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return pageSourceDelegate.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return pageSourceDelegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return pageSourceDelegate.isFinished();
    }

    @Override
    public long getMemoryUsage()
    {
        return pageSourceDelegate.getMemoryUsage() + (rowsToDelete.size() / 8);
    }

    @Override
    public void close()
    {
        pageSourceDelegate.close();
    }

    @Override
    public Page getNextPage()
    {
        Page basePage = pageSourceDelegate.getNextPage();
        if (basePage == null) {
            return null;
        }

        // Inject the row number into the rowid column, copying all the other columns
        int writeColumnCount = queryColumns.size();
        Block[] blocks = new Block[writeColumnCount];

        int readIndex = 0;
        for (int writeIndex = 0; writeIndex < writeColumnCount; writeIndex++) {
            if (writeIndex == rowIdColumnIndex) {
                blocks[writeIndex] = getRowIdBlock(basePage);
                continue;
            }

            int mappedReadChannel = queryColumnMapping.isEmpty() ? readIndex : queryColumnMapping.get()[readIndex];
            blocks[writeIndex] = basePage.getBlock(mappedReadChannel);
            readIndex++;
        }

        return new Page(basePage.getPositionCount(), blocks);
    }

    /**
     * Handle for the column containing the row index for Parquet pushdown
     */
    private static DeltaLakeColumnHandle rowIndexColumn()
    {
        return new DeltaLakeColumnHandle("$delta$dummy_row_index", BIGINT, DeltaLakeColumnType.SYNTHESIZED)
        {
            @Override
            public HiveColumnHandle toHiveColumnHandle()
            {
                return ParquetPageSourceFactory.PARQUET_ROW_INDEX_COLUMN;
            }
        };
    }

    private Block getRowIdBlock(Page basePage)
    {
        switch (writeType) {
            case UPDATE:
                return getUpdateRowIdBlock(basePage);
            case DELETE:
                return getRowIndexBlock(basePage);
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported write type: " + writeType);
    }

    private Block getUpdateRowIdBlock(Page basePage)
    {
        int[] columnMapping = rowIdColumnMapping.orElseThrow();
        Block[] rowIdBlocks;
        if (columnMapping.length > 0) {
            Block[] unmodifiedColumns = new Block[columnMapping.length];
            for (int channel = 0; channel < columnMapping.length; channel++) {
                unmodifiedColumns[channel] = basePage.getBlock(columnMapping[channel]);
            }
            rowIdBlocks = new Block[] {
                    getRowIndexBlock(basePage),
                    RowBlock.fromFieldBlocks(basePage.getPositionCount(), Optional.empty(), unmodifiedColumns),
            };
        }
        else {
            rowIdBlocks = new Block[] {getRowIndexBlock(basePage)};
        }
        return RowBlock.fromFieldBlocks(basePage.getPositionCount(), Optional.empty(), rowIdBlocks);
    }

    private Block getRowIndexBlock(Page page)
    {
        return page.getBlock(page.getChannelCount() - 1);
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        for (int position = 0; position < rowIds.getPositionCount(); position++) {
            long rowId = BIGINT.getLong(rowIds, position);
            rowsToDelete.set(toIntExact(rowId));
        }
    }

    // Delta Lake Update has two steps. First, any rows passed to updateRows are merged with the unmodified columns saved in the rowId and are immediately written to the Parquet
    // file, their row numbers are marked in the rowsToDelete BitSet. When the finish method is called the rest of the rows in the original file are copied to the new Parquet file,
    // skipping any rows which were updated.
    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        int rowIdChannel = columnValueAndRowIdChannels.get(columnValueAndRowIdChannels.size() - 1);
        List<Integer> columnChannelMapping = columnValueAndRowIdChannels.subList(0, columnValueAndRowIdChannels.size() - 1);

        Block rowIdBlock = page.getBlock(rowIdChannel);
        for (int position = 0; position < rowIdBlock.getPositionCount(); position++) {
            Block rowNumberBlock = rowIdBlock.getObject(position, Block.class);
            long rowId = rowNumberBlock.getLong(0, 0);
            rowsToDelete.set(toIntExact(rowId));
        }

        int unmodifiedColumnIndex = 0;
        int updatedColumnIndex = 0;
        Block[] fullPage = new Block[allDataColumns.size()];

        for (int targetChannel = 0; targetChannel < allDataColumns.size(); targetChannel++) {
            if (updatedColumns.contains(allDataColumns.get(targetChannel))) {
                fullPage[targetChannel] = page.getBlock(columnChannelMapping.get(updatedColumnIndex));
                updatedColumnIndex++;
            }
            else {
                Block unmodifiedColumns = rowIdBlock.getChildren().get(1);
                fullPage[targetChannel] = unmodifiedColumns.getChildren().get(unmodifiedColumnIndex);
                unmodifiedColumnIndex++;
            }
        }

        updatedFileWriter.appendRows(new Page(page.getPositionCount(), fullPage));
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        switch (writeType) {
            case UPDATE:
                return finishUpdate();
            case DELETE:
                return finishDelete();
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported write type: " + writeType);
    }

    private CompletableFuture<Collection<Slice>> finishUpdate()
    {
        if (rowsToDelete.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        return CompletableFuture.supplyAsync(this::doFinishUpdate, executorService);
    }

    private Collection<Slice> doFinishUpdate()
    {
        String relativePath = new Path(tableHandle.getLocation()).toUri().relativize(new Path(path).toUri()).toString();
        try {
            int firstRetainedRow = rowsToDelete.nextClearBit(0);
            // BitSet#nextClearBit may return an index larger than the originally specified size
            if (firstRetainedRow == -1 || firstRetainedRow >= totalRecordCount) {
                updatedFileWriter.commit();
                DataFileInfo newFileInfo = updatedFileWriter.getDataFileInfo();
                DeltaLakeUpdateResult deleteResult = new DeltaLakeUpdateResult(relativePath, Optional.of(newFileInfo));
                return ImmutableList.of(utf8Slice(updateResultJsonCodec.toJson(deleteResult)));
            }

            DataFileInfo newFileInfo = copyParquetPageSource(updatedFileWriter);
            DeltaLakeUpdateResult deleteResult = new DeltaLakeUpdateResult(relativePath, Optional.of(newFileInfo));
            return ImmutableList.of(utf8Slice(updateResultJsonCodec.toJson(deleteResult)));
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Unable to write new Parquet file for UPDATE operation", e);
        }
    }

    private CompletableFuture<Collection<Slice>> finishDelete()
    {
        if (rowsToDelete.isEmpty()) {
            return CompletableFuture.completedFuture(ImmutableList.of());
        }

        return CompletableFuture.supplyAsync(this::doFinishDelete, executorService);
    }

    private Collection<Slice> doFinishDelete()
    {
        try {
            String relativePath = new Path(tableHandle.getLocation()).toUri().relativize(new Path(path).toUri()).toString();
            int firstRetainedRow = rowsToDelete.nextClearBit(0);
            // BitSet#nextClearBit may return an index larger than the originally specified size
            if (firstRetainedRow == -1 || firstRetainedRow >= totalRecordCount) {
                return ImmutableList.of(utf8Slice(updateResultJsonCodec.toJson(new DeltaLakeUpdateResult(relativePath, Optional.empty()))));
            }

            DataFileInfo newFileInfo = copyParquetPageSource(updatedFileWriter);
            DeltaLakeUpdateResult deleteResult = new DeltaLakeUpdateResult(relativePath, Optional.of(newFileInfo));
            return ImmutableList.of(utf8Slice(updateResultJsonCodec.toJson(deleteResult)));
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Unable to write new Parquet file for DELETE operation", e);
        }
    }

    private Path getPathForNewFile()
    {
        String targetFilename = session.getQueryId() + "_" + randomUUID();
        Path dataDirectory = new Path(path).getParent();
        return new Path(dataDirectory, targetFilename);
    }

    private DataFileInfo copyParquetPageSource(DeltaLakeWriter fileWriter)
            throws IOException
    {
        ReaderPageSource readerPageSource = createParquetPageSource(
                TupleDomain.all(),
                allDataColumns.stream().map(DeltaLakeColumnHandle::toHiveColumnHandle).collect(toImmutableList()));
        ConnectorPageSource connectorPageSource = readerPageSource.get();

        boolean successfulWrite = true;

        try {
            int pageStart = 0;
            while (!connectorPageSource.isFinished()) {
                Page page = connectorPageSource.getNextPage();
                if (page == null) {
                    continue;
                }
                int pagePositionCount = page.getPositionCount();

                int nextToDelete = rowsToDelete.nextSetBit(pageStart);
                if (nextToDelete == -1 || nextToDelete >= pageStart + pagePositionCount) {
                    // page is wholly retained
                }
                else {
                    int[] retainedPositions = new int[pagePositionCount];
                    int retainedPositionsCount = 0;
                    for (int position = 0; position < pagePositionCount; position++) {
                        if (!rowsToDelete.get(pageStart + position)) {
                            retainedPositions[retainedPositionsCount] = position;
                            retainedPositionsCount++;
                        }
                    }

                    page = page.getPositions(retainedPositions, 0, retainedPositionsCount);
                }

                fileWriter.appendRows(page);
                pageStart += pagePositionCount;
            }
        }
        catch (Exception e) {
            successfulWrite = false;
            try {
                fileWriter.rollback();
            }
            catch (Exception rollbackException) {
                if (e != rollbackException) {
                    e.addSuppressed(rollbackException);
                }
            }
            throw e;
        }
        finally {
            if (successfulWrite) {
                fileWriter.commit();
            }
            connectorPageSource.close();
        }

        return fileWriter.getDataFileInfo();
    }

    /**
     * Create a page source with predicate pushdown.
     */
    private ReaderPageSource createParquetPageSource(TupleDomain<HiveColumnHandle> parquetPredicate, List<HiveColumnHandle> columns)
    {
        return ParquetPageSourceFactory.createPageSource(
                new Path(path),
                0,
                fileSize,
                fileSize,
                columns,
                parquetPredicate,
                true,
                hdfsEnvironment,
                hdfsEnvironment.getConfiguration(hdfsContext, new Path(path)),
                this.session.getIdentity(),
                parquetDateTimeZone,
                new FileFormatDataSourceStats(),
                parquetReaderOptions.withMaxReadBlockSize(getParquetMaxReadBlockSize(this.session))
                        .withUseColumnIndex(isParquetUseColumnIndex(this.session)));
    }

    private DeltaLakeWriter createWriter(Path targetFile, List<ColumnMetadata> allColumns, List<DeltaLakeColumnHandle> dataColumns)
            throws IOException
    {
        Configuration conf = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session), targetFile);
        configureCompression(conf, SNAPPY);

        Properties schema = DeltaLakePageSink.buildSchemaProperties(
                dataColumns.stream().map(DeltaLakeColumnHandle::getName).collect(toImmutableList()),
                dataColumns.stream().map(DeltaLakeColumnHandle::getType).collect(toImmutableList()));
        RecordFileWriter recordFileWriter = new RecordFileWriter(
                targetFile,
                dataColumns.stream().map(DeltaLakeColumnHandle::getName).collect(toImmutableList()),
                fromHiveStorageFormat(PARQUET),
                schema,
                PARQUET.getEstimatedWriterMemoryUsage(),
                toJobConf(conf),
                typeManager,
                DateTimeZone.UTC,
                session);

        Path tablePath = new Path(tableHandle.getLocation());
        Path relativePath = new Path(tablePath.toUri().relativize(targetFile.toUri()));

        List<String> partitionValueList = getPartitionValues(
                allColumns.stream()
                        .filter(columnMetadata -> partitionKeys.containsKey(columnMetadata.getName()))
                        .collect(toImmutableList()));

        return new DeltaLakeWriter(
                hdfsEnvironment.getFileSystem(hdfsContext, targetFile),
                recordFileWriter,
                tablePath,
                relativePath.toString(),
                partitionValueList,
                new DeltaLakeWriterStats(),
                dataColumns);
    }

    private List<String> getPartitionValues(List<ColumnMetadata> partitionColumns)
    {
        Block[] partitionValues = new Block[partitionColumns.size()];
        for (int i = 0; i < partitionValues.length; i++) {
            ColumnMetadata columnMetadata = partitionColumns.get(i);
            partitionValues[i] = nativeValueToBlock(
                    columnMetadata.getType(),
                    deserializePartitionValue(
                            new DeltaLakeColumnHandle(columnMetadata.getName(), columnMetadata.getType(), PARTITION_KEY),
                            partitionKeys.get(columnMetadata.getName())));
        }
        return createPartitionValues(
                partitionColumns.stream().map(ColumnMetadata::getType).collect(toImmutableList()),
                new Page(1, partitionValues),
                0);
    }

    @Override
    public void abort()
    {
        pageSourceDelegate.close();
    }
}
