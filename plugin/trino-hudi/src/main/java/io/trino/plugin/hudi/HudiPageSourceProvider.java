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
package io.trino.plugin.hudi;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.TransformConnectorPageSource;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hudi.file.HudiBaseFile;
import io.trino.plugin.hudi.reader.TrinoHudiReaderContext;
import io.trino.plugin.hudi.util.HudiSplitColumns;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.storage.StoragePath;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.ParquetReaderProvider;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createParquetPageSource;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetMessageType;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetTupleDomain;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CURSOR_ERROR;
import static io.trino.plugin.hudi.HudiSessionProperties.getParquetMaxReadBlockRowCount;
import static io.trino.plugin.hudi.HudiSessionProperties.getParquetMaxReadBlockSize;
import static io.trino.plugin.hudi.HudiSessionProperties.getParquetSmallFileThreshold;
import static io.trino.plugin.hudi.HudiSessionProperties.isParquetIgnoreStatistics;
import static io.trino.plugin.hudi.HudiSessionProperties.isParquetUseColumnIndex;
import static io.trino.plugin.hudi.HudiSessionProperties.isParquetVectorizedDecodingEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.shouldUseParquetColumnNames;
import static io.trino.plugin.hudi.HudiSessionProperties.useParquetBloomFilter;
import static io.trino.plugin.hudi.HudiUtil.buildTableMetaClient;
import static io.trino.plugin.hudi.HudiUtil.constructSchema;
import static io.trino.plugin.hudi.HudiUtil.convertToFileSlice;
import static io.trino.plugin.hudi.HudiUtil.getLatestTableSchema;
import static io.trino.plugin.hudi.HudiUtil.prependHudiMetaAndOrderingColumns;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HudiPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(HudiPageSourceProvider.class);
    private static final int DOMAIN_COMPACTION_THRESHOLD = 1000;

    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats dataSourceStats;
    private final ParquetReaderOptions options;
    private final DateTimeZone timeZone = DateTimeZone.forID("UTC");

    @Inject
    public HudiPageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats dataSourceStats,
            ParquetReaderConfig parquetReaderConfig)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.dataSourceStats = requireNonNull(dataSourceStats, "dataSourceStats is null");
        this.options = requireNonNull(parquetReaderConfig, "parquetReaderConfig is null").toParquetReaderOptions();
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) connectorTable;
        HudiSplit hudiSplit = (HudiSplit) connectorSplit;
        Optional<HudiBaseFile> hudiBaseFileOpt = hudiSplit.getBaseFile();

        String dataFilePath = hudiBaseFileOpt.isPresent()
                ? hudiBaseFileOpt.get().getPath()
                : hudiSplit.getLogFiles().getFirst().getPath();
        // Filter out metadata table splits
        // TODO: Move this check into a higher calling stack, such that the split is not created at all
        if (dataFilePath.contains(new StoragePath(
                ((HudiTableHandle) connectorTable).getBasePath()).toUri().getPath() + "/.hoodie/metadata")) {
            return new EmptyPageSource();
        }

        // Handle MERGE_ON_READ tables to be read in read_optimized mode
        // IMPORTANT: These tables will have a COPY_ON_WRITE table, see: `HudiTableTypeUtils#fromInputFormat`
        // TODO: Move this check into a higher calling stack, such that the split is not created at all
        if (hudiTableHandle.getTableType().equals(HoodieTableType.COPY_ON_WRITE) && !hudiSplit.getLogFiles().isEmpty()) {
            if (hudiBaseFileOpt.isEmpty()) {
                // Handle hasLogFiles=true, hasBaseFile = false
                // Ignoring log files without base files, no data required to be read
                return new EmptyPageSource();
            }
        }

        HudiBaseFile hudiBaseFile = hudiBaseFileOpt.orElseThrow(
                () -> new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "No base file present in split: " + hudiSplit));
        long start = hudiBaseFile.getStart();
        long length = hudiBaseFile.getLength();

        // Enable predicate pushdown for splits containing only base files
        boolean isBaseFileOnly = hudiSplit.getLogFiles().isEmpty();
        // Convert columns to HiveColumnHandles
        List<HiveColumnHandle> hiveColumnHandles = getHiveColumns(columns);

        // Get non-synthesized columns (columns that are available in data file)
        List<HiveColumnHandle> dataColumnHandles = hiveColumnHandles.stream()
                .filter(columnHandle -> !columnHandle.isPartitionKey() && !columnHandle.isHidden())
                .collect(toList());
        List<HiveColumnHandle> hudiMetaAndDataColumnHandles = prependHudiMetaAndOrderingColumns(hudiTableHandle, dataColumnHandles);

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(hudiBaseFile.getPath()), hudiBaseFile.getFileSize());
        ConnectorPageSource dataPageSource = createPageSource(
                session,
                isBaseFileOnly ? dataColumnHandles : hudiMetaAndDataColumnHandles,
                hudiSplit,
                inputFile,
                dataSourceStats,
                ParquetReaderOptions.builder(options)
                        .withIgnoreStatistics(isParquetIgnoreStatistics(session))
                        .withMaxReadBlockSize(getParquetMaxReadBlockSize(session))
                        .withMaxReadBlockRowCount(getParquetMaxReadBlockRowCount(session))
                        .withSmallFileThreshold(getParquetSmallFileThreshold(session))
                        .withUseColumnIndex(isParquetUseColumnIndex(session))
                        .withBloomFilter(useParquetBloomFilter(session))
                        .withVectorizedDecodingEnabled(isParquetVectorizedDecodingEnabled(session))
                        .build(),
                timeZone, dynamicFilter, isBaseFileOnly);

        // Avoid avro serialization if split/filegroup only contains base files
        if (isBaseFileOnly) {
            return wrapWithSynthesizedColumns(dataPageSource, hiveColumnHandles, dataColumnHandles, hudiSplit);
        }

        // TODO: buildTableMetaClient reads hoodie.properties from storage on every MOR split, once per worker call.
        //   The coordinator already builds HoodieTableMetaClient (held as transient lazyMetaClient in HudiTableHandle),
        //   but it is not serialized to workers. To fix this, serialize HoodieTableConfig (the content of
        //   hoodie.properties) into HudiTableHandle as a string map on the coordinator side, so workers can
        //   reconstruct a HoodieTableMetaClient from memory without any storage I/O.
        //   Changes needed: HudiTableHandle (add serialized config), HudiMetadata (populate it), and this call site.
        HoodieTableMetaClient metaClient = buildTableMetaClient(
                fileSystemFactory.create(session), hudiTableHandle.getSchemaTableName().toString(), hudiTableHandle.getBasePath());

        TrinoHudiReaderContext readerContext = new TrinoHudiReaderContext(
                metaClient.getStorageConf(),
                metaClient.getTableConfig(),
                dataPageSource,
                dataColumnHandles,
                hudiMetaAndDataColumnHandles);
        Schema dataSchema =
                Optional.ofNullable(hudiTableHandle.getTableSchema())
                        .orElseGet(() -> getLatestTableSchema(metaClient, hudiTableHandle.getTableName()));

        // Construct an Avro schema for log file reader
        Schema requestedSchema = constructSchema(dataSchema, hudiMetaAndDataColumnHandles.stream().map(HiveColumnHandle::getName).toList());
        HoodieFileGroupReader<IndexedRecord> fileGroupReader =
                HoodieFileGroupReader.<IndexedRecord>newBuilder()
                        .withReaderContext(readerContext)
                        .withHoodieTableMetaClient(metaClient)
                        .withFileSlice(convertToFileSlice(hudiSplit, hudiTableHandle.getBasePath()))
                        .withDataSchema(dataSchema)
                        .withRequestedSchema(requestedSchema)
                        .withLatestCommitTime(hudiTableHandle.getLatestCommitTime())
                        .withProps(metaClient.getTableConfig().getProps())
                        .withShouldUseRecordPosition(false)
                        .withStart(start)
                        .withLength(length)
                        .build();

        ConnectorPageSource morPageSource = new HudiPageSource(
                dataPageSource,
                fileGroupReader,
                readerContext,
                dataColumnHandles);
        return wrapWithSynthesizedColumns(morPageSource, hiveColumnHandles, dataColumnHandles, hudiSplit);
    }

    static ConnectorPageSource createPageSource(
            ConnectorSession session,
            List<HiveColumnHandle> columns,
            HudiSplit hudiSplit,
            TrinoInputFile inputFile,
            FileFormatDataSourceStats dataSourceStats,
            ParquetReaderOptions options,
            DateTimeZone timeZone,
            DynamicFilter dynamicFilter,
            boolean enablePredicatePushDown)
    {
        ParquetDataSource dataSource = null;
        boolean useColumnNames = shouldUseParquetColumnNames(session);
        HudiBaseFile baseFile = hudiSplit.getBaseFile().orElseThrow(
                () -> new IllegalStateException("Base file must be present when opening Parquet page source for split: " + hudiSplit));
        String path = baseFile.getPath();
        long start = baseFile.getStart();
        long length = baseFile.getLength();
        try {
            AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
            dataSource = createDataSource(inputFile, OptionalLong.of(baseFile.getFileSize()), options, memoryContext, dataSourceStats);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, options.getMaxFooterReadSize(), Optional.empty());
            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            // When not using columnNames, physical indexes are used and there could be cases when the physical index in HiveColumnHandle is different from the fileSchema of the
            // parquet files. This could happen when schema evolution happened. In such a case, we will need to remap the column indices in the HiveColumnHandles.
            if (!useColumnNames) {
                // HiveColumnHandle names are in lower case, case-insensitive
                columns = remapColumnIndicesToPhysical(fileSchema, columns, false);
            }

            Optional<MessageType> message = getParquetMessageType(columns, useColumnNames, fileSchema);

            MessageType requestedSchema = message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));
            MessageColumnIO messageColumn = getColumnIO(fileSchema, requestedSchema);

            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);

            TupleDomain<ColumnDescriptor> parquetTupleDomain = options.isIgnoreStatistics() || !enablePredicatePushDown
                    ? TupleDomain.all()
                    : getParquetTupleDomain(descriptorsByPath, getCombinedPredicate(hudiSplit, dynamicFilter), fileSchema, useColumnNames);

            TupleDomainParquetPredicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, timeZone);

            List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                    start,
                    length,
                    dataSource,
                    parquetMetadata,
                    ImmutableList.of(parquetTupleDomain),
                    ImmutableList.of(parquetPredicate),
                    descriptorsByPath,
                    timeZone,
                    DOMAIN_COMPACTION_THRESHOLD,
                    options);

            ParquetDataSourceId dataSourceId = dataSource.getId();
            ParquetDataSource finalDataSource = dataSource;
            ParquetReaderProvider parquetReaderProvider = (fields, appendRowNumberColumn) -> new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    fields,
                    appendRowNumberColumn,
                    rowGroups,
                    finalDataSource,
                    timeZone,
                    memoryContext,
                    options,
                    exception -> handleException(dataSourceId, exception),
                    Optional.of(parquetPredicate),
                    Optional.empty(),
                    parquetMetadata.getDecryptionContext());
            return createParquetPageSource(columns, fileSchema, messageColumn, useColumnNames, parquetReaderProvider);
        }
        catch (IOException | RuntimeException e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException _) {
            }
            if (e instanceof TrinoException trinoException) {
                throw trinoException;
            }
            if (e instanceof ParquetCorruptionException) {
                throw new TrinoException(HUDI_BAD_DATA, e);
            }
            String message = "Error opening Hudi split %s (offset=%s, length=%s): %s".formatted(path, start, length, e.getMessage());
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static TrinoException handleException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(HUDI_BAD_DATA, exception);
        }
        return new TrinoException(HUDI_CURSOR_ERROR, format("Failed to read Parquet file: %s", dataSourceId), exception);
    }

    /**
     * Creates a new list of ColumnHandles where the index associated with each handle corresponds to its physical position within the provided fileSchema (MessageType).
     * This is necessary when a downstream component relies on the handle's index for physical data access, and the logical schema order (potentially reflected in the
     * original handles) differs from the physical file layout.
     *
     * @param fileSchema The MessageType representing the physical schema of the Parquet file.
     * @param requestedColumns The original list of Trino ColumnHandles as received from the engine.
     * @param caseSensitive Whether the lookup between Trino column names (from handles) and Parquet field names (from fileSchema) should be case-sensitive.
     * @return A new list of HiveColumnHandle, preserving the original order, but with each handle containing the correct physical index relative to fileSchema.
     */
    @VisibleForTesting
    public static List<HiveColumnHandle> remapColumnIndicesToPhysical(
            MessageType fileSchema,
            List<HiveColumnHandle> requestedColumns,
            boolean caseSensitive)
    {
        // Create a map from column name to its physical index in the fileSchema.
        Map<String, Integer> physicalIndexMap = new HashMap<>();
        List<Type> fileFields = fileSchema.getFields();
        for (int i = 0; i < fileFields.size(); i++) {
            Type field = fileFields.get(i);
            String fieldName = field.getName();
            String mapKey = caseSensitive ? fieldName : fieldName.toLowerCase(Locale.getDefault());
            physicalIndexMap.put(mapKey, i);
        }

        // Iterate through the columns requested by Trino IN ORDER.
        List<HiveColumnHandle> remappedHandles = new ArrayList<>(requestedColumns.size());
        for (HiveColumnHandle originalHandle : requestedColumns) {
            String requestedName = originalHandle.getBaseColumnName();

            // Determine the key to use for looking up the physical index
            String lookupKey = caseSensitive ? requestedName : requestedName.toLowerCase(Locale.getDefault());

            // Find the physical index from the file schema map constructed from fielSchema
            Integer physicalIndex = physicalIndexMap.get(lookupKey);

            HiveColumnHandle remappedHandle = new HiveColumnHandle(
                    requestedName,
                    physicalIndex,
                    originalHandle.getBaseHiveType(),
                    originalHandle.getType(),
                    originalHandle.getHiveColumnProjectionInfo(),
                    originalHandle.getColumnType(),
                    originalHandle.getComment());
            remappedHandles.add(remappedHandle);
        }

        return remappedHandles;
    }

    private static TupleDomain<HiveColumnHandle> getCombinedPredicate(HudiSplit hudiSplit, DynamicFilter dynamicFilter)
    {
        // Combine static and dynamic predicates
        TupleDomain<HiveColumnHandle> staticPredicate = hudiSplit.getPredicate();
        TupleDomain<HiveColumnHandle> dynamicPredicate = dynamicFilter.getCurrentPredicate()
                .transformKeys(HiveColumnHandle.class::cast);
        TupleDomain<HiveColumnHandle> combinedPredicate = staticPredicate.intersect(dynamicPredicate);

        if (!combinedPredicate.isAll()) {
            log.debug("Combined predicate for Parquet read (Split: %s): %s", hudiSplit, combinedPredicate);
        }
        return combinedPredicate;
    }

    /**
     * Wraps an inner page source (which outputs only physical data columns) with a
     * {@link TransformConnectorPageSource} that adds synthesized constant columns
     * (partition keys, $path, $file_size, $file_modified_time, $partition) using RLE blocks.
     *
     * @param innerSource       page source whose channels correspond 1:1 to {@code dataColumns}
     * @param allOutputColumns  full list of columns requested by the engine (data + synthesized)
     * @param dataColumns       physical data columns emitted by {@code innerSource}
     * @param split             split whose metadata is used to compute constant values for synthesized columns
     */
    private static ConnectorPageSource wrapWithSynthesizedColumns(
            ConnectorPageSource innerSource,
            List<HiveColumnHandle> allOutputColumns,
            List<HiveColumnHandle> dataColumns,
            HudiSplit split)
    {
        Map<String, Integer> dataColumnIndexMap = new HashMap<>();
        for (int i = 0; i < dataColumns.size(); i++) {
            dataColumnIndexMap.put(dataColumns.get(i).getName().toLowerCase(Locale.ROOT), i);
        }

        TransformConnectorPageSource.Builder builder = TransformConnectorPageSource.builder();
        for (HiveColumnHandle outputColumn : allOutputColumns) {
            Integer physicalIndex = dataColumnIndexMap.get(outputColumn.getName().toLowerCase(Locale.ROOT));
            if (physicalIndex != null) {
                builder.column(physicalIndex);
            }
            else {
                builder.constantValue(HudiSplitColumns.createConstantBlock(split, outputColumn));
            }
        }
        return builder.build(innerSource);
    }

    private static List<HiveColumnHandle> getHiveColumns(List<ColumnHandle> columns)
    {
        return columns.stream()
                .map(HiveColumnHandle.class::cast)
                .toList();
    }
}
