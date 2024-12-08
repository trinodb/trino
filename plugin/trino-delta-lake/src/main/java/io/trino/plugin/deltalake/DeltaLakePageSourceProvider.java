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

import com.google.common.base.Suppliers;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.deltalake.delete.PositionDeleteFilter;
import io.trino.plugin.deltalake.delete.RoaringBitmapArray;
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.TransformConnectorPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.Utils;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeManager;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.deltalake.DeltaHiveTypeTranslator.toHiveType;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.FILE_MODIFIED_TIME_TYPE;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.FILE_SIZE_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.FILE_SIZE_TYPE;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.PATH_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.PATH_TYPE;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.ROW_ID_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.rowPositionColumnHandle;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetMaxReadBlockRowCount;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetMaxReadBlockSize;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetSmallFileThreshold;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isParquetIgnoreStatistics;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isParquetUseColumnIndex;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isParquetVectorizedDecodingEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSplitManager.partitionMatchesPredicate;
import static io.trino.plugin.deltalake.delete.DeletionVectors.readDeletionVectors;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnMappingMode;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.deserializePartitionValue;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.PARQUET_ROW_INDEX_COLUMN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DeltaLakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final JsonCodec<List<String>> PARTITIONS_CODEC = new JsonCodecFactory().listJsonCodec(String.class);

    private static final int MAX_ROW_ID_POSITIONS = 100_000;

    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;
    private final int domainCompactionThreshold;
    private final DateTimeZone parquetDateTimeZone;
    private final TypeManager typeManager;

    @Inject
    public DeltaLakePageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderConfig parquetReaderConfig,
            DeltaLakeConfig deltaLakeConfig,
            TypeManager typeManager)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetReaderOptions = parquetReaderConfig.toParquetReaderOptions().withBloomFilter(false);
        this.domainCompactionThreshold = deltaLakeConfig.getDomainCompactionThreshold();
        this.parquetDateTimeZone = deltaLakeConfig.getParquetDateTimeZone();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
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
        DeltaLakeSplit split = (DeltaLakeSplit) connectorSplit;
        DeltaLakeTableHandle table = (DeltaLakeTableHandle) connectorTable;

        List<DeltaLakeColumnHandle> deltaLakeColumns = columns.stream()
                .map(DeltaLakeColumnHandle.class::cast)
                .collect(toImmutableList());

        List<DeltaLakeColumnHandle> regularColumns = deltaLakeColumns.stream()
                .filter(column -> (column.columnType() == REGULAR) || column.baseColumnName().equals(ROW_ID_COLUMN_NAME))
                .collect(toImmutableList());

        Map<String, Optional<String>> partitionKeys = split.getPartitionKeys();
        ColumnMappingMode columnMappingMode = getColumnMappingMode(table.getMetadataEntry(), table.getProtocolEntry());
        Optional<List<String>> partitionValues = Optional.empty();
        if (deltaLakeColumns.stream().anyMatch(column -> column.baseColumnName().equals(ROW_ID_COLUMN_NAME))) {
            // using ArrayList because partition values can be null
            partitionValues = Optional.of(new ArrayList<>());
            Map<String, DeltaLakeColumnMetadata> columnsMetadataByName = extractSchema(table.getMetadataEntry(), table.getProtocolEntry(), typeManager).stream()
                    .collect(toImmutableMap(DeltaLakeColumnMetadata::name, Function.identity()));
            for (String partitionColumnName : table.getMetadataEntry().getOriginalPartitionColumns()) {
                DeltaLakeColumnMetadata partitionColumn = columnsMetadataByName.get(partitionColumnName);
                checkState(partitionColumn != null, "Partition column %s not found", partitionColumnName);
                Optional<String> value = switch (columnMappingMode) {
                    case NONE -> partitionKeys.get(partitionColumn.name());
                    case ID, NAME -> partitionKeys.get(partitionColumn.physicalName());
                    default -> throw new IllegalStateException("Unknown column mapping mode");
                };
                // Fill partition values in the same order as the partition columns are specified in the table definition
                partitionValues.get().add(value.orElse(null));
            }
        }

        // We reach here when we could not prune the split using file level stats, table predicate
        // and the dynamic filter in the coordinator during split generation. The file level stats
        // in DeltaLakeSplit#statisticsPredicate could help to prune this split when a more selective dynamic filter
        // is available now, without having to access parquet file footer for row-group stats.
        TupleDomain<DeltaLakeColumnHandle> filteredSplitPredicate = TupleDomain.intersect(ImmutableList.of(
                table.getNonPartitionConstraint(),
                split.getStatisticsPredicate(),
                dynamicFilter.getCurrentPredicate().transformKeys(DeltaLakeColumnHandle.class::cast)));
        if (filteredSplitPredicate.isNone()) {
            return new EmptyPageSource();
        }
        Map<DeltaLakeColumnHandle, Domain> partitionColumnDomains = filteredSplitPredicate.getDomains().orElseThrow().entrySet().stream()
                .filter(entry -> entry.getKey().columnType() == DeltaLakeColumnType.PARTITION_KEY)
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!partitionMatchesPredicate(split.getPartitionKeys(), partitionColumnDomains)) {
            return new EmptyPageSource();
        }
        // Skip reading the file if none of the actual file columns are being read
        if (filteredSplitPredicate.isAll() &&
                split.getStart() == 0 && split.getLength() == split.getFileSize() &&
                split.getFileRowCount().isPresent() &&
                split.getDeletionVector().isEmpty() &&
                (regularColumns.isEmpty() || onlyRowIdColumn(regularColumns))) {
            return projectColumns(
                    deltaLakeColumns,
                    ImmutableSet.of(),
                    partitionKeys,
                    partitionValues,
                    generatePages(split.getFileRowCount().get(), onlyRowIdColumn(regularColumns)),
                    split.getPath(),
                    split.getFileSize(),
                    split.getFileModifiedTime());
        }

        Location location = Location.of(split.getPath());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile = fileSystem.newInputFile(location, split.getFileSize());
        ParquetReaderOptions options = parquetReaderOptions.withMaxReadBlockSize(getParquetMaxReadBlockSize(session))
                .withMaxReadBlockRowCount(getParquetMaxReadBlockRowCount(session))
                .withSmallFileThreshold(getParquetSmallFileThreshold(session))
                .withUseColumnIndex(isParquetUseColumnIndex(session))
                .withIgnoreStatistics(isParquetIgnoreStatistics(session))
                .withVectorizedDecodingEnabled(isParquetVectorizedDecodingEnabled(session));

        Map<Integer, String> parquetFieldIdToName = columnMappingMode == ColumnMappingMode.ID ? loadParquetIdAndNameMapping(inputFile, options) : ImmutableMap.of();

        ImmutableSet.Builder<String> missingColumnNamesBuilder = ImmutableSet.builder();
        ImmutableList.Builder<HiveColumnHandle> hiveColumnHandlesBuilder = ImmutableList.builder();
        for (DeltaLakeColumnHandle column : regularColumns) {
            if (column.baseColumnName().equals(ROW_ID_COLUMN_NAME)) {
                hiveColumnHandlesBuilder.add(PARQUET_ROW_INDEX_COLUMN);
                continue;
            }
            toHiveColumnHandle(column, columnMappingMode, parquetFieldIdToName).ifPresentOrElse(
                    hiveColumnHandlesBuilder::add,
                    () -> missingColumnNamesBuilder.add(column.baseColumnName()));
        }
        if (split.getDeletionVector().isPresent() && !regularColumns.contains(rowPositionColumnHandle())) {
            hiveColumnHandlesBuilder.add(PARQUET_ROW_INDEX_COLUMN);
        }
        List<HiveColumnHandle> hiveColumnHandles = hiveColumnHandlesBuilder.build();
        Set<String> missingColumnNames = missingColumnNamesBuilder.build();

        TupleDomain<HiveColumnHandle> parquetPredicate = getParquetTupleDomain(filteredSplitPredicate.simplify(domainCompactionThreshold), columnMappingMode, parquetFieldIdToName);

        ConnectorPageSource delegate = ParquetPageSourceFactory.createPageSource(
                inputFile,
                split.getStart(),
                split.getLength(),
                hiveColumnHandles,
                ImmutableList.of(parquetPredicate),
                true,
                parquetDateTimeZone,
                fileFormatDataSourceStats,
                options,
                Optional.empty(),
                domainCompactionThreshold,
                OptionalLong.of(split.getFileSize()));

        if (split.getDeletionVector().isPresent()) {
            var pageFilterSupplier = Suppliers.memoize(() -> {
                List<DeltaLakeColumnHandle> requiredColumns = ImmutableList.<DeltaLakeColumnHandle>builderWithExpectedSize(regularColumns.size() + 1)
                        .addAll(regularColumns)
                        .add(rowPositionColumnHandle())
                        .build();
                PositionDeleteFilter deleteFilter = readDeletes(fileSystem, Location.of(table.location()), split.getDeletionVector().get());
                return deleteFilter.createPredicate(requiredColumns);
            });
            delegate = TransformConnectorPageSource.create(delegate, page -> pageFilterSupplier.get().apply(page));
        }

        return projectColumns(
                deltaLakeColumns,
                missingColumnNames,
                partitionKeys,
                partitionValues,
                delegate,
                split.getPath(),
                split.getFileSize(),
                split.getFileModifiedTime());
    }

    public static ConnectorPageSource projectColumns(
            List<DeltaLakeColumnHandle> deltaLakeColumns,
            Set<String> missingColumnNames,
            Map<String, Optional<String>> partitionKeys,
            Optional<List<String>> partitionValues,
            ConnectorPageSource delegate,
            String path,
            long fileSize,
            long fileModifiedTime)
    {
        int delegateIndex = 0;
        TransformConnectorPageSource.Builder transform = TransformConnectorPageSource.builder();
        for (DeltaLakeColumnHandle column : deltaLakeColumns) {
            if (column.isBaseColumn() && partitionKeys.containsKey(column.basePhysicalColumnName())) {
                Object prefilledValue = deserializePartitionValue(column, partitionKeys.get(column.basePhysicalColumnName()));
                transform.constantValue(Utils.nativeValueToBlock(column.baseType(), prefilledValue));
            }
            else if (column.baseColumnName().equals(PATH_COLUMN_NAME)) {
                transform.constantValue(Utils.nativeValueToBlock(PATH_TYPE, utf8Slice(path)));
            }
            else if (column.baseColumnName().equals(FILE_SIZE_COLUMN_NAME)) {
                transform.constantValue(Utils.nativeValueToBlock(FILE_SIZE_TYPE, fileSize));
            }
            else if (column.baseColumnName().equals(FILE_MODIFIED_TIME_COLUMN_NAME)) {
                long packedTimestamp = packDateTimeWithZone(fileModifiedTime, UTC_KEY);
                transform.constantValue(Utils.nativeValueToBlock(FILE_MODIFIED_TIME_TYPE, packedTimestamp));
            }
            else if (column.baseColumnName().equals(ROW_ID_COLUMN_NAME)) {
                Block pathBlock = Utils.nativeValueToBlock(VARCHAR, utf8Slice(path));
                Block partitionsBlock = Utils.nativeValueToBlock(VARCHAR, wrappedBuffer(PARTITIONS_CODEC.toJsonBytes(partitionValues.orElseThrow(() -> new IllegalStateException("partitionValues not provided")))));
                transform.transform(delegateIndex, new CreateRowIdBlock(pathBlock, partitionsBlock));
                delegateIndex++;
            }
            else if (missingColumnNames.contains(column.baseColumnName())) {
                transform.constantValue(column.baseType().createNullBlock());
            }
            else {
                transform.column(delegateIndex);
                delegateIndex++;
            }
        }
        return transform.build(delegate);
    }

    private static Block createRowIdBlock(Block pathValue, Block rowIndexBlock, Block partitionsValue)
    {
        return RowBlock.fromFieldBlocks(rowIndexBlock.getPositionCount(), new Block[] {
                RunLengthEncodedBlock.create(pathValue, rowIndexBlock.getPositionCount()),
                rowIndexBlock,
                RunLengthEncodedBlock.create(partitionsValue, rowIndexBlock.getPositionCount()),
        });
    }

    private static PositionDeleteFilter readDeletes(
            TrinoFileSystem fileSystem,
            Location tableLocation,
            DeletionVectorEntry deletionVector)
    {
        try {
            RoaringBitmapArray deletedRows = readDeletionVectors(fileSystem, tableLocation, deletionVector);
            return new PositionDeleteFilter(deletedRows);
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Failed to read deletion vectors", e);
        }
    }

    private Map<Integer, String> loadParquetIdAndNameMapping(TrinoInputFile inputFile, ParquetReaderOptions options)
    {
        try (ParquetDataSource dataSource = new TrinoParquetDataSource(inputFile, options, fileFormatDataSourceStats)) {
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            return fileSchema.getFields().stream()
                    .filter(field -> field.getId() != null) // field id returns null if undefined
                    .collect(toImmutableMap(field -> field.getId().intValue(), Type::getName));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static TupleDomain<HiveColumnHandle> getParquetTupleDomain(TupleDomain<DeltaLakeColumnHandle> effectivePredicate, ColumnMappingMode columnMapping, Map<Integer, String> fieldIdToName)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<HiveColumnHandle, Domain> predicate = ImmutableMap.builder();
        effectivePredicate.getDomains().get().forEach((columnHandle, domain) -> {
            String baseType = columnHandle.baseType().getTypeSignature().getBase();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if (!baseType.equals(StandardTypes.MAP) && !baseType.equals(StandardTypes.ARRAY) && !baseType.equals(StandardTypes.ROW)) {
                Optional<HiveColumnHandle> hiveColumnHandle = toHiveColumnHandle(columnHandle, columnMapping, fieldIdToName);
                hiveColumnHandle.ifPresent(column -> predicate.put(column, domain));
            }
        });
        return TupleDomain.withColumnDomains(predicate.buildOrThrow());
    }

    public static Optional<HiveColumnHandle> toHiveColumnHandle(DeltaLakeColumnHandle deltaLakeColumnHandle, ColumnMappingMode columnMapping, Map<Integer, String> fieldIdToName)
    {
        switch (columnMapping) {
            case ID:
                Integer fieldId = deltaLakeColumnHandle.baseFieldId().orElseThrow(() -> new IllegalArgumentException("Field ID must exist"));
                if (!fieldIdToName.containsKey(fieldId)) {
                    return Optional.empty();
                }
                String fieldName = fieldIdToName.get(fieldId);
                Optional<HiveColumnProjectionInfo> hiveColumnProjectionInfo = deltaLakeColumnHandle.projectionInfo()
                        .map(DeltaLakeColumnProjectionInfo::toHiveColumnProjectionInfo);
                return Optional.of(new HiveColumnHandle(
                        fieldName,
                        0,
                        toHiveType(deltaLakeColumnHandle.basePhysicalType()),
                        deltaLakeColumnHandle.basePhysicalType(),
                        hiveColumnProjectionInfo,
                        deltaLakeColumnHandle.columnType().toHiveColumnType(),
                        Optional.empty()));
            case NAME:
            case NONE:
                checkArgument(fieldIdToName.isEmpty(), "Mapping between field id and name must be empty: %s", fieldIdToName);
                return Optional.of(deltaLakeColumnHandle.toHiveColumnHandle());
            case UNKNOWN:
            default:
                throw new IllegalArgumentException("Unsupported column mapping: " + columnMapping);
        }
    }

    private static boolean onlyRowIdColumn(List<DeltaLakeColumnHandle> columns)
    {
        return columns.size() == 1 && getOnlyElement(columns).baseColumnName().equals(ROW_ID_COLUMN_NAME);
    }

    private static ConnectorPageSource generatePages(long totalRowCount, boolean projectRowNumber)
    {
        return new FixedPageSource(
                new AbstractIterator<>()
                {
                    private long rowIndex;

                    @Override
                    protected Page computeNext()
                    {
                        if (rowIndex == totalRowCount) {
                            return endOfData();
                        }
                        int pageSize = toIntExact(min(MAX_ROW_ID_POSITIONS, totalRowCount - rowIndex));

                        Page page;
                        if (projectRowNumber) {
                            page = new Page(pageSize, createRowNumberBlock(rowIndex, pageSize));
                        }
                        else {
                            page = new Page(pageSize);
                        }
                        rowIndex += pageSize;
                        return page;
                    }
                },
                0);
    }

    private static Block createRowNumberBlock(long baseIndex, int size)
    {
        long[] rowIndices = new long[size];
        for (int position = 0; position < size; position++) {
            rowIndices[position] = baseIndex + position;
        }
        return new LongArrayBlock(size, Optional.empty(), rowIndices);
    }

    private record CreateRowIdBlock(Block pathBlock, Block partitionsBlock)
            implements Function<Block, Block>
    {
        @Override
        public Block apply(Block block)
        {
            return createRowIdBlock(pathBlock, block, partitionsBlock);
        }
    }
}
