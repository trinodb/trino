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
import io.trino.plugin.deltalake.delete.PageFilter;
import io.trino.plugin.deltalake.delete.PositionDeleteFilter;
import io.trino.plugin.deltalake.delete.RoaringBitmapArray;
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.ReaderProjectionsAdapter;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
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
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.plugin.deltalake.DeltaHiveTypeTranslator.toHiveType;
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
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.PARQUET_ROW_INDEX_COLUMN;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DeltaLakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    // This is used whenever a query doesn't reference any data columns.
    // We need to limit the number of rows per page in case there are projections
    // in the query that can cause page sizes to explode. For example: SELECT rand() FROM some_table
    // TODO (https://github.com/trinodb/trino/issues/16824) allow connector to return pages of arbitrary row count and handle this gracefully in engine
    private static final int MAX_RLE_PAGE_SIZE = DEFAULT_MAX_PAGE_SIZE_IN_BYTES / SIZE_OF_LONG;
    private static final int MAX_RLE_ROW_ID_PAGE_SIZE = DEFAULT_MAX_PAGE_SIZE_IN_BYTES / (SIZE_OF_LONG * 2);

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
        if (filteredSplitPredicate.isAll() &&
                split.getStart() == 0 && split.getLength() == split.getFileSize() &&
                split.getFileRowCount().isPresent() &&
                split.getDeletionVector().isEmpty() &&
                (regularColumns.isEmpty() || onlyRowIdColumn(regularColumns))) {
            return new DeltaLakePageSource(
                    deltaLakeColumns,
                    ImmutableSet.of(),
                    partitionKeys,
                    partitionValues,
                    generatePages(split.getFileRowCount().get(), onlyRowIdColumn(regularColumns)),
                    Optional.empty(),
                    split.getPath(),
                    split.getFileSize(),
                    split.getFileModifiedTime(),
                    Optional::empty);
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

        ImmutableSet.Builder<String> missingColumnNames = ImmutableSet.builder();
        ImmutableList.Builder<HiveColumnHandle> hiveColumnHandles = ImmutableList.builder();
        for (DeltaLakeColumnHandle column : regularColumns) {
            if (column.baseColumnName().equals(ROW_ID_COLUMN_NAME)) {
                hiveColumnHandles.add(PARQUET_ROW_INDEX_COLUMN);
                continue;
            }
            toHiveColumnHandle(column, columnMappingMode, parquetFieldIdToName).ifPresentOrElse(
                    hiveColumnHandles::add,
                    () -> missingColumnNames.add(column.baseColumnName()));
        }
        if (split.getDeletionVector().isPresent() && !regularColumns.contains(rowPositionColumnHandle())) {
            hiveColumnHandles.add(PARQUET_ROW_INDEX_COLUMN);
        }

        TupleDomain<HiveColumnHandle> parquetPredicate = getParquetTupleDomain(filteredSplitPredicate.simplify(domainCompactionThreshold), columnMappingMode, parquetFieldIdToName);

        ReaderPageSource pageSource = ParquetPageSourceFactory.createPageSource(
                inputFile,
                split.getStart(),
                split.getLength(),
                hiveColumnHandles.build(),
                ImmutableList.of(parquetPredicate),
                true,
                parquetDateTimeZone,
                fileFormatDataSourceStats,
                options,
                Optional.empty(),
                domainCompactionThreshold,
                OptionalLong.of(split.getFileSize()));

        Optional<ReaderProjectionsAdapter> projectionsAdapter = pageSource.getReaderColumns().map(readerColumns ->
                new ReaderProjectionsAdapter(
                        hiveColumnHandles.build(),
                        readerColumns,
                        column -> ((HiveColumnHandle) column).getType(),
                        HivePageSourceProvider::getProjection));

        Supplier<Optional<PageFilter>> deletePredicate = Suppliers.memoize(() -> {
            if (split.getDeletionVector().isEmpty()) {
                return Optional.empty();
            }

            List<DeltaLakeColumnHandle> requiredColumns = ImmutableList.<DeltaLakeColumnHandle>builderWithExpectedSize(regularColumns.size() + 1)
                    .addAll(regularColumns)
                    .add(rowPositionColumnHandle())
                    .build();
            PositionDeleteFilter deleteFilter = readDeletes(fileSystem, Location.of(table.location()), split.getDeletionVector().get());
            return Optional.of(deleteFilter.createPredicate(requiredColumns));
        });

        return new DeltaLakePageSource(
                deltaLakeColumns,
                missingColumnNames.build(),
                partitionKeys,
                partitionValues,
                pageSource.get(),
                projectionsAdapter,
                split.getPath(),
                split.getFileSize(),
                split.getFileModifiedTime(),
                deletePredicate);
    }

    private PositionDeleteFilter readDeletes(
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

    public Map<Integer, String> loadParquetIdAndNameMapping(TrinoInputFile inputFile, ParquetReaderOptions options)
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
                    private static final Block[] EMPTY_BLOCKS = new Block[0];

                    private final int maxPageSize = projectRowNumber ? MAX_RLE_ROW_ID_PAGE_SIZE : MAX_RLE_PAGE_SIZE;
                    private long rowIndex;

                    @Override
                    protected Page computeNext()
                    {
                        if (rowIndex == totalRowCount) {
                            return endOfData();
                        }
                        int pageSize = toIntExact(min(maxPageSize, totalRowCount - rowIndex));
                        Block[] blocks;
                        if (projectRowNumber) {
                            blocks = new Block[] {createRowNumberBlock(rowIndex, pageSize)};
                        }
                        else {
                            blocks = EMPTY_BLOCKS;
                        }
                        rowIndex += pageSize;
                        return new Page(pageSize, blocks);
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
}
