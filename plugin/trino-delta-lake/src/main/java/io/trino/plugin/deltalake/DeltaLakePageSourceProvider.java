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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeManager;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.deltalake.DeltaHiveTypeTranslator.toHiveType;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.ROW_ID_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetMaxReadBlockRowCount;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getParquetMaxReadBlockSize;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isParquetOptimizedNestedReaderEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isParquetOptimizedReaderEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isParquetUseColumnIndex;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnMappingMode;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.PARQUET_ROW_INDEX_COLUMN;
import static java.util.Objects.requireNonNull;

public class DeltaLakePageSourceProvider
        implements ConnectorPageSourceProvider
{
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

        // We reach here when we could not prune the split using file level stats, table predicate
        // and the dynamic filter in the coordinator during split generation. The file level stats
        // in DeltaLakeSplit#filePredicate could help to prune this split when a more selective dynamic filter
        // is available now, without having to access parquet file footer for row-group stats.
        // We avoid sending DeltaLakeSplit#splitPredicate to workers by using table.getPredicate() here.
        TupleDomain<DeltaLakeColumnHandle> filteredSplitPredicate = TupleDomain.intersect(ImmutableList.of(
                table.getNonPartitionConstraint(),
                split.getStatisticsPredicate(),
                dynamicFilter.getCurrentPredicate().transformKeys(DeltaLakeColumnHandle.class::cast)));
        if (filteredSplitPredicate.isNone()) {
            return new EmptyPageSource();
        }

        List<DeltaLakeColumnHandle> deltaLakeColumns = columns.stream()
                .map(DeltaLakeColumnHandle.class::cast)
                .collect(toImmutableList());

        Map<String, Optional<String>> partitionKeys = split.getPartitionKeys();

        List<String> partitionValues = new ArrayList<>();
        if (deltaLakeColumns.stream().anyMatch(column -> column.getName().equals(ROW_ID_COLUMN_NAME))) {
            for (DeltaLakeColumnMetadata column : extractSchema(table.getMetadataEntry(), typeManager)) {
                Optional<String> value = partitionKeys.get(column.getName());
                if (value != null) {
                    partitionValues.add(value.orElse(null));
                }
            }
        }

        TrinoInputFile inputFile = fileSystemFactory.create(session).newInputFile(split.getPath(), split.getFileSize());
        ParquetReaderOptions options = parquetReaderOptions.withMaxReadBlockSize(getParquetMaxReadBlockSize(session))
                .withMaxReadBlockRowCount(getParquetMaxReadBlockRowCount(session))
                .withUseColumnIndex(isParquetUseColumnIndex(session))
                .withBatchColumnReaders(isParquetOptimizedReaderEnabled(session))
                .withBatchNestedColumnReaders(isParquetOptimizedNestedReaderEnabled(session));

        ColumnMappingMode columnMappingMode = getColumnMappingMode(table.getMetadataEntry());
        Map<Integer, String> parquetFieldIdToName = columnMappingMode == ColumnMappingMode.ID ? loadParquetIdAndNameMapping(inputFile, options) : ImmutableMap.of();

        List<DeltaLakeColumnHandle> regularColumns = deltaLakeColumns.stream()
                .filter(column -> (column.getColumnType() == REGULAR) || column.getName().equals(ROW_ID_COLUMN_NAME))
                .collect(toImmutableList());

        ImmutableSet.Builder<String> missingColumnNames = ImmutableSet.builder();
        ImmutableList.Builder<HiveColumnHandle> hiveColumnHandles = ImmutableList.builder();
        for (DeltaLakeColumnHandle column : regularColumns) {
            if (column.getName().equals(ROW_ID_COLUMN_NAME)) {
                hiveColumnHandles.add(PARQUET_ROW_INDEX_COLUMN);
                continue;
            }
            toHiveColumnHandle(column, columnMappingMode, parquetFieldIdToName).ifPresentOrElse(
                    hiveColumnHandles::add,
                    () -> missingColumnNames.add(column.getName()));
        }

        TupleDomain<HiveColumnHandle> parquetPredicate = getParquetTupleDomain(filteredSplitPredicate.simplify(domainCompactionThreshold), columnMappingMode, parquetFieldIdToName);

        ReaderPageSource pageSource = ParquetPageSourceFactory.createPageSource(
                inputFile,
                split.getStart(),
                split.getLength(),
                hiveColumnHandles.build(),
                parquetPredicate,
                true,
                parquetDateTimeZone,
                fileFormatDataSourceStats,
                options,
                Optional.empty(),
                domainCompactionThreshold);

        verify(pageSource.getReaderColumns().isEmpty(), "All columns expected to be base columns");

        return new DeltaLakePageSource(
                deltaLakeColumns,
                missingColumnNames.build(),
                partitionKeys,
                partitionValues,
                pageSource.get(),
                split.getPath(),
                split.getFileSize(),
                split.getFileModifiedTime());
    }

    public Map<Integer, String> loadParquetIdAndNameMapping(TrinoInputFile inputFile, ParquetReaderOptions options)
    {
        try (ParquetDataSource dataSource = new TrinoParquetDataSource(inputFile, options, fileFormatDataSourceStats)) {
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
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
            String baseType = columnHandle.getType().getTypeSignature().getBase();
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
                Integer fieldId = deltaLakeColumnHandle.getFieldId().orElseThrow(() -> new IllegalArgumentException("Field ID must exist"));
                if (!fieldIdToName.containsKey(fieldId)) {
                    return Optional.empty();
                }
                String fieldName = fieldIdToName.get(fieldId);
                return Optional.of(new HiveColumnHandle(
                        fieldName,
                        0,
                        toHiveType(deltaLakeColumnHandle.getPhysicalType()),
                        deltaLakeColumnHandle.getPhysicalType(),
                        Optional.empty(),
                        deltaLakeColumnHandle.getColumnType().toHiveColumnType(),
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
}
