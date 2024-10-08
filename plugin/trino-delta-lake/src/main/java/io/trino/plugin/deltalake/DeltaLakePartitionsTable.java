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
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.deserializePartitionValue;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.util.Objects.requireNonNull;

public class DeltaLakePartitionsTable
        implements SystemTable
{
    private final TableSnapshot tableSnapshot;
    private final TransactionLogAccess transactionLogAccess;
    private final TypeManager typeManager;
    private final MetadataEntry metadataEntry;
    private final ProtocolEntry protocolEntry;
    private final ConnectorTableMetadata tableMetadata;
    private final List<DeltaLakeColumnMetadata> schema;
    private final List<DeltaLakeColumnHandle> partitionColumns;
    private final List<RowType.Field> partitionFields;
    private final List<DeltaLakeColumnHandle> regularColumns;
    private final Optional<RowType> dataColumnType;
    private final List<RowType> columnMetricTypes;

    public DeltaLakePartitionsTable(
            ConnectorSession session,
            SchemaTableName tableName,
            String tableLocation,
            TransactionLogAccess transactionLogAccess,
            TypeManager typeManager)
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(tableLocation, "tableLocation is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        try {
            this.tableSnapshot = transactionLogAccess.loadSnapshot(session, tableName, tableLocation, Optional.empty());
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Error getting snapshot from location: " + tableLocation, e);
        }

        this.metadataEntry = transactionLogAccess.getMetadataEntry(session, tableSnapshot);
        this.protocolEntry = transactionLogAccess.getProtocolEntry(session, tableSnapshot);
        this.schema = extractSchema(metadataEntry, protocolEntry, typeManager);

        this.partitionColumns = getPartitionColumns();
        this.partitionFields = this.partitionColumns.stream()
                .map(column -> RowType.field(column.baseColumnName(), column.type()))
                .collect(toImmutableList());

        this.regularColumns = getColumns().stream()
                .filter(column -> column.columnType() == REGULAR)
                .collect(toImmutableList());
        this.dataColumnType = getMetricsColumnType(regularColumns);

        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();

        if (!this.partitionFields.isEmpty()) {
            columnMetadataBuilder.add(new ColumnMetadata("partition", RowType.from(this.partitionFields)));
        }

        columnMetadataBuilder.add(new ColumnMetadata("file_count", BIGINT));
        columnMetadataBuilder.add(new ColumnMetadata("total_size", BIGINT));

        if (dataColumnType.isPresent()) {
            columnMetadataBuilder.add(new ColumnMetadata("data", dataColumnType.get()));
            this.columnMetricTypes = dataColumnType.get().getFields().stream()
                    .map(RowType.Field::getType)
                    .map(RowType.class::cast)
                    .collect(toImmutableList());
        }
        else {
            this.columnMetricTypes = ImmutableList.of();
        }

        this.tableMetadata = new ConnectorTableMetadata(tableName, columnMetadataBuilder.build());
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        if (partitionColumns.isEmpty()) {
            return new EmptyPageSource();
        }

        return new FixedPageSource(buildPages(session));
    }

    private List<Page> buildPages(ConnectorSession session)
    {
        PageListBuilder pageListBuilder = PageListBuilder.forTable(tableMetadata);

        Map<Map<String, Optional<String>>, DeltaLakePartitionStatistics> statisticsByPartition;
        try (Stream<AddFileEntry> activeFiles = transactionLogAccess.loadActiveFiles(session, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            statisticsByPartition = getStatisticsByPartition(activeFiles);
        }

        for (Map.Entry<Map<String, Optional<String>>, DeltaLakePartitionStatistics> partitionEntry : statisticsByPartition.entrySet()) {
            Map<String, Optional<String>> partitionValue = partitionEntry.getKey();
            DeltaLakePartitionStatistics deltaLakePartitionStatistics = partitionEntry.getValue();

            RowType partitionValuesRowType = RowType.from(partitionFields);
            SqlRow partitionValuesRow = buildRowValue(partitionValuesRowType, fields -> {
                for (int i = 0; i < partitionColumns.size(); i++) {
                    DeltaLakeColumnHandle column = partitionColumns.get(i);
                    Type type = column.type();
                    Optional<String> value = partitionValue.get(column.basePhysicalColumnName());
                    Object deserializedPartitionValue = deserializePartitionValue(column, value);
                    writeNativeValue(type, fields.get(i), deserializedPartitionValue);
                }
            });

            pageListBuilder.beginRow();

            pageListBuilder.appendNativeValue(partitionValuesRowType, partitionValuesRow);
            pageListBuilder.appendBigint(deltaLakePartitionStatistics.fileCount());
            pageListBuilder.appendBigint(deltaLakePartitionStatistics.size());

            dataColumnType.ifPresent(dataColumnType -> {
                SqlRow dataColumnRow = buildRowValue(dataColumnType, fields -> {
                    for (int i = 0; i < columnMetricTypes.size(); i++) {
                        String fieldName = regularColumns.get(i).baseColumnName();
                        Object min = deltaLakePartitionStatistics.minValues().getOrDefault(fieldName, null);
                        Object max = deltaLakePartitionStatistics.maxValues().getOrDefault(fieldName, null);
                        Long nullCount = deltaLakePartitionStatistics.nullCounts().getOrDefault(fieldName, null);
                        RowType columnMetricType = columnMetricTypes.get(i);
                        columnMetricType.writeObject(fields.get(i), getColumnMetricBlock(columnMetricType, min, max, nullCount));
                    }
                });
                pageListBuilder.appendNativeValue(dataColumnType, dataColumnRow);
            });

            pageListBuilder.endRow();
        }

        return pageListBuilder.build();
    }

    private Map<Map<String, Optional<String>>, DeltaLakePartitionStatistics> getStatisticsByPartition(Stream<AddFileEntry> addFileEntryStream)
    {
        Map<Map<String, Optional<String>>, DeltaLakePartitionStatistics.Builder> partitionValueStatistics = new HashMap<>();

        addFileEntryStream.forEach(addFileEntry -> {
            Map<String, Optional<String>> partitionValues = addFileEntry.getCanonicalPartitionValues();
            partitionValueStatistics.computeIfAbsent(partitionValues, key -> new DeltaLakePartitionStatistics.Builder(regularColumns, typeManager))
                    .acceptAddFileEntry(addFileEntry);
        });

        return partitionValueStatistics.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().build()));
    }

    private List<DeltaLakeColumnHandle> getPartitionColumns()
    {
        // the list of partition columns returned maintains the ordering of partitioned_by
        Map<String, DeltaLakeColumnMetadata> columnsMetadataByName = schema.stream()
                .collect(toImmutableMap(DeltaLakeColumnMetadata::name, Function.identity()));
        return metadataEntry.getOriginalPartitionColumns().stream()
                .map(partitionColumnName -> {
                    DeltaLakeColumnMetadata columnMetadata = columnsMetadataByName.get(partitionColumnName);
                    return new DeltaLakeColumnHandle(
                            columnMetadata.name(),
                            columnMetadata.type(), OptionalInt.empty(),
                            columnMetadata.physicalName(),
                            columnMetadata.physicalColumnType(), PARTITION_KEY, Optional.empty());
                })
                .collect(toImmutableList());
    }

    private List<DeltaLakeColumnHandle> getColumns()
    {
        return schema.stream()
                .map(column -> {
                    boolean isPartitionKey = metadataEntry.getOriginalPartitionColumns().contains(column.name());
                    return new DeltaLakeColumnHandle(
                            column.name(),
                            column.type(),
                            column.fieldId(),
                            column.physicalName(),
                            column.physicalColumnType(),
                            isPartitionKey ? PARTITION_KEY : REGULAR,
                            Optional.empty());
                })
                .collect(toImmutableList());
    }

    private static SqlRow getColumnMetricBlock(RowType columnMetricType, Object min, Object max, Long nullCount)
    {
        return buildRowValue(columnMetricType, fieldBuilders -> {
            List<RowType.Field> fields = columnMetricType.getFields();
            writeNativeValue(fields.get(0).getType(), fieldBuilders.get(0), min);
            writeNativeValue(fields.get(1).getType(), fieldBuilders.get(1), max);
            writeNativeValue(fields.get(2).getType(), fieldBuilders.get(2), nullCount);
        });
    }

    private static Optional<RowType> getMetricsColumnType(List<DeltaLakeColumnHandle> columns)
    {
        List<RowType.Field> metricColumns = columns.stream()
                .map(column -> RowType.field(
                        column.baseColumnName(),
                        RowType.from(ImmutableList.of(
                                new RowType.Field(Optional.of("min"), column.type()),
                                new RowType.Field(Optional.of("max"), column.type()),
                                new RowType.Field(Optional.of("null_count"), BIGINT)))))
                .collect(toImmutableList());
        if (metricColumns.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(RowType.from(metricColumns));
    }

    private record DeltaLakePartitionStatistics(long fileCount, long size, Map<String, Object> minValues, Map<String, Object> maxValues, Map<String, Long> nullCounts)
    {
        private DeltaLakePartitionStatistics
        {
            minValues = ImmutableMap.copyOf(requireNonNull(minValues, "minValues is null"));
            maxValues = ImmutableMap.copyOf(requireNonNull(maxValues, "maxValues is null"));
            nullCounts = ImmutableMap.copyOf(requireNonNull(nullCounts, "nullCounts is null"));
        }

        private static class Builder
        {
            private final List<DeltaLakeColumnHandle> columns;
            private final TypeManager typeManager;
            private long fileCount;
            private long size;
            private final Map<String, ColumnStatistics> columnStatistics = new HashMap<>();
            private final Map<String, Long> nullCounts = new HashMap<>();
            private boolean ignoreDataColumn;

            public Builder(List<DeltaLakeColumnHandle> columns, TypeManager typeManager)
            {
                this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
                this.typeManager = requireNonNull(typeManager, "typeManager is null");
            }

            public void acceptAddFileEntry(AddFileEntry addFileEntry)
            {
                // skipping because entry is deleted in the presence of deletion vector
                if (addFileEntry.getDeletionVector().isPresent()) {
                    return;
                }

                fileCount++;
                size += addFileEntry.getSize();

                if (ignoreDataColumn) {
                    return;
                }

                addFileEntry.getStats().ifPresentOrElse(stats -> {
                    for (DeltaLakeColumnHandle column : columns) {
                        updateMinMaxStats(
                                column.baseColumnName(),
                                column.type(),
                                stats.getMinColumnValue(column).orElse(null),
                                stats.getMaxColumnValue(column).orElse(null),
                                stats.getNumRecords().orElse(0L));
                        updateNullCountStats(column.baseColumnName(), stats.getNullCount(column.basePhysicalColumnName()).orElse(null));
                    }
                }, () -> {
                    columnStatistics.clear();
                    nullCounts.clear();
                    ignoreDataColumn = true;
                });
            }

            public DeltaLakePartitionStatistics build()
            {
                ImmutableMap.Builder<String, Object> minValues = ImmutableMap.builder();
                ImmutableMap.Builder<String, Object> maxValues = ImmutableMap.builder();

                columnStatistics.forEach((key, statistics) -> {
                    statistics.getMin().ifPresent(min -> minValues.put(key, min));
                    statistics.getMax().ifPresent(max -> maxValues.put(key, max));
                });

                return new DeltaLakePartitionStatistics(fileCount, size, minValues.buildOrThrow(), maxValues.buildOrThrow(), ImmutableMap.copyOf(nullCounts));
            }

            private void updateNullCountStats(String key, Long nullCount)
            {
                if (nullCount != null) {
                    nullCounts.merge(key, nullCount, Long::sum);
                }
            }

            private void updateMinMaxStats(
                    String key,
                    Type type,
                    Object lowerBound,
                    Object upperBound,
                    long recordCount)
            {
                if (type.isOrderable() && recordCount != 0L) {
                    // Capture the initial bounds during construction so there are always valid min/max values to compare to. This does make the first call to
                    // `ColumnStatistics#updateMinMax` a no-op.
                    columnStatistics.computeIfAbsent(key, ignored -> {
                        MethodHandle comparisonHandle = typeManager.getTypeOperators()
                                .getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
                        return new ColumnStatistics(comparisonHandle, lowerBound, upperBound);
                    }).updateMinMax(lowerBound, upperBound);
                }
            }

            private static class ColumnStatistics
            {
                private final MethodHandle comparisonHandle;

                private Optional<Object> min;
                private Optional<Object> max;

                public ColumnStatistics(MethodHandle comparisonHandle, Object initialMin, Object initialMax)
                {
                    this.comparisonHandle = requireNonNull(comparisonHandle, "comparisonHandle is null");
                    this.min = Optional.ofNullable(initialMin);
                    this.max = Optional.ofNullable(initialMax);
                }

                /**
                 * Gets the minimum value accumulated during stats collection.
                 *
                 * @return Empty if the statistics contained values which were not comparable, otherwise returns the min value.
                 */
                public Optional<Object> getMin()
                {
                    return min;
                }

                /**
                 * Gets the maximum value accumulated during stats collection.
                 *
                 * @return Empty if the statistics contained values which were not comparable, otherwise returns the max value.
                 */
                public Optional<Object> getMax()
                {
                    return max;
                }

                /**
                 * Update the stats, as long as they haven't already been invalidated
                 *
                 * @param lowerBound Trino encoded lower bound value from a file
                 * @param upperBound Trino encoded upper bound value from a file
                 */
                public void updateMinMax(Object lowerBound, Object upperBound)
                {
                    if (min.isPresent()) {
                        if (lowerBound == null) {
                            min = Optional.empty();
                        }
                        else if (compareTrinoValue(lowerBound, min.get()) < 0) {
                            min = Optional.of(lowerBound);
                        }
                    }

                    if (max.isPresent()) {
                        if (upperBound == null) {
                            max = Optional.empty();
                        }
                        else if (compareTrinoValue(upperBound, max.get()) > 0) {
                            max = Optional.of(upperBound);
                        }
                    }
                }

                private long compareTrinoValue(Object value, Object otherValue)
                {
                    try {
                        return (Long) comparisonHandle.invoke(value, otherValue);
                    }
                    catch (Throwable throwable) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unable to compare Delta min/max values", throwable);
                    }
                }
            }
        }
    }
}
