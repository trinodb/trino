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
package io.trino.plugin.deltalake.statistics;

import com.google.inject.Inject;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeColumnMetadata;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TypeManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.createStatisticsPredicate;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isExtendedStatisticsEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSplitManager.partitionMatchesPredicate;
import static io.trino.spi.statistics.StatsUtil.toStatsRepresentation;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.util.Objects.requireNonNull;

public class FileBasedTableStatisticsProvider
        implements DeltaLakeTableStatisticsProvider
{
    private final TypeManager typeManager;
    private final TransactionLogAccess transactionLogAccess;
    private final CachingExtendedStatisticsAccess statisticsAccess;

    @Inject
    public FileBasedTableStatisticsProvider(
            TypeManager typeManager,
            TransactionLogAccess transactionLogAccess,
            CachingExtendedStatisticsAccess statisticsAccess)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.statisticsAccess = requireNonNull(statisticsAccess, "statisticsAccess is null");
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, DeltaLakeTableHandle tableHandle, TableSnapshot tableSnapshot)
    {
        double numRecords = 0L;

        MetadataEntry metadata = tableHandle.getMetadataEntry();
        List<DeltaLakeColumnMetadata> columnMetadata = DeltaLakeSchemaSupport.extractSchema(metadata, typeManager);
        List<DeltaLakeColumnHandle> columns = columnMetadata.stream()
                .map(columnMeta -> new DeltaLakeColumnHandle(
                        columnMeta.getName(),
                        columnMeta.getType(),
                        columnMeta.getFieldId(),
                        columnMeta.getPhysicalName(),
                        columnMeta.getPhysicalColumnType(),
                        metadata.getOriginalPartitionColumns().contains(columnMeta.getName()) ? PARTITION_KEY : REGULAR,
                        Optional.empty()))
                .collect(toImmutableList());

        Map<DeltaLakeColumnHandle, Double> nullCounts = new HashMap<>();
        columns.forEach(column -> nullCounts.put(column, 0.0));
        Map<DeltaLakeColumnHandle, Double> minValues = new HashMap<>();
        Map<DeltaLakeColumnHandle, Double> maxValues = new HashMap<>();
        Map<DeltaLakeColumnHandle, Set<String>> partitioningColumnsDistinctValues = new HashMap<>();
        columns.stream()
                .filter(column -> column.getColumnType() == PARTITION_KEY)
                .forEach(column -> partitioningColumnsDistinctValues.put(column, new HashSet<>()));

        if (tableHandle.getEnforcedPartitionConstraint().isNone() || tableHandle.getNonPartitionConstraint().isNone()) {
            return createZeroStatistics(columns);
        }

        Set<String> predicatedColumnNames = tableHandle.getNonPartitionConstraint().getDomains().orElseThrow().keySet().stream()
                // TODO Statistics for column inside complex type is not collected (https://github.com/trinodb/trino/issues/17164)
                .filter(DeltaLakeColumnHandle::isBaseColumn)
                .map(DeltaLakeColumnHandle::getBaseColumnName)
                .collect(toImmutableSet());
        List<DeltaLakeColumnMetadata> predicatedColumns = columnMetadata.stream()
                .filter(column -> predicatedColumnNames.contains(column.getName()))
                .collect(toImmutableList());

        for (AddFileEntry addEntry : transactionLogAccess.getActiveFiles(tableSnapshot, session)) {
            Optional<? extends DeltaLakeFileStatistics> fileStatistics = addEntry.getStats();
            if (fileStatistics.isEmpty()) {
                // Open source Delta Lake does not collect stats
                return TableStatistics.empty();
            }
            DeltaLakeFileStatistics stats = fileStatistics.get();
            if (!partitionMatchesPredicate(addEntry.getCanonicalPartitionValues(), tableHandle.getEnforcedPartitionConstraint().getDomains().orElseThrow())) {
                continue;
            }

            TupleDomain<DeltaLakeColumnHandle> statisticsPredicate = createStatisticsPredicate(
                    addEntry,
                    predicatedColumns,
                    tableHandle.getMetadataEntry().getLowercasePartitionColumns());
            if (!tableHandle.getNonPartitionConstraint().overlaps(statisticsPredicate)) {
                continue;
            }

            if (stats.getNumRecords().isEmpty()) {
                // Not clear if it's possible for stats to be present with no row count, but bail out if that happens
                return TableStatistics.empty();
            }
            numRecords += stats.getNumRecords().get();
            for (DeltaLakeColumnHandle column : columns) {
                if (column.getColumnType() == PARTITION_KEY) {
                    Optional<String> partitionValue = addEntry.getCanonicalPartitionValues().get(column.getBasePhysicalColumnName());
                    if (partitionValue.isEmpty()) {
                        nullCounts.merge(column, (double) stats.getNumRecords().get(), Double::sum);
                    }
                    else {
                        // NULL is not counted as a distinct value
                        // Code below assumes that values returned by addEntry.getCanonicalPartitionValues() are normalized,
                        // it may not be true in case of real, doubles, timestamps etc
                        partitioningColumnsDistinctValues.get(column).add(partitionValue.get());
                    }
                }
                else {
                    Optional<Long> maybeNullCount = column.isBaseColumn() ? stats.getNullCount(column.getBasePhysicalColumnName()) : Optional.empty();
                    if (maybeNullCount.isPresent()) {
                        nullCounts.put(column, nullCounts.get(column) + maybeNullCount.get());
                    }
                    else {
                        // If any individual file fails to report null counts, fail to calculate the total for the table
                        nullCounts.put(column, NaN);
                    }
                }

                // Math.min returns NaN if any operand is NaN
                stats.getMinColumnValue(column)
                        .map(parsedValue -> toStatsRepresentation(column.getBaseType(), parsedValue))
                        .filter(OptionalDouble::isPresent)
                        .map(OptionalDouble::getAsDouble)
                        .ifPresent(parsedValueAsDouble -> minValues.merge(column, parsedValueAsDouble, Math::min));

                stats.getMaxColumnValue(column)
                        .map(parsedValue -> toStatsRepresentation(column.getBaseType(), parsedValue))
                        .filter(OptionalDouble::isPresent)
                        .map(OptionalDouble::getAsDouble)
                        .ifPresent(parsedValueAsDouble -> maxValues.merge(column, parsedValueAsDouble, Math::max));
            }
        }

        if (numRecords == 0) {
            return createZeroStatistics(columns);
        }

        TableStatistics.Builder statsBuilder = new TableStatistics.Builder().setRowCount(Estimate.of(numRecords));

        Optional<ExtendedStatistics> statistics = Optional.empty();
        if (isExtendedStatisticsEnabled(session)) {
            statistics = statisticsAccess.readExtendedStatistics(session, tableHandle.getSchemaTableName(), tableHandle.getLocation());
        }

        for (DeltaLakeColumnHandle column : columns) {
            ColumnStatistics.Builder columnStatsBuilder = new ColumnStatistics.Builder();
            Double nullCount = nullCounts.get(column);
            columnStatsBuilder.setNullsFraction(nullCount.isNaN() ? Estimate.unknown() : Estimate.of(nullCount / numRecords));

            Double maxValue = maxValues.get(column);
            Double minValue = minValues.get(column);

            if (isValidInRange(maxValue) && isValidInRange(minValue)) {
                columnStatsBuilder.setRange(new DoubleRange(minValue, maxValue));
            }
            else if (isValidInRange(maxValue)) {
                columnStatsBuilder.setRange(new DoubleRange(NEGATIVE_INFINITY, maxValue));
            }
            else if (isValidInRange(minValue)) {
                columnStatsBuilder.setRange(new DoubleRange(minValue, POSITIVE_INFINITY));
            }

            // extend statistics with NDV
            if (column.getColumnType() == PARTITION_KEY) {
                columnStatsBuilder.setDistinctValuesCount(Estimate.of(partitioningColumnsDistinctValues.get(column).size()));
            }
            if (statistics.isPresent()) {
                DeltaLakeColumnStatistics deltaLakeColumnStatistics = statistics.get().getColumnStatistics().get(column.getBasePhysicalColumnName());
                if (deltaLakeColumnStatistics != null && column.getColumnType() != PARTITION_KEY) {
                    deltaLakeColumnStatistics.getTotalSizeInBytes().ifPresent(size -> columnStatsBuilder.setDataSize(Estimate.of(size)));
                    columnStatsBuilder.setDistinctValuesCount(Estimate.of(deltaLakeColumnStatistics.getNdvSummary().cardinality()));
                }
            }

            statsBuilder.setColumnStatistics(column, columnStatsBuilder.build());
        }

        return statsBuilder.build();
    }

    private TableStatistics createZeroStatistics(List<DeltaLakeColumnHandle> columns)
    {
        TableStatistics.Builder statsBuilder = new TableStatistics.Builder().setRowCount(Estimate.of(0));
        for (DeltaLakeColumnHandle column : columns) {
            ColumnStatistics.Builder columnStatistics = ColumnStatistics.builder();
            columnStatistics.setNullsFraction(Estimate.of(0));
            columnStatistics.setDistinctValuesCount(Estimate.of(0));
            statsBuilder.setColumnStatistics(column, columnStatistics.build());
        }

        return statsBuilder.build();
    }

    private boolean isValidInRange(Double d)
    {
        // Delta considers NaN a valid min/max value but Trino does not
        return d != null && !d.isNaN();
    }
}
