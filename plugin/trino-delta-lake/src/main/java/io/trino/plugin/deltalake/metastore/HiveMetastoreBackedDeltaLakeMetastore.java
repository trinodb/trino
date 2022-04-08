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
package io.trino.plugin.deltalake.metastore;

import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.DeltaLakeColumnStatistics;
import io.trino.plugin.deltalake.statistics.ExtendedStatistics;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
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
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_TABLE;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.PATH_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.createStatisticsPredicate;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isExtendedStatisticsEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSplitManager.partitionMatchesPredicate;
import static io.trino.spi.statistics.StatsUtil.toStatsRepresentation;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveMetastoreBackedDeltaLakeMetastore
        implements DeltaLakeMetastore
{
    public static final String TABLE_PROVIDER_PROPERTY = "spark.sql.sources.provider";
    public static final String TABLE_PROVIDER_VALUE = "DELTA";

    private final HiveMetastore delegate;
    private final TransactionLogAccess transactionLogAccess;
    private final TypeManager typeManager;
    private final CachingExtendedStatisticsAccess statisticsAccess;

    public HiveMetastoreBackedDeltaLakeMetastore(
            HiveMetastore delegate,
            TransactionLogAccess transactionLogAccess,
            TypeManager typeManager,
            CachingExtendedStatisticsAccess statisticsAccess)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogSupport is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.statisticsAccess = requireNonNull(statisticsAccess, "statisticsAccess is null");
    }

    @Override
    public List<String> getAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return delegate.getDatabase(databaseName);
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        // it would be nice to filter out non-Delta tables; however, we can not call
        // metastore.getTablesWithParameter(schema, TABLE_PROVIDER_PROP, TABLE_PROVIDER_VALUE), because that property
        // contains a dot and must be compared case-insensitive
        return delegate.getAllTables(databaseName);
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        Optional<Table> candidate = delegate.getTable(databaseName, tableName);
        candidate.ifPresent(table -> {
            if (!TABLE_PROVIDER_VALUE.equalsIgnoreCase(table.getParameters().get(TABLE_PROVIDER_PROPERTY))) {
                throw new NotADeltaLakeTableException(databaseName, tableName);
            }
        });
        return candidate;
    }

    @Override
    public void createDatabase(Database database)
    {
        delegate.createDatabase(database);
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        delegate.dropDatabase(databaseName, deleteData);
    }

    @Override
    public void createTable(ConnectorSession session, Table table, PrincipalPrivileges principalPrivileges)
    {
        String tableLocation = table.getStorage().getLocation();
        statisticsAccess.invalidateCache(tableLocation);
        transactionLogAccess.invalidateCaches(tableLocation);
        try {
            TableSnapshot tableSnapshot = transactionLogAccess.loadSnapshot(table.getSchemaTableName(), new Path(tableLocation), session);
            Optional<MetadataEntry> maybeMetadata = transactionLogAccess.getMetadataEntry(tableSnapshot, session);
            if (maybeMetadata.isEmpty()) {
                throw new TrinoException(DELTA_LAKE_INVALID_TABLE, "Provided location did not contain a valid Delta Lake table: " + tableLocation);
            }
        }
        catch (IOException | RuntimeException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_TABLE, "Failed to access table location: " + tableLocation, e);
        }
        delegate.createTable(table, principalPrivileges);
    }

    @Override
    public void dropTable(ConnectorSession session, String databaseName, String tableName)
    {
        String tableLocation = getTableLocation(new SchemaTableName(databaseName, tableName), session);
        delegate.dropTable(databaseName, tableName, true);
        statisticsAccess.invalidateCache(tableLocation);
        transactionLogAccess.invalidateCaches(tableLocation);
    }

    @Override
    public Optional<MetadataEntry> getMetadata(TableSnapshot tableSnapshot, ConnectorSession session)
    {
        return transactionLogAccess.getMetadataEntry(tableSnapshot, session);
    }

    @Override
    public ProtocolEntry getProtocol(ConnectorSession session, TableSnapshot tableSnapshot)
    {
        return transactionLogAccess.getProtocolEntries(tableSnapshot, session)
                .reduce((first, second) -> second)
                .orElseThrow(() -> new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Protocol entry not found in transaction log for table " + tableSnapshot.getTable()));
    }

    @Override
    public String getTableLocation(SchemaTableName table, ConnectorSession session)
    {
        Map<String, String> serdeParameters = getTable(table.getSchemaName(), table.getTableName())
                .orElseThrow(() -> new TableNotFoundException(table))
                .getStorage().getSerdeParameters();
        String location = serdeParameters.get(PATH_PROPERTY);
        if (location == null) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("No %s property defined for table: %s", PATH_PROPERTY, table));
        }
        return location;
    }

    @Override
    public TableSnapshot getSnapshot(SchemaTableName table, ConnectorSession session)
    {
        try {
            return transactionLogAccess.loadSnapshot(table, new Path(getTableLocation(table, session)), session);
        }
        catch (NotADeltaLakeTableException e) {
            throw e;
        }
        catch (IOException | RuntimeException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Error getting snapshot for " + table, e);
        }
    }

    @Override
    public List<AddFileEntry> getValidDataFiles(SchemaTableName table, ConnectorSession session)
    {
        return transactionLogAccess.getActiveFiles(getSnapshot(table, session), session);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, DeltaLakeTableHandle tableHandle)
    {
        TableSnapshot tableSnapshot = getSnapshot(tableHandle.getSchemaTableName(), session);

        double numRecords = 0L;

        MetadataEntry metadata = transactionLogAccess.getMetadataEntry(tableSnapshot, session)
                .orElseThrow(() -> new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Metadata not found in transaction log for " + tableHandle.getTableName()));
        List<ColumnMetadata> columnMetadata = DeltaLakeSchemaSupport.extractSchema(metadata, typeManager);
        List<DeltaLakeColumnHandle> columns = columnMetadata.stream()
                .map(columnMeta -> new DeltaLakeColumnHandle(
                        columnMeta.getName(),
                        columnMeta.getType(),
                        metadata.getCanonicalPartitionColumns().contains(columnMeta.getName()) ? PARTITION_KEY : REGULAR))
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
                .map(DeltaLakeColumnHandle::getName)
                .collect(toImmutableSet());
        List<ColumnMetadata> predicatedColumns = columnMetadata.stream()
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
                    tableHandle.getMetadataEntry().getCanonicalPartitionColumns());
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
                    Optional<String> partitionValue = addEntry.getCanonicalPartitionValues().get(column.getName());
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
                    Optional<Long> maybeNullCount = stats.getNullCount(column.getName());
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
                        .map(parsedValue -> toStatsRepresentation(column.getType(), parsedValue))
                        .filter(OptionalDouble::isPresent)
                        .map(OptionalDouble::getAsDouble)
                        .ifPresent(parsedValueAsDouble -> minValues.merge(column, parsedValueAsDouble, Math::min));

                stats.getMaxColumnValue(column)
                        .map(parsedValue -> toStatsRepresentation(column.getType(), parsedValue))
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
            statistics = statisticsAccess.readExtendedStatistics(session, tableHandle.getLocation());
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
                DeltaLakeColumnStatistics deltaLakeColumnStatistics = statistics.get().getColumnStatistics().get(column.getName());
                if (deltaLakeColumnStatistics != null && column.getColumnType() != PARTITION_KEY) {
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

    @Override
    public HiveMetastore getHiveMetastore()
    {
        return delegate;
    }
}
