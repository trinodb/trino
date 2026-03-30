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
package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.ducklake.catalog.DucklakeCatalog;
import io.trino.plugin.ducklake.catalog.DucklakeColumn;
import io.trino.plugin.ducklake.catalog.DucklakeColumnStats;
import io.trino.plugin.ducklake.catalog.DucklakePartitionField;
import io.trino.plugin.ducklake.catalog.DucklakePartitionSpec;
import io.trino.plugin.ducklake.catalog.DucklakeSchema;
import io.trino.plugin.ducklake.catalog.DucklakeTable;
import io.trino.plugin.ducklake.catalog.DucklakeTableStats;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

/**
 * Metadata implementation for Ducklake connector.
 * Provides read-only access to Ducklake tables via SQL catalog.
 */
public class DucklakeMetadata
        implements ConnectorMetadata
{
    private final DucklakeCatalog catalog;
    private final DucklakeTypeConverter typeConverter;

    public DucklakeMetadata(DucklakeCatalog catalog, DucklakeTypeConverter typeConverter)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.typeConverter = requireNonNull(typeConverter, "typeConverter is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        return catalog.listSchemas(snapshotId).stream()
                .map(DucklakeSchema::schemaName)
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        requireNonNull(tableName, "tableName is null");

        // Get snapshot ID (current or from version if time travel is requested)
        long snapshotId = catalog.getCurrentSnapshotId();
        // TODO: Handle startVersion for time travel queries

        Optional<DucklakeTable> table = catalog.getTable(tableName, snapshotId);
        if (table.isEmpty()) {
            return null;
        }

        return new DucklakeTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.get().tableId(),
                snapshotId);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DucklakeTableHandle ducklakeTableHandle = (DucklakeTableHandle) tableHandle;

        List<DucklakeColumn> columns = catalog.getTableColumns(
                ducklakeTableHandle.tableId(),
                ducklakeTableHandle.snapshotId());

        List<ColumnMetadata> columnMetadata = columns.stream()
                .map(column -> ColumnMetadata.builder()
                        .setName(column.columnName())
                        .setType(typeConverter.toTrinoType(column.columnType()))
                        .setNullable(column.nullsAllowed())
                        .build())
                .collect(toImmutableList());

        return new ConnectorTableMetadata(
                ducklakeTableHandle.getSchemaTableName(),
                columnMetadata);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        long snapshotId = catalog.getCurrentSnapshotId();

        if (schemaName.isPresent()) {
            Optional<DucklakeSchema> schema = catalog.getSchema(schemaName.get(), snapshotId);
            if (schema.isEmpty()) {
                return ImmutableList.of();
            }

            return catalog.listTables(schema.get().schemaId(), snapshotId).stream()
                    .map(table -> new SchemaTableName(schemaName.get(), table.tableName()))
                    .collect(toImmutableList());
        }

        // List all tables across all schemas
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (DucklakeSchema schema : catalog.listSchemas(snapshotId)) {
            for (DucklakeTable table : catalog.listTables(schema.schemaId(), snapshotId)) {
                tables.add(new SchemaTableName(schema.schemaName(), table.tableName()));
            }
        }
        return tables.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DucklakeTableHandle ducklakeTableHandle = (DucklakeTableHandle) tableHandle;

        List<DucklakeColumn> columns = catalog.getTableColumns(
                ducklakeTableHandle.tableId(),
                ducklakeTableHandle.snapshotId());

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (DucklakeColumn column : columns) {
            columnHandles.put(
                    column.columnName(),
                    new DucklakeColumnHandle(
                            column.columnId(),
                            column.columnName(),
                            typeConverter.toTrinoType(column.columnType()),
                            column.nullsAllowed()));
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        DucklakeColumnHandle ducklakeColumnHandle = (DucklakeColumnHandle) columnHandle;

        return ColumnMetadata.builder()
                .setName(ducklakeColumnHandle.columnName())
                .setType(ducklakeColumnHandle.columnType())
                .setNullable(ducklakeColumnHandle.nullable())
                .build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DucklakeTableHandle table = (DucklakeTableHandle) tableHandle;

        Optional<DucklakeTableStats> tableStats = catalog.getTableStats(table.tableId());
        if (tableStats.isEmpty()) {
            return TableStatistics.empty();
        }

        long recordCount = tableStats.get().recordCount();
        TableStatistics.Builder stats = TableStatistics.builder()
                .setRowCount(Estimate.of(recordCount));

        if (recordCount == 0) {
            return stats.build();
        }

        Map<String, ColumnHandle> columnHandles = getColumnHandles(session, tableHandle);
        List<DucklakeColumnStats> columnStatsList = catalog.getColumnStats(table.tableId(), table.snapshotId());

        // Index column stats by column ID
        Map<Long, DucklakeColumnStats> statsById = columnStatsList.stream()
                .collect(toImmutableMap(DucklakeColumnStats::columnId, s -> s));

        for (ColumnHandle handle : columnHandles.values()) {
            DucklakeColumnHandle column = (DucklakeColumnHandle) handle;
            DucklakeColumnStats colStats = statsById.get(column.columnId());
            if (colStats == null) {
                continue;
            }

            ColumnStatistics.Builder colBuilder = ColumnStatistics.builder();

            if (colStats.totalValueCount() > 0) {
                colBuilder.setNullsFraction(Estimate.of((double) colStats.totalNullCount() / colStats.totalValueCount()));
            }

            if (colStats.totalSizeBytes() > 0) {
                colBuilder.setDataSize(Estimate.of(colStats.totalSizeBytes()));
            }

            toDoubleRange(column.columnType(), colStats).ifPresent(colBuilder::setRange);

            stats.setColumnStatistics(column, colBuilder.build());
        }

        return stats.build();
    }

    private static Optional<DoubleRange> toDoubleRange(Type type, DucklakeColumnStats stats)
    {
        if (stats.minValue().isEmpty() || stats.maxValue().isEmpty()) {
            return Optional.empty();
        }

        try {
            String minStr = stats.minValue().get();
            String maxStr = stats.maxValue().get();

            if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
                return Optional.of(new DoubleRange(Double.parseDouble(minStr), Double.parseDouble(maxStr)));
            }
            if (type.equals(DOUBLE) || type.equals(REAL)) {
                return Optional.of(new DoubleRange(Double.parseDouble(minStr), Double.parseDouble(maxStr)));
            }
            if (type.equals(DATE)) {
                long minDays = java.time.LocalDate.parse(minStr).toEpochDay();
                long maxDays = java.time.LocalDate.parse(maxStr).toEpochDay();
                return Optional.of(new DoubleRange(minDays, maxDays));
            }
        }
        catch (RuntimeException _) {
            // If parsing fails, skip range
        }
        return Optional.empty();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint constraint)
    {
        DucklakeTableHandle table = (DucklakeTableHandle) handle;

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll()) {
            return Optional.empty();
        }

        TupleDomain<DucklakeColumnHandle> newPredicate = extractDucklakePredicate(summary);

        // Classify predicates as enforced (partition-prunable) or unenforced (best-effort)
        List<DucklakePartitionSpec> partitionSpecs = catalog.getPartitionSpecs(
                table.tableId(), table.snapshotId());

        ImmutableMap.Builder<DucklakeColumnHandle, Domain> enforced = ImmutableMap.builder();
        ImmutableMap.Builder<DucklakeColumnHandle, Domain> unenforced = ImmutableMap.builder();

        if (!newPredicate.isNone()) {
            for (Map.Entry<DucklakeColumnHandle, Domain> entry : newPredicate.getDomains().orElse(Map.of()).entrySet()) {
                if (canEnforceColumnConstraint(partitionSpecs, entry.getKey())) {
                    enforced.put(entry.getKey(), entry.getValue());
                }
                else {
                    unenforced.put(entry.getKey(), entry.getValue());
                }
            }
        }

        TupleDomain<DucklakeColumnHandle> newEnforced = newPredicate.isNone()
                ? TupleDomain.none()
                : toTupleDomain(enforced.buildOrThrow());
        TupleDomain<DucklakeColumnHandle> newUnenforced = newPredicate.isNone()
                ? TupleDomain.all()
                : toTupleDomain(unenforced.buildOrThrow());

        TupleDomain<DucklakeColumnHandle> combinedEnforced = table.enforcedPredicate().intersect(newEnforced);
        TupleDomain<DucklakeColumnHandle> combinedUnenforced = table.unenforcedPredicate().intersect(newUnenforced);

        if (combinedEnforced.equals(table.enforcedPredicate())
                && combinedUnenforced.equals(table.unenforcedPredicate())) {
            return Optional.empty();
        }

        DucklakeTableHandle newHandle = new DucklakeTableHandle(
                table.schemaName(),
                table.tableName(),
                table.tableId(),
                table.snapshotId(),
                combinedUnenforced,
                combinedEnforced);

        // Enforced predicates are NOT returned as remaining filter — the connector
        // guarantees correct elimination via partition-value pruning.
        // Unenforced predicates are returned so the engine still verifies them.
        TupleDomain<ColumnHandle> remainingFilter = newUnenforced.transformKeys(ColumnHandle.class::cast);

        return Optional.of(new ConstraintApplicationResult<>(
                newHandle,
                remainingFilter,
                constraint.getExpression(),
                false));
    }

    private static boolean canEnforceColumnConstraint(
            List<DucklakePartitionSpec> specs,
            DucklakeColumnHandle column)
    {
        if (specs.isEmpty()) {
            return false;
        }
        // A predicate can only be enforced if it is enforceable in EVERY active spec
        // (spec evolution means different files may have different partition schemes)
        return specs.stream().allMatch(spec ->
                spec.fields().stream().anyMatch(field ->
                        field.columnId() == column.columnId() && canEnforceWithTransform(field, column)));
    }

    private static boolean canEnforceWithTransform(DucklakePartitionField field, DucklakeColumnHandle column)
    {
        if (field.transform().isIdentity()) {
            return true;
        }
        if (field.transform().isTemporal()) {
            // Temporal transforms (year/month/day/hour) on date/timestamp columns are enforceable
            // because the transform is monotonic — if a predicate range maps to a set of
            // transformed values, files outside that set can be safely skipped.
            Type type = column.columnType();
            return type.equals(DATE) || type.equals(TIMESTAMP_MILLIS) || type.equals(TIMESTAMP_MICROS);
        }
        return false;
    }

    private static TupleDomain<DucklakeColumnHandle> extractDucklakePredicate(TupleDomain<ColumnHandle> summary)
    {
        if (summary.isNone()) {
            return TupleDomain.none();
        }

        Optional<Map<ColumnHandle, Domain>> domains = summary.getDomains();
        if (domains.isEmpty()) {
            return TupleDomain.all();
        }

        ImmutableMap.Builder<DucklakeColumnHandle, Domain> ducklakeDomains = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
            if (entry.getKey() instanceof DucklakeColumnHandle columnHandle) {
                // Only push down primitive types (arrays/complex types can't be pruned)
                if (!columnHandle.columnType().getTypeParameters().isEmpty()) {
                    continue;
                }
                ducklakeDomains.put(columnHandle, entry.getValue());
            }
        }

        Map<DucklakeColumnHandle, Domain> result = ducklakeDomains.buildOrThrow();
        if (result.isEmpty()) {
            return TupleDomain.all();
        }
        return TupleDomain.withColumnDomains(result);
    }

    private static TupleDomain<DucklakeColumnHandle> toTupleDomain(Map<DucklakeColumnHandle, Domain> domains)
    {
        if (domains.isEmpty()) {
            return TupleDomain.all();
        }
        return TupleDomain.withColumnDomains(domains);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        long snapshotId = catalog.getCurrentSnapshotId();
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tables = prefix.getTable()
                .map(table -> List.of(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        for (SchemaTableName tableName : tables) {
            Optional<DucklakeTable> table = catalog.getTable(tableName, snapshotId);
            if (table.isPresent()) {
                List<DucklakeColumn> tableColumns = catalog.getTableColumns(table.get().tableId(), snapshotId);
                columns.put(
                        tableName,
                        tableColumns.stream()
                                .map(column -> ColumnMetadata.builder()
                                        .setName(column.columnName())
                                        .setType(typeConverter.toTrinoType(column.columnType()))
                                        .setNullable(column.nullsAllowed())
                                        .build())
                                .collect(toImmutableList()));
            }
        }

        return columns.buildOrThrow();
    }
}
