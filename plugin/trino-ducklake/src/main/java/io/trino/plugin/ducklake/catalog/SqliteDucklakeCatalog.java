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
package io.trino.plugin.ducklake.catalog;

import com.google.inject.Inject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;
import io.trino.plugin.ducklake.DucklakeConfig;
import io.trino.spi.connector.SchemaTableName;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * SQLite implementation of DucklakeCatalog.
 * Queries the 28 Ducklake metadata tables via JDBC.
 */
public class SqliteDucklakeCatalog
        implements DucklakeCatalog
{
    private static final Logger log = Logger.get(SqliteDucklakeCatalog.class);

    private final DataSource dataSource;
    private final HikariDataSource hikariDataSource;

    @Inject
    public SqliteDucklakeCatalog(DucklakeConfig config)
    {
        requireNonNull(config, "config is null");

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getCatalogDatabaseUrl());
        if (config.getCatalogDatabaseUser() != null) {
            hikariConfig.setUsername(config.getCatalogDatabaseUser());
        }
        if (config.getCatalogDatabasePassword() != null) {
            hikariConfig.setPassword(config.getCatalogDatabasePassword());
        }
        hikariConfig.setMaximumPoolSize(config.getMaxCatalogConnections());
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setConnectionTimeout(30000);

        this.hikariDataSource = new HikariDataSource(hikariConfig);
        this.dataSource = hikariDataSource;

        log.info("Initialized SQLite Ducklake catalog: %s", config.getCatalogDatabaseUrl());
    }

    @Override
    public long getCurrentSnapshotId()
    {
        String sql = "SELECT snapshot_id FROM ducklake_snapshot WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getLong("snapshot_id");
            }
            throw new IllegalStateException("No snapshots found in ducklake_snapshot table");
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get current snapshot", e);
        }
    }

    @Override
    public Optional<DucklakeSnapshot> getSnapshot(long snapshotId)
    {
        String sql = "SELECT snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id " +
                     "FROM ducklake_snapshot WHERE snapshot_id = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new DucklakeSnapshot(
                            rs.getLong("snapshot_id"),
                            rs.getTimestamp("snapshot_time").toInstant(),
                            rs.getLong("schema_version"),
                            rs.getLong("next_catalog_id"),
                            rs.getLong("next_file_id")));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get snapshot: " + snapshotId, e);
        }
    }

    @Override
    public List<DucklakeSchema> listSchemas(long snapshotId)
    {
        String sql = "SELECT schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative " +
                     "FROM ducklake_schema " +
                     "WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        List<DucklakeSchema> schemas = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, snapshotId);
            stmt.setLong(2, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    schemas.add(new DucklakeSchema(
                            rs.getLong("schema_id"),
                            UUID.fromString(rs.getString("schema_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getString("schema_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list schemas at snapshot: " + snapshotId, e);
        }

        return schemas;
    }

    @Override
    public Optional<DucklakeSchema> getSchema(String schemaName, long snapshotId)
    {
        String sql = "SELECT schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative " +
                     "FROM ducklake_schema " +
                     "WHERE schema_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, schemaName);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new DucklakeSchema(
                            rs.getLong("schema_id"),
                            UUID.fromString(rs.getString("schema_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getString("schema_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get schema: " + schemaName + " at snapshot: " + snapshotId, e);
        }
    }

    @Override
    public List<DucklakeTable> listTables(long schemaId, long snapshotId)
    {
        String sql = "SELECT table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative " +
                     "FROM ducklake_table " +
                     "WHERE schema_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        List<DucklakeTable> tables = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, schemaId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tables.add(new DucklakeTable(
                            rs.getLong("table_id"),
                            UUID.fromString(rs.getString("table_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("schema_id"),
                            rs.getString("table_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list tables for schema: " + schemaId + " at snapshot: " + snapshotId, e);
        }

        return tables;
    }

    @Override
    public Optional<DucklakeTable> getTable(SchemaTableName tableName, long snapshotId)
    {
        // First get the schema
        Optional<DucklakeSchema> schema = getSchema(tableName.getSchemaName(), snapshotId);
        if (schema.isEmpty()) {
            return Optional.empty();
        }

        String sql = "SELECT table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative " +
                     "FROM ducklake_table " +
                     "WHERE schema_id = ? AND table_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, schema.get().schemaId());
            stmt.setString(2, tableName.getTableName());
            stmt.setLong(3, snapshotId);
            stmt.setLong(4, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new DucklakeTable(
                            rs.getLong("table_id"),
                            UUID.fromString(rs.getString("table_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("schema_id"),
                            rs.getString("table_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get table: " + tableName + " at snapshot: " + snapshotId, e);
        }
    }

    @Override
    public Optional<DucklakeTable> getTableById(long tableId, long snapshotId)
    {
        String sql = "SELECT table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative " +
                     "FROM ducklake_table " +
                     "WHERE table_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new DucklakeTable(
                            rs.getLong("table_id"),
                            UUID.fromString(rs.getString("table_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("schema_id"),
                            rs.getString("table_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get table by ID: " + tableId + " at snapshot: " + snapshotId, e);
        }
    }

    @Override
    public List<DucklakeColumn> getTableColumns(long tableId, long snapshotId)
    {
        String sql = "SELECT column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, nulls_allowed, parent_column " +
                     "FROM ducklake_column " +
                     "WHERE table_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL) " +
                     "ORDER BY column_order, column_id";

        List<DucklakeColumn> allColumns = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    allColumns.add(new DucklakeColumn(
                            rs.getLong("column_id"),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("table_id"),
                            rs.getLong("column_order"),
                            rs.getString("column_name"),
                            rs.getString("column_type"),
                            rs.getBoolean("nulls_allowed"),
                            getLongOptional(rs, "parent_column")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get columns for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

        Map<Long, List<DucklakeColumn>> childrenByParent = new HashMap<>();
        for (DucklakeColumn column : allColumns) {
            column.parentColumn().ifPresent(parent ->
                    childrenByParent.computeIfAbsent(parent, ignored -> new ArrayList<>()).add(column));
        }

        List<DucklakeColumn> topLevelColumns = new ArrayList<>();
        for (DucklakeColumn column : allColumns) {
            if (column.parentColumn().isEmpty()) {
                topLevelColumns.add(new DucklakeColumn(
                        column.columnId(),
                        column.beginSnapshot(),
                        column.endSnapshot(),
                        column.tableId(),
                        column.columnOrder(),
                        column.columnName(),
                        resolveColumnType(column, childrenByParent),
                        column.nullsAllowed(),
                        Optional.empty()));
            }
        }

        return topLevelColumns;
    }

    @Override
    public List<DucklakeDataFile> getDataFiles(long tableId, long snapshotId)
    {
        String sql = "SELECT data.data_file_id, data.table_id, data.begin_snapshot, data.end_snapshot, data.file_order, " +
                     "       data.path, data.path_is_relative, data.file_format, data.record_count, data.file_size_bytes, " +
                     "       data.footer_size, data.row_id_start, del.path AS delete_file_path, del.path_is_relative AS delete_path_is_relative " +
                     "FROM ducklake_data_file AS data " +
                     "LEFT JOIN ducklake_delete_file AS del ON data.data_file_id = del.data_file_id " +
                     "  AND ? >= del.begin_snapshot AND (? < del.end_snapshot OR del.end_snapshot IS NULL) " +
                     "WHERE data.table_id = ? AND ? >= data.begin_snapshot AND (? < data.end_snapshot OR data.end_snapshot IS NULL) " +
                     "ORDER BY data.file_order";

        List<DucklakeDataFile> files = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, snapshotId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, tableId);
            stmt.setLong(4, snapshotId);
            stmt.setLong(5, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    files.add(new DucklakeDataFile(
                            rs.getLong("data_file_id"),
                            rs.getLong("table_id"),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("file_order"),
                            rs.getString("path"),
                            rs.getBoolean("path_is_relative"),
                            rs.getString("file_format"),
                            rs.getLong("record_count"),
                            rs.getLong("file_size_bytes"),
                            rs.getLong("footer_size"),
                            rs.getLong("row_id_start"),
                            getStringOptional(rs, "delete_file_path"),
                            getBooleanOptional(rs, "delete_path_is_relative")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get data files for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

        return files;
    }

    @Override
    public List<Long> getDataFileIdsForPredicate(long tableId, long columnId, long snapshotId, Object minValue, Object maxValue)
    {
        Optional<String> columnType = getColumnType(tableId, columnId, snapshotId);
        if (columnType.isEmpty()) {
            return List.of();
        }

        String sql = "SELECT stats.data_file_id, stats.min_value, stats.max_value " +
                     "FROM ducklake_file_column_stats AS stats " +
                     "JOIN ducklake_data_file AS data ON stats.data_file_id = data.data_file_id " +
                     "WHERE stats.table_id = ? AND stats.column_id = ? " +
                     "  AND data.table_id = ? " +
                     "  AND ? >= data.begin_snapshot AND (? < data.end_snapshot OR data.end_snapshot IS NULL)";

        List<Long> fileIds = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, columnId);
            stmt.setLong(3, tableId);
            stmt.setLong(4, snapshotId);
            stmt.setLong(5, snapshotId);

            Comparable<?> lowerBound = normalizePredicateValue(columnType.get(), minValue);
            Comparable<?> upperBound = normalizePredicateValue(columnType.get(), maxValue);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Comparable<?> minStat = parseStatValue(columnType.get(), rs.getString("min_value"));
                    Comparable<?> maxStat = parseStatValue(columnType.get(), rs.getString("max_value"));

                    if (isWithinBounds(lowerBound, upperBound, minStat, maxStat)) {
                        fileIds.add(rs.getLong("data_file_id"));
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get file IDs for predicate on table: " + tableId + ", column: " + columnId, e);
        }

        return fileIds;
    }

    @Override
    public Optional<DucklakeTableStats> getTableStats(long tableId)
    {
        String sql = "SELECT table_id, record_count, file_size_bytes FROM ducklake_table_stats WHERE table_id = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new DucklakeTableStats(
                            rs.getLong("table_id"),
                            rs.getLong("record_count"),
                            rs.getLong("file_size_bytes")));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get table stats for table: " + tableId, e);
        }
    }

    @Override
    public List<DucklakeColumnStats> getColumnStats(long tableId, long snapshotId)
    {
        String sql = "SELECT stats.column_id, " +
                     "       SUM(stats.value_count) AS total_value_count, " +
                     "       SUM(stats.null_count) AS total_null_count, " +
                     "       SUM(stats.column_size_bytes) AS total_size_bytes, " +
                     "       MIN(stats.min_value) AS min_value, " +
                     "       MAX(stats.max_value) AS max_value " +
                     "FROM ducklake_file_column_stats AS stats " +
                     "JOIN ducklake_data_file AS data ON stats.data_file_id = data.data_file_id " +
                     "WHERE stats.table_id = ? AND data.table_id = ? " +
                     "  AND ? >= data.begin_snapshot AND (? < data.end_snapshot OR data.end_snapshot IS NULL) " +
                     "GROUP BY stats.column_id";

        List<DucklakeColumnStats> result = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, tableId);
            stmt.setLong(3, snapshotId);
            stmt.setLong(4, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(new DucklakeColumnStats(
                            rs.getLong("column_id"),
                            rs.getLong("total_value_count"),
                            rs.getLong("total_null_count"),
                            rs.getLong("total_size_bytes"),
                            getStringOptional(rs, "min_value"),
                            getStringOptional(rs, "max_value")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get column stats for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

        return result;
    }

    @Override
    public List<DucklakePartitionSpec> getPartitionSpecs(long tableId, long snapshotId)
    {
        String sql = "SELECT pi.partition_id, pi.table_id, " +
                     "       pc.partition_key_index, pc.column_id, pc.transform " +
                     "FROM ducklake_partition_info pi " +
                     "JOIN ducklake_partition_column pc ON pi.partition_id = pc.partition_id AND pi.table_id = pc.table_id " +
                     "WHERE pi.table_id = ? " +
                     "  AND ? >= pi.begin_snapshot AND (? < pi.end_snapshot OR pi.end_snapshot IS NULL) " +
                     "ORDER BY pi.partition_id, pc.partition_key_index";

        Map<Long, List<DucklakePartitionField>> fieldsByPartition = new LinkedHashMap<>();
        Map<Long, Long> tableIdByPartition = new HashMap<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    long partitionId = rs.getLong("partition_id");
                    tableIdByPartition.put(partitionId, rs.getLong("table_id"));
                    fieldsByPartition.computeIfAbsent(partitionId, _ -> new ArrayList<>())
                            .add(new DucklakePartitionField(
                                    rs.getInt("partition_key_index"),
                                    rs.getLong("column_id"),
                                    DucklakePartitionTransform.fromString(rs.getString("transform"))));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get partition specs for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

        List<DucklakePartitionSpec> specs = new ArrayList<>();
        for (Map.Entry<Long, List<DucklakePartitionField>> entry : fieldsByPartition.entrySet()) {
            specs.add(new DucklakePartitionSpec(entry.getKey(), tableIdByPartition.get(entry.getKey()), entry.getValue()));
        }
        return specs;
    }

    @Override
    public Map<Long, List<DucklakeFilePartitionValue>> getFilePartitionValues(long tableId, long snapshotId)
    {
        String sql = "SELECT fpv.data_file_id, fpv.partition_key_index, fpv.partition_value " +
                     "FROM ducklake_file_partition_value fpv " +
                     "JOIN ducklake_data_file df ON fpv.data_file_id = df.data_file_id AND fpv.table_id = df.table_id " +
                     "WHERE fpv.table_id = ? " +
                     "  AND ? >= df.begin_snapshot AND (? < df.end_snapshot OR df.end_snapshot IS NULL)";

        Map<Long, List<DucklakeFilePartitionValue>> result = new HashMap<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    long dataFileId = rs.getLong("data_file_id");
                    result.computeIfAbsent(dataFileId, _ -> new ArrayList<>())
                            .add(new DucklakeFilePartitionValue(
                                    dataFileId,
                                    rs.getInt("partition_key_index"),
                                    rs.getString("partition_value")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get file partition values for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

        return result;
    }

    @Override
    public Optional<String> getDataPath()
    {
        String sql = "SELECT value FROM ducklake_metadata WHERE key = 'data_path'";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return Optional.of(rs.getString("value"));
            }
            return Optional.empty();
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get data path from ducklake_metadata", e);
        }
    }

    @Override
    public void close()
    {
        if (hikariDataSource != null) {
            hikariDataSource.close();
        }
    }

    // Helper methods for handling nullable columns

    private Optional<Long> getLongOptional(ResultSet rs, String columnName)
            throws SQLException
    {
        long value = rs.getLong(columnName);
        return rs.wasNull() ? Optional.empty() : Optional.of(value);
    }

    private Optional<String> getStringOptional(ResultSet rs, String columnName)
            throws SQLException
    {
        String value = rs.getString(columnName);
        return Optional.ofNullable(value);
    }

    private Optional<Boolean> getBooleanOptional(ResultSet rs, String columnName)
            throws SQLException
    {
        boolean value = rs.getBoolean(columnName);
        return rs.wasNull() ? Optional.empty() : Optional.of(value);
    }

    private String resolveColumnType(DucklakeColumn column, Map<Long, List<DucklakeColumn>> childrenByParent)
    {
        String columnType = column.columnType();
        switch (columnType.toLowerCase()) {
            case "list": {
                List<DucklakeColumn> children = childrenByParent.getOrDefault(column.columnId(), List.of());
                if (children.size() != 1) {
                    throw new IllegalStateException("List column must have exactly one child column: " + column.columnName());
                }
                return "list<" + resolveColumnType(children.get(0), childrenByParent) + ">";
            }
            case "struct": {
                List<DucklakeColumn> children = childrenByParent.getOrDefault(column.columnId(), List.of());
                String fields = children.stream()
                        .map(child -> child.columnName() + ":" + resolveColumnType(child, childrenByParent))
                        .collect(Collectors.joining(","));
                return "struct<" + fields + ">";
            }
            case "map": {
                List<DucklakeColumn> children = childrenByParent.getOrDefault(column.columnId(), List.of());
                if (children.size() != 2) {
                    throw new IllegalStateException("Map column must have exactly two child columns: " + column.columnName());
                }
                return "map<" + resolveColumnType(children.get(0), childrenByParent) + "," + resolveColumnType(children.get(1), childrenByParent) + ">";
            }
            default:
                return columnType;
        }
    }

    private Optional<String> getColumnType(long tableId, long columnId, long snapshotId)
    {
        String sql = "SELECT column_type " +
                     "FROM ducklake_column " +
                     "WHERE table_id = ? AND column_id = ? " +
                     "  AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, columnId);
            stmt.setLong(3, snapshotId);
            stmt.setLong(4, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(rs.getString("column_type"));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get column type for table: " + tableId + ", column: " + columnId, e);
        }
    }

    private Comparable<?> normalizePredicateValue(String columnType, Object value)
    {
        if (value == null) {
            return null;
        }
        return parseStatValue(columnType, value.toString());
    }

    private Comparable<?> parseStatValue(String columnType, String value)
    {
        if (value == null) {
            return null;
        }

        String normalizedType = columnType.toLowerCase();
        try {
            if (isNumericType(normalizedType)) {
                return new java.math.BigDecimal(value);
            }
            if (normalizedType.equals("boolean")) {
                return parseBoolean(value);
            }
            return value;
        }
        catch (RuntimeException e) {
            // If parsing fails we avoid false negatives by not pruning on this value.
            return null;
        }
    }

    private boolean isNumericType(String type)
    {
        return type.equals("int8")
                || type.equals("int16")
                || type.equals("int32")
                || type.equals("int64")
                || type.equals("uint8")
                || type.equals("uint16")
                || type.equals("uint32")
                || type.equals("uint64")
                || type.equals("float32")
                || type.equals("float64")
                || type.startsWith("decimal(");
    }

    private Boolean parseBoolean(String value)
    {
        if (value.equalsIgnoreCase("true") || value.equals("1")) {
            return true;
        }
        if (value.equalsIgnoreCase("false") || value.equals("0")) {
            return false;
        }
        throw new IllegalArgumentException("Invalid boolean value: " + value);
    }

    private boolean isWithinBounds(
            Comparable<?> lowerBound,
            Comparable<?> upperBound,
            Comparable<?> minStat,
            Comparable<?> maxStat)
    {
        OptionalInt lowerVsMax = compareValues(lowerBound, maxStat);
        if (lowerVsMax.isPresent() && lowerVsMax.getAsInt() > 0) {
            return false;
        }

        OptionalInt upperVsMin = compareValues(upperBound, minStat);
        return upperVsMin.isEmpty() || upperVsMin.getAsInt() >= 0;
    }

    @SuppressWarnings("unchecked")
    private OptionalInt compareValues(Comparable<?> left, Comparable<?> right)
    {
        if (left == null || right == null) {
            return OptionalInt.empty();
        }
        try {
            return OptionalInt.of(((Comparable<Object>) left).compareTo(right));
        }
        catch (RuntimeException e) {
            // Type mismatch or non-comparable values: avoid pruning to prevent false negatives.
            return OptionalInt.empty();
        }
    }
}
