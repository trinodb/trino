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
import java.util.List;
import java.util.Optional;
import java.util.UUID;

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
                     "WHERE table_id = ? AND parent_column IS NULL AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL) " +
                     "ORDER BY column_order";

        List<DucklakeColumn> columns = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    columns.add(new DucklakeColumn(
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

        return columns;
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
        // File pruning query - for predicate pushdown
        // Note: min_value and max_value are stored as VARCHAR in ducklake_file_column_stats
        // We need to cast them appropriately based on the column type
        String sql = "SELECT data_file_id " +
                     "FROM ducklake_file_column_stats " +
                     "WHERE table_id = ? AND column_id = ? " +
                     "  AND (? >= min_value OR min_value IS NULL) " +
                     "  AND (? <= max_value OR max_value IS NULL)";

        List<Long> fileIds = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, columnId);
            stmt.setString(3, minValue != null ? minValue.toString() : null);
            stmt.setString(4, maxValue != null ? maxValue.toString() : null);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    fileIds.add(rs.getLong("data_file_id"));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get file IDs for predicate on table: " + tableId + ", column: " + columnId, e);
        }

        return fileIds;
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
}
