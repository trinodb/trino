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
package io.trino.plugin.starrocks;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class JdbcStarRocksMetadataClient
        implements StarRocksMetadataClient
{
    private static final String LIST_SCHEMAS_SQL =
            """
            SELECT SCHEMA_NAME
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE LOWER(SCHEMA_NAME) NOT IN ('information_schema', '_statistics_', 'sys')
            ORDER BY SCHEMA_NAME
            """;
    private static final String LIST_SCHEMAS_IN_CATALOG_SQL =
            """
            SELECT SCHEMA_NAME
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE CATALOG_NAME = ?
              AND LOWER(SCHEMA_NAME) NOT IN ('information_schema', '_statistics_', 'sys')
            ORDER BY SCHEMA_NAME
            """;
    private static final String LIST_SCHEMAS_IN_CATALOG_FROM_TABLES_SQL =
            """
            SELECT DISTINCT TABLE_SCHEMA AS SCHEMA_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = ?
              AND LOWER(TABLE_SCHEMA) NOT IN ('information_schema', '_statistics_', 'sys')
            ORDER BY TABLE_SCHEMA
            """;
    private static final String LIST_ALL_TABLES_SQL =
            """
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE LOWER(TABLE_SCHEMA) NOT IN ('information_schema', '_statistics_', 'sys')
              AND UPPER(TABLE_TYPE) IN ('TABLE', 'BASE TABLE', 'VIEW')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
    private static final String LIST_ALL_TABLES_IN_CATALOG_SQL =
            """
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = ?
              AND LOWER(TABLE_SCHEMA) NOT IN ('information_schema', '_statistics_', 'sys')
              AND UPPER(TABLE_TYPE) IN ('TABLE', 'BASE TABLE', 'VIEW')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
    private static final String LIST_TABLES_IN_SCHEMA_SQL =
            """
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE LOWER(TABLE_SCHEMA) = LOWER(?)
              AND UPPER(TABLE_TYPE) IN ('TABLE', 'BASE TABLE', 'VIEW')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
    private static final String LIST_TABLES_IN_CATALOG_SCHEMA_SQL =
            """
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = ?
              AND LOWER(TABLE_SCHEMA) = LOWER(?)
              AND UPPER(TABLE_TYPE) IN ('TABLE', 'BASE TABLE', 'VIEW')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
    private static final String RESOLVE_TABLE_SQL =
            """
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE LOWER(TABLE_SCHEMA) = LOWER(?)
              AND LOWER(TABLE_NAME) = LOWER(?)
              AND UPPER(TABLE_TYPE) IN ('TABLE', 'BASE TABLE', 'VIEW')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
    private static final String RESOLVE_TABLE_IN_CATALOG_SQL =
            """
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = ?
              AND LOWER(TABLE_SCHEMA) = LOWER(?)
              AND LOWER(TABLE_NAME) = LOWER(?)
              AND UPPER(TABLE_TYPE) IN ('TABLE', 'BASE TABLE', 'VIEW')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
    private static final String LIST_COLUMNS_SQL =
            """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                COLUMN_TYPE,
                COALESCE(CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION) AS COLUMN_SIZE,
                COALESCE(NUMERIC_SCALE, DATETIME_PRECISION) AS DECIMAL_DIGITS,
                ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE LOWER(TABLE_SCHEMA) = LOWER(?)
              AND LOWER(TABLE_NAME) = LOWER(?)
            ORDER BY ORDINAL_POSITION
            """;
    private static final String LIST_COLUMNS_IN_CATALOG_SQL =
            """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                COLUMN_TYPE,
                COALESCE(CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION) AS COLUMN_SIZE,
                COALESCE(NUMERIC_SCALE, DATETIME_PRECISION) AS DECIMAL_DIGITS,
                ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_CATALOG = ?
              AND LOWER(TABLE_SCHEMA) = LOWER(?)
              AND LOWER(TABLE_NAME) = LOWER(?)
            ORDER BY ORDINAL_POSITION
            """;
    private static final String TABLE_ROW_COUNT_SQL =
            """
            SELECT TABLE_ROWS
            FROM INFORMATION_SCHEMA.TABLES
            WHERE LOWER(TABLE_SCHEMA) = LOWER(?)
              AND LOWER(TABLE_NAME) = LOWER(?)
            """;
    private static final String TABLE_ROW_COUNT_IN_CATALOG_SQL =
            """
            SELECT TABLE_ROWS
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = ?
              AND LOWER(TABLE_SCHEMA) = LOWER(?)
              AND LOWER(TABLE_NAME) = LOWER(?)
            """;

    private final StarRocksJdbcConnectionFactory connectionFactory;
    private final Optional<String> catalogName;

    @Inject
    public JdbcStarRocksMetadataClient(StarRocksJdbcConnectionFactory connectionFactory, StarRocksConfig config)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.catalogName = requireNonNull(config, "config is null").getCatalogName();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        if (catalogName.isPresent()) {
            return loadSchemaNamesFromInformationSchema(session);
        }

        List<String> schemas = loadSchemaNamesFromMetadata(session);
        if (!schemas.isEmpty()) {
            return schemas;
        }
        return loadSchemaNamesFromInformationSchema(session);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return listResolvedTables(session, schemaName).stream()
                .map(ResolvedTable::schemaTableName)
                .distinct()
                .toList();
    }

    @Override
    public Optional<StarRocksRemoteTable> getTable(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<ResolvedTable> resolvedTable = resolveTable(session, tableName);
        if (resolvedTable.isEmpty()) {
            return Optional.empty();
        }

        ResolvedTable table = resolvedTable.orElseThrow();
        List<StarRocksRemoteColumn> columns = loadColumns(session, table);
        if (columns.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new StarRocksRemoteTable(
                table.schemaTableName(),
                table.remoteCatalogName(),
                table.remoteSchemaName(),
                table.remoteTableName(),
                table.relationType(),
                columns));
    }

    @Override
    public OptionalLong getTableRowCount(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<ResolvedTable> resolvedTable = resolveTable(session, tableName);
        if (resolvedTable.isEmpty()) {
            return OptionalLong.empty();
        }

        String sql = catalogName.isPresent() ? TABLE_ROW_COUNT_IN_CATALOG_SQL : TABLE_ROW_COUNT_SQL;
        try (Connection connection = openConnection(session);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            if (catalogName.isPresent()) {
                statement.setString(index++, catalogName.orElseThrow());
            }
            statement.setString(index++, resolvedTable.orElseThrow().remoteSchemaName());
            statement.setString(index, resolvedTable.orElseThrow().remoteTableName());

            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return OptionalLong.empty();
                }

                long rowCount = resultSet.getLong("TABLE_ROWS");
                if (resultSet.wasNull()) {
                    return OptionalLong.empty();
                }
                return OptionalLong.of(Math.max(0L, rowCount));
            }
        }
        catch (SQLException e) {
            throw StarRocksJdbcConnectionFactory.jdbcOperationFailed("Failed to load StarRocks table row count for table '%s'".formatted(tableName), e);
        }
    }

    private List<String> loadSchemaNamesFromMetadata(ConnectorSession session)
    {
        try (Connection connection = openConnection(session);
                ResultSet resultSet = connection.getMetaData().getSchemas()) {
            List<String> schemas = new ArrayList<>();
            while (resultSet.next()) {
                String schema = resultSet.getString("TABLE_SCHEM");
                if (shouldExposeSchema(schema)) {
                    schemas.add(lowercaseName(schema));
                }
            }
            return schemas.stream().distinct().toList();
        }
        catch (SQLException ignored) {
            return List.of();
        }
    }

    private List<String> loadSchemaNamesFromInformationSchema(ConnectorSession session)
    {
        String sql = catalogName.isPresent() ? LIST_SCHEMAS_IN_CATALOG_SQL : LIST_SCHEMAS_SQL;
        List<String> schemas = loadSchemaNames(session, sql);
        if (catalogName.isPresent() && schemas.isEmpty()) {
            return loadSchemaNames(session, LIST_SCHEMAS_IN_CATALOG_FROM_TABLES_SQL);
        }
        return schemas;
    }

    private List<String> loadSchemaNames(ConnectorSession session, String sql)
    {
        try (Connection connection = openConnection(session);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            if (catalogName.isPresent()) {
                statement.setString(1, catalogName.orElseThrow());
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                List<String> schemas = new ArrayList<>();
                while (resultSet.next()) {
                    schemas.add(lowercaseName(resultSet.getString("SCHEMA_NAME")));
                }
                return schemas.stream().distinct().toList();
            }
        }
        catch (SQLException e) {
            throw StarRocksJdbcConnectionFactory.jdbcOperationFailed("Failed to list StarRocks schemas", e);
        }
    }

    private List<ResolvedTable> listResolvedTables(ConnectorSession session, Optional<String> schemaName)
    {
        List<ResolvedTable> tables = loadTablesFromMetadata(session, schemaName);
        if (!tables.isEmpty()) {
            return tables;
        }
        return loadTablesFromInformationSchema(session, schemaName);
    }

    private List<ResolvedTable> loadTablesFromMetadata(ConnectorSession session, Optional<String> schemaName)
    {
        try (Connection connection = openConnection(session);
                ResultSet resultSet = connection.getMetaData().getTables(
                        catalogName.orElse(connection.getCatalog()),
                        schemaName.orElse(null),
                        null,
                        new String[] {"TABLE", "VIEW"})) {
            List<ResolvedTable> tables = new ArrayList<>();
            while (resultSet.next()) {
                String remoteSchema = resultSet.getString("TABLE_SCHEM");
                String remoteTable = resultSet.getString("TABLE_NAME");
                String tableType = resultSet.getString("TABLE_TYPE");
                if (!shouldExposeTable(remoteSchema, tableType)) {
                    continue;
                }
                tables.add(new ResolvedTable(catalogName, remoteSchema, remoteTable, toRelationType(tableType)));
            }
            return tables.stream().distinct().toList();
        }
        catch (SQLException ignored) {
            return List.of();
        }
    }

    private List<ResolvedTable> loadTablesFromInformationSchema(ConnectorSession session, Optional<String> schemaName)
    {
        String sql;
        if (catalogName.isPresent()) {
            sql = schemaName.isPresent() ? LIST_TABLES_IN_CATALOG_SCHEMA_SQL : LIST_ALL_TABLES_IN_CATALOG_SQL;
        }
        else {
            sql = schemaName.isPresent() ? LIST_TABLES_IN_SCHEMA_SQL : LIST_ALL_TABLES_SQL;
        }
        try (Connection connection = openConnection(session);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            if (catalogName.isPresent()) {
                statement.setString(index++, catalogName.orElseThrow());
            }
            if (schemaName.isPresent()) {
                statement.setString(index, schemaName.get());
            }

            try (ResultSet resultSet = statement.executeQuery()) {
                List<ResolvedTable> tables = new ArrayList<>();
                while (resultSet.next()) {
                    tables.add(new ResolvedTable(
                            catalogName,
                            resultSet.getString("TABLE_SCHEMA"),
                            resultSet.getString("TABLE_NAME"),
                            toRelationType(resultSet.getString("TABLE_TYPE"))));
                }
                return tables.stream().distinct().toList();
            }
        }
        catch (SQLException e) {
            throw StarRocksJdbcConnectionFactory.jdbcOperationFailed("Failed to list StarRocks tables", e);
        }
    }

    private Optional<ResolvedTable> resolveTable(ConnectorSession session, SchemaTableName tableName)
    {
        List<ResolvedTable> matches = loadResolvedTableMatches(session, tableName);
        if (matches.isEmpty()) {
            return Optional.empty();
        }

        ResolvedTable match = matches.getFirst();
        if (matches.stream().skip(1).anyMatch(candidate -> !candidate.equals(match))) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "StarRocks objects that differ only by case are not addressable in Trino: " + tableName);
        }
        return Optional.of(match);
    }

    private List<ResolvedTable> loadResolvedTableMatches(ConnectorSession session, SchemaTableName tableName)
    {
        List<ResolvedTable> fromMetadata = listResolvedTables(session, Optional.of(tableName.getSchemaName())).stream()
                .filter(candidate -> candidate.schemaTableName().getTableName().equals(tableName.getTableName().toLowerCase(ENGLISH)))
                .toList();
        if (!fromMetadata.isEmpty()) {
            return fromMetadata;
        }

        String sql = catalogName.isPresent() ? RESOLVE_TABLE_IN_CATALOG_SQL : RESOLVE_TABLE_SQL;
        try (Connection connection = openConnection(session);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            if (catalogName.isPresent()) {
                statement.setString(index++, catalogName.orElseThrow());
            }
            statement.setString(index++, tableName.getSchemaName());
            statement.setString(index, tableName.getTableName());

            try (ResultSet resultSet = statement.executeQuery()) {
                List<ResolvedTable> matches = new ArrayList<>();
                while (resultSet.next()) {
                    matches.add(new ResolvedTable(
                            catalogName,
                            resultSet.getString("TABLE_SCHEMA"),
                            resultSet.getString("TABLE_NAME"),
                            toRelationType(resultSet.getString("TABLE_TYPE"))));
                }
                return matches;
            }
        }
        catch (SQLException e) {
            throw StarRocksJdbcConnectionFactory.jdbcOperationFailed("Failed to resolve StarRocks table '%s'".formatted(tableName), e);
        }
    }

    private List<StarRocksRemoteColumn> loadColumns(ConnectorSession session, ResolvedTable resolvedTable)
    {
        SQLException informationSchemaFailure = null;
        try {
            List<StarRocksRemoteColumn> columns = loadColumnsFromInformationSchema(session, resolvedTable);
            if (!columns.isEmpty()) {
                return columns;
            }
        }
        catch (SQLException e) {
            informationSchemaFailure = e;
        }
        if (catalogName.isPresent()) {
            try {
                List<StarRocksRemoteColumn> columns = loadColumnsFromShowFullColumns(session, resolvedTable);
                if (!columns.isEmpty()) {
                    return columns;
                }
            }
            catch (SQLException e) {
                if (informationSchemaFailure == null) {
                    informationSchemaFailure = e;
                }
                else {
                    informationSchemaFailure.addSuppressed(e);
                }
            }
        }

        List<StarRocksRemoteColumn> columns = loadColumnsFromMetadata(session, resolvedTable);
        if (!columns.isEmpty()) {
            return columns;
        }
        if (informationSchemaFailure != null) {
            throw StarRocksJdbcConnectionFactory.jdbcOperationFailed(
                    "Failed to load StarRocks columns for table '%s'".formatted(resolvedTable.schemaTableName()),
                    informationSchemaFailure);
        }
        return List.of();
    }

    private List<StarRocksRemoteColumn> loadColumnsFromMetadata(ConnectorSession session, ResolvedTable resolvedTable)
    {
        try (Connection connection = openConnection(session);
                ResultSet resultSet = connection.getMetaData().getColumns(
                        resolvedTable.remoteCatalogName().orElse(connection.getCatalog()),
                        resolvedTable.remoteSchemaName(),
                        resolvedTable.remoteTableName(),
                        null)) {
            List<StarRocksRemoteColumn> columns = new ArrayList<>();
            while (resultSet.next()) {
                String remoteColumnName = resultSet.getString("COLUMN_NAME");
                columns.add(new StarRocksRemoteColumn(
                        lowercaseName(remoteColumnName),
                        remoteColumnName,
                        requireNonNullElseEmpty(resultSet.getString("TYPE_NAME")),
                        getOptionalInt(resultSet, "COLUMN_SIZE"),
                        getOptionalInt(resultSet, "DECIMAL_DIGITS"),
                        resultSet.getInt("ORDINAL_POSITION"),
                        Optional.ofNullable(resultSet.getString("TYPE_NAME"))));
            }
            return deduplicateColumns(columns);
        }
        catch (SQLException ignored) {
            return List.of();
        }
    }

    private List<StarRocksRemoteColumn> loadColumnsFromInformationSchema(ConnectorSession session, ResolvedTable resolvedTable)
            throws SQLException
    {
        String sql = catalogName.isPresent() ? LIST_COLUMNS_IN_CATALOG_SQL : LIST_COLUMNS_SQL;
        try (Connection connection = openConnection(session);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            if (catalogName.isPresent()) {
                statement.setString(index++, catalogName.orElseThrow());
            }
            statement.setString(index++, resolvedTable.remoteSchemaName());
            statement.setString(index, resolvedTable.remoteTableName());

            return readColumns(statement);
        }
    }

    private List<StarRocksRemoteColumn> loadColumnsFromShowFullColumns(ConnectorSession session, ResolvedTable resolvedTable)
            throws SQLException
    {
        try (Connection connection = openConnection(session);
                PreparedStatement statement = connection.prepareStatement("SHOW FULL COLUMNS FROM " + qualifiedRemoteTableName(resolvedTable))) {
            try (ResultSet resultSet = statement.executeQuery()) {
                List<StarRocksRemoteColumn> columns = new ArrayList<>();
                int ordinalPosition = 1;
                while (resultSet.next()) {
                    String remoteColumnName = resultSet.getString("Field");
                    String typeDefinition = requireNonNullElseEmpty(resultSet.getString("Type"));
                    columns.add(new StarRocksRemoteColumn(
                            lowercaseName(remoteColumnName),
                            remoteColumnName,
                            typeDefinition,
                            Optional.empty(),
                            Optional.empty(),
                            ordinalPosition,
                            Optional.of(typeDefinition)));
                    ordinalPosition++;
                }
                return deduplicateColumns(columns);
            }
        }
    }

    private static List<StarRocksRemoteColumn> readColumns(PreparedStatement statement)
            throws SQLException
    {
        try (ResultSet resultSet = statement.executeQuery()) {
            List<StarRocksRemoteColumn> columns = new ArrayList<>();
            while (resultSet.next()) {
                String remoteColumnName = resultSet.getString("COLUMN_NAME");
                columns.add(new StarRocksRemoteColumn(
                        lowercaseName(remoteColumnName),
                        remoteColumnName,
                        resultSet.getString("DATA_TYPE"),
                        getOptionalInt(resultSet, "COLUMN_SIZE"),
                        getOptionalInt(resultSet, "DECIMAL_DIGITS"),
                        resultSet.getInt("ORDINAL_POSITION"),
                        Optional.ofNullable(resultSet.getString("COLUMN_TYPE"))));
            }
            return deduplicateColumns(columns);
        }
    }

    private Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        if (session == null) {
            return connectionFactory.openConnection();
        }
        return connectionFactory.openConnection(session);
    }

    private static List<StarRocksRemoteColumn> deduplicateColumns(List<StarRocksRemoteColumn> columns)
    {
        Map<String, StarRocksRemoteColumn> deduplicated = new LinkedHashMap<>();
        for (StarRocksRemoteColumn column : columns) {
            StarRocksRemoteColumn previous = deduplicated.putIfAbsent(column.columnName(), column);
            if (previous != null && !previous.remoteColumnName().equals(column.remoteColumnName())) {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        "StarRocks columns that differ only by case are not addressable in Trino: " + column.remoteColumnName());
            }
        }
        return List.copyOf(deduplicated.values());
    }

    private static boolean shouldExposeSchema(String schemaName)
    {
        if (schemaName == null) {
            return false;
        }
        String normalized = lowercaseName(schemaName);
        return !normalized.equals("information_schema") &&
                !normalized.equals("_statistics_") &&
                !normalized.equals("sys");
    }

    private static boolean shouldExposeTable(String schemaName, String tableType)
    {
        return shouldExposeSchema(schemaName) &&
                tableType != null &&
                (tableType.equalsIgnoreCase("TABLE") ||
                        tableType.equalsIgnoreCase("BASE TABLE") ||
                        tableType.equalsIgnoreCase("VIEW"));
    }

    private static StarRocksRelationType toRelationType(String tableType)
    {
        if (tableType != null && tableType.equalsIgnoreCase("VIEW")) {
            return StarRocksRelationType.VIEW;
        }
        return StarRocksRelationType.TABLE;
    }

    private static Optional<Integer> getOptionalInt(ResultSet resultSet, String columnLabel)
            throws SQLException
    {
        int value = resultSet.getInt(columnLabel);
        if (resultSet.wasNull()) {
            return Optional.empty();
        }
        return Optional.of(value);
    }

    private static String requireNonNullElseEmpty(String value)
    {
        return value == null ? "" : value;
    }

    private static String qualifiedRemoteTableName(ResolvedTable table)
    {
        return table.remoteCatalogName()
                .map(catalog -> StarRocksQueryBuilder.quoteIdentifier(catalog) + ".")
                .orElse("") +
                StarRocksQueryBuilder.quoteIdentifier(table.remoteSchemaName()) +
                "." +
                StarRocksQueryBuilder.quoteIdentifier(table.remoteTableName());
    }

    private static String lowercaseName(String value)
    {
        return value.toLowerCase(Locale.ENGLISH);
    }

    private record ResolvedTable(Optional<String> remoteCatalogName, String remoteSchemaName, String remoteTableName, StarRocksRelationType relationType)
    {
        private SchemaTableName schemaTableName()
        {
            return new SchemaTableName(lowercaseName(remoteSchemaName), lowercaseName(remoteTableName));
        }
    }
}
