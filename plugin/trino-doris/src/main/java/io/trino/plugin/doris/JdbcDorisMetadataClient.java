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
package io.trino.plugin.doris;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class JdbcDorisMetadataClient
        implements DorisMetadataClient
{
    private static final String VISIBLE_SCHEMAS_PREDICATE =
            """
            LOWER(SCHEMA_NAME) NOT IN ('information_schema', '__internal_schema', 'mysql')
            """;
    private static final String READABLE_TABLES_PREDICATE =
            """
            LOWER(TABLE_SCHEMA) NOT IN ('information_schema', '__internal_schema', 'mysql')
                AND (
                    (TABLE_TYPE = 'BASE TABLE' AND UPPER(COALESCE(ENGINE, '')) IN ('OLAP', 'DORIS'))
                    OR TABLE_TYPE = 'VIEW'
                )
            """;
    private static final String LIST_SCHEMAS_SQL =
            """
            SELECT SCHEMA_NAME
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE %s
            ORDER BY SCHEMA_NAME
            """;
    private static final String LIST_ALL_TABLES_SQL =
            """
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE %s
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
    private static final String LIST_TABLES_IN_SCHEMA_SQL =
            """
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE %s
              AND LOWER(TABLE_SCHEMA) = LOWER(?)
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
    private static final String RESOLVE_TABLE_SQL =
            """
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE %s
              AND LOWER(TABLE_SCHEMA) = LOWER(?)
              AND LOWER(TABLE_NAME) = LOWER(?)
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
    private static final String LIST_COLUMNS_SQL =
            """
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_SIZE, DECIMAL_DIGITS, ORDINAL_POSITION, COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE LOWER(TABLE_SCHEMA) = LOWER(?) AND LOWER(TABLE_NAME) = LOWER(?)
            ORDER BY ORDINAL_POSITION
            """;
    private static final String TABLE_ROW_COUNT_SQL =
            """
            SELECT TABLE_ROWS
            FROM INFORMATION_SCHEMA.TABLES
            WHERE LOWER(TABLE_SCHEMA) = LOWER(?) AND LOWER(TABLE_NAME) = LOWER(?)
            """;

    private final DorisJdbcConnectionFactory connectionFactory;

    @Inject
    public JdbcDorisMetadataClient(DorisJdbcConnectionFactory connectionFactory)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        try (Connection connection = openConnection(session);
                PreparedStatement statement = connection.prepareStatement(LIST_SCHEMAS_SQL.formatted(VISIBLE_SCHEMAS_PREDICATE));
                ResultSet resultSet = statement.executeQuery()) {
            List<String> schemas = new ArrayList<>();
            while (resultSet.next()) {
                schemas.add(resultSet.getString("SCHEMA_NAME").toLowerCase(ENGLISH));
            }
            return schemas.stream()
                    .distinct()
                    .toList();
        }
        catch (SQLException e) {
            throw DorisJdbcConnectionFactory.jdbcOperationFailed("Failed to list Doris schemas", e);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        try (Connection connection = openConnection(session);
                PreparedStatement statement = connection.prepareStatement(schemaName.isPresent()
                        ? LIST_TABLES_IN_SCHEMA_SQL.formatted(READABLE_TABLES_PREDICATE)
                        : LIST_ALL_TABLES_SQL.formatted(READABLE_TABLES_PREDICATE))) {
            if (schemaName.isPresent()) {
                statement.setString(1, schemaName.get());
            }

            try (ResultSet resultSet = statement.executeQuery()) {
                List<SchemaTableName> tables = new ArrayList<>();
                while (resultSet.next()) {
                    tables.add(new SchemaTableName(
                            resultSet.getString("TABLE_SCHEMA"),
                            resultSet.getString("TABLE_NAME")));
                }
                return tables.stream()
                        .distinct()
                        .toList();
            }
        }
        catch (SQLException e) {
            throw DorisJdbcConnectionFactory.jdbcOperationFailed("Failed to list Doris tables", e);
        }
    }

    @Override
    public Optional<DorisRemoteTable> getTable(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<ResolvedTable> resolvedTable = resolveTable(session, tableName);
        if (resolvedTable.isEmpty()) {
            return Optional.empty();
        }

        ResolvedTable remoteTable = resolvedTable.orElseThrow();
        List<DorisRemoteColumn> columns = loadColumns(session, remoteTable.toSchemaTableName());
        if (columns.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new DorisRemoteTable(
                tableName,
                remoteTable.schemaName(),
                remoteTable.tableName(),
                remoteTable.relationType(),
                columns));
    }

    @Override
    public OptionalLong getTableRowCount(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<ResolvedTable> resolvedTable = resolveTable(session, tableName);
        if (resolvedTable.isEmpty()) {
            return OptionalLong.empty();
        }

        try (Connection connection = openConnection(session);
                PreparedStatement statement = connection.prepareStatement(TABLE_ROW_COUNT_SQL)) {
            statement.setString(1, resolvedTable.orElseThrow().schemaName());
            statement.setString(2, resolvedTable.orElseThrow().tableName());

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
            throw DorisJdbcConnectionFactory.jdbcOperationFailed("Failed to load Doris table row count for table '%s'".formatted(tableName), e);
        }
    }

    private Optional<ResolvedTable> resolveTable(ConnectorSession session, SchemaTableName tableName)
    {
        try (Connection connection = openConnection(session);
                PreparedStatement statement = connection.prepareStatement(RESOLVE_TABLE_SQL.formatted(READABLE_TABLES_PREDICATE))) {
            statement.setString(1, tableName.getSchemaName());
            statement.setString(2, tableName.getTableName());

            try (ResultSet resultSet = statement.executeQuery()) {
                List<ResolvedTable> matches = new ArrayList<>();
                while (resultSet.next()) {
                    matches.add(new ResolvedTable(
                            resultSet.getString("TABLE_SCHEMA"),
                            resultSet.getString("TABLE_NAME"),
                            toRelationType(resultSet.getString("TABLE_TYPE"))));
                }

                if (matches.isEmpty()) {
                    return Optional.empty();
                }

                ResolvedTable match = matches.getFirst();
                if (matches.stream().skip(1).anyMatch(candidate -> !candidate.equals(match))) {
                    throw new TrinoException(
                            NOT_SUPPORTED,
                            "Doris objects that differ only by case are not addressable in Trino: " + tableName);
                }
                return Optional.of(match);
            }
        }
        catch (SQLException e) {
            throw DorisJdbcConnectionFactory.jdbcOperationFailed("Failed to resolve Doris table '%s'".formatted(tableName), e);
        }
    }

    private List<DorisRemoteColumn> loadColumns(ConnectorSession session, SchemaTableName tableName)
    {
        try (Connection connection = openConnection(session);
                PreparedStatement statement = connection.prepareStatement(LIST_COLUMNS_SQL)) {
            statement.setString(1, tableName.getSchemaName());
            statement.setString(2, tableName.getTableName());

            try (ResultSet resultSet = statement.executeQuery()) {
                List<DorisRemoteColumn> columns = new ArrayList<>();
                while (resultSet.next()) {
                    columns.add(new DorisRemoteColumn(
                            resultSet.getString("COLUMN_NAME"),
                            resultSet.getString("DATA_TYPE"),
                            getOptionalInt(resultSet, "COLUMN_SIZE"),
                            getOptionalInt(resultSet, "DECIMAL_DIGITS"),
                            resultSet.getInt("ORDINAL_POSITION"),
                            Optional.ofNullable(resultSet.getString("COLUMN_TYPE"))));
                }
                return List.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw DorisJdbcConnectionFactory.jdbcOperationFailed("Failed to load Doris columns for table '%s'".formatted(tableName), e);
        }
    }

    private Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        return connectionFactory.openConnection(session);
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

    private static DorisRelationType toRelationType(String tableType)
    {
        if ("VIEW".equalsIgnoreCase(tableType)) {
            return DorisRelationType.VIEW;
        }
        return DorisRelationType.TABLE;
    }

    private record ResolvedTable(String schemaName, String tableName, DorisRelationType relationType)
    {
        private SchemaTableName toSchemaTableName()
        {
            return new SchemaTableName(schemaName, tableName);
        }
    }
}
