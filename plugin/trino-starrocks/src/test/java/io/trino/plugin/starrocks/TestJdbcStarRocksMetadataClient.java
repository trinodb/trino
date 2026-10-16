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

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestJdbcStarRocksMetadataClient
{
    @Test
    void testListSchemaNamesFallsBackToInformationSchemaWhenMetadataFails()
    {
        AtomicReference<String> preparedSql = new AtomicReference<>();
        JdbcStarRocksMetadataClient client = metadataClient(connectionFactory(
                () -> {
                    throw new SQLException("getSchemas failed");
                },
                (_, _, _) -> List.of(),
                preparedSql::set,
                List.of(List.of(
                        row("SCHEMA_NAME", "analytics"),
                        row("SCHEMA_NAME", "reporting"))),
                new ArrayList<>()));

        assertThat(client.listSchemaNames(SESSION)).containsExactly("analytics", "reporting");
        assertThat(preparedSql.get()).contains("FROM INFORMATION_SCHEMA.SCHEMATA");
    }

    @Test
    void testGetTablePrefersInformationSchemaColumnsAndPreservesRemoteNames()
    {
        List<String> preparedSql = new ArrayList<>();
        List<List<String>> boundParameters = new ArrayList<>();
        JdbcStarRocksMetadataClient client = metadataClient(connectionFactory(
                () -> createResultSet(List.of()),
                (_, _, _) -> List.of(
                        row(
                                "COLUMN_NAME",
                                "ignored_by_information_schema",
                                "TYPE_NAME",
                                "BIGINT",
                                "COLUMN_SIZE",
                                20,
                                "DECIMAL_DIGITS",
                                null,
                                "ORDINAL_POSITION",
                                1)),
                preparedSql::add,
                List.of(
                        List.of(row("TABLE_SCHEMA", "Analytics", "TABLE_NAME", "EventsView", "TABLE_TYPE", "VIEW")),
                        List.of(
                                row(
                                        "COLUMN_NAME",
                                        "event_id",
                                        "DATA_TYPE",
                                        "BIGINT",
                                        "COLUMN_TYPE",
                                        "bigint",
                                        "COLUMN_SIZE",
                                        20,
                                        "DECIMAL_DIGITS",
                                        null,
                                        "ORDINAL_POSITION",
                                        1),
                                row(
                                        "COLUMN_NAME",
                                        "created_at",
                                        "DATA_TYPE",
                                        "DATETIME",
                                        "COLUMN_TYPE",
                                        "datetimev2(3)",
                                        "COLUMN_SIZE",
                                        null,
                                        "DECIMAL_DIGITS",
                                        3,
                                        "ORDINAL_POSITION",
                                        2),
                                row(
                                        "COLUMN_NAME",
                                        "largeint_summary",
                                        "DATA_TYPE",
                                        "DECIMAL",
                                        "COLUMN_TYPE",
                                        "largeint",
                                        "COLUMN_SIZE",
                                        null,
                                        "DECIMAL_DIGITS",
                                        null,
                                        "ORDINAL_POSITION",
                                        3))),
                boundParameters));

        StarRocksRemoteTable table = client.getTable(SESSION, new SchemaTableName("analytics", "eventsview")).orElseThrow();

        assertThat(table.remoteSchemaName()).isEqualTo("Analytics");
        assertThat(table.remoteTableName()).isEqualTo("EventsView");
        assertThat(table.relationType()).isEqualTo(StarRocksRelationType.VIEW);
        assertThat(table.columns()).extracting(StarRocksRemoteColumn::columnName)
                .containsExactly("event_id", "created_at", "largeint_summary");
        assertThat(table.columns()).extracting(StarRocksRemoteColumn::remoteColumnName)
                .containsExactly("event_id", "created_at", "largeint_summary");
        assertThat(table.columns()).extracting(StarRocksRemoteColumn::typeDefinition)
                .containsExactly(Optional.of("bigint"), Optional.of("datetimev2(3)"), Optional.of("largeint"));
        assertThat(preparedSql.getFirst()).contains("FROM INFORMATION_SCHEMA.TABLES");
        assertThat(preparedSql.get(1)).contains("FROM INFORMATION_SCHEMA.COLUMNS");
        assertThat(boundParameters.get(0)).containsExactly("analytics");
        assertThat(boundParameters.get(1)).containsExactly("Analytics", "EventsView");
    }

    @Test
    void testGetTableFallsBackToJdbcMetadataColumnsWhenInformationSchemaReturnsNoColumns()
    {
        List<String> preparedSql = new ArrayList<>();
        List<List<String>> boundParameters = new ArrayList<>();
        JdbcStarRocksMetadataClient client = metadataClient(connectionFactory(
                () -> createResultSet(List.of()),
                (_, _, _) -> List.of(
                        row(
                                "COLUMN_NAME",
                                "metric_key",
                                "TYPE_NAME",
                                "BIGINT",
                                "COLUMN_SIZE",
                                20,
                                "DECIMAL_DIGITS",
                                null,
                                "ORDINAL_POSITION",
                                1),
                        row(
                                "COLUMN_NAME",
                                "created_at",
                                "TYPE_NAME",
                                "DATETIME",
                                "COLUMN_SIZE",
                                null,
                                "DECIMAL_DIGITS",
                                6,
                                "ORDINAL_POSITION",
                                2)),
                preparedSql::add,
                List.of(
                        List.of(row("TABLE_SCHEMA", "analytics", "TABLE_NAME", "events", "TABLE_TYPE", "TABLE")),
                        List.of()),
                boundParameters));

        StarRocksRemoteTable table = client.getTable(SESSION, new SchemaTableName("analytics", "events")).orElseThrow();

        assertThat(table.relationType()).isEqualTo(StarRocksRelationType.TABLE);
        assertThat(table.columns()).extracting(StarRocksRemoteColumn::columnName)
                .containsExactly("metric_key", "created_at");
        assertThat(table.columns()).extracting(StarRocksRemoteColumn::typeName)
                .containsExactly("BIGINT", "DATETIME");
        assertThat(table.columns()).extracting(StarRocksRemoteColumn::typeDefinition)
                .containsExactly(Optional.of("BIGINT"), Optional.of("DATETIME"));
        assertThat(preparedSql.getFirst()).contains("FROM INFORMATION_SCHEMA.TABLES");
        assertThat(preparedSql.get(1)).contains("FROM INFORMATION_SCHEMA.COLUMNS");
        assertThat(boundParameters.get(0)).containsExactly("analytics");
        assertThat(boundParameters.get(1)).containsExactly("analytics", "events");
    }

    @Test
    void testGetTableUsesConfiguredCatalog()
    {
        List<String> preparedSql = new ArrayList<>();
        List<List<String>> boundParameters = new ArrayList<>();
        JdbcStarRocksMetadataClient client = metadataClient(
                connectionFactory(
                        () -> createResultSet(List.of()),
                        (_, _, _) -> List.of(),
                        preparedSql::add,
                        List.of(
                                List.of(row("TABLE_SCHEMA", "analytics", "TABLE_NAME", "events", "TABLE_TYPE", "BASE TABLE")),
                                List.of(row(
                                        "COLUMN_NAME",
                                        "event_id",
                                        "DATA_TYPE",
                                        "BIGINT",
                                        "COLUMN_TYPE",
                                        "bigint",
                                        "COLUMN_SIZE",
                                        20,
                                        "DECIMAL_DIGITS",
                                        null,
                                        "ORDINAL_POSITION",
                                        1))),
                        boundParameters),
                new StarRocksConfig().setCatalogName("external_catalog"));

        StarRocksRemoteTable table = client.getTable(SESSION, new SchemaTableName("analytics", "events")).orElseThrow();

        assertThat(table.remoteCatalogName()).contains("external_catalog");
        assertThat(preparedSql.getFirst()).contains("TABLE_CATALOG = ?");
        assertThat(preparedSql.get(1)).contains("TABLE_CATALOG = ?");
        assertThat(boundParameters.get(0)).containsExactly("external_catalog", "analytics");
        assertThat(boundParameters.get(1)).containsExactly("external_catalog", "analytics", "events");
    }

    @Test
    void testGetTableWithConfiguredCatalogFallsBackWhenColumnsCatalogIsNull()
    {
        List<String> preparedSql = new ArrayList<>();
        List<List<String>> boundParameters = new ArrayList<>();
        JdbcStarRocksMetadataClient client = metadataClient(
                connectionFactory(
                        () -> createResultSet(List.of()),
                        (_, _, _) -> List.of(),
                        preparedSql::add,
                        List.of(
                                List.of(row("TABLE_SCHEMA", "analytics", "TABLE_NAME", "events", "TABLE_TYPE", "TABLE")),
                                List.of(),
                                List.of(row(
                                        "Field",
                                        "event_id",
                                        "Type",
                                        "bigint"))),
                        boundParameters),
                new StarRocksConfig().setCatalogName("external_catalog"));

        StarRocksRemoteTable table = client.getTable(SESSION, new SchemaTableName("analytics", "events")).orElseThrow();

        assertThat(table.remoteCatalogName()).contains("external_catalog");
        assertThat(table.columns()).extracting(StarRocksRemoteColumn::columnName)
                .containsExactly("event_id");
        assertThat(preparedSql.get(1)).contains("TABLE_CATALOG = ?");
        assertThat(preparedSql.get(2)).isEqualTo("SHOW FULL COLUMNS FROM `external_catalog`.`analytics`.`events`");
        assertThat(boundParameters.get(1)).containsExactly("external_catalog", "analytics", "events");
        assertThat(boundParameters.get(2)).isEmpty();
    }

    @Test
    void testGetTableFallsBackToJdbcMetadataColumnsWhenInformationSchemaFails()
    {
        List<String> preparedSql = new ArrayList<>();
        List<List<String>> boundParameters = new ArrayList<>();
        List<List<Map<String, Object>>> rowsByStatement = new ArrayList<>();
        rowsByStatement.add(List.of(row("TABLE_SCHEMA", "analytics", "TABLE_NAME", "events", "TABLE_TYPE", "BASE TABLE")));
        rowsByStatement.add(null);

        JdbcStarRocksMetadataClient client = metadataClient(connectionFactory(
                () -> createResultSet(List.of()),
                (_, _, _) -> List.of(
                        row(
                                "COLUMN_NAME",
                                "metric_key",
                                "TYPE_NAME",
                                "BIGINT",
                                "COLUMN_SIZE",
                                20,
                                "DECIMAL_DIGITS",
                                null,
                                "ORDINAL_POSITION",
                                1),
                        row(
                                "COLUMN_NAME",
                                "created_at",
                                "TYPE_NAME",
                                "DATETIME",
                                "COLUMN_SIZE",
                                null,
                                "DECIMAL_DIGITS",
                                6,
                                "ORDINAL_POSITION",
                                2)),
                preparedSql::add,
                rowsByStatement,
                boundParameters));

        StarRocksRemoteTable table = client.getTable(SESSION, new SchemaTableName("analytics", "events")).orElseThrow();

        assertThat(table.columns()).extracting(StarRocksRemoteColumn::columnName)
                .containsExactly("metric_key", "created_at");
        assertThat(table.columns()).extracting(StarRocksRemoteColumn::typeName)
                .containsExactly("BIGINT", "DATETIME");
        assertThat(preparedSql.getFirst()).contains("FROM INFORMATION_SCHEMA.TABLES");
        assertThat(preparedSql.get(1)).contains("FROM INFORMATION_SCHEMA.COLUMNS");
    }

    private static StarRocksJdbcConnectionFactory connectionFactory(
            ResultSetSupplier schemasSupplier,
            ColumnsSupplier columnsSupplier,
            Consumer<String> preparedSqlConsumer,
            List<List<Map<String, Object>>> rowsByStatement,
            List<List<String>> boundParameters)
    {
        AtomicInteger statementIndex = new AtomicInteger();
        return new StarRocksJdbcConnectionFactory(new TestingDriver(), "jdbc:starrocks://example.invalid:9030", new Properties())
        {
            @Override
            public Connection openConnection()
            {
                return createConnection(schemasSupplier, columnsSupplier, preparedSqlConsumer, rowsByStatement, boundParameters, statementIndex);
            }

            @Override
            public Connection openConnection(ConnectorSession session)
            {
                return openConnection();
            }
        };
    }

    private static JdbcStarRocksMetadataClient metadataClient(StarRocksJdbcConnectionFactory connectionFactory)
    {
        return new JdbcStarRocksMetadataClient(connectionFactory, new StarRocksConfig());
    }

    private static JdbcStarRocksMetadataClient metadataClient(StarRocksJdbcConnectionFactory connectionFactory, StarRocksConfig config)
    {
        return new JdbcStarRocksMetadataClient(connectionFactory, config);
    }

    private static Connection createConnection(
            ResultSetSupplier schemasSupplier,
            ColumnsSupplier columnsSupplier,
            Consumer<String> preparedSqlConsumer,
            List<List<Map<String, Object>>> rowsByStatement,
            List<List<String>> boundParameters,
            AtomicInteger statementIndex)
    {
        return (Connection) Proxy.newProxyInstance(
                TestJdbcStarRocksMetadataClient.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getMetaData" -> createDatabaseMetadata(schemasSupplier, columnsSupplier);
                    case "prepareStatement" -> {
                        int index = statementIndex.getAndIncrement();
                        preparedSqlConsumer.accept((String) args[0]);
                        List<String> parameters = new ArrayList<>();
                        boundParameters.add(parameters);
                        yield createPreparedStatement(rowsByStatement.get(index), parameters);
                    }
                    case "getCatalog" -> null;
                    case "close" -> null;
                    case "isClosed" -> false;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "TestConnection";
                    default -> throw new UnsupportedOperationException("Unexpected connection method: " + method.getName());
                });
    }

    private static DatabaseMetaData createDatabaseMetadata(ResultSetSupplier schemasSupplier, ColumnsSupplier columnsSupplier)
    {
        return (DatabaseMetaData) Proxy.newProxyInstance(
                TestJdbcStarRocksMetadataClient.class.getClassLoader(),
                new Class<?>[] {DatabaseMetaData.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getSchemas" -> schemasSupplier.get();
                    case "getTables" -> createResultSet(List.of());
                    case "getColumns" -> createResultSet(columnsSupplier.get((String) args[0], (String) args[1], (String) args[2]));
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "TestDatabaseMetaData";
                    default -> throw new UnsupportedOperationException("Unexpected metadata method: " + method.getName());
                });
    }

    private static PreparedStatement createPreparedStatement(List<Map<String, Object>> rows, List<String> boundParameters)
    {
        return (PreparedStatement) Proxy.newProxyInstance(
                TestJdbcStarRocksMetadataClient.class.getClassLoader(),
                new Class<?>[] {PreparedStatement.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "setString" -> {
                        boundParameters.add((String) args[1]);
                        yield null;
                    }
                    case "executeQuery" -> {
                        if (rows == null) {
                            throw new SQLException("information schema query failed");
                        }
                        yield createResultSet(rows);
                    }
                    case "close" -> null;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "TestPreparedStatement";
                    default -> throw new UnsupportedOperationException("Unexpected prepared statement method: " + method.getName());
                });
    }

    private static ResultSet createResultSet(List<Map<String, Object>> rows)
    {
        AtomicInteger index = new AtomicInteger(-1);
        AtomicBoolean wasNull = new AtomicBoolean();

        return (ResultSet) Proxy.newProxyInstance(
                TestJdbcStarRocksMetadataClient.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "next" -> index.incrementAndGet() < rows.size();
                    case "getString" -> {
                        Object value = rows.get(index.get()).get(args[0]);
                        wasNull.set(value == null);
                        yield value == null ? null : value.toString();
                    }
                    case "getInt" -> {
                        Object value = rows.get(index.get()).get(args[0]);
                        wasNull.set(value == null);
                        yield value == null ? 0 : ((Number) value).intValue();
                    }
                    case "getLong" -> {
                        Object value = rows.get(index.get()).get(args[0]);
                        wasNull.set(value == null);
                        yield value == null ? 0L : ((Number) value).longValue();
                    }
                    case "wasNull" -> wasNull.get();
                    case "close" -> null;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "TestResultSet";
                    default -> throw new UnsupportedOperationException("Unexpected result set method: " + method.getName());
                });
    }

    private static Map<String, Object> row(Object... values)
    {
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index += 2) {
            row.put((String) values[index], values[index + 1]);
        }
        return row;
    }

    @FunctionalInterface
    private interface ResultSetSupplier
    {
        ResultSet get()
                throws SQLException;
    }

    @FunctionalInterface
    private interface ColumnsSupplier
    {
        List<Map<String, Object>> get(String catalog, String schemaPattern, String tableNamePattern);
    }

    private static final class TestingDriver
            implements Driver
    {
        @Override
        public Connection connect(String url, Properties info)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean acceptsURL(String url)
        {
            return true;
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
        {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion()
        {
            return 1;
        }

        @Override
        public int getMinorVersion()
        {
            return 0;
        }

        @Override
        public boolean jdbcCompliant()
        {
            return false;
        }

        @Override
        public Logger getParentLogger()
        {
            return Logger.getGlobal();
        }
    }
}
