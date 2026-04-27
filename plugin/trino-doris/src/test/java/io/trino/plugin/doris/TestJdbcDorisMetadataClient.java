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

import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestJdbcDorisMetadataClient
{
    @Test
    void testListSchemaNamesListsVisibleSchemasIncludingEmptyOnes()
    {
        AtomicReference<String> preparedSql = new AtomicReference<>();
        JdbcDorisMetadataClient client = new JdbcDorisMetadataClient(connectionFactory(
                preparedSql,
                List.of(
                        Map.of("SCHEMA_NAME", "test"),
                        Map.of("SCHEMA_NAME", "empty_db")),
                new ArrayList<>()));

        assertThat(client.listSchemaNames(SESSION)).containsExactly("test", "empty_db");
        assertThat(preparedSql.get())
                .contains("FROM INFORMATION_SCHEMA.SCHEMATA")
                .contains("LOWER(SCHEMA_NAME) NOT IN ('information_schema', '__internal_schema', 'mysql')")
                .doesNotContain("UPPER(COALESCE(ENGINE, '')) IN ('OLAP', 'DORIS')");
    }

    @Test
    void testListTablesKeepsReadableTablesPredicateWhenFilteringSchema()
    {
        AtomicReference<String> preparedSql = new AtomicReference<>();
        List<String> boundParameters = new ArrayList<>();
        JdbcDorisMetadataClient client = new JdbcDorisMetadataClient(connectionFactory(
                preparedSql,
                List.of(
                        Map.of("TABLE_SCHEMA", "Test", "TABLE_NAME", "Nation", "TABLE_TYPE", "BASE TABLE"),
                        Map.of("TABLE_SCHEMA", "Test", "TABLE_NAME", "Revenue0", "TABLE_TYPE", "VIEW")),
                boundParameters));

        assertThat(client.listTables(SESSION, Optional.of("test")))
                .containsExactly(
                        new SchemaTableName("Test", "Nation"),
                        new SchemaTableName("Test", "Revenue0"));
        assertThat(boundParameters).containsExactly("test");
        assertThat(preparedSql.get())
                .contains("LOWER(TABLE_SCHEMA) NOT IN ('information_schema', '__internal_schema', 'mysql')")
                .contains("(TABLE_TYPE = 'BASE TABLE' AND UPPER(COALESCE(ENGINE, '')) IN ('OLAP', 'DORIS'))")
                .contains("OR TABLE_TYPE = 'VIEW'");
    }

    @Test
    void testGetTableLoadsColumnsCaseInsensitively()
    {
        List<String> preparedSql = new ArrayList<>();
        List<List<String>> boundParameters = new ArrayList<>();
        JdbcDorisMetadataClient client = new JdbcDorisMetadataClient(scriptedConnectionFactory(
                preparedSql,
                boundParameters,
                List.of(
                        List.of(row("TABLE_SCHEMA", "mixedcase_db", "TABLE_NAME", "orderevents_mix")),
                        List.of(
                                row(
                                        "COLUMN_NAME",
                                        "event_id",
                                        "DATA_TYPE",
                                        "BIGINT",
                                        "COLUMN_SIZE",
                                        20,
                                        "DECIMAL_DIGITS",
                                        null,
                                        "ORDINAL_POSITION",
                                        1,
                                        "COLUMN_TYPE",
                                        "BIGINT"),
                                row(
                                        "COLUMN_NAME",
                                        "created_at",
                                        "DATA_TYPE",
                                        "DATETIME",
                                        "COLUMN_SIZE",
                                        null,
                                        "DECIMAL_DIGITS",
                                        3,
                                        "ORDINAL_POSITION",
                                        2,
                                        "COLUMN_TYPE",
                                        "DATETIMEV2(3)")))));

        DorisRemoteTable remoteTable = client.getTable(SESSION, new SchemaTableName("mixedcase_db", "orderevents_mix")).orElseThrow();

        assertThat(remoteTable.remoteSchemaName()).isEqualTo("mixedcase_db");
        assertThat(remoteTable.remoteTableName()).isEqualTo("orderevents_mix");
        assertThat(remoteTable.columns()).extracting(DorisRemoteColumn::columnName)
                .containsExactly("event_id", "created_at");
        assertThat(preparedSql.get(0)).contains("LOWER(TABLE_SCHEMA) = LOWER(?)");
        assertThat(preparedSql.get(0)).contains("LOWER(TABLE_NAME) = LOWER(?)");
        assertThat(preparedSql.get(1)).contains("FROM INFORMATION_SCHEMA.COLUMNS");
        assertThat(preparedSql.get(1)).contains("LOWER(TABLE_SCHEMA) = LOWER(?) AND LOWER(TABLE_NAME) = LOWER(?)");
        assertThat(boundParameters.get(0)).containsExactly("mixedcase_db", "orderevents_mix");
        assertThat(boundParameters.get(1)).containsExactly("mixedcase_db", "orderevents_mix");
    }

    @Test
    void testGetTableMarksViewsAsViews()
    {
        JdbcDorisMetadataClient client = new JdbcDorisMetadataClient(scriptedConnectionFactory(
                new ArrayList<>(),
                new ArrayList<>(),
                List.of(
                        List.of(row("TABLE_SCHEMA", "tpch", "TABLE_NAME", "revenue0", "TABLE_TYPE", "VIEW")),
                        List.of(
                                row(
                                        "COLUMN_NAME",
                                        "supplier_no",
                                        "DATA_TYPE",
                                        "BIGINT",
                                        "COLUMN_SIZE",
                                        20,
                                        "DECIMAL_DIGITS",
                                        null,
                                        "ORDINAL_POSITION",
                                        1,
                                        "COLUMN_TYPE",
                                        "BIGINT"),
                                row(
                                        "COLUMN_NAME",
                                        "total_revenue",
                                        "DATA_TYPE",
                                        "DECIMAL",
                                        "COLUMN_SIZE",
                                        15,
                                        "DECIMAL_DIGITS",
                                        4,
                                        "ORDINAL_POSITION",
                                        2,
                                        "COLUMN_TYPE",
                                        "DECIMAL(15,4)")))));

        DorisRemoteTable remoteTable = client.getTable(SESSION, new SchemaTableName("tpch", "revenue0")).orElseThrow();

        assertThat(remoteTable.relationType()).isEqualTo(DorisRelationType.VIEW);
        assertThat(remoteTable.columns()).extracting(DorisRemoteColumn::columnName)
                .containsExactly("supplier_no", "total_revenue");
    }

    private static DorisJdbcConnectionFactory connectionFactory(AtomicReference<String> preparedSql, List<Map<String, Object>> rows, List<String> boundParameters)
    {
        return new DorisJdbcConnectionFactory(new DorisConfig().setJdbcUrl("jdbc:mysql://example.invalid:9030/"))
        {
            @Override
            public Connection openConnection()
            {
                return createConnection(preparedSql, rows, boundParameters);
            }
        };
    }

    private static DorisJdbcConnectionFactory scriptedConnectionFactory(List<String> preparedSql, List<List<String>> boundParameters, List<List<Map<String, Object>>> rowsByStatement)
    {
        AtomicInteger statementIndex = new AtomicInteger();
        return new DorisJdbcConnectionFactory(new DorisConfig().setJdbcUrl("jdbc:mysql://example.invalid:9030/"))
        {
            @Override
            public Connection openConnection()
            {
                return createScriptedConnection(preparedSql, boundParameters, rowsByStatement, statementIndex);
            }
        };
    }

    private static Connection createConnection(AtomicReference<String> preparedSql, List<Map<String, Object>> rows, List<String> boundParameters)
    {
        return (Connection) Proxy.newProxyInstance(
                TestJdbcDorisMetadataClient.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "prepareStatement" -> {
                        preparedSql.set((String) args[0]);
                        yield createPreparedStatement(rows, boundParameters);
                    }
                    case "close" -> null;
                    case "isClosed" -> false;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "TestConnection";
                    default -> throw new UnsupportedOperationException("Unexpected connection method: " + method.getName());
                });
    }

    private static Connection createScriptedConnection(List<String> preparedSql, List<List<String>> boundParameters, List<List<Map<String, Object>>> rowsByStatement, AtomicInteger statementIndex)
    {
        return (Connection) Proxy.newProxyInstance(
                TestJdbcDorisMetadataClient.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "prepareStatement" -> {
                        int index = statementIndex.getAndIncrement();
                        preparedSql.add((String) args[0]);
                        List<String> parameters = new ArrayList<>();
                        boundParameters.add(parameters);
                        yield createPreparedStatement(rowsByStatement.get(index), parameters);
                    }
                    case "close" -> null;
                    case "isClosed" -> false;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "ScriptedTestConnection";
                    default -> throw new UnsupportedOperationException("Unexpected connection method: " + method.getName());
                });
    }

    private static PreparedStatement createPreparedStatement(List<Map<String, Object>> rows, List<String> boundParameters)
    {
        return (PreparedStatement) Proxy.newProxyInstance(
                TestJdbcDorisMetadataClient.class.getClassLoader(),
                new Class<?>[] {PreparedStatement.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "setString" -> {
                        boundParameters.add((String) args[1]);
                        yield null;
                    }
                    case "executeQuery" -> createResultSet(rows);
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
                TestJdbcDorisMetadataClient.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "next" -> index.incrementAndGet() < rows.size();
                    case "getString" -> {
                        Object value = rows.get(index.get()).get(args[0]);
                        wasNull.set(value == null);
                        yield (value == null) ? null : value.toString();
                    }
                    case "getInt" -> {
                        Object value = rows.get(index.get()).get(args[0]);
                        wasNull.set(value == null);
                        yield (value == null) ? 0 : ((Number) value).intValue();
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
}
