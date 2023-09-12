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
package io.trino.plugin.jdbc;

import io.trino.plugin.jdbc.credential.EmptyCredentialProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.SchemaTableName;
import org.h2.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.function.Function.identity;

final class TestingDatabase
        implements AutoCloseable
{
    private final String databaseName;
    private final Connection connection;
    private final JdbcClient jdbcClient;

    public TestingDatabase()
            throws SQLException
    {
        databaseName = "TEST" + System.nanoTime() + ThreadLocalRandom.current().nextLong();
        String connectionUrl = "jdbc:h2:mem:" + databaseName + ";NON_KEYWORDS=KEY,VALUE"; // key and value are reserved keywords in H2 2.x
        jdbcClient = new TestingH2JdbcClient(
                new BaseJdbcConfig(),
                new DriverConnectionFactory(new Driver(), connectionUrl, new Properties(), new EmptyCredentialProvider()));

        connection = DriverManager.getConnection(connectionUrl);
        connection.createStatement().execute("CREATE SCHEMA example");

        connection.createStatement().execute("CREATE TABLE example.numbers(text varchar primary key, text_short varchar(32), value bigint)");
        connection.createStatement().execute("CREATE TABLE example.timestamps(ts_3 timestamp(3) primary key, ts_6 timestamp(6), ts_9 timestamp(9))");
        connection.createStatement().execute("INSERT INTO example.numbers(text, text_short, value) VALUES " +
                "('one', 'one', 1)," +
                "('two', 'two', 2)," +
                "('three', 'three', 3)," +
                "('ten', 'ten', 10)," +
                "('eleven', 'eleven', 11)," +
                "('twelve', 'twelve', 12)" +
                "");
        connection.createStatement().execute("CREATE TABLE example.view_source(id varchar primary key)");
        connection.createStatement().execute("CREATE VIEW example.view AS SELECT id FROM example.view_source");
        connection.createStatement().execute("CREATE SCHEMA tpch");
        connection.createStatement().execute("CREATE TABLE tpch.orders(orderkey bigint primary key, custkey bigint)");
        connection.createStatement().execute("CREATE TABLE tpch.lineitem(orderkey bigint primary key, partkey bigint)");

        connection.createStatement().execute("CREATE SCHEMA exa_ple");
        connection.createStatement().execute("CREATE TABLE exa_ple.num_ers(te_t varchar primary key, \"VA%UE\" bigint)");
        connection.createStatement().execute("CREATE TABLE exa_ple.table_with_float_col(col1 bigint, col2 double, col3 float, col4 real)");

        connection.commit();
    }

    @Override
    public void close()
            throws SQLException
    {
        connection.close();
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public Connection getConnection()
    {
        return connection;
    }

    public JdbcClient getJdbcClient()
    {
        return jdbcClient;
    }

    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName table)
    {
        return jdbcClient.getTableHandle(session, table)
                .orElseThrow(() -> new IllegalArgumentException("table not found: " + table));
    }

    public JdbcSplit getSplit(ConnectorSession session, JdbcTableHandle table)
    {
        ConnectorSplitSource splits = jdbcClient.getSplits(session, table);
        return (JdbcSplit) getOnlyElement(getFutureValue(splits.getNextBatch(1000)).getSplits());
    }

    public Map<String, JdbcColumnHandle> getColumnHandles(ConnectorSession session, JdbcTableHandle table)
    {
        return jdbcClient.getColumns(session, table).stream()
                .collect(toImmutableMap(column -> column.getColumnMetadata().getName(), identity()));
    }
}
