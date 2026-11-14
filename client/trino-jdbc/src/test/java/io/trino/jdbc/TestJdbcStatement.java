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
package io.trino.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logging;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.jdbc.TestJdbcConnection.assertThatFutureIsBlocked;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.sql.Types.VARCHAR;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJdbcStatement
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getName()));
    private TestingTrinoServer server;

    @BeforeAll
    public void setupServer()
            throws Exception
    {
        Logging.initialize();
        server = TestingTrinoServer.create();

        server.installPlugin(new BlackHolePlugin());
        server.createCatalog("blackhole", "blackhole", ImmutableMap.of());

        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE blackhole.default.devzero(dummy bigint) " +
                            "WITH (split_count = 100000, pages_per_split = 100000, rows_per_page = 10000)");
            statement.execute(
                    "CREATE TABLE blackhole.default.delay(dummy bigint) " +
                            "WITH (split_count = 1, pages_per_split = 1, rows_per_page = 1, page_processing_delay = '60s')");
        }
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        closeAll(
                server,
                executor::shutdownNow);
        server = null;
    }

    @Test
    @Timeout(60)
    public void testCancellationOnStatementClose()
            throws Exception
    {
        String sql = "SELECT * FROM blackhole.default.devzero -- test cancellation " + randomUUID();
        try (Connection connection = createConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(sql);
            ResultSet resultSet = statement.getResultSet();

            // read some data
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.next()).isTrue();

            // Make sure that query is still running
            assertThat(listQueryStatuses(sql))
                    .containsExactly("RUNNING")
                    .hasSize(1);

            // Closing statement should cancel queries and invalidate the result set
            statement.close();

            // verify that the query was cancelled
            assertThatThrownBy(resultSet::next)
                    .isInstanceOf(SQLException.class)
                    .hasMessage("ResultSet is closed");
            assertThat(listQueryErrorCodes(sql))
                    .containsExactly("USER_CANCELED")
                    .hasSize(1);
        }
    }

    /**
     * @see TestJdbcConnection#testConcurrentCancellationOnConnectionClose
     */
    @Test
    @Timeout(60)
    public void testConcurrentCancellationOnStatementClose()
            throws Exception
    {
        String sql = "SELECT * FROM blackhole.default.delay -- test cancellation " + randomUUID();
        Future<?> future;
        try (Connection connection = createConnection()) {
            Statement statement = connection.createStatement();
            future = executor.submit(() -> {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    //noinspection StatementWithEmptyBody
                    while (resultSet.next()) {
                        // consume results
                    }
                }
                return null;
            });

            // Wait for the queries to be started
            assertEventually(() -> {
                assertThatFutureIsBlocked(future);
                assertThat(listQueryStatuses(sql))
                        .contains("RUNNING")
                        .hasSize(1);
            });

            // Closing statement should cancel queries
            statement.close();

            // verify that the query was cancelled
            assertThatThrownBy(future::get).isNotNull();
            assertThat(listQueryErrorCodes(sql))
                    .allMatch(errorCode -> "TRANSACTION_ALREADY_ABORTED".equals(errorCode) || "USER_CANCELED".equals(errorCode))
                    .hasSize(1);
        }
    }

    @Test
    public void testExplainAnalyzeOfUpdate()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                TrinoStatement trinoStatement = statement.unwrap(TrinoStatement.class);
                statement.execute("CREATE TABLE blackhole.default.explain_analyze_update (x bigint)");

                // the update count is followed by the query plan
                assertThat(statement.execute("EXPLAIN ANALYZE INSERT INTO blackhole.default.explain_analyze_update VALUES 1")).isFalse();
                assertThat(trinoStatement.getUpdateType()).isEqualTo("INSERT");
                assertThat(statement.getUpdateCount()).isEqualTo(0);
                assertThat(trinoStatement.getReportedUpdateCount()).isEmpty();
                assertThat(statement.getMoreResults()).isTrue();
                try (ResultSet resultSet = statement.getResultSet()) {
                    assertThat(resultSet.getStatement()).isSameAs(statement);
                    assertThat(resultSet.getMetaData().getColumnName(1)).isEqualTo("Query Plan");
                    assertThat(resultSet.next()).isTrue();
                    assertThat(resultSet.getString(1)).contains("Fragment");
                    assertThat(resultSet.next()).isFalse();
                }
                assertThat(statement.getMoreResults()).isFalse();
                assertThat(statement.getUpdateCount()).isEqualTo(-1);

                assertThat(statement.execute("INSERT INTO blackhole.default.explain_analyze_update VALUES 1, 2")).isFalse();
                assertThat(trinoStatement.getUpdateType()).isEqualTo("INSERT");
                assertThat(statement.getUpdateCount()).isEqualTo(2);
                assertThat(statement.getMoreResults()).isFalse();
            }
            finally {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP TABLE IF EXISTS blackhole.default.explain_analyze_update");
                }
            }
        }
    }

    @Test
    public void testReportedUpdateCount()
            throws Exception
    {
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            TrinoStatement trinoStatement = statement.unwrap(TrinoStatement.class);

            assertThat(statement.execute("CREATE TABLE blackhole.default.reported_update_count (x bigint)")).isFalse();
            assertThat(trinoStatement.getUpdateType()).isEqualTo("CREATE TABLE");
            assertThat(statement.getLargeUpdateCount()).isEqualTo(0);
            assertThat(trinoStatement.getReportedUpdateCount()).isEmpty();

            assertThat(statement.execute("INSERT INTO blackhole.default.reported_update_count VALUES 1, 2")).isFalse();
            assertThat(trinoStatement.getUpdateType()).isEqualTo("INSERT");
            assertThat(trinoStatement.getReportedUpdateCount()).hasValue(2);

            assertThat(statement.execute("SELECT 1")).isTrue();
            assertThat(trinoStatement.getReportedUpdateCount()).isEmpty();

            statement.execute("DROP TABLE blackhole.default.reported_update_count");
        }
    }

    @Test
    public void testFailureDetails()
            throws Exception
    {
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.execute("SELECT 1\nFROM blackhole.default.no_such_table"))
                    .isInstanceOfSatisfying(TrinoSqlException.class, exception -> {
                        assertThat(exception.getQueryId()).isNotEmpty();
                        assertThat(exception.getMessage()).isEqualTo(format("Query failed (#%s): %s", exception.getQueryId(), exception.getErrorMessage()));
                        assertThat(exception.getErrorMessage()).contains("no_such_table");
                        assertThat(exception.getErrorLocation()).hasValueSatisfying(location -> {
                            assertThat(location.getLineNumber()).isEqualTo(2);
                            assertThat(location.getColumnNumber()).isEqualTo(6);
                        });
                    });
        }
    }

    @Test
    public void testRawObject()
            throws Exception
    {
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT " +
                        "TIMESTAMP '2020-01-02 03:04:05', " +
                        "TIMESTAMP '2020-01-02 03:04:05.123456789123 UTC', " +
                        "DECIMAL '0.0000000001', " +
                        "ARRAY[TIME '01:02:03.456'], " +
                        "MAP(ARRAY['b', 'a'], ARRAY[DATE '2020-01-02', NULL]), " +
                        "CAST(ROW(1, X'0102') AS ROW(id bigint, payload varbinary)), " +
                        "CAST(JSON '{\"b\":1.50,\"a\":[1,true,null]}' AS variant)")) {
            TrinoResultSet trinoResultSet = resultSet.unwrap(TrinoResultSet.class);
            assertThat(resultSet.next()).isTrue();

            assertThat(trinoResultSet.getRawObject(1)).isEqualTo("2020-01-02 03:04:05");
            assertThat(trinoResultSet.getRawObject(2)).isEqualTo("2020-01-02 03:04:05.123456789123 UTC");
            assertThat(trinoResultSet.getRawObject(3)).isEqualTo("0.0000000001");
            assertThat(trinoResultSet.getRawObject(4)).isEqualTo(ImmutableList.of("01:02:03.456"));
            Map<String, String> expectedMap = new HashMap<>();
            expectedMap.put("a", null);
            expectedMap.put("b", "2020-01-02");
            assertThat(trinoResultSet.getRawObject(5)).isEqualTo(expectedMap);
            assertThat(trinoResultSet.getRawObject(6)).isInstanceOfSatisfying(Row.class, row -> {
                assertThat(row.getFields()).extracting(RowField::getName).containsExactly(Optional.of("id"), Optional.of("payload"));
                assertThat(row.getFields().get(0).getValue()).isEqualTo(1L);
                assertThat(row.getFields().get(1).getValue()).isEqualTo(new byte[] {1, 2});
            });
            assertThat(trinoResultSet.getRawObject(7)).isEqualTo("{\"a\":[1,true,null],\"b\":1.5}");
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:trino://%s/", server.getAddress());
        return DriverManager.getConnection(url, "a_user", null);
    }

    private List<String> listQueryStatuses(String sql)
    {
        return listSingleStringColumn(format("SELECT state FROM system.runtime.queries WHERE query = '%s'", sql));
    }

    private List<String> listQueryErrorCodes(String sql)
    {
        return listSingleStringColumn(format("SELECT error_code FROM system.runtime.queries WHERE query = '%s'", sql));
    }

    private List<String> listSingleStringColumn(String sql)
    {
        ImmutableList.Builder<String> statuses = ImmutableList.builder();
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertThat(resultSet.getMetaData().getColumnCount()).isOne();
            assertThat(resultSet.getMetaData().getColumnType(1)).isEqualTo(VARCHAR);
            while (resultSet.next()) {
                statuses.add(resultSet.getString(1));
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return statuses.build();
    }
}
