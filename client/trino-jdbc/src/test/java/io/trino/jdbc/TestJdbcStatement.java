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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
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

public class TestJdbcStatement
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getName()));
    private TestingTrinoServer server;

    @BeforeClass
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

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        closeAll(
                server,
                executor::shutdownNow);
        server = null;
    }

    @Test(timeOut = 60_000)
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
    @Test(timeOut = 60_000)
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
