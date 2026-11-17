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
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.Column;
import io.trino.client.QueryResults;
import io.trino.client.StatementStats;
import io.trino.client.TypedQueryData;
import io.trino.spi.type.StandardTypes;
import mockwebserver3.MockWebServer;
import mockwebserver3.junit5.StartStop;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static io.trino.client.ClientTypeSignature.VARCHAR_UNBOUNDED_LENGTH;
import static io.trino.jdbc.TestingJdbcUtils.column;
import static io.trino.jdbc.TestingJdbcUtils.createConnection;
import static io.trino.jdbc.TestingJdbcUtils.enqueueResults;
import static io.trino.jdbc.TestingJdbcUtils.readRows;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Covers result shapes of statements with an update type, including ones sent only by older servers.
 */
@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestUpdateResults
{
    private static final String QUERY_ID = "20160128_214710_00012_rk68b";
    private static final Column QUERY_PLAN = new Column("Query Plan", StandardTypes.VARCHAR, varchar());
    private static final List<Column> TABLE_PROCEDURE_METRICS = ImmutableList.of(
            new Column("metric_name", StandardTypes.VARCHAR, varchar()),
            column("metric_value", StandardTypes.BIGINT));

    @StartStop
    private final MockWebServer server = new MockWebServer();

    @Test
    public void testLegacyResultOfStatementWithoutOutput()
            throws SQLException
    {
        // servers before version 398 return a single true value for DDL statements
        enqueue(ImmutableList.of(column("result", StandardTypes.BOOLEAN)), ImmutableList.of(ImmutableList.of(true)), "CREATE TABLE");

        try (Connection connection = createConnection(server);
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("CREATE TABLE test (x bigint)")).isFalse();
            assertThat(statement.getUpdateCount()).isEqualTo(0);
            assertThat(statement.unwrap(TrinoStatement.class).getUpdateType()).isEqualTo("CREATE TABLE");
            assertNoMoreResults(statement);
        }
    }

    @Test
    public void testUnknownUpdateCount()
            throws SQLException
    {
        // connectors that delete whole partitions or tables do not know how many rows they deleted
        enqueue(ImmutableList.of(column("rows", StandardTypes.BIGINT)), ImmutableList.of(Collections.singletonList(null)), "DELETE");

        try (Connection connection = createConnection(server);
                Statement statement = connection.createStatement()) {
            assertThat(statement.executeUpdate("DELETE FROM test")).isEqualTo(0);
            assertThat(statement.unwrap(TrinoStatement.class).getUpdateType()).isEqualTo("DELETE");
            assertNoMoreResults(statement);
        }
    }

    @Test
    public void testRowsAfterUpdateCount()
            throws SQLException
    {
        enqueue(ImmutableList.of(QUERY_PLAN), ImmutableList.of(ImmutableList.of("Fragment 0")), "INSERT");

        try (Connection connection = createConnection(server);
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("EXPLAIN ANALYZE INSERT INTO test VALUES 1")).isFalse();
            assertThat(statement.getUpdateCount()).isEqualTo(0);
            assertThat(statement.unwrap(TrinoStatement.class).getUpdateType()).isEqualTo("INSERT");
            assertThat(statement.getMoreResults()).isTrue();
            try (ResultSet resultSet = statement.getResultSet()) {
                assertThat(resultSet.getStatement()).isSameAs(statement);
                assertThat(readRows(resultSet)).containsExactly(ImmutableList.of("Fragment 0"));
            }
            assertNoMoreResults(statement);
        }
    }

    @Test
    public void testTableProcedureMetrics()
            throws SQLException
    {
        enqueue(TABLE_PROCEDURE_METRICS,
                ImmutableList.of(
                        ImmutableList.of("rewritten_data_files_count", 3L),
                        ImmutableList.of("added_data_files_count", 1L)),
                "ALTER TABLE EXECUTE");

        try (Connection connection = createConnection(server);
                Statement statement = connection.createStatement()) {
            assertThat(statement.executeUpdate("ALTER TABLE test EXECUTE optimize")).isEqualTo(0);
            // closing all results is the same as closing the current one, because only one result is open
            assertThat(statement.getMoreResults(Statement.CLOSE_ALL_RESULTS)).isTrue();
            try (ResultSet resultSet = statement.getResultSet()) {
                assertThat(readRows(resultSet)).containsExactly(
                        ImmutableList.of("rewritten_data_files_count", 3L),
                        ImmutableList.of("added_data_files_count", 1L));
            }
            assertNoMoreResults(statement);
        }
    }

    @Test
    public void testSeveralBigintRowsAreNotUpdateCount()
            throws SQLException
    {
        // only a single bigint value is the update count, like on the server
        enqueue(ImmutableList.of(column("value", StandardTypes.BIGINT)), ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(2L)), "CALL");

        try (Connection connection = createConnection(server);
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("CALL test()")).isFalse();
            assertThat(statement.getMoreResults()).isTrue();
            try (ResultSet resultSet = statement.getResultSet()) {
                assertThat(readRows(resultSet)).containsExactly(ImmutableList.of(1L), ImmutableList.of(2L));
            }
            assertNoMoreResults(statement);
        }
    }

    @Test
    public void testExecuteDiscardsPendingRows()
            throws SQLException
    {
        enqueue(ImmutableList.of(QUERY_PLAN), ImmutableList.of(ImmutableList.of("Fragment 0")), "INSERT");
        enqueue(ImmutableList.of(column("rows", StandardTypes.BIGINT)), ImmutableList.of(ImmutableList.of(1L)), "INSERT", OptionalLong.of(1));

        try (Connection connection = createConnection(server);
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("EXPLAIN ANALYZE INSERT INTO test VALUES 1")).isFalse();
            assertThat(statement.execute("INSERT INTO test VALUES 1")).isFalse();
            assertThat(statement.getUpdateCount()).isEqualTo(1);
            assertNoMoreResults(statement);
        }
    }

    @Test
    public void testKeepCurrentResultWithPendingRows()
            throws SQLException
    {
        enqueue(ImmutableList.of(QUERY_PLAN), ImmutableList.of(ImmutableList.of("Fragment 0")), "INSERT");

        try (Connection connection = createConnection(server);
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("EXPLAIN ANALYZE INSERT INTO test VALUES 1")).isFalse();
            assertThatThrownBy(() -> statement.getMoreResults(Statement.KEEP_CURRENT_RESULT))
                    .isInstanceOf(SQLFeatureNotSupportedException.class)
                    .hasMessage("Multiple open results not supported");
            assertThat(statement.getUpdateCount()).isEqualTo(0);
            // the rows are still the next result
            assertThat(statement.getMoreResults()).isTrue();
        }
    }

    @Test
    public void testCloseAllResultsWithoutPendingRows()
            throws SQLException
    {
        enqueue(ImmutableList.of(column("rows", StandardTypes.BIGINT)), ImmutableList.of(ImmutableList.of(1L)), "INSERT", OptionalLong.of(1));

        try (Connection connection = createConnection(server);
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("INSERT INTO test VALUES 1")).isFalse();
            assertThat(statement.getMoreResults(Statement.CLOSE_ALL_RESULTS)).isFalse();
            assertThat(statement.getResultSet()).isNull();
            assertThat(statement.getUpdateCount()).isEqualTo(-1);
        }
    }

    @Test
    public void testExecuteQueryOfUpdateWithRows()
            throws SQLException
    {
        enqueue(ImmutableList.of(QUERY_PLAN), ImmutableList.of(ImmutableList.of("Fragment 0")), "INSERT");

        try (Connection connection = createConnection(server);
                Statement statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.executeQuery("EXPLAIN ANALYZE INSERT INTO test VALUES 1"))
                    .isInstanceOf(SQLException.class)
                    .hasMessage("SQL statement is not a query: EXPLAIN ANALYZE INSERT INTO test VALUES 1");
        }
    }

    private static void assertNoMoreResults(Statement statement)
            throws SQLException
    {
        assertThat(statement.getMoreResults()).isFalse();
        assertThat(statement.getResultSet()).isNull();
        assertThat(statement.getUpdateCount()).isEqualTo(-1);
    }

    private void enqueue(List<Column> columns, List<List<Object>> rows, String updateType)
    {
        enqueue(columns, rows, updateType, OptionalLong.empty());
    }

    private void enqueue(List<Column> columns, List<List<Object>> rows, String updateType, OptionalLong updateCount)
    {
        enqueueResults(server, new QueryResults(
                QUERY_ID,
                server.url("/query.html?" + QUERY_ID).uri(),
                null,
                null,
                columns,
                TypedQueryData.of(rows),
                StatementStats.builder()
                        .setState("FINISHED")
                        .setProgressPercentage(OptionalDouble.empty())
                        .setRunningPercentage(OptionalDouble.empty())
                        .build(),
                null,
                ImmutableList.of(),
                updateType,
                updateCount));
    }

    private static ClientTypeSignature varchar()
    {
        return new ClientTypeSignature(StandardTypes.VARCHAR, ImmutableList.of(ClientTypeSignatureParameter.ofLong(VARCHAR_UNBOUNDED_LENGTH)));
    }
}
