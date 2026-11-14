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
package io.trino.cli;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.Column;
import io.trino.client.ErrorLocation;
import io.trino.client.QueryData;
import io.trino.client.QueryError;
import io.trino.client.QueryResults;
import io.trino.client.StatementStats;
import io.trino.client.TrinoJsonCodec;
import io.trino.client.TypedQueryData;
import io.trino.client.Warning;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.junit5.StartStop;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Properties;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.SET_COOKIE;
import static io.trino.cli.ClientOptions.OutputFormat.CSV;
import static io.trino.cli.TerminalUtils.getTerminal;
import static io.trino.cli.TrinoConnectionProperties.EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS;
import static io.trino.cli.TrinoConnectionProperties.SSL;
import static io.trino.cli.TrinoConnectionProperties.SSL_VERIFICATION;
import static io.trino.cli.TrinoConnectionProperties.USER;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.BOOLEAN;
import static io.trino.client.ClientStandardTypes.VARCHAR;
import static io.trino.client.TrinoJsonCodec.jsonCodec;
import static java.io.OutputStream.nullOutputStream;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestQueryRunner
{
    private static final TrinoJsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private static final String QUERY_ID = "20160128_214710_00012_rk68b";

    @StartStop
    private final MockWebServer server = new MockWebServer();

    @Test
    public void testCookie()
            throws Exception
    {
        server.enqueue(new MockResponse.Builder()
                .code(307)
                .addHeader(LOCATION, server.url("/v1/statement"))
                .addHeader(SET_COOKIE, "a=apple")
                .build());
        server.enqueue(jsonResponse(createResults(server)));
        server.enqueue(jsonResponse(createResults(server)));

        try (QueryRunner queryRunner = createQueryRunner(createConnectionFactory(server, false))) {
            try (Query query = queryRunner.startQuery("first query will introduce a cookie")) {
                query.renderOutput(getTerminal(), nullPrintStream(), nullPrintStream(), CSV, Optional.of(""), false, false);
            }
            try (Query query = queryRunner.startQuery("second query should carry the cookie")) {
                query.renderOutput(getTerminal(), nullPrintStream(), nullPrintStream(), CSV, Optional.of(""), false, false);
            }
        }

        assertThat(server.takeRequest().getHeaders().get("Cookie")).isNull();
        assertThat(server.takeRequest().getHeaders().get("Cookie")).isEqualTo("a=apple");
        assertThat(server.takeRequest().getHeaders().get("Cookie")).isEqualTo("a=apple");
    }

    @Test
    public void testCloseClosesConnectionsAndFactory()
            throws Exception
    {
        RecordingConnectionFactory connectionFactory = new RecordingConnectionFactory(createConnectionFactory(server, false));
        QueryRunner queryRunner = createQueryRunner(connectionFactory);
        queryRunner.withMetadataConnection(connection -> null);

        queryRunner.close();

        assertThat(connectionFactory.getConnections()).hasSize(2);
        for (Connection connection : connectionFactory.getConnections()) {
            assertThat(connection.isClosed()).isTrue();
        }
        assertThat(connectionFactory.isClosed()).isTrue();
    }

    @Test
    public void testRenderResults()
            throws Exception
    {
        server.enqueue(jsonResponse(createResults(server)));

        assertThat(execute("SELECT 123", false)).isEqualTo(new Output(true, "\"123\"\n", ""));
    }

    @Test
    public void testRenderUpdateStatus()
            throws Exception
    {
        server.enqueue(jsonResponse(createUpdateResults(server, "CREATE TABLE", OptionalLong.empty())));
        server.enqueue(jsonResponse(createUpdateResults(server, "INSERT", OptionalLong.of(2))));
        server.enqueue(jsonResponse(createUpdateResults(server, "DELETE", OptionalLong.of(1))));

        assertThat(execute("CREATE TABLE test (x bigint)", false)).isEqualTo(new Output(true, "", "CREATE TABLE\n"));
        assertThat(execute("INSERT INTO test VALUES 1, 2", false)).isEqualTo(new Output(true, "", "INSERT: 2 rows\n"));
        assertThat(execute("DELETE FROM test WHERE x = 1", false)).isEqualTo(new Output(true, "", "DELETE: 1 row\n"));
    }

    @Test
    public void testRenderUpdateWithRows()
            throws Exception
    {
        // like EXPLAIN ANALYZE of an INSERT, which returns the plan without an update count
        server.enqueue(jsonResponse(createResults(
                server,
                Optional.of(ImmutableList.of(new Column("Query Plan", VARCHAR, new ClientTypeSignature(VARCHAR, ImmutableList.of(ClientTypeSignatureParameter.ofLong(Integer.MAX_VALUE)))))),
                TypedQueryData.of(ImmutableList.of(ImmutableList.of("Fragment 0"))),
                Optional.empty(),
                ImmutableList.of(),
                "INSERT",
                OptionalLong.empty())));

        assertThat(execute("EXPLAIN ANALYZE INSERT INTO test VALUES 1", false)).isEqualTo(new Output(true, "\"Fragment 0\"\n", "INSERT\n"));
    }

    @Test
    public void testRenderTableProcedureMetrics()
            throws Exception
    {
        server.enqueue(jsonResponse(createResults(
                server,
                Optional.of(ImmutableList.of(
                        new Column("rewritten_data_files_count", BIGINT, new ClientTypeSignature(BIGINT)),
                        new Column("added_data_files_count", BIGINT, new ClientTypeSignature(BIGINT)))),
                TypedQueryData.of(ImmutableList.of(ImmutableList.of(3, 1))),
                Optional.empty(),
                ImmutableList.of(),
                "ALTER TABLE EXECUTE",
                OptionalLong.empty())));

        assertThat(execute("ALTER TABLE test EXECUTE optimize", false)).isEqualTo(new Output(true, "\"3\",\"1\"\n", "ALTER TABLE EXECUTE\n"));
    }

    @Test
    public void testRenderFailure()
            throws Exception
    {
        QueryError error = new QueryError("line 2:6: Table 'no_such_table' does not exist", null, 1, "TABLE_NOT_FOUND", "USER_ERROR", new ErrorLocation(2, 6), null);
        server.enqueue(jsonResponse(createResults(server, Optional.empty(), QueryData.NULL, Optional.of(error), ImmutableList.of(), null, OptionalLong.empty())));

        assertThat(execute("SELECT *\nFROM no_such_table", false)).isEqualTo(new Output(
                false,
                "",
                format("Query %s failed: line 2:6: Table 'no_such_table' does not exist\n" +
                        "LINE 2: FROM no_such_table\n" +
                        "             ^\n" +
                        "\n", QUERY_ID)));
    }

    @Test
    public void testRenderWarnings()
            throws Exception
    {
        List<Warning> warnings = ImmutableList.of(
                new Warning(new Warning.Code(1, "DEPRECATED"), "first"),
                new Warning(new Warning.Code(1, "DEPRECATED"), "second"));
        server.enqueue(jsonResponse(createResults(
                server,
                Optional.of(ImmutableList.of(new Column("_col0", BIGINT, new ClientTypeSignature(BIGINT)))),
                TypedQueryData.of(ImmutableList.of(ImmutableList.of(123))),
                Optional.empty(),
                warnings,
                null,
                OptionalLong.empty())));

        assertThat(execute("SELECT 123", false)).isEqualTo(new Output(true, "\"123\"\n", "WARNING: first\nWARNING: second\n\n"));
    }

    @Test
    public void testRenderFinalStats()
            throws Exception
    {
        server.enqueue(jsonResponse(createResults(server)));

        Output output = execute("SELECT 123", true);
        assertThat(output.success()).isTrue();
        assertThat(output.out()).isEqualTo("\"123\"\n");
        assertThat(output.err())
                .startsWith(format("\nQuery %s, FINISHED, 1 node\n", QUERY_ID))
                .contains("Splits: 1 total, 1 done (100.00%)");
    }

    private Output execute(String sql, boolean showProgress)
            throws Exception
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        boolean success;
        try (QueryRunner queryRunner = createQueryRunner(createConnectionFactory(server, false));
                Query query = queryRunner.startQuery(sql)) {
            success = query.renderOutput(getTerminal(), new PrintStream(out, true, UTF_8), new PrintStream(err, true, UTF_8), CSV, Optional.of(""), showProgress, false);
        }
        return new Output(success, out.toString(UTF_8), err.toString(UTF_8));
    }

    static ConnectionFactory createConnectionFactory(MockWebServer server, boolean insecureSsl)
    {
        Properties properties = new Properties();
        properties.setProperty(USER, "user");
        properties.setProperty(EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS, "PRINT");
        properties.setProperty(SSL, Boolean.toString(insecureSsl));
        if (insecureSsl) {
            properties.setProperty(SSL_VERIFICATION, "NONE");
        }
        String url = "jdbc:trino://" + server.getHostName() + ":" + server.getPort();
        return new TrinoConnectionFactory(url, properties, new Duration(2, MINUTES));
    }

    static String createResults(MockWebServer server)
    {
        return createResults(
                server,
                Optional.of(ImmutableList.of(new Column("_col0", BIGINT, new ClientTypeSignature(BIGINT)))),
                TypedQueryData.of(ImmutableList.of(ImmutableList.of(123))),
                Optional.empty(),
                ImmutableList.of(),
                null,
                OptionalLong.empty());
    }

    private static String createUpdateResults(MockWebServer server, String updateType, OptionalLong updateCount)
    {
        return createResults(
                server,
                Optional.of(ImmutableList.of(new Column("result", BOOLEAN, new ClientTypeSignature(BOOLEAN)))),
                TypedQueryData.of(ImmutableList.of(ImmutableList.of(true))),
                Optional.empty(),
                ImmutableList.of(),
                updateType,
                updateCount);
    }

    private static String createResults(MockWebServer server, Optional<List<Column>> columns, QueryData data, Optional<QueryError> error, List<Warning> warnings, String updateType, OptionalLong updateCount)
    {
        QueryResults queryResults = new QueryResults(
                QUERY_ID,
                server.url("/query.html?" + QUERY_ID).uri(),
                null,
                null,
                columns.orElse(null),
                data,
                StatementStats.builder()
                        .setState(error.isPresent() ? "FAILED" : "FINISHED")
                        .setNodes(1)
                        .setTotalSplits(1)
                        .setCompletedSplits(1)
                        .setProgressPercentage(OptionalDouble.of(100))
                        .setRunningPercentage(OptionalDouble.empty())
                        .build(),
                error.orElse(null),
                warnings,
                updateType,
                updateCount);
        return QUERY_RESULTS_CODEC.toJson(queryResults);
    }

    static QueryRunner createQueryRunner(ConnectionFactory connectionFactory)
            throws SQLException
    {
        return new QueryRunner(
                Theme.NONE,
                connectionFactory,
                false,
                1000,
                500);
    }

    static PrintStream nullPrintStream()
    {
        return new PrintStream(nullOutputStream());
    }

    private static MockResponse jsonResponse(String body)
    {
        return new MockResponse.Builder()
                .addHeader(CONTENT_TYPE, "application/json")
                .body(body)
                .build();
    }

    private static final class Output
    {
        private final boolean success;
        private final String out;
        private final String err;

        private Output(boolean success, String out, String err)
        {
            this.success = success;
            this.out = out;
            this.err = err;
        }

        public boolean success()
        {
            return success;
        }

        public String out()
        {
            return out;
        }

        public String err()
        {
            return err;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof Output)) {
                return false;
            }
            Output other = (Output) o;
            return success == other.success && out.equals(other.out) && err.equals(other.err);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(success, out, err);
        }

        @Override
        public String toString()
        {
            return format("success=%s, out=[%s], err=[%s]", success, out, err);
        }
    }

    private static class RecordingConnectionFactory
            implements ConnectionFactory
    {
        private final ConnectionFactory delegate;
        private final List<Connection> connections = new ArrayList<>();
        private boolean closed;

        public RecordingConnectionFactory(ConnectionFactory delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public synchronized Connection openConnection()
                throws SQLException
        {
            Connection connection = delegate.openConnection();
            connections.add(connection);
            return connection;
        }

        public synchronized List<Connection> getConnections()
        {
            return ImmutableList.copyOf(connections);
        }

        public synchronized boolean isClosed()
        {
            return closed;
        }

        @Override
        public synchronized void close()
        {
            closed = true;
            delegate.close();
        }
    }
}
