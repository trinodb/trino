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
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import io.trino.client.QueryResults;
import io.trino.client.StatementStats;
import io.trino.client.TypedQueryData;
import io.trino.server.protocol.spooling.QueryDataJacksonModule;
import io.trino.spi.type.StandardTypes;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.OptionalDouble;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestProgressMonitor
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = new JsonCodecFactory(new ObjectMapperProvider()
            .withModules(ImmutableSet.of(new QueryDataJacksonModule())))
            .jsonCodec(QueryResults.class);

    private MockWebServer server;

    @BeforeEach
    public void setup()
            throws IOException
    {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    public void teardown()
            throws IOException
    {
        server.close();
        server = null;
    }

    private List<String> createResults()
    {
        List<Column> columns = ImmutableList.of(new Column("_col0", StandardTypes.BIGINT, new ClientTypeSignature(StandardTypes.BIGINT)));
        return ImmutableList.<String>builder()
                .add(newQueryResults(null, 1, null, null, "QUEUED"))
                .add(newQueryResults(1, 2, columns, null, "RUNNING"))
                .add(newQueryResults(1, 3, columns, null, "RUNNING"))
                .add(newQueryResults(0, 4, columns, ImmutableList.of(ImmutableList.of(253161)), "RUNNING"))
                .add(newQueryResults(null, null, columns, null, "FINISHED"))
                .build();
    }

    private String newQueryResults(Integer partialCancelId, Integer nextUriId, List<Column> responseColumns, List<List<Object>> data, String state)
    {
        String queryId = "20160128_214710_00012_rk68b";
        QueryResults queryResults = new QueryResults(
                queryId,
                server.url("/query.html?" + queryId).uri(),
                partialCancelId == null ? null : server.url(format("/v1/statement/partialCancel/%s.%s", queryId, partialCancelId)).uri(),
                nextUriId == null ? null : server.url(format("/v1/statement/%s/%s", queryId, nextUriId)).uri(),
                responseColumns,
                TypedQueryData.of(data),
                new StatementStats(state, state.equals("QUEUED"), true, OptionalDouble.of(0), OptionalDouble.of(0), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null),
                null,
                ImmutableList.of(),
                null,
                null);

        return QUERY_RESULTS_CODEC.toJson(queryResults);
    }

    @Test
    public void test()
            throws SQLException
    {
        for (String result : createResults()) {
            server.enqueue(new MockResponse()
                    .addHeader(CONTENT_TYPE, "application/json")
                    .setBody(result));
        }

        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                TrinoStatement trinoStatement = statement.unwrap(TrinoStatement.class);
                RecordingProgressMonitor progressMonitor = new RecordingProgressMonitor();
                trinoStatement.setProgressMonitor(progressMonitor);
                try (ResultSet rs = statement.executeQuery("bogus query for testing")) {
                    ResultSetMetaData metadata = rs.getMetaData();
                    assertThat(metadata.getColumnCount()).isEqualTo(1);
                    assertThat(metadata.getColumnName(1)).isEqualTo("_col0");

                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getLong(1)).isEqualTo(253161L);
                    assertThat(rs.getLong("_col0")).isEqualTo(253161L);

                    assertThat(rs.next()).isFalse();
                }
                trinoStatement.clearProgressMonitor();

                List<QueryStats> queryStatsList = progressMonitor.finish();
                assertThat(queryStatsList).hasSizeGreaterThanOrEqualTo(5); // duplicate stats is possible
                assertThat(queryStatsList.get(0).getState()).isEqualTo("QUEUED");
                assertThat(queryStatsList.get(queryStatsList.size() - 1).getState()).isEqualTo("FINISHED");
            }
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:trino://%s", server.url("/").uri().getAuthority());
        return DriverManager.getConnection(url, "test", null);
    }

    private static class RecordingProgressMonitor
            implements Consumer<QueryStats>
    {
        private final ImmutableList.Builder<QueryStats> builder = ImmutableList.builder();
        private boolean finished;

        @Override
        public synchronized void accept(QueryStats queryStats)
        {
            checkState(!finished);
            builder.add(queryStats);
        }

        public synchronized List<QueryStats> finish()
        {
            finished = true;
            return builder.build();
        }
    }
}
