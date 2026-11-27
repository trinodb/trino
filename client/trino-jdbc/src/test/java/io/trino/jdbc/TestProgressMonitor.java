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
import io.trino.client.Column;
import io.trino.client.QueryData;
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.jdbc.TestingJdbcUtils.column;
import static io.trino.jdbc.TestingJdbcUtils.createConnection;
import static io.trino.jdbc.TestingJdbcUtils.enqueueResults;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestProgressMonitor
{
    @StartStop
    private final MockWebServer server = new MockWebServer();

    private List<QueryResults> createResults()
    {
        List<Column> columns = ImmutableList.of(column("_col0", StandardTypes.BIGINT));
        return ImmutableList.<QueryResults>builder()
                .add(newQueryResults(null, 1, null, null, "QUEUED"))
                .add(newQueryResults(1, 2, columns, null, "RUNNING"))
                .add(newQueryResults(1, 3, columns, null, "RUNNING"))
                .add(newQueryResults(0, 4, columns, ImmutableList.of(ImmutableList.of(253161)), "RUNNING"))
                .add(newQueryResults(null, null, columns, null, "FINISHED"))
                .build();
    }

    private QueryResults newQueryResults(Integer partialCancelId, Integer nextUriId, List<Column> responseColumns, List<List<Object>> data, String state)
    {
        String queryId = "20160128_214710_00012_rk68b";
        return new QueryResults(
                queryId,
                server.url("/query.html?" + queryId).uri(),
                partialCancelId == null ? null : server.url(format("/v1/statement/partialCancel/%s.%s", queryId, partialCancelId)).uri(),
                nextUriId == null ? null : server.url(format("/v1/statement/%s/%s", queryId, nextUriId)).uri(),
                responseColumns,
                data == null ? QueryData.NULL : TypedQueryData.of(data),
                new StatementStats(state, state.equals("QUEUED"), true, OptionalDouble.of(0), OptionalDouble.of(0), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null),
                null,
                ImmutableList.of(),
                null,
                OptionalLong.empty());
    }

    @Test
    public void test()
            throws SQLException
    {
        for (QueryResults results : createResults()) {
            enqueueResults(server, results);
        }

        try (Connection connection = createConnection(server)) {
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
