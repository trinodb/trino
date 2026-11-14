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
import io.trino.client.Warning;
import io.trino.spi.type.StandardTypes;
import mockwebserver3.MockWebServer;
import mockwebserver3.junit5.StartStop;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static io.trino.jdbc.TestingJdbcUtils.column;
import static io.trino.jdbc.TestingJdbcUtils.createConnection;
import static io.trino.jdbc.TestingJdbcUtils.enqueueResults;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestWarningsBeforeColumns
{
    private static final String QUERY_ID = "20160128_214710_00012_rk68b";

    @StartStop
    private final MockWebServer server = new MockWebServer();

    @Test
    public void testWarningBeforeColumns()
            throws SQLException
    {
        List<Column> columns = ImmutableList.of(column("_col0", StandardTypes.BIGINT));
        Warning warning = new Warning(new Warning.Code(1, "QUEUED_WARNING"), "reported while queued");
        // the server resends the warning with every response, but only the first one comes before the columns
        enqueueResults(server, results(1, null, QueryData.NULL, ImmutableList.of(warning), "QUEUED"));
        enqueueResults(server, results(null, columns, TypedQueryData.of(ImmutableList.of(ImmutableList.of(1))), ImmutableList.of(warning), "FINISHED"));

        try (Connection connection = createConnection(server);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT 1")) {
            assertThat(warningMessages(statement.getWarnings())).containsExactly("reported while queued");

            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.next()).isFalse();
            assertThat(warningMessages(statement.getWarnings())).containsExactly("reported while queued");
        }
    }

    private static List<String> warningMessages(SQLWarning warning)
    {
        ImmutableList.Builder<String> messages = ImmutableList.builder();
        for (SQLWarning current = warning; current != null; current = current.getNextWarning()) {
            messages.add(current.getMessage());
        }
        return messages.build();
    }

    private QueryResults results(Integer nextUriId, List<Column> columns, QueryData data, List<Warning> warnings, String state)
    {
        return new QueryResults(
                QUERY_ID,
                server.url("/query.html?" + QUERY_ID).uri(),
                null,
                nextUriId == null ? null : server.url(format("/v1/statement/%s/%s", QUERY_ID, nextUriId)).uri(),
                columns,
                data,
                StatementStats.builder()
                        .setState(state)
                        .setQueued(state.equals("QUEUED"))
                        .setProgressPercentage(OptionalDouble.empty())
                        .setRunningPercentage(OptionalDouble.empty())
                        .build(),
                null,
                warnings,
                null,
                OptionalLong.empty());
    }
}
