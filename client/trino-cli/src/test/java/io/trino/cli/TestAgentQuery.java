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
import io.trino.client.ClientSession;
import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import io.trino.client.ErrorLocation;
import io.trino.client.QueryError;
import io.trino.client.QueryResults;
import io.trino.client.StatementStats;
import io.trino.client.TrinoJsonCodec;
import io.trino.client.TypedQueryData;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.junit5.StartStop;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.trino.cli.ClientOptions.OutputFormat.AGENT;
import static io.trino.cli.TerminalUtils.getTerminal;
import static io.trino.cli.TestQueryRunner.createClientSession;
import static io.trino.cli.TestQueryRunner.createTrinoUri;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.VARCHAR;
import static io.trino.client.TrinoJsonCodec.jsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestAgentQuery
{
    private static final TrinoJsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    @StartStop
    private final MockWebServer server = new MockWebServer();

    @Test
    public void testResult()
    {
        server.enqueue(jsonResponse(results(3, false)));

        Output output = render(new AgentLimits(1000, 10_000, 500));

        assertThat(output.success()).isTrue();
        assertThat(output.out()).isEqualTo("|_col0:bigint|\n|---|\n|0|\n|1|\n|2|\n\n");
        assertThat(output.err()).isEmpty();
    }

    @Test
    public void testQueryPlanIsPrintedAsText()
    {
        String plan = "Fragment 0 [SINGLE]\n    Output layout: [name]\n        - TableScan[nation]\n" + "x".repeat(600) + "\n";
        server.enqueue(jsonResponse(textResult(plan)));

        Output output = render(AgentLimits.DEFAULT);

        // a plan is neither put in a table nor cut at the width of a cell
        assertThat(output.success()).isTrue();
        assertThat(output.out()).isEqualTo(plan + "\n");
        assertThat(output.err()).isEmpty();
    }

    @Test
    public void testResultOverLimit()
    {
        server.enqueue(jsonResponse(results(50, false)));

        Output output = render(new AgentLimits(30, 0, 0));

        assertThat(output.success()).isTrue();
        assertThat(output.out()).contains("|14|\n…(+20 rows)\n|35|\n");
        assertThat(output.out()).contains("(50 rows, showing the first 15 and last 15, --agent-max-rows 0 for all, hash: ");
        assertThat(output.err()).isEmpty();
    }

    @Test
    public void testQueryIsCancelledAfterManyRows()
            throws Exception
    {
        server.enqueue(jsonResponse(results(10 + 1 + AgentTablePrinter.EARLY_STOP_ROWS + 5000, true)));
        server.enqueue(new MockResponse.Builder().code(204).build());

        Output output = render(new AgentLimits(10, 0, 0));

        assertThat(output.success()).isTrue();
        assertThat(output.out()).contains("|4|\n…(+");
        assertThat(output.out()).contains("(at least 100011 rows, showing the first 5, query stopped early, --agent-max-rows 0 for all)");
        assertThat(output.err()).isEmpty();
        assertThat(waitForNoRowReaderThread()).as("thread reading rows is still running").isTrue();
    }

    @Test
    public void testFailureAfterEarlyStopIsReported()
    {
        // The rows after the last full buffer are only printed once the reader has ended. The queue is big
        // enough for the whole first page, so the reader has read the failure page by then, and the printer
        // stops on a query that has already failed. This depends on the buffer size of the output handler.
        server.enqueue(jsonResponse(results(10 + 1 + AgentTablePrinter.EARLY_STOP_ROWS, true)));
        server.enqueue(jsonResponse(failure()));

        Output output = render(new AgentLimits(10, 0, 0), 200_000);

        assertThat(output.success()).isFalse();
        assertThat(output.err()).contains("\"error_name\":\"TABLE_NOT_FOUND\"");
    }

    @Test
    public void testFailureAfterRowsIsNotACompleteResult()
    {
        server.enqueue(jsonResponse(results(10, true)));
        server.enqueue(jsonResponse(failure()));

        Output output = render(AgentLimits.DEFAULT);

        assertThat(output.success()).isFalse();
        assertThat(output.out()).doesNotContain("hash");
        assertThat(output.out()).endsWith("|9|\n\n(incomplete: 10 rows received before the query ended)\n");
        assertThat(output.err()).contains("\"error_name\":\"TABLE_NOT_FOUND\"");
    }

    @Test
    public void testHttpErrorAfterRowsIsNotACompleteResult()
    {
        server.enqueue(jsonResponse(results(10, true)));
        server.enqueue(new MockResponse.Builder().code(400).build());

        Output output = render(AgentLimits.DEFAULT);

        assertThat(output.failure()).isNotNull();
        assertThat(output.out()).doesNotContain("hash");
        assertThat(output.out()).endsWith("|9|\n\n(incomplete: 10 rows received before the query ended)\n");
    }

    @Test
    public void testHttpErrorBeforeEarlyStopIsReported()
    {
        // the queue holds the whole first page, so the client has failed on the next page when the printer stops
        server.enqueue(jsonResponse(results(10 + 1 + AgentTablePrinter.EARLY_STOP_ROWS, true)));
        server.enqueue(new MockResponse.Builder().code(400).build());

        Output output = render(new AgentLimits(10, 0, 0), 200_000);

        assertThat(output.success()).isFalse();
        assertThat(output.failure()).isNotNull();
    }

    @Test
    public void testQueryInTransactionIsNotCancelled()
    {
        server.enqueue(jsonResponse(results(10 + 1 + AgentTablePrinter.EARLY_STOP_ROWS + 50, true)));
        server.enqueue(jsonResponse(results(0, false)));
        ClientSession session = ClientSession.builder(createClientSession(server))
                .transactionId("transaction")
                .build();

        Output output = render(new AgentLimits(10, 0, 0), 200_000, Optional.of(""), session);

        assertThat(output.success()).isTrue();
        assertThat(output.out()).doesNotContain("stopped early");
        assertThat(output.out()).contains("(100061 rows, showing the first 5 and last 5, --agent-max-rows 0 for all, hash: ");
        assertThat(output.err()).isEmpty();
        // the statement and the next page, and no request that cancels the query
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    public void testQueryIsCancelledWithPager()
    {
        server.enqueue(jsonResponse(results(10 + 1 + AgentTablePrinter.EARLY_STOP_ROWS + 5000, true)));
        server.enqueue(new MockResponse.Builder().code(204).build());

        Output output = render(new AgentLimits(10, 0, 0), 1000, Optional.of("cat"), createClientSession(server));

        // the rows go to the pager, which must not turn stopping early into an abort by the user
        assertThat(output.success()).isTrue();
        assertThat(output.err()).isEmpty();
    }

    // the thread that reads rows for OutputHandler must not outlive the query
    private static boolean waitForNoRowReaderThread()
            throws InterruptedException
    {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < deadline) {
            boolean running = Thread.getAllStackTraces().values().stream()
                    .anyMatch(stack -> Stream.of(stack).anyMatch(frame -> frame.getClassName().startsWith(OutputHandler.class.getName())));
            if (!running) {
                return true;
            }
            Thread.sleep(50);
        }
        return false;
    }

    @Test
    public void testFailedQuery()
    {
        server.enqueue(jsonResponse(failure()));

        Output output = render(AgentLimits.DEFAULT);

        assertThat(output.success()).isFalse();
        assertThat(output.out()).isEmpty();
        assertThat(output.err()).isEqualTo("" +
                "{\"type\":\"error\",\"query_id\":\"20160128_214710_00012_rk68b\",\"error_name\":\"TABLE_NOT_FOUND\"," +
                "\"error_type\":\"USER_ERROR\",\"error_code\":46,\"sql_state\":\"42000\"," +
                "\"message\":\"line 1:15: Table 'catalog.schema.t' does not exist\",\"location\":{\"line\":1,\"column\":15}}\n");
    }

    private Output render(AgentLimits limits)
    {
        return render(limits, 1000);
    }

    private Output render(AgentLimits limits, int maxQueuedRows)
    {
        return render(limits, maxQueuedRows, Optional.of(""), createClientSession(server));
    }

    private Output render(AgentLimits limits, int maxQueuedRows, Optional<String> pager, ClientSession session)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        QueryRunner queryRunner = new QueryRunner(
                Theme.DARK,
                createTrinoUri(server, false),
                session,
                false,
                maxQueuedRows,
                500,
                limits,
                true);
        boolean success = false;
        RuntimeException failure = null;
        try (Query query = queryRunner.startQuery("select * from t")) {
            success = query.renderOutput(getTerminal(), new PrintStream(out), new PrintStream(err), AGENT, pager, false, false);
        }
        catch (RuntimeException e) {
            failure = e;
        }
        return new Output(success, out.toString(UTF_8), err.toString(UTF_8), failure);
    }

    private MockResponse jsonResponse(QueryResults results)
    {
        return new MockResponse.Builder()
                .addHeader(CONTENT_TYPE, "application/json")
                .body(QUERY_RESULTS_CODEC.toJson(results))
                .build();
    }

    private QueryResults failure()
    {
        return new QueryResults(
                "20160128_214710_00012_rk68b",
                server.url("/query.html?20160128_214710_00012_rk68b").uri(),
                null,
                null,
                null,
                null,
                StatementStats.builder()
                        .setState("FAILED")
                        .setProgressPercentage(OptionalDouble.empty())
                        .setRunningPercentage(OptionalDouble.empty())
                        .build(),
                new QueryError("line 1:15: Table 'catalog.schema.t' does not exist", "42000", 46, "TABLE_NOT_FOUND", "USER_ERROR", new ErrorLocation(1, 15), null),
                ImmutableList.of(),
                null,
                OptionalLong.empty());
    }

    private QueryResults textResult(String value)
    {
        return new QueryResults(
                "20160128_214710_00012_rk68b",
                server.url("/query.html?20160128_214710_00012_rk68b").uri(),
                null,
                null,
                ImmutableList.of(new Column("Query Plan", VARCHAR, new ClientTypeSignature(VARCHAR))),
                TypedQueryData.of(ImmutableList.of(ImmutableList.of(value))),
                StatementStats.builder()
                        .setState("FINISHED")
                        .setProgressPercentage(OptionalDouble.empty())
                        .setRunningPercentage(OptionalDouble.empty())
                        .build(),
                null,
                ImmutableList.of(),
                null,
                OptionalLong.empty());
    }

    private QueryResults results(int rows, boolean hasNext)
    {
        List<List<Object>> data = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            data.add(ImmutableList.of((long) i));
        }
        return new QueryResults(
                "20160128_214710_00012_rk68b",
                server.url("/query.html?20160128_214710_00012_rk68b").uri(),
                null,
                hasNext ? server.url("/v1/statement/executing/20160128_214710_00012_rk68b/y1/1").uri() : null,
                ImmutableList.of(new Column("_col0", BIGINT, new ClientTypeSignature(BIGINT))),
                TypedQueryData.of(data),
                StatementStats.builder()
                        .setState(hasNext ? "RUNNING" : "FINISHED")
                        .setProgressPercentage(OptionalDouble.empty())
                        .setRunningPercentage(OptionalDouble.empty())
                        .build(),
                null,
                ImmutableList.of(),
                null,
                OptionalLong.empty());
    }

    private static class Output
    {
        private final boolean success;
        private final String out;
        private final String err;
        private final RuntimeException failure;

        Output(boolean success, String out, String err, RuntimeException failure)
        {
            this.success = success;
            this.out = out;
            this.err = err;
            this.failure = failure;
        }

        // thrown by rendering the output, null if there was none
        RuntimeException failure()
        {
            return failure;
        }

        boolean success()
        {
            return success;
        }

        String out()
        {
            return out;
        }

        String err()
        {
            return err;
        }
    }
}
