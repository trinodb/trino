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
package io.trino.server.protocol;

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.client.QueryDataJacksonModule;
import io.trino.client.QueryResults;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.util.Set;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Covers the pacing of {@code /v1/statement/executing} polls. The endpoint is a long poll bounded by
 * {@code ExecutingStatementResource.MAX_WAIT_TIME} so a client that follows {@code nextUri} as fast as the server
 * answers must never be able to issue substantially more requests than that bound allows.
 */
@TestInstance(PER_CLASS)
public class TestExecutingStatementLongPoll
{
    private static final HeaderName REQUEST_USER_HEADER = HeaderName.of(TRINO_HEADERS.requestUser());

    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider()
            .withModules(Set.of(new QueryDataJacksonModule()))
            .get();

    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = new JsonCodecFactory(JSON_MAPPER)
            .jsonCodec(QueryResults.class);

    // MAX_WAIT_TIME is one second. Allow a poll every 500ms to absorb scheduling jitter on a loaded machine, plus one
    // trailing poll for the response that ends the query.
    private static final long MIN_MILLIS_PER_POLL = 500;

    private HttpClient client;
    private TestingTrinoServer server;

    @BeforeAll
    public void setup()
    {
        client = new JettyHttpClient();
        server = TestingTrinoServer.create();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
    }

    @AfterAll
    public void teardown()
            throws Exception
    {
        closeAll(server, client);
        server = null;
        client = null;
    }

    @Test
    public void testPollsAreNotAnsweredImmediatelyAfterQueryCompletes()
    {
        PollStats stats = runQuery("SELECT count(*) FROM tpch.tiny.lineitem l JOIN tpch.tiny.orders o ON l.orderkey = o.orderkey");

        assertThat(stats.error()).isNull();
        assertThat(stats.pollsAfterTerminalState())
                .describedAs("polls issued after the query reached a terminal state, over %sms", stats.millisAfterTerminalState())
                .isLessThanOrEqualTo(maxPolls(stats.millisAfterTerminalState()));
    }

    private static int maxPolls(long windowMillis)
    {
        return (int) (2 + (windowMillis / MIN_MILLIS_PER_POLL));
    }

    private PollStats runQuery(String sql)
    {
        QueryResults results = client.execute(
                preparePost()
                        .setHeader(REQUEST_USER_HEADER, "user")
                        .setUri(uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build())
                        .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                        .build(),
                createJsonResponseHandler(QUERY_RESULTS_CODEC));

        int pollsAfterTerminalState = 0;
        long terminalStateReachedAt = 0;
        URI nextUri = results.getNextUri();
        while (nextUri != null) {
            if (terminalStateReachedAt == 0 && isTerminalState(results.getStats().getState())) {
                terminalStateReachedAt = System.nanoTime();
            }
            results = client.execute(
                    prepareGet()
                            .setHeader(REQUEST_USER_HEADER, "user")
                            .setUri(nextUri)
                            .build(),
                    createJsonResponseHandler(QUERY_RESULTS_CODEC));
            if (terminalStateReachedAt != 0) {
                pollsAfterTerminalState++;
            }
            nextUri = results.getNextUri();
        }

        long millisAfterTerminalState = 0;
        if (terminalStateReachedAt != 0) {
            millisAfterTerminalState = NANOSECONDS.toMillis(System.nanoTime() - terminalStateReachedAt);
        }
        return new PollStats(pollsAfterTerminalState, millisAfterTerminalState, results.getError() == null ? null : results.getError().getMessage());
    }

    private static boolean isTerminalState(String state)
    {
        return state.equals("FINISHED") || state.equals("FAILED");
    }

    private record PollStats(int pollsAfterTerminalState, long millisAfterTerminalState, String error) {}
}
