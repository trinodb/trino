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
package io.trino.client;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.SocketEffect;
import mockwebserver3.junit5.StartStop;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.time.ZoneId;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.client.TrinoJsonCodec.jsonCodec;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestRetry
{
    @StartStop
    private final MockWebServer server = new MockWebServer();

    private static final TrinoJsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    @Test
    public void testRetryIncludeErrorOnRetryable()
    {
        java.time.Duration timeout = java.time.Duration.ofMillis(100);
        OkHttpClient httpClient = new OkHttpClient.Builder()
                .connectTimeout(timeout)
                .readTimeout(timeout)
                .writeTimeout(timeout)
                .callTimeout(timeout)
                .build();
        ClientSession session = ClientSession.builder()
                .server(URI.create("http://" + server.getHostName() + ":" + server.getPort()))
                .timeZone(ZoneId.of("UTC"))
                .source("test")
                .clientRequestTimeout(Duration.valueOf("2s"))
                .build();

        for (int i = 0; i < 100; i++) {
            server.enqueue(statusAndBody(HTTP_BAD_GATEWAY, "Failed to proxy request").build());
        }

        assertThatThrownBy(() -> newStatementClient(httpClient, session, "SELECT 1", Optional.empty()))
                .hasMessageContaining("Error fetching next")
                .hasRootCauseInstanceOf(ClientException.class)
                .hasRootCauseMessage("Expected http code 200 but got 502\n" +
                        "Response body was: Failed to proxy request");
    }

    @Test
    public void testRetryOnInitial()
    {
        java.time.Duration timeout = java.time.Duration.ofMillis(100);
        OkHttpClient httpClient = new OkHttpClient.Builder()
                .connectTimeout(timeout)
                .readTimeout(timeout)
                .writeTimeout(timeout)
                .callTimeout(timeout)
                .build();
        ClientSession session = ClientSession.builder()
                .server(URI.create("http://" + server.getHostName() + ":" + server.getPort()))
                .timeZone(ZoneId.of("UTC"))
                .source("test")
                .clientRequestTimeout(Duration.valueOf("2s"))
                .build();

        server.enqueue(statusAndBody(HTTP_OK, newQueryResults("RUNNING"))
                        .onRequestStart(SocketEffect.Stall.INSTANCE)
                        .build());
        server.enqueue(statusAndBody(HTTP_OK, newQueryResults("FINISHED")).build());

        try (StatementClient client = newStatementClient(httpClient, session, "SELECT 1", Optional.empty())) {
            while (client.advance()) {
                // consume all client data
            }
            assertThat(client.isFinished()).isTrue();
        }
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    public void testRetryOnBrokenStream()
    {
        java.time.Duration timeout = java.time.Duration.ofMillis(100);
        OkHttpClient httpClient = new OkHttpClient.Builder()
                .connectTimeout(timeout)
                .readTimeout(timeout)
                .writeTimeout(timeout)
                .callTimeout(timeout)
                .build();
        ClientSession session = ClientSession.builder()
                .server(URI.create("http://" + server.getHostName() + ":" + server.getPort()))
                .timeZone(ZoneId.of("UTC"))
                .source("test")
                .clientRequestTimeout(Duration.valueOf("2s"))
                .build();

        server.enqueue(statusAndBody(HTTP_OK, newQueryResults("RUNNING")).build());
        server.enqueue(statusAndBody(HTTP_OK, newQueryResults("FINISHED"))
                .onResponseBody(SocketEffect.Stall.INSTANCE)
                .build());
        server.enqueue(statusAndBody(HTTP_OK, newQueryResults("FINISHED")).build());

        try (StatementClient client = newStatementClient(httpClient, session, "SELECT 1", Optional.empty())) {
            while (client.advance()) {
                // consume all client data
            }
            assertThat(client.isFinished()).isTrue();
        }
        assertThat(server.getRequestCount()).isEqualTo(3);
    }

    private String newQueryResults(String state)
    {
        String queryId = "20160128_214710_00012_rk68b";
        int numRecords = 10;

        QueryResults queryResults = new QueryResults(
                queryId,
                server.url("/query.html?" + queryId).uri(),
                null,
                state.equals("RUNNING") ? server.url(format("/v1/statement/%s/%s", queryId, "aa")).uri() : null,
                Stream.of(new Column("id", INTEGER, new ClientTypeSignature("integer")),
                                new Column("name", VARCHAR, new ClientTypeSignature("varchar")))
                        .collect(toList()),
                TypedQueryData.of(IntStream.range(0, numRecords)
                        .mapToObj(index -> Stream.of((Object) index, "a").collect(toList()))
                        .collect(toList())),
                new StatementStats(state, state.equals("QUEUED"), true, OptionalDouble.of(0), OptionalDouble.of(0), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null),
                null,
                ImmutableList.of(),
                null,
                OptionalLong.empty());

        return QUERY_RESULTS_CODEC.toJson(queryResults);
    }

    private static MockResponse.Builder statusAndBody(int status, String body)
    {
        return new MockResponse.Builder()
                .code(status)
                .addHeader(CONTENT_TYPE, JSON_UTF_8)
                .body(body);
    }
}
