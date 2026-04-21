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
import io.airlift.json.JsonCodec;
import io.trino.client.uri.TrinoUri;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import mockwebserver3.junit5.StartStop;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.LOCATION;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.client.uri.HttpClientFactory.toHttpClientBuilder;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

final class TestClientRedirect
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    @StartStop
    private final MockWebServer proxyServer = new MockWebServer();

    @StartStop
    private final MockWebServer trinoServer = new MockWebServer();

    @BeforeEach
    void setup()
            throws IOException
    {
        proxyServer.enqueue(new MockResponse.Builder()
                .code(307)
                .addHeader(LOCATION, trinoServer.url("/v1/statement"))
                .build());
        trinoServer.enqueue(new MockResponse.Builder()
                .addHeader(CONTENT_TYPE, "application/json")
                .body(newQueryResults(proxyServer))
                .build());
    }

    @Test
    void testAccessToken()
            throws InterruptedException
    {
        String accessToken = "access_t0ken";
        TrinoUri trinoUri = TrinoUri.builder()
                .setUri(proxyServer.url("/").uri())
                .setAccessToken(accessToken)
                .setSsl(true)
                .build();

        try (StatementClient client = createStatementClient(proxyServer, trinoUri)) {
            while (client.advance()) {
                // consume all client data
            }
            assertThat(client.isFinished()).isTrue();
        }

        RecordedRequest redirectedRequest = trinoServer.takeRequest();
        assertThat(redirectedRequest.getHeaders().get(AUTHORIZATION)).isEqualTo("Bearer " + accessToken);
    }

    @Test
    void testMultipleRedirects()
            throws Exception
    {
        try (MockWebServer firstProxy = new MockWebServer()) {
            firstProxy.start();
            proxyServer.enqueue(new MockResponse.Builder()
                    .code(307)
                    .addHeader(LOCATION, proxyServer.url("/v1/statement"))
                    .build());
            String accessToken = "access_t0ken";
            TrinoUri trinoUri = TrinoUri.builder()
                    .setUri(firstProxy.url("/").uri())
                    .setAccessToken(accessToken)
                    .setSsl(true)
                    .build();

            try (StatementClient client = createStatementClient(proxyServer, trinoUri)) {
                while (client.advance()) {
                    // consume all client data
                }
                assertThat(client.isFinished()).isTrue();
            }

            RecordedRequest redirectedRequest = trinoServer.takeRequest();
            assertThat(redirectedRequest.getHeaders().get(AUTHORIZATION)).isEqualTo("Bearer " + accessToken);
        }
    }

    @Test
    void testBasicAuth()
            throws InterruptedException
    {
        String user = "alice";
        String password = "passw0rd";
        TrinoUri trinoUri = TrinoUri.builder()
                .setUri(proxyServer.url("/").uri())
                .setUser(user)
                .setPassword(password)
                .setSsl(true)
                .build();

        try (StatementClient client = createStatementClient(proxyServer, trinoUri)) {
            while (client.advance()) {
                // consume all client data
            }
            assertThat(client.isFinished()).isTrue();
        }

        RecordedRequest redirectedRequest = trinoServer.takeRequest();
        assertThat(redirectedRequest.getHeaders().get(AUTHORIZATION)).isEqualTo(Credentials.basic(user, password));
    }

    private String newQueryResults(MockWebServer server)
    {
        String queryId = "20160128_214710_00012_rk68b";
        int numRecords = 10;

        QueryResults queryResults = new QueryResults(
                queryId,
                server.url("/query.html?" + queryId).uri(),
                null,
                null,
                Stream.of(new Column("id", INTEGER, new ClientTypeSignature("integer")),
                                new Column("name", VARCHAR, new ClientTypeSignature("varchar")))
                        .collect(toList()),
                TypedQueryData.of(IntStream.range(0, numRecords)
                        .mapToObj(index -> Stream.of((Object) index, "a").collect(toList()))
                        .collect(toList())),
                StatementStats.builder()
                        .setState("FINISHED")
                        .setProgressPercentage(OptionalDouble.empty())
                        .setRunningPercentage(OptionalDouble.empty())
                        .build(),
                null,
                ImmutableList.of(),
                null,
                OptionalLong.empty());

        return QUERY_RESULTS_CODEC.toJson(queryResults);
    }

    private static StatementClient createStatementClient(MockWebServer server, TrinoUri trinoUri)
    {
        OkHttpClient httpClient = toHttpClientBuilder(trinoUri, "Trino JDBC Test Driver").build();
        ClientSession session = ClientSession.builder()
                .server(server.url("/").uri())
                .source("test")
                .timeZone(ZoneId.of("UTC"))
                .build();
        return newStatementClient(httpClient, session, "SELECT 1", Optional.empty());
    }
}
