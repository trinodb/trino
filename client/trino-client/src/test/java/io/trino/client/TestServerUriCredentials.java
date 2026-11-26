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
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.client.uri.TrinoUri;
import mockwebserver3.Dispatcher;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import mockwebserver3.junit5.StartStop;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.client.uri.HttpClientFactory.toHttpClientBuilder;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestServerUriCredentials
{
    private static final JsonCodec<Map<String, Object>> JSON_CODEC = mapJsonCodec(String.class, Object.class);
    private static final String ACCESS_TOKEN = "access_t0ken";
    private static final String QUERY_ID = "20160128_214710_00012_rk68b";
    private static final String NEXT_URI_PATH = "/v1/statement/executing/" + QUERY_ID + "/slug/1";
    private static final String TOKEN_PATH = "/oauth2/token/" + QUERY_ID;
    private static final String PARTIAL_CANCEL_PATH = "/v1/statement/executing/partialCancel/" + QUERY_ID + "/0/slug/1";

    @StartStop
    private final MockWebServer server = new MockWebServer();

    @Test
    @Timeout(10)
    void testAccessToken()
            throws InterruptedException
    {
        TrinoUri uri = TrinoUri.builder()
                .setUri(server.url("/").uri())
                .setAccessToken(ACCESS_TOKEN)
                .setSsl(true)
                .build();

        assertThat(authorizationByMethod(uri)).isEqualTo(ImmutableMap.of(
                "POST", Optional.of("Bearer " + ACCESS_TOKEN),
                "HEAD", Optional.empty(),
                "GET", Optional.empty(),
                "DELETE", Optional.empty()));
    }

    @Test
    @Timeout(10)
    void testBasicAuth()
            throws InterruptedException
    {
        TrinoUri uri = TrinoUri.builder()
                .setUri(server.url("/").uri())
                .setUser("alice")
                .setPassword("passw0rd")
                .setSsl(true)
                .build();

        assertThat(authorizationByMethod(uri)).isEqualTo(ImmutableMap.of(
                "POST", Optional.of(Credentials.basic("alice", "passw0rd")),
                "HEAD", Optional.empty(),
                "GET", Optional.empty(),
                "DELETE", Optional.empty()));
    }

    @Test
    @Timeout(10)
    void testNextUriChallengeIsNotAnswered()
            throws InterruptedException
    {
        TrinoUri uri = TrinoUri.builder()
                .setUri(server.url("/").uri())
                .setAccessToken(ACCESS_TOKEN)
                .setSsl(true)
                .build();
        server.setDispatcher(new StatementDispatcher(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader(WWW_AUTHENTICATE, format("Bearer x_token_server=\"%s\"", server.url("/token")))
                .build()));

        try (StatementClient client = createStatementClient(uri)) {
            assertThatThrownBy(client::advance)
                    .isInstanceOf(ClientException.class)
                    .hasMessageStartingWith("Authentication failed");
        }

        assertThat(server.takeRequest().getMethod()).isEqualTo("POST");
        RecordedRequest nextUriRequest = server.takeRequest();
        assertThat(nextUriRequest.getMethod()).isEqualTo("GET");
        assertThat(nextUriRequest.getHeaders().get(AUTHORIZATION)).isNull();
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    @Timeout(10)
    void testPartialCancel()
            throws InterruptedException
    {
        TrinoUri uri = TrinoUri.builder()
                .setUri(server.url("/").uri())
                .setAccessToken(ACCESS_TOKEN)
                .setSsl(true)
                .build();
        server.setDispatcher(new StatementDispatcher(queryResults()));

        try (StatementClient client = createStatementClient(uri)) {
            client.cancelLeafStage();
        }

        assertThat(server.takeRequest().getHeaders().get(AUTHORIZATION)).isEqualTo("Bearer " + ACCESS_TOKEN);
        RecordedRequest partialCancelRequest = server.takeRequest();
        assertThat(partialCancelRequest.getUrl().encodedPath()).isEqualTo(PARTIAL_CANCEL_PATH);
        assertThat(partialCancelRequest.getHeaders().get(AUTHORIZATION)).isNull();
    }

    @Test
    @Timeout(10)
    void testTokenPolling()
            throws InterruptedException
    {
        TrinoUri uri = TrinoUri.builder()
                .setUri(server.url("/").uri())
                .setUser("alice")
                .setPassword("passw0rd")
                .setAccessToken(ACCESS_TOKEN)
                .setSsl(true)
                .build();
        server.setDispatcher(new Dispatcher()
        {
            @Override
            public MockResponse dispatch(RecordedRequest request)
            {
                if (request.getUrl().encodedPath().startsWith(TOKEN_PATH)) {
                    return json(ImmutableMap.of("token", "refreshed_t0ken"));
                }
                if (!request.getMethod().equals("POST")) {
                    return new MockResponse.Builder()
                            .code(HTTP_NO_CONTENT)
                            .build();
                }
                if ("Bearer refreshed_t0ken".equals(request.getHeaders().get(AUTHORIZATION))) {
                    return queryResults();
                }
                return new MockResponse.Builder()
                        .code(HTTP_UNAUTHORIZED)
                        .addHeader(WWW_AUTHENTICATE, format("Bearer x_token_server=\"%s\"", server.url(TOKEN_PATH)))
                        .build();
            }
        });

        try (StatementClient client = createStatementClient(uri)) {
            assertThat(client.isRunning()).isTrue();
        }

        RecordedRequest tokenRequest = null;
        for (int i = 0; i < server.getRequestCount(); i++) {
            RecordedRequest request = server.takeRequest();
            if (request.getUrl().encodedPath().equals(TOKEN_PATH) && request.getMethod().equals("GET")) {
                tokenRequest = request;
            }
        }
        assertThat(tokenRequest).isNotNull();
        assertThat(tokenRequest.getHeaders().get(AUTHORIZATION)).isNull();
    }

    private Map<String, Optional<String>> authorizationByMethod(TrinoUri uri)
            throws InterruptedException
    {
        server.setDispatcher(new StatementDispatcher(queryResults()));

        try (StatementClient client = createStatementClient(uri)) {
            // reading rows sends a heartbeat, because the heartbeat interval is zero
            client.currentRows().forEach(row -> {});
            assertThat(client.advance()).isTrue();
        }

        Map<String, Optional<String>> authorizationByMethod = new HashMap<>();
        for (int i = 0; i < 4; i++) {
            RecordedRequest request = server.takeRequest(5, SECONDS);
            assertThat(request).isNotNull();
            authorizationByMethod.put(request.getMethod(), Optional.ofNullable(request.getHeaders().get(AUTHORIZATION)));
        }
        return authorizationByMethod;
    }

    private StatementClient createStatementClient(TrinoUri uri)
    {
        OkHttpClient httpClient = toHttpClientBuilder(uri, "test").build();
        ClientSession session = ClientSession.builder()
                .server(server.url("/").uri())
                .source("test")
                .timeZone(ZoneId.of("UTC"))
                .heartbeatInterval(new Duration(0, MILLISECONDS))
                .build();
        return newStatementClient(httpClient, session, "SELECT 1", Optional.empty());
    }

    private MockResponse queryResults()
    {
        // plain maps, because the airlift JSON codec doesn't serialize QueryData
        Map<String, Object> queryResults = ImmutableMap.of(
                "id", QUERY_ID,
                "infoUri", server.url("/query.html?" + QUERY_ID).toString(),
                "nextUri", server.url(NEXT_URI_PATH).toString(),
                "partialCancelUri", server.url(PARTIAL_CANCEL_PATH).toString(),
                "columns", ImmutableList.of(ImmutableMap.of(
                        "name", "id",
                        "type", "integer",
                        "typeSignature", ImmutableMap.of("rawType", "integer", "arguments", ImmutableList.of()))),
                "data", ImmutableList.of(ImmutableList.of(1)),
                "stats", ImmutableMap.of("state", "RUNNING"));
        return json(queryResults);
    }

    private static MockResponse json(Map<String, Object> value)
    {
        return new MockResponse.Builder()
                .addHeader(CONTENT_TYPE, "application/json")
                .body(JSON_CODEC.toJson(value))
                .build();
    }

    // Responds by method, because heartbeats are sent asynchronously and can arrive in any order
    private class StatementDispatcher
            extends Dispatcher
    {
        private final MockResponse nextUriResponse;

        public StatementDispatcher(MockResponse nextUriResponse)
        {
            this.nextUriResponse = nextUriResponse;
        }

        @Override
        public MockResponse dispatch(RecordedRequest request)
        {
            switch (request.getMethod()) {
                case "POST":
                    return queryResults();
                case "GET":
                    return nextUriResponse;
                case "HEAD":
                    return new MockResponse.Builder().build();
                default:
                    return new MockResponse.Builder()
                            .code(HTTP_NO_CONTENT)
                            .build();
            }
        }
    }
}
