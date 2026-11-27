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
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.trino.client.uri.TrinoUri;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.junit5.StartStop;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.client.uri.HttpClientFactory.toHttpClientBuilder;
import static io.trino.client.uri.HttpClientFactory.unauthenticatedClientBuilder;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHostRestriction
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private static final char[] KEY_STORE_PASSWORD = "Pass1234".toCharArray();
    private static final String ACCESS_TOKEN = "access_t0ken";
    private static final String SERVER_HOST = "trino.test";
    private static final String OTHER_HOST = "other.test";
    private static final String STORAGE_HOST = "bucket.s3.us-east-1.amazonaws.com";

    @StartStop
    private final MockWebServer server = new MockWebServer();

    @StartStop
    private final MockWebServer otherServer = new MockWebServer();

    @BeforeEach
    void setupTls()
            throws Exception
    {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream in = Resources.getResource("certs/certs.jks").openStream()) {
            keyStore.load(in, KEY_STORE_PASSWORD);
        }
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, KEY_STORE_PASSWORD);
        SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(keyManagerFactory.getKeyManagers(), null, new SecureRandom());
        server.useHttps(serverContext.getSocketFactory());
        otherServer.useHttps(serverContext.getSocketFactory());
    }

    @Test
    void testQueryOnServerHost()
    {
        server.enqueue(queryResults(Optional.of(url(server, SERVER_HOST, "/v1/statement/executing/1"))));
        server.enqueue(queryResults(Optional.empty()));

        try (StatementClient client = createStatementClient(restrictedUri().build())) {
            consumeAll(client);
            assertThat(client.isFinished()).isTrue();
        }
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    void testRedirectToOtherHostIsRejected()
    {
        server.enqueue(new MockResponse.Builder()
                .code(307)
                .addHeader(LOCATION, url(otherServer, OTHER_HOST, "/v1/statement"))
                .build());

        assertThatThrownBy(() -> createStatementClient(restrictedUri().build()))
                .hasMessageContaining("Connecting to host 'other.test' is not allowed");
        assertThat(otherServer.getRequestCount()).isZero();
    }

    @Test
    void testRedirectToAllowedHostIsFollowed()
            throws InterruptedException
    {
        server.enqueue(new MockResponse.Builder()
                .code(307)
                .addHeader(LOCATION, url(otherServer, OTHER_HOST, "/v1/statement"))
                .build());
        otherServer.enqueue(queryResults(Optional.empty()));

        TrinoUri uri = restrictedUri()
                .setAllowedHosts(ImmutableSet.of("*.test"))
                .build();
        try (StatementClient client = createStatementClient(uri)) {
            consumeAll(client);
            assertThat(client.isFinished()).isTrue();
        }

        assertThat(otherServer.takeRequest().getHeaders().get(AUTHORIZATION)).isEqualTo("Bearer " + ACCESS_TOKEN);
    }

    @Test
    @Timeout(10)
    void testNextUriOnOtherHostIsRejected()
    {
        server.enqueue(queryResults(Optional.of(url(otherServer, OTHER_HOST, "/v1/statement/executing/1"))));

        try (StatementClient client = createStatementClient(restrictedUri().build())) {
            assertThatThrownBy(client::advance)
                    .hasMessageContaining("Connecting to host 'other.test' is not allowed");
            assertThat(client.isClientError()).isTrue();
        }
        assertThat(server.getRequestCount()).isEqualTo(1);
        assertThat(otherServer.getRequestCount()).isZero();
    }

    @Test
    @Timeout(10)
    void testNextUriWithoutTlsIsRejected()
    {
        HttpUrl nextUri = url(server, SERVER_HOST, "/v1/statement/executing/1").newBuilder()
                .scheme("http")
                .build();
        server.enqueue(queryResults(Optional.of(nextUri)));

        try (StatementClient client = createStatementClient(restrictedUri().build())) {
            assertThatThrownBy(client::advance)
                    .hasMessageContaining("Connecting to 'trino.test' without TLS is not allowed");
        }
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void testTokenEndpointIsAllowedOnlyForTokenRequests()
            throws InterruptedException
    {
        HttpUrl tokenEndpoint = url(otherServer, OTHER_HOST, "/token");
        server.enqueue(new MockResponse.Builder()
                .code(HTTP_UNAUTHORIZED)
                .addHeader(WWW_AUTHENTICATE, format("Bearer x_token_endpoint=\"%s\", scope=\"openid\"", tokenEndpoint))
                .build());
        otherServer.enqueue(new MockResponse.Builder()
                .addHeader(CONTENT_TYPE, "application/json")
                .body(format("{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":3600}", ACCESS_TOKEN))
                .build());
        server.enqueue(queryResults(Optional.of(url(otherServer, OTHER_HOST, "/v1/statement/executing/1"))));

        TrinoUri uri = baseUri()
                .setOauth2ClientId("client")
                .setOauth2ClientSecret("secret")
                .setOauth2TokenEndpoint(tokenEndpoint.toString())
                .build();
        try (StatementClient client = createStatementClient(uri)) {
            assertThatThrownBy(client::advance)
                    .hasMessageContaining("Connecting to host 'other.test' is not allowed");
        }

        assertThat(otherServer.getRequestCount()).isEqualTo(1);
        assertThat(otherServer.takeRequest().getUrl().encodedPath()).isEqualTo("/token");
        server.takeRequest();
        assertThat(server.takeRequest().getHeaders().get(AUTHORIZATION)).isEqualTo("Bearer " + ACCESS_TOKEN);
    }

    @Test
    void testSegmentClientAllowsObjectStorage()
            throws IOException
    {
        otherServer.enqueue(segment());
        OkHttpClient client = unauthenticatedClientBuilder(restrictedUri().build(), "test").build();

        try (Response response = client.newCall(request(url(otherServer, STORAGE_HOST, "/segment"))).execute()) {
            assertThat(response.body().string()).isEqualTo("segment");
        }
        assertThatThrownBy(() -> client.newCall(request(url(otherServer, OTHER_HOST, "/segment"))).execute())
                .isInstanceOf(HostNotAllowedException.class);
    }

    @Test
    void testSegmentClientAllowsAllowedHosts()
            throws IOException
    {
        otherServer.enqueue(segment());
        TrinoUri uri = restrictedUri()
                .setAllowedHosts(ImmutableSet.of(OTHER_HOST))
                .build();
        OkHttpClient client = unauthenticatedClientBuilder(uri, "test").build();

        try (Response response = client.newCall(request(url(otherServer, OTHER_HOST, "/segment"))).execute()) {
            assertThat(response.body().string()).isEqualTo("segment");
        }
    }

    @Test
    void testAuthenticatedClientRejectsObjectStorage()
    {
        OkHttpClient client = toHttpClientBuilder(restrictedUri().build(), "test").build();

        assertThatThrownBy(() -> client.newCall(request(url(otherServer, STORAGE_HOST, "/v1/statement"))).execute())
                .isInstanceOf(HostNotAllowedException.class);
        assertThat(otherServer.getRequestCount()).isZero();
    }

    private TrinoUri.Builder restrictedUri()
    {
        return baseUri()
                .setAccessToken(ACCESS_TOKEN);
    }

    private TrinoUri.Builder baseUri()
    {
        return TrinoUri.builder()
                .setUri(url(server, SERVER_HOST, "/").uri())
                .setSsl(true)
                .setSslVerificationNone()
                .setDnsResolver(LocalhostDnsResolver.class)
                .setRestrictHosts(true);
    }

    private StatementClient createStatementClient(TrinoUri uri)
    {
        OkHttpClient httpClient = toHttpClientBuilder(uri, "test").build();
        ClientSession session = ClientSession.builder()
                .server(url(server, SERVER_HOST, "/").uri())
                .source("test")
                .timeZone(ZoneId.of("UTC"))
                .build();
        return newStatementClient(httpClient, session, "SELECT 1", Optional.empty());
    }

    private static void consumeAll(StatementClient client)
    {
        while (client.advance()) {
            // consume all client data
        }
    }

    private MockResponse queryResults(Optional<HttpUrl> nextUri)
    {
        String queryId = "20160128_214710_00012_rk68b";
        QueryResults queryResults = new QueryResults(
                queryId,
                url(server, SERVER_HOST, "/query.html?" + queryId).uri(),
                null,
                nextUri.map(HttpUrl::uri).orElse(null),
                ImmutableList.of(new Column("id", INTEGER, new ClientTypeSignature("integer"))),
                TypedQueryData.of(ImmutableList.of(ImmutableList.of(1))),
                StatementStats.builder()
                        .setState(nextUri.isPresent() ? "RUNNING" : "FINISHED")
                        .setProgressPercentage(OptionalDouble.empty())
                        .setRunningPercentage(OptionalDouble.empty())
                        .build(),
                null,
                ImmutableList.of(),
                null,
                OptionalLong.empty());
        return new MockResponse.Builder()
                .addHeader(CONTENT_TYPE, "application/json")
                .body(QUERY_RESULTS_CODEC.toJson(queryResults))
                .build();
    }

    private static MockResponse segment()
    {
        return new MockResponse.Builder()
                .body("segment")
                .build();
    }

    private static HttpUrl url(MockWebServer server, String host, String path)
    {
        return server.url(path).newBuilder()
                .host(host)
                .build();
    }

    private static Request request(HttpUrl url)
    {
        return new Request.Builder()
                .url(url)
                .build();
    }

    public static final class LocalhostDnsResolver
            implements DnsResolver
    {
        public LocalhostDnsResolver(String context) {}

        @Override
        public List<InetAddress> lookup(String hostname)
                throws UnknownHostException
        {
            return ImmutableList.copyOf(InetAddress.getAllByName("localhost"));
        }
    }
}
