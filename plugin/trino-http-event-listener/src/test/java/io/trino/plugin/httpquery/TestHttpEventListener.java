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
package io.trino.plugin.httpquery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.operator.RetryPolicy;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.eventlistener.SplitStatistics;
import io.trino.spi.eventlistener.StageOutputBufferUtilization;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.ResourceEstimates;
import okhttp3.TlsVersion;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.KeyStore;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.lang.String.format;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@SuppressWarnings("FieldNamingConvention")
@TestInstance(PER_METHOD)
class TestHttpEventListener
{
    private MockWebServer server;

    private final EventListenerFactory factory = new HttpEventListenerFactory();

    private static final JsonCodec<QueryCompletedEvent> queryCompleteEventJsonCodec = jsonCodec(QueryCompletedEvent.class);
    private static final JsonCodec<QueryCreatedEvent> queryCreateEventJsonCodec = jsonCodec(QueryCreatedEvent.class);
    private static final JsonCodec<SplitCompletedEvent> splitCompleteEventJsonCodec = jsonCodec(SplitCompletedEvent.class);

    private static final QueryIOMetadata queryIOMetadata;
    private static final QueryContext queryContext;
    private static final QueryMetadata queryMetadata;
    private static final SplitStatistics splitStatistics;
    private static final QueryStatistics queryStatistics;
    private static final SplitCompletedEvent splitCompleteEvent;
    private static final QueryCreatedEvent queryCreatedEvent;
    private static final QueryCompletedEvent queryCompleteEvent;

    private static final String queryCreatedEventJson;
    private static final String queryCompleteEventJson;
    private static final String splitCompleteEventJson;

    static {
        queryIOMetadata = new QueryIOMetadata(Collections.emptyList(), Optional.empty());

        queryContext = new QueryContext(
                "user",
                "originalUser",
                Optional.of("principal"),
                Set.of(), // enabledRoles
                Set.of(), // groups
                Optional.empty(), // traceToken
                Optional.empty(), // remoteClientAddress
                Optional.empty(), // userAgent
                Optional.empty(), // clientInfo
                new HashSet<>(), // clientTags
                new HashSet<>(), // clientCapabilities
                Optional.of("source"),
                UTC_KEY.getId(),
                Optional.of("catalog"),
                Optional.of("schema"),
                Optional.of(new ResourceGroupId("name")),
                new HashMap<>(), // sessionProperties
                new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.of(1000L)),
                "serverAddress", "serverVersion", "environment",
                Optional.of(QueryType.SELECT),
                RetryPolicy.QUERY.toString());

        queryMetadata = new QueryMetadata(
                "queryId",
                Optional.empty(),
                "query",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "queryState",
                List.of(),
                List.of(),
                URI.create("http://localhost"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        splitStatistics = new SplitStatistics(
                ofMillis(1000),
                ofMillis(2000),
                ofMillis(3000),
                ofMillis(4000),
                1,
                2,
                Optional.of(ofMillis(100)),
                Optional.of(ofMillis(200)));

        queryStatistics = new QueryStatistics(
                ofSeconds(1),
                ofSeconds(1),
                ofSeconds(1),
                ofSeconds(1),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0.0f,
                Collections.emptyList(),
                0,
                true,
                Collections.emptyList(),
                List.of(new StageOutputBufferUtilization(0, 10, 0.1, 0.5, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.0, 1.0, ofSeconds(1234))),
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.empty());

        splitCompleteEvent = new SplitCompletedEvent(
                "queryId",
                "stageId",
                "taskId",
                Optional.of("catalogName"),
                Instant.now(),
                Optional.of(Instant.now()),
                Optional.of(Instant.now()),
                splitStatistics,
                Optional.empty(),
                "payload");

        queryCreatedEvent = new QueryCreatedEvent(
                Instant.now(),
                queryContext,
                queryMetadata);

        queryCompleteEvent = new QueryCompletedEvent(
                queryMetadata,
                queryStatistics,
                queryContext,
                queryIOMetadata,
                Optional.empty(),
                Collections.emptyList(),
                Instant.now(),
                Instant.now(),
                Instant.now());

        queryCompleteEventJson = queryCompleteEventJsonCodec.toJson(queryCompleteEvent);
        queryCreatedEventJson = queryCreateEventJsonCodec.toJson(queryCreatedEvent);
        splitCompleteEventJson = splitCompleteEventJsonCodec.toJson(splitCompleteEvent);
    }

    @BeforeEach
    void setup()
            throws IOException
    {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void teardown()
    {
        try {
            server.close();
        }
        catch (IOException ignored) {
            // MockWebServer.close() method sometimes throws 'Gave up waiting for executor to shut down'
        }
        server = null;
    }

    /**
     * Listener created without exceptions but not requests sent
     */
    @Test
    void testAllLoggingDisabledShouldTimeout()
            throws Exception
    {
        server.enqueue(new MockResponse()
                .setResponseCode(200));

        EventListener eventListener = createEventListener(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString()));

        eventListener.queryCreated(null);
        eventListener.queryCompleted(null);
        eventListener.splitCompleted(null);

        assertThat(server.takeRequest(5, TimeUnit.SECONDS)).isNull();
    }

    @Test
    void testAllLoggingEnabledShouldSendCorrectEvent()
            throws Exception
    {
        EventListener eventListener = createEventListener(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.log-created", "true",
                "http-event-listener.log-split", "true"));

        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));

        eventListener.queryCreated(queryCreatedEvent);
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), queryCreatedEventJson);

        eventListener.queryCompleted(queryCompleteEvent);
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), queryCompleteEventJson);

        eventListener.splitCompleted(splitCompleteEvent);
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), splitCompleteEventJson);
    }

    @Test
    void testContentTypeDefaultHeaderShouldAlwaysBeSet()
            throws Exception
    {
        EventListener eventListener = createEventListener(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true"));

        server.enqueue(new MockResponse().setResponseCode(200));

        eventListener.queryCompleted(queryCompleteEvent);

        assertThat(server.takeRequest(5, TimeUnit.SECONDS))
                .extracting(request -> request.getHeader("Content-Type"))
                .isEqualTo("application/json; charset=utf-8");
    }

    @Test
    void testHttpHeadersShouldBePresent()
            throws Exception
    {
        EventListener eventListener = createEventListener(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.connect-http-headers", "Authorization: Trust Me!, Cache-Control: no-cache"));

        server.enqueue(new MockResponse().setResponseCode(200));

        eventListener.queryCompleted(queryCompleteEvent);

        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), Map.of(
                "Authorization", "Trust Me!",
                "Cache-Control", "no-cache"), queryCompleteEventJson);
    }

    @Test
    void testHttpsEnabledShouldUseTLSv13()
            throws Exception
    {
        setupServerTLSCertificate();
        server.enqueue(new MockResponse().setResponseCode(200));

        EventListener eventListener = createEventListener(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.http-client.key-store-path", "src/test/resources/trino-httpquery-test.p12",
                "http-event-listener.http-client.key-store-password", "testing-ssl"));
        eventListener.queryCompleted(queryCompleteEvent);

        RecordedRequest recordedRequest = server.takeRequest(5, TimeUnit.SECONDS);

        assertThat(recordedRequest)
                .as("Handshake probably failed")
                .extracting(RecordedRequest::getTlsVersion)
                .extracting(TlsVersion::javaName)
                .isEqualTo("TLSv1.3");

        checkRequest(recordedRequest, queryCompleteEventJson);
    }

    @Test
    void testDifferentCertificatesShouldNotSendRequest()
            throws Exception
    {
        setupServerTLSCertificate();
        server.enqueue(new MockResponse().setResponseCode(200));

        EventListener eventListener = createEventListener(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.http-client.key-store-path", "src/test/resources/trino-httpquery-test2.p12",
                "http-event-listener.http-client.key-store-password", "testing-ssl"));
        eventListener.queryCompleted(queryCompleteEvent);

        RecordedRequest recordedRequest = server.takeRequest(5, TimeUnit.SECONDS);

        assertThat(recordedRequest)
                .describedAs("Handshake should have failed")
                .isNull();
    }

    @Test
    void testNoServerCertificateShouldNotSendRequest()
            throws Exception
    {
        server.enqueue(new MockResponse().setResponseCode(200));

        EventListener eventListener = createEventListener(Map.of(
                "http-event-listener.connect-ingest-uri", "https://%s:%s/".formatted(server.getHostName(), server.getPort()),
                "http-event-listener.log-completed", "true",
                "http-event-listener.http-client.key-store-path", "src/test/resources/trino-httpquery-test.p12",
                "http-event-listener.http-client.key-store-password", "testing-ssl"));
        eventListener.queryCompleted(queryCompleteEvent);

        RecordedRequest recordedRequest = server.takeRequest(5, TimeUnit.SECONDS);

        assertThat(recordedRequest)
                .describedAs("Handshake should have failed")
                .isNull();
    }

    @Test
    void testServerShouldRetry()
            throws Exception
    {
        testServerShouldRetry(503);
        testServerShouldRetry(500);
        testServerShouldRetry(429);
        testServerShouldRetry(408);
    }

    private void testServerShouldRetry(int responseCode)
            throws Exception
    {
        EventListener eventListener = createEventListener(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.connect-retry-count", "1"));

        server.enqueue(new MockResponse().setResponseCode(responseCode));
        server.enqueue(new MockResponse().setResponseCode(200));

        eventListener.queryCompleted(queryCompleteEvent);

        assertThat(server.takeRequest(5, TimeUnit.SECONDS)).isNotNull();
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), queryCompleteEventJson);
    }

    @Test
    void testServerDisconnectShouldRetry()
            throws Exception
    {
        EventListener eventListener = createEventListener(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.connect-retry-count", "1",
                "http-event-listener.http-client.min-threads", "1",
                "http-event-listener.http-client.max-threads", "4"));

        server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));
        server.enqueue(new MockResponse().setResponseCode(200));

        eventListener.queryCompleted(queryCompleteEvent);

        assertThat(server.takeRequest(5, TimeUnit.SECONDS)).isNotNull(); // First request, causes exception
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), queryCompleteEventJson);
    }

    @Test
    void testServerDelayDoesNotBlock()
            throws Exception
    {
        EventListener eventListener = createEventListener(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true"));

        server.enqueue(new MockResponse().setResponseCode(200).setHeadersDelay(5, TimeUnit.SECONDS));

        long startTime = System.nanoTime();
        eventListener.queryCompleted(queryCompleteEvent);
        long endTime = System.nanoTime();

        assertThat(Duration.of(endTime - startTime, ChronoUnit.NANOS).compareTo(Duration.of(1, ChronoUnit.SECONDS)) < 0)
                .describedAs("Server delay is blocking main thread")
                .isTrue();

        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), queryCompleteEventJson);
    }

    private static void checkRequest(RecordedRequest recordedRequest, String eventJson)
            throws JsonProcessingException
    {
        checkRequest(recordedRequest, ImmutableMap.of(), eventJson);
    }

    private static void checkRequest(RecordedRequest recordedRequest, Map<String, String> customHeaders, String eventJson)
            throws JsonProcessingException
    {
        assertThat(recordedRequest)
                .describedAs("No request sent when logging is enabled")
                .isNotNull();
        customHeaders.forEach((key, value) -> {
            assertThat(recordedRequest.getHeader(key))
                    .describedAs(format("Custom header %s not present in request", key))
                    .isNotNull();
            assertThat(recordedRequest.getHeader(key))
                    .describedAs(format("Expected value %s for header %s but got %s", customHeaders.get(key), key, recordedRequest.getHeader(key)))
                    .isEqualTo(customHeaders.get(key));
        });
        String body = recordedRequest.getBody().readUtf8();
        assertThat(body.isEmpty())
                .describedAs("Body is empty")
                .isFalse();

        ObjectMapper objectMapper = new ObjectMapper();
        assertThat(objectMapper.readTree(body))
                .as("Json value is wrong, expected %s but found %s", eventJson, body)
                .isEqualTo(objectMapper.readTree(eventJson));
    }

    private void setupServerTLSCertificate()
            throws Exception
    {
        KeyStore keyStore = KeyStore.getInstance(new File("src/test/resources/trino-httpquery-test.p12"), "testing-ssl".toCharArray());

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);

        TrustManager x509TrustManager = Stream.of(trustManagerFactory.getTrustManagers())
                .filter(X509TrustManager.class::isInstance)
                .collect(onlyElement());

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, "testing-ssl".toCharArray());

        SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
        sslContext.init(keyManagerFactory.getKeyManagers(), new TrustManager[] {x509TrustManager}, null);

        SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
        server.useHttps(sslSocketFactory, false);
    }

    private EventListener createEventListener(Map<String, String> config)
    {
        return factory.create(ImmutableMap.<String, String>builder()
                .putAll(config)
                .put("bootstrap.quiet", "true")
                .buildOrThrow());
    }
}
