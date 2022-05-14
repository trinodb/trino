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
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.eventlistener.SplitStatistics;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.ResourceEstimates;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
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

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static java.time.Duration.ofMillis;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHttpEventListener
{
    private MockWebServer server;

    private final HttpEventListenerFactory factory = new HttpEventListenerFactory();

    private static JsonCodec<QueryCompletedEvent> queryCompleteEventJsonCodec = jsonCodec(QueryCompletedEvent.class);
    private static JsonCodec<QueryCreatedEvent> queryCreateEventJsonCodec = jsonCodec(QueryCreatedEvent.class);
    private static JsonCodec<SplitCompletedEvent> splitCompleteEventJsonCodec = jsonCodec(SplitCompletedEvent.class);

    private static final QueryIOMetadata QUERY_IO_METADATA;
    private static final QueryContext QUERY_CONTEXT;
    private static final QueryMetadata QUERY_METADATA;
    private static final SplitStatistics SPLIT_STATISTICS;
    private static final QueryStatistics QUERY_STATISTICS;
    private static final SplitCompletedEvent SPLIT_COMPLETE_EVENT;
    private static final QueryCreatedEvent QUERY_CREATED_EVENT;
    private static final QueryCompletedEvent QUERY_COMPLETE_EVENT;

    private static final String QUERY_CREATED_EVENT_JSON;
    private static final String QUERY_COMPLETE_EVENT_JSON;
    private static final String SPLIT_COMPLETE_EVENT_JSON;

    static {
        QUERY_IO_METADATA = new QueryIOMetadata(Collections.emptyList(), Optional.empty());

        QUERY_CONTEXT = new QueryContext(
                "user",
                Optional.of("principal"),
                Set.of(), // groups
                Optional.empty(), // traceToken
                Optional.empty(), // remoteClientAddress
                Optional.empty(), // userAgent
                Optional.empty(), // clientInfo
                new HashSet<>(), // clientTags
                new HashSet<>(), // clientCapabilities
                Optional.of("source"),
                Optional.of("catalog"),
                Optional.of("schema"),
                Optional.of(new ResourceGroupId("name")),
                new HashMap<>(), // sessionProperties
                new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.of(1000L)),
                "serverAddress", "serverVersion", "environment",
                Optional.of(QueryType.SELECT));

        QUERY_METADATA = new QueryMetadata(
                "queryId",
                Optional.empty(),
                "query",
                Optional.of("updateType"),
                Optional.of("preparedQuery"),
                "queryState",
                List.of(),
                List.of(),
                URI.create("http://localhost"),
                Optional.empty(), Optional.empty());

        SPLIT_STATISTICS = new SplitStatistics(
                ofMillis(1000),
                ofMillis(2000),
                ofMillis(3000),
                ofMillis(4000),
                1,
                2,
                Optional.of(Duration.ofMillis(100)),
                Optional.of(Duration.ofMillis(200)));

        QUERY_STATISTICS = new QueryStatistics(
                ofMillis(1000),
                ofMillis(1000),
                ofMillis(1000),
                ofMillis(1000),
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
                0.0f,
                Collections.emptyList(),
                0,
                true,
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.empty());

        SPLIT_COMPLETE_EVENT = new SplitCompletedEvent(
                "queryId",
                "stageId",
                "taskId",
                Optional.of("catalogName"),
                Instant.now(),
                Optional.of(Instant.now()),
                Optional.of(Instant.now()),
            SPLIT_STATISTICS,
                Optional.empty(),
                "payload");

        QUERY_CREATED_EVENT = new QueryCreatedEvent(
                Instant.now(),
            QUERY_CONTEXT,
            QUERY_METADATA);

        QUERY_COMPLETE_EVENT = new QueryCompletedEvent(
            QUERY_METADATA,
            QUERY_STATISTICS,
            QUERY_CONTEXT,
            QUERY_IO_METADATA,
                Optional.empty(),
                Collections.emptyList(),
                Instant.now(),
                Instant.now(),
                Instant.now());

        QUERY_COMPLETE_EVENT_JSON = queryCompleteEventJsonCodec.toJson(QUERY_COMPLETE_EVENT);
        QUERY_CREATED_EVENT_JSON = queryCreateEventJsonCodec.toJson(QUERY_CREATED_EVENT);
        SPLIT_COMPLETE_EVENT_JSON = splitCompleteEventJsonCodec.toJson(SPLIT_COMPLETE_EVENT);
    }

    @BeforeMethod
    public void setup()
            throws IOException
    {
        server = new MockWebServer();
        server.start();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
    {
        try {
            server.close();
        }
        catch (IOException ignored) {
            // MockWebServer.close() method sometimes throws 'Gave up waiting for executor to shut down'
        }
    }

    /**
     * Listener created without exceptions but not requests sent
     */
    @Test
    public void testAllLoggingDisabledShouldTimeout()
            throws Exception
    {
        server.enqueue(new MockResponse()
                .setResponseCode(200));

        EventListener eventListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString()));

        eventListener.queryCreated(null);
        eventListener.queryCompleted(null);
        eventListener.splitCompleted(null);

        assertNull(server.takeRequest(5, TimeUnit.SECONDS));
    }

    @Test
    public void testAllLoggingEnabledShouldSendCorrectEvent()
            throws Exception
    {
        EventListener eventListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.log-created", "true",
                "http-event-listener.log-split", "true"));

        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));

        eventListener.queryCreated(QUERY_CREATED_EVENT);
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), QUERY_CREATED_EVENT_JSON);

        eventListener.queryCompleted(QUERY_COMPLETE_EVENT);
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), QUERY_COMPLETE_EVENT_JSON);

        eventListener.splitCompleted(SPLIT_COMPLETE_EVENT);
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), SPLIT_COMPLETE_EVENT_JSON);
    }

    @Test
    public void testContentTypeDefaultHeaderShouldAlwaysBeSet()
            throws Exception
    {
        EventListener eventListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true"));

        server.enqueue(new MockResponse().setResponseCode(200));

        eventListener.queryCompleted(QUERY_COMPLETE_EVENT);

        assertEquals(server.takeRequest(5, TimeUnit.SECONDS).getHeader("Content-Type"), "application/json; charset=utf-8");
    }

    @Test
    public void testHttpHeadersShouldBePresent()
            throws Exception
    {
        EventListener eventListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.connect-http-headers", "Authorization: Trust Me!, Cache-Control: no-cache"));

        server.enqueue(new MockResponse().setResponseCode(200));

        eventListener.queryCompleted(QUERY_COMPLETE_EVENT);

        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), Map.of(
                "Authorization", "Trust Me!",
                "Cache-Control", "no-cache"), QUERY_COMPLETE_EVENT_JSON);
    }

    @Test
    public void testHttpsEnabledShouldUseTLSv13()
            throws Exception
    {
        setupServerTLSCertificate();
        server.enqueue(new MockResponse().setResponseCode(200));

        EventListener eventListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.http-client.key-store-path", "src/test/resources/trino-httpquery-test.p12",
                "http-event-listener.http-client.key-store-password", "testing-ssl"));
        eventListener.queryCompleted(QUERY_COMPLETE_EVENT);

        RecordedRequest recordedRequest = server.takeRequest(5, TimeUnit.SECONDS);

        assertNotNull(recordedRequest, "Handshake probably failed");
        assertEquals(recordedRequest.getTlsVersion().javaName(), "TLSv1.3");

        checkRequest(recordedRequest, QUERY_COMPLETE_EVENT_JSON);
    }

    @Test
    public void testDifferentCertificatesShouldNotSendRequest()
            throws Exception
    {
        setupServerTLSCertificate();
        server.enqueue(new MockResponse().setResponseCode(200));

        EventListener eventListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.http-client.key-store-path", "src/test/resources/trino-httpquery-test2.p12",
                "http-event-listener.http-client.key-store-password", "testing-ssl"));
        eventListener.queryCompleted(QUERY_COMPLETE_EVENT);

        RecordedRequest recordedRequest = server.takeRequest(5, TimeUnit.SECONDS);

        assertNull(recordedRequest, "Handshake should have failed");
    }

    @Test
    public void testNoServerCertificateShouldNotSendRequest()
            throws Exception
    {
        server.enqueue(new MockResponse().setResponseCode(200));

        EventListener eventListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", new URL("https", server.getHostName(), server.getPort(), "/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.http-client.key-store-path", "src/test/resources/trino-httpquery-test.p12",
                "http-event-listener.http-client.key-store-password", "testing-ssl"));
        eventListener.queryCompleted(QUERY_COMPLETE_EVENT);

        RecordedRequest recordedRequest = server.takeRequest(5, TimeUnit.SECONDS);

        assertNull(recordedRequest, "Handshake should have failed");
    }

    @DataProvider(name = "retryStatusCodes")
    public static Object[][] retryStatusCodes()
    {
        return new Object[][] {{503}, {500}, {429}, {408}};
    }

    @Test(dataProvider = "retryStatusCodes")
    public void testServerShouldRetry(int responseCode)
            throws Exception
    {
        EventListener eventListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.connect-retry-count", "1"));

        server.enqueue(new MockResponse().setResponseCode(responseCode));
        server.enqueue(new MockResponse().setResponseCode(200));

        eventListener.queryCompleted(QUERY_COMPLETE_EVENT);

        assertNotNull(server.takeRequest(5, TimeUnit.SECONDS));
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), QUERY_COMPLETE_EVENT_JSON);
    }

    @Test
    public void testServerDisconnectShouldRetry()
            throws Exception
    {
        EventListener eventListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.connect-retry-count", "1",
                "http-event-listener.http-client.min-threads", "1",
                "http-event-listener.http-client.max-threads", "4"));

        server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));
        server.enqueue(new MockResponse().setResponseCode(200));

        eventListener.queryCompleted(QUERY_COMPLETE_EVENT);

        assertNotNull(server.takeRequest(5, TimeUnit.SECONDS)); // First request, causes exception
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), QUERY_COMPLETE_EVENT_JSON);
    }

    @Test
    public void testServerDelayDoesNotBlock()
            throws Exception
    {
        EventListener eventListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true"));

        server.enqueue(new MockResponse().setResponseCode(200).setHeadersDelay(5, TimeUnit.SECONDS));

        long startTime = System.nanoTime();
        eventListener.queryCompleted(QUERY_COMPLETE_EVENT);
        long endTime = System.nanoTime();

        assertTrue(Duration.of(endTime - startTime, ChronoUnit.NANOS).compareTo(Duration.of(1, ChronoUnit.SECONDS)) < 0,
                "Server delay is blocking main thread");

        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), QUERY_COMPLETE_EVENT_JSON);
    }

    private void checkRequest(RecordedRequest recordedRequest, String eventJson)
            throws JsonProcessingException
    {
        checkRequest(recordedRequest, ImmutableMap.of(), eventJson);
    }

    private void checkRequest(RecordedRequest recordedRequest, Map<String, String> customHeaders, String eventJson)
            throws JsonProcessingException
    {
        assertNotNull(recordedRequest, "No request sent when logging is enabled");
        for (String key : customHeaders.keySet()) {
            assertNotNull(recordedRequest.getHeader(key), format("Custom header %s not present in request", key));
            assertEquals(recordedRequest.getHeader(key), customHeaders.get(key),
                    format("Expected value %s for header %s but got %s", customHeaders.get(key), key, recordedRequest.getHeader(key)));
        }
        String body = recordedRequest.getBody().readUtf8();
        assertFalse(body.isEmpty(), "Body is empty");

        ObjectMapper objectMapper = new ObjectMapper();
        assertEquals(objectMapper.readTree(body), objectMapper.readTree(eventJson), format("Json value is wrong, expected %s but found %s", eventJson, body));
    }

    private void setupServerTLSCertificate()
            throws Exception
    {
        KeyStore keyStore = KeyStore.getInstance(new File("src/test/resources/trino-httpquery-test.p12"), "testing-ssl".toCharArray());

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);

        X509TrustManager x509TrustManager = (X509TrustManager) List.of(trustManagerFactory.getTrustManagers()).stream().filter(tm -> tm instanceof X509TrustManager)
                .findFirst().get();

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, "testing-ssl".toCharArray());

        SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
        sslContext.init(keyManagerFactory.getKeyManagers(), new TrustManager[] {x509TrustManager}, null);

        SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
        server.useHttps(sslSocketFactory, false);
    }
}
