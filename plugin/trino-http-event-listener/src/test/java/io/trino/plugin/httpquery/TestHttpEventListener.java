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
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableMap;
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

import static java.lang.String.format;
import static java.time.Duration.ofMillis;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHttpEventListener
{
    private MockWebServer server;

    private final HttpEventListenerFactory factory = new HttpEventListenerFactory();
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new Jdk8Module()).registerModule(new JavaTimeModule());

    private static final QueryIOMetadata queryIOMetadata;
    private static final QueryContext queryContext;
    private static final QueryMetadata queryMetadata;
    private static final SplitStatistics splitStatistics;
    private static final QueryStatistics queryStatistics;
    private static final SplitCompletedEvent splitCompleteEvent;
    private static final QueryCreatedEvent queryCreatedEvent;
    private static final QueryCompletedEvent queryCompleteEvent;

    static {
        queryIOMetadata = new QueryIOMetadata(Collections.emptyList(), Optional.empty());

        queryContext = new QueryContext(
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
                Optional.empty(), Optional.empty());

        splitStatistics = new SplitStatistics(
                ofMillis(1000),
                ofMillis(2000),
                ofMillis(3000),
                ofMillis(4000),
                1,
                2,
                Optional.of(Duration.ofMillis(100)),
                Optional.of(Duration.ofMillis(200)));

        queryStatistics = new QueryStatistics(
                ofMillis(1000),
                ofMillis(1000),
                ofMillis(1000),
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
                0.0f,
                Collections.emptyList(),
                0,
                true,
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

        EventListener querylogListener = factory.create(Map.of("http-event-listener.connect-ingest-uri", server.url("/").toString()));
        querylogListener.queryCreated(null);
        querylogListener.queryCompleted(null);
        querylogListener.splitCompleted(null);

        assertNull(server.takeRequest(5, TimeUnit.SECONDS));
    }

    @Test
    public void testAllLoggingEnabledShouldSendCorrectEvent()
            throws Exception
    {
        EventListener querylogListener = factory.create(Map.of(
                "http-event-listener.log-created", "true",
                "http-event-listener.log-completed", "true",
                "http-event-listener.log-split", "true",
                "http-event-listener.connect-ingest-uri", server.url("/").toString()));

        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));

        querylogListener.queryCreated(queryCreatedEvent);
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), queryCreatedEvent);

        querylogListener.queryCompleted(queryCompleteEvent);
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), queryCompleteEvent);

        querylogListener.splitCompleted(splitCompleteEvent);
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), splitCompleteEvent);
    }

    @Test
    public void testContentTypeDefaultHeaderShouldAlwaysBeSet()
            throws Exception
    {
        EventListener querylogListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true"));

        server.enqueue(new MockResponse().setResponseCode(200));

        querylogListener.queryCompleted(queryCompleteEvent);

        assertEquals(server.takeRequest(5, TimeUnit.SECONDS).getHeader("Content-Type"), "application/json; charset=utf-8");
    }

    @Test
    public void testHttpHeadersShouldBePresent()
            throws Exception
    {
        EventListener querylogListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.connect-http-headers", "Authorization: Trust Me!, Cache-Control: no-cache"));

        server.enqueue(new MockResponse().setResponseCode(200));

        querylogListener.queryCompleted(queryCompleteEvent);

        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), Map.of(
                "Authorization", "Trust Me!",
                "Cache-Control", "no-cache"), queryCompleteEvent);
    }

    @Test
    public void testHttpsEnabledShouldUseTLSv13()
            throws Exception
    {
        setupServerTLSCertificate();
        server.enqueue(new MockResponse().setResponseCode(200));

        EventListener querylogListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.http-client.key-store-path", "src/test/resources/trino-httpquery-test.p12",
                "http-event-listener.http-client.key-store-password", "testing-ssl"));
        querylogListener.queryCompleted(queryCompleteEvent);

        RecordedRequest recordedRequest = server.takeRequest(5, TimeUnit.SECONDS);

        assertNotNull(recordedRequest, "Handshake probably failed");
        assertEquals(recordedRequest.getTlsVersion().javaName(), "TLSv1.3");

        checkRequest(recordedRequest, queryCompleteEvent);
    }

    @Test
    public void testDifferentCertificatesShouldNotSendRequest()
            throws Exception
    {
        setupServerTLSCertificate();
        server.enqueue(new MockResponse().setResponseCode(200));

        EventListener querylogListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.http-client.key-store-path", "src/test/resources/trino-httpquery-test2.p12",
                "http-event-listener.http-client.key-store-password", "testing-ssl"));
        querylogListener.queryCompleted(queryCompleteEvent);

        RecordedRequest recordedRequest = server.takeRequest(5, TimeUnit.SECONDS);

        assertNull(recordedRequest, "Handshake should have failed");
    }

    @Test
    public void testNoServerCertificateShouldNotSendRequest()
            throws Exception
    {
        server.enqueue(new MockResponse().setResponseCode(200));

        EventListener querylogListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", new URL("https", server.getHostName(), server.getPort(), "/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.http-client.key-store-path", "src/test/resources/trino-httpquery-test.p12",
                "http-event-listener.http-client.key-store-password", "testing-ssl"));
        querylogListener.queryCompleted(queryCompleteEvent);

        RecordedRequest recordedRequest = server.takeRequest(5, TimeUnit.SECONDS);

        assertNull(recordedRequest, "Handshake should have failed");
    }

    @Test
    public void testServer500ShouldRetry()
            throws Exception
    {
        EventListener querylogListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.connect-retry-count", "1"));

        server.enqueue(new MockResponse().setResponseCode(500));
        server.enqueue(new MockResponse().setResponseCode(200));

        querylogListener.queryCompleted(queryCompleteEvent);

        assertNotNull(server.takeRequest(5, TimeUnit.SECONDS)); // First request that responds with 500
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), queryCompleteEvent); // The retry that responds with 200
    }

    @Test
    public void testServer400ShouldRetry()
            throws Exception
    {
        EventListener querylogListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.connect-retry-count", "1"));

        server.enqueue(new MockResponse().setResponseCode(400));
        server.enqueue(new MockResponse().setResponseCode(200));

        querylogListener.queryCompleted(queryCompleteEvent);

        assertNotNull(server.takeRequest(5, TimeUnit.SECONDS)); // First request, send back 400
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), queryCompleteEvent); // The retry that responds with 200
    }

    @Test
    public void testServerDisconnectShouldRetry()
            throws Exception
    {
        EventListener querylogListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true",
                "http-event-listener.connect-retry-count", "1"));

        server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));
        server.enqueue(new MockResponse().setResponseCode(200));

        querylogListener.queryCompleted(queryCompleteEvent);

        assertNotNull(server.takeRequest(5, TimeUnit.SECONDS)); // First request, causes exception
        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), queryCompleteEvent);
    }

    @Test
    public void testServerDelayDoesNotBlock()
            throws Exception
    {
        EventListener querylogListener = factory.create(Map.of(
                "http-event-listener.connect-ingest-uri", server.url("/").toString(),
                "http-event-listener.log-completed", "true"));

        server.enqueue(new MockResponse().setResponseCode(200).setHeadersDelay(5, TimeUnit.SECONDS));

        long startTime = System.nanoTime();
        querylogListener.queryCompleted(queryCompleteEvent);
        long endTime = System.nanoTime();

        assertTrue(Duration.of(endTime - startTime, ChronoUnit.NANOS).compareTo(Duration.of(1, ChronoUnit.SECONDS)) < 0,
                "Server delay is blocking main thread");

        checkRequest(server.takeRequest(5, TimeUnit.SECONDS), queryCompleteEvent);
    }

    private void checkRequest(RecordedRequest recordedRequest, Object event)
            throws JsonProcessingException
    {
        checkRequest(recordedRequest, ImmutableMap.of(), event);
    }

    private void checkRequest(RecordedRequest recordedRequest, Map<String, String> customHeaders, Object event)
            throws JsonProcessingException
    {
        assertNotNull(recordedRequest, format("No request sent when logging is enabled for %s", event));
        for (String key : customHeaders.keySet()) {
            assertNotNull(recordedRequest.getHeader(key), format("Custom header %s not present in request for %s event", key, event));
            assertEquals(recordedRequest.getHeader(key), customHeaders.get(key),
                    format("Expected value %s for header %s but got %s for %s event", customHeaders.get(key), key, recordedRequest.getHeader(key), event));
        }
        String body = recordedRequest.getBody().readUtf8();
        assertFalse(body.isEmpty(), format("Body is empty for %s event", event));
        String eventJson = objectMapper.writeValueAsString(event);
        assertTrue(objectMapper.readTree(eventJson).equals(objectMapper.readTree(body)), format("Json value is wrong for event %s, expected %s but found %s", event, eventJson, body));
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
