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
package io.trino.plugin.querylog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.time.Duration.ofMillis;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

@Test(singleThreaded = true)
public class TestQuerylogListener
{
    private MockWebServer server;

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
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
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
            throws IOException
    {
        server.close();
    }

    /**
     * Querylog created without exceptions but not requests sent
     */
    @Test
    public void testAllLoggingDisabled_Should_Timeout()
            throws Exception
    {
        server.enqueue(new MockResponse()
                .setResponseCode(200));

        QuerylogListener querylogListener = new QuerylogListener(server.getHostName(), server.getPort(), "http", new HashMap<>(), false, false, false);

        querylogListener.queryCreated(null);
        querylogListener.queryCompleted(null);
        querylogListener.splitCompleted(null);

        assertNull(server.takeRequest(1, TimeUnit.SECONDS));
    }

    @Test
    public void testAllLoggingEnabled_Should_SendCorrectEvent()
            throws Exception
    {
        Map<String, String> headers = new HashMap<>() {{
                put("Authorization", "trust me");
                put("Cache-Control", "no-cache");
            }};

        QuerylogListener querylogListener = new QuerylogListener(server.getHostName(), server.getPort(), "http", headers, true, true, true);

        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));

        querylogListener.queryCreated(queryCreatedEvent);
        querylogListener.queryCompleted(queryCompleteEvent);
        querylogListener.splitCompleted(splitCompleteEvent);

        checkRequest(server.takeRequest(1, TimeUnit.SECONDS), headers, queryCreatedEvent);
        checkRequest(server.takeRequest(1, TimeUnit.SECONDS), headers, queryCompleteEvent);
        checkRequest(server.takeRequest(1, TimeUnit.SECONDS), headers, splitCompleteEvent);
    }

    @Test
    public void testContentTypeDefaultHeader_Should_AlwaysBeSet()
            throws Exception
    {
        QuerylogListener querylogListener = new QuerylogListener(server.getHostName(), server.getPort(), "http", new HashMap<>(), true, true, true);

        server.enqueue(new MockResponse().setResponseCode(200));

        querylogListener.queryCompleted(queryCompleteEvent);

        assertEquals(server.takeRequest(1, TimeUnit.SECONDS).getHeader("Content-Type"), "application/json; charset=UTF-8");
    }

    @Test
    public void testBadHostName_ShouldThrow_UnknownHostException()
    {
        Exception e = expectThrows(UnknownHostException.class,
                () -> new QuerylogListener("this-is+not_a+hostname", 8090, "http", new HashMap<>(), true, true, true));

        assertTrue(e.getMessage().contains("this-is+not_a+hostname"));
    }

    @Test
    public void testEmptyHeaders_ShouldThrow_NullPointerException()
    {
        Exception e = expectThrows(NullPointerException.class,
                () -> new QuerylogListener("google.com", 8090, "http", null, true, true, true));
        assertTrue(e.getMessage().contains("headers"), String.format("\"%s\" should contain the word headers", e.getMessage()));
    }

    @Test
    public void testEmptyScheme_ShouldThrow_NullPointerException()
    {
        Exception e = expectThrows(NullPointerException.class,
                () -> new QuerylogListener("google.com", 8090, null, new HashMap<>(), true, true, true));
        assertTrue(e.getMessage().contains("scheme"), String.format("\"%s\" should contain the word scheme", e.getMessage()));
    }

    private void checkRequest(RecordedRequest recordedRequest, Map<String, String> customHeaders, Object event)
            throws JsonProcessingException
    {
        assertNotNull(recordedRequest, String.format("No request sent when logging is enabled for %s", event));
        for (String key : customHeaders.keySet()) {
            assertNotNull(recordedRequest.getHeader(key), String.format("Custom header %s not present in request for %s event", key, event));
            assertEquals(recordedRequest.getHeader(key), customHeaders.get(key),
                    String.format("Expected value %s for header %s but got %s for %s event", customHeaders.get(key), key, recordedRequest.getHeader(key), event));
        }
        String body = recordedRequest.getBody().readUtf8();
        assertFalse(body.isEmpty(), String.format("Body is empty for %s event", event));
        String eventJson = objectMapper.writeValueAsString(event);
        assertTrue(objectMapper.readTree(eventJson).equals(objectMapper.readTree(body)), String.format("Json value is wrong for event %s", event));
    }
}
