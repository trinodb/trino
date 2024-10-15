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

import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.trino.operator.RetryPolicy;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.StageOutputBufferUtilization;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.ResourceEstimates;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestHttpServerEventListener
{
    private final HttpServerEventListenerFactory factory = new HttpServerEventListenerFactory();
    HttpClient httpClient = new JettyHttpClient();

    private static final JsonCodec<QueryCompletedEvent> queryCompleteEventJsonCodec = jsonCodec(QueryCompletedEvent.class);

    private static final QueryCompletedEvent queryCompleteEvent;
    private static final QueryCompletedEvent queryCompleteEvent2;

    private static final String queryCompleteEventJson;
    private static final String queryCompleteEventJson2;

    static {
        QueryIOMetadata queryIOMetadata = new QueryIOMetadata(Collections.emptyList(), Optional.empty());

        QueryContext queryContext = new QueryContext(
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

        QueryMetadata queryMetadata = new QueryMetadata(
                "queryId",
                Optional.empty(),
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

        QueryMetadata queryMetadata2 = new QueryMetadata(
                "queryId2",
                Optional.empty(),
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

        QueryStatistics queryStatistics = new QueryStatistics(
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
                Collections.emptyList(),
                Optional.empty());

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

        queryCompleteEvent2 = new QueryCompletedEvent(
                queryMetadata2,
                queryStatistics,
                queryContext,
                queryIOMetadata,
                Optional.empty(),
                Collections.emptyList(),
                Instant.now(),
                Instant.now(),
                Instant.now());

        queryCompleteEventJson = queryCompleteEventJsonCodec.toJson(queryCompleteEvent);
        queryCompleteEventJson2 = queryCompleteEventJsonCodec.toJson(queryCompleteEvent2);
    }

    @AfterAll
    void cleanUp()
    {
        if (httpClient != null) {
            httpClient.close();
        }
    }

    @Test
    void testHttpServerEventListener()
    {
        HttpServerEventListener eventListener = null;
        try {
            eventListener = createEventListener();
            int serverPort = eventListener.getServerPort();

            // list completed queries
            List<String> events = httpClient.execute(
                    Request.Builder.prepareGet().setUri(getQueryCompletedListUri(serverPort)).build(),
                    createJsonResponseHandler(listJsonCodec(String.class)));
            assertThat(events).isEmpty();

            // register first completion event for 'queryId'
            eventListener.queryCompleted(queryCompleteEvent);

            events = httpClient.execute(
                    Request.Builder.prepareGet().setUri(getQueryCompletedListUri(serverPort)).build(),
                    createJsonResponseHandler(listJsonCodec(String.class)));
            assertThat(events).containsExactlyInAnyOrder("queryId");

            QueryCompletedEvent receivedEvent = httpClient.execute(
                    Request.Builder.prepareGet().setUri(getQueryCompletedGetUri(serverPort, queryCompleteEvent.getMetadata().getQueryId())).build(),
                    createJsonResponseHandler(queryCompleteEventJsonCodec));
            assertThat(queryCompleteEventJsonCodec.toJson(receivedEvent)).isEqualTo(queryCompleteEventJson);

            // get queryId2 before registered
            assertThatThrownBy(() -> httpClient.execute(
                    Request.Builder.prepareGet().setUri(getQueryCompletedGetUri(serverPort, queryCompleteEvent2.getMetadata().getQueryId())).build(),
                    createJsonResponseHandler(queryCompleteEventJsonCodec)))
                    .isInstanceOf(UnexpectedResponseException.class)
                    .matches(e -> ((UnexpectedResponseException) e).getStatusCode() == 404);

            // register first completion event for 'queryId'
            eventListener.queryCompleted(queryCompleteEvent2);

            events = httpClient.execute(
                    Request.Builder.prepareGet().setUri(getQueryCompletedListUri(serverPort)).build(),
                    createJsonResponseHandler(listJsonCodec(String.class)));
            assertThat(events).containsExactlyInAnyOrder("queryId", "queryId2");

            receivedEvent = httpClient.execute(
                    Request.Builder.prepareGet().setUri(getQueryCompletedGetUri(serverPort, queryCompleteEvent2.getMetadata().getQueryId())).build(),
                    createJsonResponseHandler(queryCompleteEventJsonCodec));
            assertThat(queryCompleteEventJsonCodec.toJson(receivedEvent)).isEqualTo(queryCompleteEventJson2);
        }
        finally {
            if (eventListener != null) {
                eventListener.shutdown();
            }
        }
    }

    private static URI getQueryCompletedListUri(int serverPort)
    {
        return URI.create(String.format("http://localhost:%s/v1/events/completedQueries/list", serverPort));
    }

    private static URI getQueryCompletedGetUri(int serverPort, String queryId)
    {
        return URI.create(String.format("http://localhost:%s/v1/events/completedQueries/get/%s", serverPort, queryId));
    }

    private HttpServerEventListener createEventListener()
    {
        return factory.createInternal(
                ImmutableMap.<String, String>builder()
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                true);
    }
}
