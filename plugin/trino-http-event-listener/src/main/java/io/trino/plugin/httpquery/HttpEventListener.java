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
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import jakarta.annotation.PreDestroy;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Verify.verify;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.StatusResponse;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * Implement an EventListener that send events, serialized as JSON, to a ingest server.
 * <p>
 * For configuration see {@link io.airlift.http.client.HttpClientConfig}, prefixed with "http-event-listener"
 */
public class HttpEventListener
        implements EventListener
{
    private final Logger log = Logger.get(HttpEventListener.class);

    private final LifeCycleManager lifecycleManager;

    private final JsonCodec<QueryCompletedEvent> queryCompletedEventJsonCodec;
    private final JsonCodec<QueryCreatedEvent> queryCreatedEventJsonCodec;
    private final JsonCodec<SplitCompletedEvent> splitCompletedEventJsonCodec;

    private final HttpClient client;

    private final boolean logCreated;
    private final boolean logCompleted;
    private final boolean logSplit;
    private final int retryCount;
    private final Duration retryDelay;
    private final Duration maxDelay;
    private final double backoffBase;
    private final Map<String, String> httpHeaders;
    private final URI ingestUri;
    private final ScheduledExecutorService executor;

    @Inject
    public HttpEventListener(
            LifeCycleManager lifecycleManager,
            JsonCodec<QueryCompletedEvent> queryCompletedEventJsonCodec,
            JsonCodec<QueryCreatedEvent> queryCreatedEventJsonCodec,
            JsonCodec<SplitCompletedEvent> splitCompletedEventJsonCodec,
            HttpEventListenerConfig config,
            @ForHttpEventListener HttpClient httpClient)
    {
        this.lifecycleManager = requireNonNull(lifecycleManager, "lifecycleManager is null");
        requireNonNull(config, "http event listener config is null");
        this.client = requireNonNull(httpClient, "http event listener http client is null");

        this.queryCompletedEventJsonCodec = requireNonNull(queryCompletedEventJsonCodec, "queryCompletedEventJsonCodec is null");
        this.queryCreatedEventJsonCodec = requireNonNull(queryCreatedEventJsonCodec, "queryCreatedEventJsonCodec is null");
        this.splitCompletedEventJsonCodec = requireNonNull(splitCompletedEventJsonCodec, "splitCompletedEventJsonCodec is null");
        this.logCreated = config.getLogCreated();
        this.logCompleted = config.getLogCompleted();
        this.logSplit = config.getLogSplit();
        this.retryCount = config.getRetryCount();
        this.retryDelay = config.getRetryDelay();
        this.maxDelay = config.getMaxDelay();
        this.backoffBase = config.getBackoffBase();
        this.httpHeaders = ImmutableMap.copyOf(config.getHttpHeaders());

        try {
            ingestUri = new URI(config.getIngestUri());
        }
        catch (URISyntaxException e) {
            throw new IllegalStateException(String.format("Ingest URI %s for HTTP event listener is not valid", config.getIngestUri()), e);
        }

        this.executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("http-event-listener-%s"));
    }

    @PreDestroy
    public void destroy()
    {
        executor.shutdownNow();
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (logCreated) {
            sendLog(jsonBodyGenerator(queryCreatedEventJsonCodec, queryCreatedEvent), queryCreatedEvent.getMetadata().getQueryId());
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (logCompleted) {
            sendLog(jsonBodyGenerator(queryCompletedEventJsonCodec, queryCompletedEvent), queryCompletedEvent.getMetadata().getQueryId());
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        if (logSplit) {
            sendLog(jsonBodyGenerator(splitCompletedEventJsonCodec, splitCompletedEvent), splitCompletedEvent.getQueryId());
        }
    }

    private void sendLog(BodyGenerator eventBodyGenerator, String queryId)
    {
        Request request = preparePost()
                .addHeaders(Multimaps.forMap(httpHeaders))
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setUri(ingestUri)
                .setBodyGenerator(eventBodyGenerator)
                .build();

        attemptToSend(request, 0, Duration.valueOf("0s"), queryId);
    }

    private void attemptToSend(Request request, int attempt, Duration delay, String queryId)
    {
        this.executor.schedule(
                () -> Futures.addCallback(client.executeAsync(request, createStatusResponseHandler()),
                        new FutureCallback<>()
                        {
                            @Override
                            public void onSuccess(StatusResponse result)
                            {
                                verify(result != null);

                                if (shouldRetry(result)) {
                                    if (attempt < retryCount) {
                                        Duration nextDelay = nextDelay(delay);
                                        int nextAttempt = attempt + 1;

                                        log.warn("QueryId = \"%s\", attempt = %d/%d, URL = %s | Ingest server responded with code %d, will retry after approximately %d seconds",
                                                queryId, attempt + 1, retryCount + 1, request.getUri().toString(),
                                                result.getStatusCode(), nextDelay.roundTo(TimeUnit.SECONDS));

                                        attemptToSend(request, nextAttempt, nextDelay, queryId);
                                    }
                                    else {
                                        log.error("QueryId = \"%s\", attempt = %d/%d, URL = %s | Ingest server responded with code %d, fatal error",
                                                queryId, attempt + 1, retryCount + 1, request.getUri().toString(),
                                                result.getStatusCode());
                                    }
                                }
                                else {
                                    log.debug("QueryId = \"%s\", attempt = %d/%d, URL = %s | Query event delivered successfully",
                                            queryId, attempt + 1, retryCount + 1, request.getUri().toString());
                                }
                            }

                            @Override
                            public void onFailure(Throwable t)
                            {
                                if (attempt < retryCount) {
                                    Duration nextDelay = nextDelay(delay);
                                    int nextAttempt = attempt + 1;

                                    log.warn(t, "QueryId = \"%s\", attempt = %d/%d, URL = %s | Sending event caused an exception, will retry after %d seconds",
                                            queryId, attempt + 1, retryCount + 1, request.getUri().toString(),
                                            nextDelay.roundTo(TimeUnit.SECONDS));

                                    attemptToSend(request, nextAttempt, nextDelay, queryId);
                                }
                                else {
                                    log.error(t, "QueryId = \"%s\", attempt = %d/%d, URL = %s | Error sending HTTP request",
                                            queryId, attempt + 1, retryCount + 1, request.getUri().toString());
                                }
                            }
                        }, executor),
                (long) delay.getValue(), delay.getUnit());
    }

    private boolean shouldRetry(StatusResponse response)
    {
        int statusCode = response.getStatusCode();

        // 1XX Information, requests can't be split
        if (statusCode < 200) {
            return false;
        }
        // 2XX - OK
        if (200 <= statusCode && statusCode < 300) {
            return false;
        }
        // 3XX Redirects, not following redirects
        if (300 <= statusCode && statusCode <= 400) {
            return false;
        }
        // 4XX - client error, no retry except 408 Request Timeout and 429 Too Many Requests
        if (400 <= statusCode && statusCode < 500 && statusCode != 408 && statusCode != 429) {
            return false;
        }

        return true;
    }

    private Duration nextDelay(Duration delay)
    {
        if (delay.compareTo(Duration.valueOf("0s")) == 0) {
            return retryDelay;
        }

        Duration newDuration = Duration.succinctDuration(delay.getValue(TimeUnit.SECONDS) * backoffBase, TimeUnit.SECONDS);
        if (newDuration.compareTo(maxDelay) > 0) {
            return maxDelay;
        }
        return newDuration;
    }

    @Override
    public void shutdown()
    {
        lifecycleManager.stop();
    }
}
