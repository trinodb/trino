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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Verify.verify;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.StatusResponse;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.util.Objects.requireNonNull;

/**
 * Implement an EventListener that send events, serialized as JSON, to a ingest server.
 *
 * For configuration see {@link io.airlift.http.client.HttpClientConfig}, prefixed with "http-event-listener"
 */
public class HttpEventListener
        implements EventListener
{
    private final Logger log = Logger.get(HttpEventListener.class);

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private final ObjectWriter objectWriter = new ObjectMapper().registerModule(new Jdk8Module()).registerModule(new JavaTimeModule()).writer();

    private final HttpClient client;

    private final HttpEventListenerConfig config;

    private final URI ingestUri;

    @Inject
    public HttpEventListener(HttpEventListenerConfig config, @ForHttpEventListener HttpClient httpClient)
    {
        this.config = requireNonNull(config, "http event listener config is null");
        this.client = requireNonNull(httpClient, "http event listener http client is null");

        try {
            ingestUri = new URI(this.config.getIngestUri());
        }
        catch (URISyntaxException e) {
            throw new IllegalStateException(String.format("Ingest URI %s for HTTP event listener is not valid", this.config.getIngestUri()), e);
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (config.getLogCreated()) {
            sendLog(queryCreatedEvent);
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (config.getLogCompleted()) {
            sendLog(queryCompletedEvent);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        if (config.getLogSplit()) {
            sendLog(splitCompletedEvent);
        }
    }

    private <T> void sendLog(T event)
    {
        Request request = preparePost()
                .addHeaders(Multimaps.forMap(config.getHttpHeaders()))
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setUri(ingestUri)
                .setBodyGenerator(out -> objectWriter.writeValue(out, event))
                .build();

        attemptToSend(request, 0, Duration.valueOf("0s"));
    }

    private void attemptToSend(Request request, int attempt, Duration delay)
    {
        this.executor.schedule(
                () -> Futures.addCallback(client.executeAsync(request, createStatusResponseHandler()),
                        new FutureCallback<>() {
                            @Override
                            public void onSuccess(StatusResponse result)
                            {
                                verify(result != null);

                                if (result.getStatusCode() >= 500 && attempt < config.getRetryCount()) {
                                    attemptToSend(request, attempt + 1, nextDelay(delay));
                                    return;
                                }

                                if (!(result.getStatusCode() >= 200 && result.getStatusCode() < 300)) {
                                    log.error("Received status code %d from ingest server URI %s; expecting status 200", result.getStatusCode(), request.getUri().toString());
                                }
                            }

                            @Override
                            public void onFailure(Throwable t)
                            {
                                log.error("Error sending HTTP request to ingest server with URL %s: %s", request.getUri().toString(), t.toString());
                            }
                        }, executor),
                (long) delay.getValue(), delay.getUnit());
    }

    private Duration nextDelay(Duration delay)
    {
        Duration newDuration = Duration.succinctDuration(delay.getValue(TimeUnit.SECONDS) * this.config.getBackoffBase(), TimeUnit.SECONDS);
        if (newDuration.compareTo(config.getMaxDelay()) > 0) {
            return config.getMaxDelay();
        }
        return newDuration;
    }
}
