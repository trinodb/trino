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
package io.trino.plugin.openlineage;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.openlineage.client.OpenLineage;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Verify.verify;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;

public class OpenLineageClient
        implements OpenLineageEmitter
{
    private final HttpClient client = new JettyHttpClient();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final String url;
    private static final ObjectMapper objectMapper = createMapper();
    private final Integer retryCount;
    private final Duration retryDelay;
    private final Duration maxDelay;
    private final double backoffBase;
    private final Optional<String> apiKey;
    private static final Logger logger = Logger.get(OpenLineageClient.class);

    @Inject
    public OpenLineageClient(OpenLineageClientConfig clientConfig)
    {
        this.url = clientConfig.getUrl();
        this.apiKey = clientConfig.getApiKey();

        this.retryCount = clientConfig.getRetryCount();
        this.retryDelay = clientConfig.getRetryDelay();
        this.maxDelay = clientConfig.getMaxDelay();
        this.backoffBase = clientConfig.getBackoffBase();

        logger.info(clientConfig.getSink().toString());
    }

    @Override
    public void emit(OpenLineage.RunEvent runEvent, String queryId)
            throws JsonProcessingException
    {
        String json = objectMapper.writeValueAsString(runEvent);
        logger.debug(json);

        Request.Builder requestBuilder = preparePost()
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setUri(URI.create(url + "/api/v1/lineage"))
                .setBodyGenerator(
                        StaticBodyGenerator.createStaticBodyGenerator(
                                json.getBytes(StandardCharsets.UTF_8)));

        apiKey.ifPresent(s -> requestBuilder.addHeader("Authorization", "Bearer " + s));

        attemptToSend(requestBuilder.build(), 0, Duration.valueOf("0s"), queryId);
    }

    private void attemptToSend(Request request, int attempt, Duration delay, String queryId)
    {
        this.executor.schedule(
                () -> Futures.addCallback(client.executeAsync(request, createStatusResponseHandler()),
                        new FutureCallback<>() {
                            @Override
                            public void onSuccess(StatusResponseHandler.StatusResponse result)
                            {
                                verify(result != null);

                                if (shouldRetry(result)) {
                                    if (attempt < retryCount) {
                                        Duration nextDelay = nextDelay(delay);
                                        int nextAttempt = attempt + 1;

                                        logger.warn("QueryId = \"%s\", attempt = %d/%d, URL = %s | Ingest server responded with code %d, will retry after approximately %d seconds",
                                                queryId, attempt + 1, retryCount + 1, request.getUri().toString(),
                                                result.getStatusCode(), nextDelay.roundTo(TimeUnit.SECONDS));

                                        attemptToSend(request, nextAttempt, nextDelay, queryId);
                                    }
                                    else {
                                        logger.error("QueryId = \"%s\", attempt = %d/%d, URL = %s | Ingest server responded with code %d, fatal error",
                                                queryId, attempt + 1, retryCount + 1, request.getUri().toString(),
                                                result.getStatusCode());
                                    }
                                }
                                else {
                                    logger.debug("QueryId = \"%s\", attempt = %d/%d, URL = %s | Query event delivered successfully",
                                            queryId, attempt + 1, retryCount + 1, request.getUri().toString());
                                }
                            }

                            @Override
                            public void onFailure(Throwable t)
                            {
                                if (attempt < retryCount) {
                                    Duration nextDelay = nextDelay(delay);
                                    int nextAttempt = attempt + 1;

                                    logger.warn(t, "QueryId = \"%s\", attempt = %d/%d, URL = %s | Sending event caused an exception, will retry after %d seconds",
                                            queryId, attempt + 1, retryCount + 1, request.getUri().toString(),
                                            nextDelay.roundTo(TimeUnit.SECONDS));

                                    attemptToSend(request, nextAttempt, nextDelay, queryId);
                                }
                                else {
                                    logger.error(t, "QueryId = \"%s\", attempt = %d/%d, URL = %s | Error sending HTTP request",
                                            queryId, attempt + 1, retryCount + 1, request.getUri().toString());
                                }
                            }
                        }, executor),
                (long) delay.getValue(), delay.getUnit());
    }

    private boolean shouldRetry(StatusResponseHandler.StatusResponse response)
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

    private static ObjectMapper createMapper()
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
        return mapper;
    }

    @Override
    public void close()
    {
        executor.shutdown();
        client.close();
    }
}
