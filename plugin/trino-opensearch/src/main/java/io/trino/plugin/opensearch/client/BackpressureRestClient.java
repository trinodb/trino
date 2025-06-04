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
package io.trino.plugin.opensearch.client;

import com.google.common.base.Stopwatch;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import dev.failsafe.event.ExecutionAttemptedEvent;
import dev.failsafe.event.ExecutionCompletedEvent;
import dev.failsafe.function.CheckedSupplier;
import io.airlift.log.Logger;
import io.airlift.stats.TimeStat;
import io.trino.plugin.opensearch.OpenSearchConfig;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.opensearch.client.Node;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BackpressureRestClient
{
    private static final Logger log = Logger.get(BackpressureRestClient.class);

    private final RestClient delegate;
    private final RetryPolicy<Response> retryPolicy;
    private final TimeStat backpressureStats;
    private final ThreadLocal<Stopwatch> stopwatch = ThreadLocal.withInitial(Stopwatch::createUnstarted);

    public BackpressureRestClient(RestClient delegate, OpenSearchConfig config, TimeStat backpressureStats)
    {
        this.delegate = requireNonNull(delegate, "restClient is null");
        this.backpressureStats = requireNonNull(backpressureStats, "backpressureStats is null");
        retryPolicy = RetryPolicy.<Response>builder()
                .withMaxAttempts(-1)
                .withMaxDuration(java.time.Duration.ofMillis(config.getMaxRetryTime().toMillis()))
                .withBackoff(config.getBackoffInitDelay().toMillis(), config.getBackoffMaxDelay().toMillis(), MILLIS)
                .withJitter(0.125)
                .handleIf(BackpressureRestClient::isBackpressure)
                .onFailedAttempt(this::onFailedAttempt)
                .onSuccess(this::onComplete)
                .onFailure(this::onComplete)
                .build();
    }

    public void setHosts(HttpHost... hosts)
    {
        delegate.setNodes(stream(hosts)
                .map(Node::new)
                .collect(toImmutableList()));
    }

    public Response performRequest(String method, String endpoint, Header... headers)
            throws IOException
    {
        return executeWithRetries(() -> delegate.performRequest(toRequest(method, endpoint, headers)));
    }

    public Response performRequest(String method, String endpoint, Map<String, String> params, HttpEntity entity, Header... headers)
            throws IOException
    {
        return executeWithRetries(() -> delegate.performRequest(toRequest(method, endpoint, params, entity, headers)));
    }

    private static Request toRequest(String method, String endpoint, Map<String, String> params, HttpEntity entity, Header... headers)
    {
        Request request = toRequest(method, endpoint, headers);
        requireNonNull(params, "parameters cannot be null");
        for (Map.Entry<String, String> entry : params.entrySet()) {
            request.addParameter(entry.getKey(), entry.getValue());
        }
        request.setEntity(entity);
        return request;
    }

    private static Request toRequest(String method, String endpoint, Header... headers)
    {
        requireNonNull(headers, "headers cannot be null");
        Request request = new Request(method, endpoint);
        RequestOptions.Builder options = request.getOptions().toBuilder();
        for (Header header : headers) {
            options.addHeader(header.getName(), header.getValue());
        }
        request.setOptions(options);
        return request;
    }

    public void close()
            throws IOException
    {
        delegate.close();
    }

    private static boolean isBackpressure(Throwable throwable)
    {
        return throwable instanceof ResponseException responseException &&
                responseException.getResponse().getStatusLine().getStatusCode() == RestStatus.TOO_MANY_REQUESTS.getStatus();
    }

    private void onComplete(ExecutionCompletedEvent<Response> executionCompletedEvent)
    {
        if (stopwatch.get().isRunning()) {
            long delayMillis = stopwatch.get().elapsed(MILLISECONDS);
            log.debug("Adding %s milliseconds to backpressure stats", delayMillis);
            stopwatch.get().reset();
            backpressureStats.add(delayMillis, MILLISECONDS);
        }
    }

    private Response executeWithRetries(CheckedSupplier<Response> supplier)
            throws IOException
    {
        try {
            return Failsafe.with(retryPolicy).get(supplier);
        }
        catch (FailsafeException e) {
            Throwable throwable = e.getCause();
            throwIfInstanceOf(throwable, IOException.class);
            throwIfUnchecked(throwable);
            throw new RuntimeException("Unexpected cause from FailsafeException", throwable);
        }
    }

    private void onFailedAttempt(ExecutionAttemptedEvent<Response> executionAttemptedEvent)
    {
        log.debug("REST attempt failed: %s", executionAttemptedEvent.getLastException());
        if (!stopwatch.get().isRunning()) {
            stopwatch.get().start();
        }
    }
}
