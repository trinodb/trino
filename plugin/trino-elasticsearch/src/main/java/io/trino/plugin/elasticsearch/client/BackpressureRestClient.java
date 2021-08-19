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
package io.trino.plugin.elasticsearch.client;

import com.google.common.base.Stopwatch;
import io.airlift.log.Logger;
import io.airlift.stats.TimeStat;
import io.trino.plugin.elasticsearch.ElasticsearchConfig;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.event.ExecutionAttemptedEvent;
import net.jodah.failsafe.event.ExecutionCompletedEvent;
import net.jodah.failsafe.function.CheckedSupplier;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BackpressureRestClient
{
    private static final Logger log = Logger.get(BackpressureRestClient.class);

    private final RestClient delegate;
    private final RetryPolicy<Response> retryPolicy;
    private final TimeStat backpressureStats;
    private final ThreadLocal<Stopwatch> stopwatch = ThreadLocal.withInitial(Stopwatch::createUnstarted);

    public BackpressureRestClient(RestClient delegate, ElasticsearchConfig config, TimeStat backpressureStats)
    {
        this.delegate = requireNonNull(delegate, "restClient is null");
        this.backpressureStats = requireNonNull(backpressureStats, "backpressureStats is null");
        requireNonNull(config, "config is null");
        retryPolicy = new RetryPolicy<Response>()
                .withMaxAttempts(-1)
                .withMaxDuration(java.time.Duration.ofMillis(config.getMaxRetryTime().toMillis()))
                .withBackoff(config.getBackoffInitDelay().toMillis(), config.getBackoffMaxDelay().toMillis(), MILLIS)
                .withJitter(0.125)
                .handleIf(BackpressureRestClient::isBackpressure)
                .onFailedAttempt(this::onFailedAttempt)
                .onSuccess(this::onComplete)
                .onFailure(this::onComplete);
    }

    public void setHosts(HttpHost... hosts)
    {
        delegate.setHosts(hosts);
    }

    public Response performRequest(String method, String endpoint, Header... headers)
            throws IOException
    {
        return executeWithRetries(() -> delegate.performRequest(method, endpoint, headers));
    }

    public Response performRequest(String method, String endpoint, Map<String, String> params, HttpEntity entity, Header... headers)
            throws IOException
    {
        return executeWithRetries(() -> delegate.performRequest(method, endpoint, params, entity, headers));
    }

    public void close()
            throws IOException
    {
        delegate.close();
    }

    private static boolean isBackpressure(Throwable throwable)
    {
        return (throwable instanceof ResponseException) &&
                (((ResponseException) throwable).getResponse().getStatusLine().getStatusCode() == RestStatus.TOO_MANY_REQUESTS.getStatus());
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
        log.debug("REST attempt failed: %s", executionAttemptedEvent.getLastFailure());
        if (!stopwatch.get().isRunning()) {
            stopwatch.get().start();
        }
    }
}
