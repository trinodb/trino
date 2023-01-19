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
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import dev.failsafe.event.ExecutionAttemptedEvent;
import dev.failsafe.event.ExecutionCompletedEvent;
import dev.failsafe.function.CheckedSupplier;
import io.airlift.log.Logger;
import io.airlift.stats.TimeStat;
import io.trino.plugin.elasticsearch.ElasticsearchConfig;
import org.apache.http.Header;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;

import java.io.Closeable;
import java.io.IOException;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BackpressureRestHighLevelClient
        implements Closeable
{
    private static final Logger log = Logger.get(BackpressureRestHighLevelClient.class);

    private final RestHighLevelClient delegate;
    private final BackpressureRestClient backpressureRestClient;
    private final RetryPolicy<ActionResponse> retryPolicy;
    private final TimeStat backpressureStats;
    private final ThreadLocal<Stopwatch> stopwatch = ThreadLocal.withInitial(Stopwatch::createUnstarted);

    public BackpressureRestHighLevelClient(RestClientBuilder restClientBuilder, ElasticsearchConfig config, TimeStat backpressureStats)
    {
        this.backpressureStats = requireNonNull(backpressureStats, "backpressureStats is null");
        delegate = new RestHighLevelClient(requireNonNull(restClientBuilder, "restClientBuilder is null"));
        backpressureRestClient = new BackpressureRestClient(delegate.getLowLevelClient(), config, backpressureStats);
        retryPolicy = RetryPolicy.<ActionResponse>builder()
                .withMaxAttempts(-1)
                .withMaxDuration(java.time.Duration.ofMillis(config.getMaxRetryTime().toMillis()))
                .withBackoff(config.getBackoffInitDelay().toMillis(), config.getBackoffMaxDelay().toMillis(), MILLIS)
                .withJitter(0.125)
                .handleIf(BackpressureRestHighLevelClient::isBackpressure)
                .onFailedAttempt(this::onFailedAttempt)
                .onSuccess(this::onComplete)
                .onFailure(this::onComplete)
                .build();
    }

    public BackpressureRestClient getLowLevelClient()
    {
        return backpressureRestClient;
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    public SearchResponse search(SearchRequest searchRequest, Header... headers)
            throws IOException
    {
        return executeWithRetries(() -> delegate.search(searchRequest, headers));
    }

    public SearchResponse searchScroll(SearchScrollRequest searchScrollRequest, Header... headers)
            throws IOException
    {
        return executeWithRetries(() -> delegate.searchScroll(searchScrollRequest, headers));
    }

    public ClearScrollResponse clearScroll(ClearScrollRequest clearScrollRequest, Header... headers)
            throws IOException
    {
        return executeWithRetries(() -> delegate.clearScroll(clearScrollRequest, headers));
    }

    private static boolean isBackpressure(Throwable throwable)
    {
        return (throwable instanceof ElasticsearchStatusException) &&
                (((ElasticsearchStatusException) throwable).status() == RestStatus.TOO_MANY_REQUESTS);
    }

    private void onComplete(ExecutionCompletedEvent<ActionResponse> executionCompletedEvent)
    {
        if (stopwatch.get().isRunning()) {
            long delayMillis = stopwatch.get().elapsed(MILLISECONDS);
            log.debug("Adding %s milliseconds to backpressure stats", delayMillis);
            stopwatch.get().reset();
            backpressureStats.add(delayMillis, MILLISECONDS);
        }
    }

    private <T extends ActionResponse> T executeWithRetries(CheckedSupplier<T> supplier)
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

    private void onFailedAttempt(ExecutionAttemptedEvent<ActionResponse> executionAttemptedEvent)
    {
        log.debug("REST attempt failed: %s", executionAttemptedEvent.getLastException());
        if (!stopwatch.get().isRunning()) {
            stopwatch.get().start();
        }
    }
}
