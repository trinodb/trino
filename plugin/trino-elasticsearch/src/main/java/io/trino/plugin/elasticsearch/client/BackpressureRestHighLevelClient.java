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
import io.airlift.log.Logger;
import io.airlift.stats.TimeStat;
import io.trino.plugin.elasticsearch.ElasticsearchConfig;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

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
        delegate = new RestHighLevelClientBuilder(requireNonNull(restClientBuilder, "restClientBuilder is null").build())
                .build();
        backpressureRestClient = new BackpressureRestClient(delegate.getLowLevelClient(), config, backpressureStats);
        retryPolicy = RetryPolicy.<ActionResponse>builder()
                .withMaxAttempts(-1)
                .withMaxDuration(Duration.ofMillis(config.getMaxRetryTime().toMillis()))
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

    public SearchResponse search(SearchRequest searchRequest)
            throws IOException
    {
        return executeWithRetries(listener -> delegate.searchAsync(searchRequest, RequestOptions.DEFAULT, listener));
    }

    public SearchResponse searchScroll(SearchScrollRequest searchScrollRequest)
            throws IOException
    {
        return executeWithRetries(listener -> delegate.scrollAsync(searchScrollRequest, RequestOptions.DEFAULT, listener));
    }

    public ClearScrollResponse clearScroll(ClearScrollRequest clearScrollRequest)
            throws IOException
    {
        return executeWithRetries(listener -> delegate.clearScrollAsync(clearScrollRequest, RequestOptions.DEFAULT, listener));
    }

    private static boolean isBackpressure(Throwable throwable)
    {
        return throwable instanceof ElasticsearchStatusException elasticsearchStatusException &&
                elasticsearchStatusException.status() == RestStatus.TOO_MANY_REQUESTS;
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

    // Synchronous ES client methods keep requests running in the background when the calling thread is interrupted.
    // So we use async methods and cancel them explicitly on InterruptedException.
    private static <T extends ActionResponse> T executeCancellable(Function<ActionListener<T>, Cancellable> request)
            throws IOException
    {
        PlainActionFuture<T> future = PlainActionFuture.newFuture();
        Cancellable cancellable = request.apply(future);
        try {
            return future.get();
        }
        catch (InterruptedException e) {
            cancellable.cancel();
            Thread.currentThread().interrupt();
            throw new RuntimeException("ES request interrupted", e);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throwIfInstanceOf(cause, IOException.class);
            throwIfUnchecked(cause);
            throw new IOException(cause);
        }
    }

    private <T extends ActionResponse> T executeWithRetries(Function<ActionListener<T>, Cancellable> request)
            throws IOException
    {
        try {
            return Failsafe.with(retryPolicy).get(() -> executeCancellable(request));
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
