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
package io.trino.server.remotetask;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.SpanBuilder;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.TaskId;
import io.trino.server.DynamicFilterService;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.util.concurrent.Futures.addCallback;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTERS_VERSION;
import static io.trino.server.InternalHeaders.TRINO_CURRENT_VERSION;
import static io.trino.server.InternalHeaders.TRINO_MAX_WAIT;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class DynamicFiltersFetcher
{
    private final TaskId taskId;
    private final URI taskUri;
    private final Consumer<Throwable> onFail;
    private final JsonCodec<VersionedDynamicFilterDomains> dynamicFilterDomainsCodec;
    private final Duration refreshMaxWait;
    private final Executor executor;
    private final HttpClient httpClient;
    private final Supplier<SpanBuilder> spanBuilderFactory;
    private final RequestErrorTracker errorTracker;
    private final RemoteTaskStats stats;
    private final DynamicFilterService dynamicFilterService;

    @GuardedBy("this")
    private long dynamicFiltersVersion = INITIAL_DYNAMIC_FILTERS_VERSION;
    @GuardedBy("this")
    private long localDynamicFiltersVersion = INITIAL_DYNAMIC_FILTERS_VERSION;
    @GuardedBy("this")
    private boolean running;
    @GuardedBy("this")
    private ListenableFuture<JsonResponse<VersionedDynamicFilterDomains>> future;

    public DynamicFiltersFetcher(
            Consumer<Throwable> onFail,
            TaskId taskId,
            URI taskUri,
            Duration refreshMaxWait,
            JsonCodec<VersionedDynamicFilterDomains> dynamicFilterDomainsCodec,
            Executor executor,
            HttpClient httpClient,
            Supplier<SpanBuilder> spanBuilderFactory,
            Duration maxErrorDuration,
            ScheduledExecutorService errorScheduledExecutor,
            RemoteTaskStats stats,
            DynamicFilterService dynamicFilterService)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskUri = requireNonNull(taskUri, "taskUri is null");
        this.onFail = requireNonNull(onFail, "onFail is null");

        this.refreshMaxWait = requireNonNull(refreshMaxWait, "refreshMaxWait is null");
        this.dynamicFilterDomainsCodec = requireNonNull(dynamicFilterDomainsCodec, "dynamicFilterDomainsCodec is null");

        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.spanBuilderFactory = requireNonNull(spanBuilderFactory, "spanBuilderFactory is null");

        this.errorTracker = new RequestErrorTracker(taskId, taskUri, maxErrorDuration, errorScheduledExecutor, "getting dynamic filter domains");
        this.stats = requireNonNull(stats, "stats is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
    }

    public synchronized void start()
    {
        if (running) {
            // already running
            return;
        }
        running = true;
        fetchDynamicFiltersIfNecessary();
    }

    public synchronized void updateDynamicFiltersVersionAndFetchIfNecessary(long newDynamicFiltersVersion)
    {
        if (dynamicFiltersVersion >= newDynamicFiltersVersion) {
            return;
        }

        dynamicFiltersVersion = newDynamicFiltersVersion;
        fetchDynamicFiltersIfNecessary();
    }

    private synchronized void stop()
    {
        running = false;
    }

    @VisibleForTesting
    synchronized boolean isRunning()
    {
        return running;
    }

    private synchronized void fetchDynamicFiltersIfNecessary()
    {
        // stopped?
        if (!running) {
            return;
        }

        // local dynamic filters are up to date
        if (localDynamicFiltersVersion >= dynamicFiltersVersion) {
            return;
        }

        // outstanding request?
        if (future != null && !future.isDone()) {
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<Void> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::fetchDynamicFiltersIfNecessary, executor);
            return;
        }

        Request request = prepareGet()
                .setUri(uriBuilderFrom(taskUri).appendPath("dynamicfilters").build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(TRINO_CURRENT_VERSION, Long.toString(localDynamicFiltersVersion))
                .setHeader(TRINO_MAX_WAIT, refreshMaxWait.toString())
                .setSpanBuilder(spanBuilderFactory.get())
                .build();

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, createFullJsonResponseHandler(dynamicFilterDomainsCodec));
        addCallback(future, new SimpleHttpResponseHandler<>(new DynamicFiltersResponseCallback(dynamicFiltersVersion), request.getUri(), stats), executor);
    }

    private class DynamicFiltersResponseCallback
            implements SimpleHttpResponseCallback<VersionedDynamicFilterDomains>
    {
        private final long requestStartNanos = System.nanoTime();
        private final long requestedDynamicFiltersVersion;

        public DynamicFiltersResponseCallback(long requestedDynamicFiltersVersion)
        {
            this.requestedDynamicFiltersVersion = requestedDynamicFiltersVersion;
        }

        @Override
        public void success(VersionedDynamicFilterDomains newDynamicFilterDomains)
        {
            try (SetThreadName _ = new SetThreadName("DynamicFiltersFetcher-" + taskId)) {
                updateStats(requestStartNanos);
                if (newDynamicFilterDomains.getVersion() < requestedDynamicFiltersVersion) {
                    // Receiving older dynamic filter shouldn't happen unless
                    // a worker is restarted and a new task was created as a byproduct
                    // of the dynamic filter request. In that case, the task should be failed.
                    stop();
                    onFail.accept(new TrinoException(
                            GENERIC_INTERNAL_ERROR,
                            format("Dynamic filter response version (%s) is older than requested version (%s)", newDynamicFilterDomains.getVersion(), requestedDynamicFiltersVersion)));
                }
                else {
                    updateDynamicFilterDomains(newDynamicFilterDomains);
                    errorTracker.requestSucceeded();
                    fetchDynamicFiltersIfNecessary();
                }
            }
            finally {
                cleanupRequest();
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            try (SetThreadName _ = new SetThreadName("DynamicFiltersFetcher-" + taskId)) {
                updateStats(requestStartNanos);
                errorTracker.requestFailed(cause);
                fetchDynamicFiltersIfNecessary();
            }
            catch (Error e) {
                stop();
                onFail.accept(e);
                throw e;
            }
            catch (RuntimeException e) {
                stop();
                onFail.accept(e);
            }
            finally {
                cleanupRequest();
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName _ = new SetThreadName("DynamicFiltersFetcher-" + taskId)) {
                updateStats(requestStartNanos);
                stop();
                onFail.accept(cause);
            }
            finally {
                cleanupRequest();
            }
        }
    }

    private synchronized void cleanupRequest()
    {
        if (future != null && future.isDone()) {
            // remove outstanding reference to JSON response
            future = null;
        }
    }

    private void updateDynamicFilterDomains(VersionedDynamicFilterDomains newDynamicFilterDomains)
    {
        synchronized (this) {
            if (localDynamicFiltersVersion >= newDynamicFilterDomains.getVersion()) {
                // newer dynamic filters were already received
                return;
            }

            localDynamicFiltersVersion = newDynamicFilterDomains.getVersion();
            if (dynamicFiltersVersion < localDynamicFiltersVersion) {
                dynamicFiltersVersion = localDynamicFiltersVersion;
            }
        }

        // Subsequent DF versions can be narrowing down only. Therefore order in which they are intersected
        // (and passed to dynamic filter service) doesn't matter.
        dynamicFilterService.addTaskDynamicFilters(taskId, newDynamicFilterDomains.getDynamicFilterDomains());
    }

    private void updateStats(long currentRequestStartNanos)
    {
        stats.statusRoundTripMillis(nanosSince(currentRequestStartNanos).toMillis());
    }
}
