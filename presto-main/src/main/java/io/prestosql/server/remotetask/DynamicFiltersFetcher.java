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
package io.prestosql.server.remotetask;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.prestosql.execution.DynamicFiltersCollector.VersionedDomain;
import io.prestosql.execution.TaskId;
import io.prestosql.server.DynamicFilterService;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.sql.planner.plan.DynamicFilterId;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.util.concurrent.Futures.addCallback;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static io.prestosql.execution.DynamicFiltersCollector.IGNORED_DYNAMIC_FILTER_VERSION;
import static io.prestosql.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTER_VERSION;
import static java.util.Objects.requireNonNull;

class DynamicFiltersFetcher
        implements SimpleHttpResponseCallback<Map<DynamicFilterId, VersionedDomain>>
{
    private final TaskId taskId;
    private final URI taskUri;
    private final Consumer<Throwable> onFail;
    private final JsonCodec<Map<DynamicFilterId, Long>> dynamicFilterVersionsCodec;
    private final JsonCodec<Map<DynamicFilterId, VersionedDomain>> dynamicFilterDomainsCodec;
    private final Duration refreshMaxWait;
    private final Executor executor;
    private final HttpClient httpClient;
    private final RequestErrorTracker errorTracker;
    private final RemoteTaskStats stats;
    private final DynamicFilterService dynamicFilterService;
    private final AtomicLong currentRequestStartNanos = new AtomicLong();

    @GuardedBy("this")
    private Map<DynamicFilterId, Long> dynamicFilterVersions = new HashMap<>();
    @GuardedBy("this")
    private Map<DynamicFilterId, Long> localDynamicFilterVersions = new HashMap<>();
    @GuardedBy("this")
    private boolean running;
    @GuardedBy("this")
    private ListenableFuture<JsonResponse<Map<DynamicFilterId, VersionedDomain>>> future;

    public DynamicFiltersFetcher(
            Consumer<Throwable> onFail,
            TaskId taskId,
            URI taskUri,
            Duration refreshMaxWait,
            JsonCodec<Map<DynamicFilterId, Long>> dynamicFilterVersionsCodec,
            JsonCodec<Map<DynamicFilterId, VersionedDomain>> dynamicFilterDomainsCodec,
            Executor executor,
            HttpClient httpClient,
            Duration maxErrorDuration,
            ScheduledExecutorService errorScheduledExecutor,
            RemoteTaskStats stats,
            DynamicFilterService dynamicFilterService)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskUri = requireNonNull(taskUri, "taskUri is null");
        this.onFail = requireNonNull(onFail, "onFail is null");

        this.refreshMaxWait = requireNonNull(refreshMaxWait, "refreshMaxWait is null");
        this.dynamicFilterVersionsCodec = requireNonNull(dynamicFilterVersionsCodec, "dynamicFilterVersionsCodec is null");
        this.dynamicFilterDomainsCodec = requireNonNull(dynamicFilterDomainsCodec, "dynamicFilterDomainsCodec is null");

        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

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

    public synchronized void stop()
    {
        running = false;
        if (future != null) {
            future.cancel(true);
            future = null;
        }
    }

    public synchronized void updateDynamicFilterVersions(Map<DynamicFilterId, Long> newDynamicFilterVersions)
    {
        boolean versionUpdated = false;
        for (DynamicFilterId filterId : newDynamicFilterVersions.keySet()) {
            if (dynamicFilterVersions.getOrDefault(filterId, INITIAL_DYNAMIC_FILTER_VERSION) >= newDynamicFilterVersions.get(filterId)) {
                // newer version of remote filter was already reported
                continue;
            }

            dynamicFilterVersions.put(filterId, newDynamicFilterVersions.get(filterId));
            versionUpdated = true;

            if (!localDynamicFilterVersions.containsKey(filterId)) {
                // dynamic filter was observed for the first time
                if (dynamicFilterService.isDynamicFilterNeeded(taskId.getQueryId(), filterId)) {
                    localDynamicFilterVersions.put(filterId, INITIAL_DYNAMIC_FILTER_VERSION);
                }
                else {
                    localDynamicFilterVersions.put(filterId, IGNORED_DYNAMIC_FILTER_VERSION);
                }
            }
        }

        if (!versionUpdated) {
            // no new version of remote filter was reported
            return;
        }

        fetchDynamicFiltersIfNecessary();
    }

    private synchronized void fetchDynamicFiltersIfNecessary()
    {
        // stopped?
        if (!running) {
            return;
        }

        // Check if local and remote dynamic filters have matching versions.
        // Filters can have non-matching version when:
        // 1. remote task has new domains available
        // 2. dynamic filter is ignored and IGNORED_DYNAMIC_FILTER_VERSION was set locally
        boolean filtersMatchVersion = localDynamicFilterVersions.entrySet().stream()
                .allMatch(entry -> dynamicFilterVersions.get(entry.getKey()).equals(entry.getValue()));
        if (filtersMatchVersion) {
            return;
        }

        // outstanding request?
        if (future != null && !future.isDone()) {
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::fetchDynamicFiltersIfNecessary, executor);
            return;
        }

        byte[] localDynamicFilterVersionsJson = dynamicFilterVersionsCodec.toJsonBytes(localDynamicFilterVersions);

        Request request = preparePost()
                .setUri(uriBuilderFrom(taskUri).appendPath("dynamicfilters").build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(PRESTO_MAX_WAIT, refreshMaxWait.toString())
                .setBodyGenerator(createStaticBodyGenerator(localDynamicFilterVersionsJson))
                .build();

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, createFullJsonResponseHandler(dynamicFilterDomainsCodec));
        currentRequestStartNanos.set(System.nanoTime());
        addCallback(future, new SimpleHttpResponseHandler<>(this, request.getUri(), stats), executor);
    }

    @Override
    public void success(Map<DynamicFilterId, VersionedDomain> newDynamicFilterDomains)
    {
        try (SetThreadName ignored = new SetThreadName("DynamicFiltersFetcher-%s", taskId)) {
            updateStats(currentRequestStartNanos.get());
            try {
                updateDynamicFilterDomains(newDynamicFilterDomains);
                errorTracker.requestSucceeded();
            }
            finally {
                fetchDynamicFiltersIfNecessary();
            }
        }
    }

    @Override
    public void failed(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("DynamicFiltersFetcher-%s", taskId)) {
            updateStats(currentRequestStartNanos.get());
            try {
                errorTracker.requestFailed(cause);
            }
            catch (Error e) {
                onFail.accept(e);
                throw e;
            }
            catch (RuntimeException e) {
                onFail.accept(e);
            }
            finally {
                fetchDynamicFiltersIfNecessary();
            }
        }
    }

    @Override
    public void fatal(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("DynamicFiltersFetcher-%s", taskId)) {
            updateStats(currentRequestStartNanos.get());
            onFail.accept(cause);
        }
    }

    private void updateDynamicFilterDomains(Map<DynamicFilterId, VersionedDomain> newVersionedDomains)
    {
        ImmutableMap.Builder<DynamicFilterId, Domain> newDomainsBuilder = ImmutableMap.builder();
        synchronized (this) {
            for (DynamicFilterId filterId : newVersionedDomains.keySet()) {
                VersionedDomain versionedDomain = newVersionedDomains.get(filterId);
                if (localDynamicFilterVersions.get(filterId) >= versionedDomain.getVersion()) {
                    // newer dynamic filter was already received
                    continue;
                }

                localDynamicFilterVersions.put(filterId, versionedDomain.getVersion());
                newDomainsBuilder.put(filterId, versionedDomain.getDomain().get());
            }

            updateDynamicFilterVersions(
                    newVersionedDomains.entrySet().stream()
                            .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getVersion())));
        }

        Map<DynamicFilterId, Domain> newDomains = newDomainsBuilder.build();
        if (!newDomains.isEmpty()) {
            dynamicFilterService.addTaskDynamicFilters(taskId, newDomains);
        }
    }

    private void updateStats(long currentRequestStartNanos)
    {
        stats.statusRoundTripMillis(nanosSince(currentRequestStartNanos).toMillis());
    }
}
