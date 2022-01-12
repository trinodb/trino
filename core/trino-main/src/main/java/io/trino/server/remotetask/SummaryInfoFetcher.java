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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.execution.DynamicFilterSummary;
import io.trino.execution.TaskId;
import io.trino.execution.VersionedSummaryInfoCollector.VersionedSummaryInfo;
import io.trino.server.DynamicFilterService;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.util.concurrent.Futures.addCallback;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.execution.SummaryInfo.Type.DYNAMIC_FILTER;
import static io.trino.execution.VersionedSummaryInfoCollector.INITIAL_SUMMARY_VERSION;
import static io.trino.server.InternalHeaders.TRINO_CURRENT_VERSION;
import static io.trino.server.InternalHeaders.TRINO_MAX_WAIT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class SummaryInfoFetcher
        implements SimpleHttpResponseCallback<VersionedSummaryInfo>
{
    private final TaskId taskId;
    private final URI taskUri;
    private final Consumer<Throwable> onFail;
    private final JsonCodec<VersionedSummaryInfo> versionedSummaryInfoCodec;
    private final Duration refreshMaxWait;
    private final Executor executor;
    private final HttpClient httpClient;
    private final RequestErrorTracker errorTracker;
    private final RemoteTaskStats stats;
    private final DynamicFilterService dynamicFilterService;
    private final AtomicLong currentRequestStartNanos = new AtomicLong();

    @GuardedBy("this")
    private long summaryVersion = INITIAL_SUMMARY_VERSION;
    @GuardedBy("this")
    private long localSummaryInfoVersion = INITIAL_SUMMARY_VERSION;
    @GuardedBy("this")
    private boolean running;
    @GuardedBy("this")
    private ListenableFuture<JsonResponse<VersionedSummaryInfo>> future;

    public SummaryInfoFetcher(
            Consumer<Throwable> onFail,
            TaskId taskId,
            URI taskUri,
            Duration refreshMaxWait,
            JsonCodec<VersionedSummaryInfo> versionedSummaryInfoCodec,
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
        this.versionedSummaryInfoCodec = requireNonNull(versionedSummaryInfoCodec, "versionedSummaryInfoCodec is null");

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
        fetchNewSummariesIfNecessary();
    }

    public synchronized void stop()
    {
        running = false;
        if (future != null) {
            future.cancel(true);
            future = null;
        }
    }

    public synchronized void updateSummaryInfoVersion(long newSummaryVersion)
    {
        if (summaryVersion >= newSummaryVersion) {
            return;
        }

        summaryVersion = newSummaryVersion;
        fetchNewSummariesIfNecessary();
    }

    private synchronized void fetchNewSummariesIfNecessary()
    {
        // stopped?
        if (!running) {
            return;
        }

        // local summary info are up-to-date
        if (localSummaryInfoVersion >= summaryVersion) {
            return;
        }

        // outstanding request?
        if (future != null && !future.isDone()) {
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<Void> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::fetchNewSummariesIfNecessary, executor);
            return;
        }

        Request request = prepareGet()
                .setUri(uriBuilderFrom(taskUri).appendPath("summaries").build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(TRINO_CURRENT_VERSION, Long.toString(localSummaryInfoVersion))
                .setHeader(TRINO_MAX_WAIT, refreshMaxWait.toString())
                .build();

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, createFullJsonResponseHandler(versionedSummaryInfoCodec));
        currentRequestStartNanos.set(System.nanoTime());
        addCallback(future, new SimpleHttpResponseHandler<>(this, request.getUri(), stats), executor);
    }

    @Override
    public void success(VersionedSummaryInfo newDynamicFilterDomains)
    {
        try (SetThreadName ignored = new SetThreadName("SummaryInfoFetcher-%s", taskId)) {
            updateStats(currentRequestStartNanos.get());
            try {
                updateSummaryInfo(newDynamicFilterDomains);
                errorTracker.requestSucceeded();
            }
            finally {
                fetchNewSummariesIfNecessary();
            }
        }
    }

    @Override
    public void failed(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("SummaryInfoFetcher-%s", taskId)) {
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
                fetchNewSummariesIfNecessary();
            }
        }
    }

    @Override
    public void fatal(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("SummaryInfoFetcher-%s", taskId)) {
            updateStats(currentRequestStartNanos.get());
            onFail.accept(cause);
        }
    }

    private void updateSummaryInfo(VersionedSummaryInfo newSummaryStats)
    {
        synchronized (this) {
            if (localSummaryInfoVersion >= newSummaryStats.getVersion()) {
                // newer summary stats were already received
                return;
            }

            localSummaryInfoVersion = newSummaryStats.getVersion();
            updateSummaryInfoVersion(localSummaryInfoVersion);
        }

        newSummaryStats.getSummaryInfo().stream()
                .filter(summaryInfo -> !summaryInfo.isEmpty())
                .forEach(summaryInfo -> {
                    if (summaryInfo.getType() == DYNAMIC_FILTER) {
                        DynamicFilterSummary newDynamicFilterSummary = ((DynamicFilterSummary) summaryInfo);
                        // Subsequent DF versions can be narrowing down only. Therefore, order in which they are intersected
                        // (and passed to dynamic filter service) doesn't matter.
                        dynamicFilterService.addTaskDynamicFilters(taskId, newDynamicFilterSummary.getDynamicFilterDomains());
                    }
                    else {
                        throw new IllegalArgumentException(format("Summary info of type %s is not supported", summaryInfo.getType()));
                    }
                });
    }

    private void updateStats(long currentRequestStartNanos)
    {
        stats.statusRoundTripMillis(nanosSince(currentRequestStartNanos).toMillis());
    }
}
