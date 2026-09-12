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
import io.trino.execution.TaskId;
import io.trino.server.DynamicFilterService;
import io.trino.spi.TrinoException;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintContributionBatch;

import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.util.concurrent.Futures.addCallback;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.server.InternalHeaders.TRINO_MAX_WAIT_HEADER;
import static io.trino.server.InternalHeaders.TRINO_RUNTIME_CONSTRAINT_SEQUENCE_HEADER;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class RuntimeConstraintFetcher
{
    private final TaskId taskId;
    private final URI taskUri;
    private final Consumer<Throwable> onFail;
    private final JsonCodec<RuntimeConstraintContributionBatch> codec;
    private final Duration refreshMaxWait;
    private final Executor executor;
    private final HttpClient httpClient;
    private final Supplier<SpanBuilder> spanBuilderFactory;
    private final RequestErrorTracker errorTracker;
    private final RemoteTaskStats stats;
    private final DynamicFilterService dynamicFilterService;
    private final Runnable contributionReceived;

    @GuardedBy("this")
    private long targetSequence;
    @GuardedBy("this")
    private long localSequence;
    @GuardedBy("this")
    private long confirmedSequence;
    @GuardedBy("this")
    private boolean running;
    @GuardedBy("this")
    private ListenableFuture<JsonResponse<RuntimeConstraintContributionBatch>> future;

    public RuntimeConstraintFetcher(
            Consumer<Throwable> onFail,
            TaskId taskId,
            URI taskUri,
            Duration refreshMaxWait,
            JsonCodec<RuntimeConstraintContributionBatch> codec,
            Executor executor,
            HttpClient httpClient,
            Supplier<SpanBuilder> spanBuilderFactory,
            Duration maxErrorDuration,
            ScheduledExecutorService errorScheduledExecutor,
            RemoteTaskStats stats,
            DynamicFilterService dynamicFilterService,
            Runnable contributionReceived)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskUri = requireNonNull(taskUri, "taskUri is null");
        this.onFail = requireNonNull(onFail, "onFail is null");
        this.refreshMaxWait = requireNonNull(refreshMaxWait, "refreshMaxWait is null");
        this.codec = requireNonNull(codec, "codec is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.spanBuilderFactory = requireNonNull(spanBuilderFactory, "spanBuilderFactory is null");
        this.errorTracker = new RequestErrorTracker(taskId, taskUri, maxErrorDuration, errorScheduledExecutor, "getting runtime constraint contributions");
        this.stats = requireNonNull(stats, "stats is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        this.contributionReceived = requireNonNull(contributionReceived, "contributionReceived is null");
    }

    public synchronized void start()
    {
        if (running) {
            return;
        }
        running = true;
        fetchIfNecessary();
    }

    public synchronized void updateSequenceAndFetchIfNecessary(long sequence)
    {
        if (targetSequence >= sequence && confirmedSequence >= localSequence) {
            return;
        }
        targetSequence = max(targetSequence, sequence);
        fetchIfNecessary();
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

    private synchronized void fetchIfNecessary()
    {
        if (!running || (localSequence >= targetSequence && confirmedSequence >= localSequence)) {
            return;
        }
        // A completed HTTP future can still have a queued response callback. Keep
        // that request in flight until its batch has been applied and acknowledged.
        if (future != null) {
            return;
        }

        ListenableFuture<Void> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::fetchIfNecessary, executor);
            return;
        }

        Request request = prepareGet()
                .setUri(uriBuilderFrom(taskUri).appendPath("runtimeconstraints").build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(TRINO_RUNTIME_CONSTRAINT_SEQUENCE_HEADER, Long.toString(localSequence))
                .setHeader(TRINO_MAX_WAIT_HEADER, refreshMaxWait.toString())
                .setSpanBuilder(spanBuilderFactory.get())
                .build();

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, createFullJsonResponseHandler(codec));
        addCallback(future, new SimpleHttpResponseHandler<>(
                new ResponseCallback(targetSequence, localSequence),
                request.getUri(),
                stats), executor);
    }

    private class ResponseCallback
            implements SimpleHttpResponseCallback<RuntimeConstraintContributionBatch>
    {
        private final long requestStartNanos = System.nanoTime();
        private final long requestedSequence;
        private final long acknowledgedSequence;

        private ResponseCallback(long requestedSequence, long acknowledgedSequence)
        {
            this.requestedSequence = requestedSequence;
            this.acknowledgedSequence = acknowledgedSequence;
        }

        @Override
        public void success(RuntimeConstraintContributionBatch batch)
        {
            try (SetThreadName _ = new SetThreadName("RuntimeConstraintFetcher-" + taskId)) {
                updateStats(requestStartNanos);
                boolean madeProgress = requestedSequence <= acknowledgedSequence || batch.sequence() > acknowledgedSequence;
                if (batch.sequence() < acknowledgedSequence || !madeProgress) {
                    stop();
                    onFail.accept(new TrinoException(
                            GENERIC_INTERNAL_ERROR,
                            format("Runtime constraint response sequence %s is older than requested sequence %s", batch.sequence(), acknowledgedSequence)));
                }
                else {
                    confirm(acknowledgedSequence);
                    update(batch);
                    errorTracker.requestSucceeded();
                }
            }
            finally {
                cleanupRequest();
                fetchIfNecessary();
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            try (SetThreadName _ = new SetThreadName("RuntimeConstraintFetcher-" + taskId)) {
                updateStats(requestStartNanos);
                errorTracker.requestFailed(cause);
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
                fetchIfNecessary();
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName _ = new SetThreadName("RuntimeConstraintFetcher-" + taskId)) {
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
            future = null;
        }
    }

    private synchronized void confirm(long sequence)
    {
        confirmedSequence = max(confirmedSequence, sequence);
    }

    private void update(RuntimeConstraintContributionBatch batch)
    {
        synchronized (this) {
            if (localSequence >= batch.sequence()) {
                return;
            }
            localSequence = batch.sequence();
            targetSequence = max(targetSequence, localSequence);
        }
        dynamicFilterService.addTaskRuntimeConstraintContributions(taskId, batch);
        contributionReceived.run();
    }

    public synchronized long getSequence()
    {
        return localSequence;
    }

    private void updateStats(long currentRequestStartNanos)
    {
        stats.statusRoundTripMillis(nanosSince(currentRequestStartNanos).toMillis());
    }
}
