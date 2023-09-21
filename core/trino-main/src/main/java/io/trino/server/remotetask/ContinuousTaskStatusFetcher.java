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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.SpanBuilder;
import io.trino.execution.StateMachine;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStatus;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.server.InternalHeaders.TRINO_CURRENT_VERSION;
import static io.trino.server.InternalHeaders.TRINO_MAX_WAIT;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static io.trino.util.Failures.REMOTE_TASK_MISMATCH_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class ContinuousTaskStatusFetcher
{
    private static final Logger log = Logger.get(ContinuousTaskStatusFetcher.class);

    private final TaskId taskId;
    private final Consumer<Throwable> onFail;
    private final StateMachine<TaskStatus> taskStatus;
    private final JsonCodec<TaskStatus> taskStatusCodec;
    private final DynamicFiltersFetcher dynamicFiltersFetcher;

    private final Duration refreshMaxWait;
    private final Executor executor;
    private final HttpClient httpClient;
    private final Supplier<SpanBuilder> spanBuilderFactory;
    private final RequestErrorTracker errorTracker;
    private final RemoteTaskStats stats;

    @GuardedBy("this")
    private boolean running;

    @GuardedBy("this")
    private ListenableFuture<FullJsonResponseHandler.JsonResponse<TaskStatus>> future;

    public ContinuousTaskStatusFetcher(
            Consumer<Throwable> onFail,
            TaskStatus initialTaskStatus,
            Duration refreshMaxWait,
            JsonCodec<TaskStatus> taskStatusCodec,
            DynamicFiltersFetcher dynamicFiltersFetcher,
            Executor executor,
            HttpClient httpClient,
            Supplier<SpanBuilder> spanBuilderFactory,
            Duration maxErrorDuration,
            ScheduledExecutorService errorScheduledExecutor,
            RemoteTaskStats stats)
    {
        requireNonNull(initialTaskStatus, "initialTaskStatus is null");

        this.taskId = initialTaskStatus.getTaskId();
        this.onFail = requireNonNull(onFail, "onFail is null");
        this.taskStatus = new StateMachine<>("task-" + taskId, executor, initialTaskStatus);

        this.refreshMaxWait = requireNonNull(refreshMaxWait, "refreshMaxWait is null");
        this.taskStatusCodec = requireNonNull(taskStatusCodec, "taskStatusCodec is null");
        this.dynamicFiltersFetcher = requireNonNull(dynamicFiltersFetcher, "dynamicFiltersFetcher is null");

        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.spanBuilderFactory = requireNonNull(spanBuilderFactory, "spanBuilderFactory is null");

        this.errorTracker = new RequestErrorTracker(taskId, initialTaskStatus.getSelf(), maxErrorDuration, errorScheduledExecutor, "getting task status");
        this.stats = requireNonNull(stats, "stats is null");
    }

    public synchronized void start()
    {
        if (running) {
            // already running
            return;
        }
        running = true;
        scheduleNextRequest();
    }

    public synchronized void stop()
    {
        running = false;
        if (future != null) {
            future.cancel(true);
            future = null;
        }
    }

    private synchronized void scheduleNextRequest()
    {
        // stopped or done?
        TaskStatus taskStatus = getTaskStatus();
        if (!running || taskStatus.getState().isDone()) {
            return;
        }

        // outstanding request?
        if (future != null && !future.isDone()) {
            // this should never happen
            log.error("Cannot reschedule update because an update is already running");
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<Void> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::scheduleNextRequest, executor);
            return;
        }

        Request request = prepareGet()
                .setUri(uriBuilderFrom(taskStatus.getSelf()).appendPath("status").build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(TRINO_CURRENT_VERSION, Long.toString(taskStatus.getVersion()))
                .setHeader(TRINO_MAX_WAIT, refreshMaxWait.toString())
                .setSpanBuilder(spanBuilderFactory.get())
                .build();

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskStatusCodec));
        Futures.addCallback(future, new SimpleHttpResponseHandler<>(new TaskStatusResponseCallback(), request.getUri(), stats), executor);
    }

    TaskStatus getTaskStatus()
    {
        return taskStatus.get();
    }

    private class TaskStatusResponseCallback
            implements SimpleHttpResponseCallback<TaskStatus>
    {
        private final long requestStartNanos = System.nanoTime();

        @Override
        public void success(TaskStatus value)
        {
            try (SetThreadName ignored = new SetThreadName("ContinuousTaskStatusFetcher-%s", taskId)) {
                updateStats(requestStartNanos);
                try {
                    updateTaskStatus(value);
                    errorTracker.requestSucceeded();
                }
                finally {
                    scheduleNextRequest();
                }
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("ContinuousTaskStatusFetcher-%s", taskId)) {
                updateStats(requestStartNanos);
                try {
                    // if task not already done, record error
                    TaskStatus taskStatus = getTaskStatus();
                    if (!taskStatus.getState().isDone()) {
                        errorTracker.requestFailed(cause);
                    }
                }
                catch (Error e) {
                    onFail.accept(e);
                    throw e;
                }
                catch (RuntimeException e) {
                    onFail.accept(e);
                }
                finally {
                    scheduleNextRequest();
                }
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("ContinuousTaskStatusFetcher-%s", taskId)) {
                updateStats(requestStartNanos);
                onFail.accept(cause);
            }
        }
    }

    void updateTaskStatus(TaskStatus newValue)
    {
        // change to new value if old value is not changed and new value has a newer version
        AtomicBoolean taskMismatch = new AtomicBoolean();
        taskStatus.setIf(newValue, oldValue -> {
            // did the task instance id change
            if (!isNullOrEmpty(oldValue.getTaskInstanceId()) && !oldValue.getTaskInstanceId().equals(newValue.getTaskInstanceId())) {
                taskMismatch.set(true);
                return false;
            }

            if (oldValue.getState().isDone()) {
                // never update if the task has reached a terminal state
                return false;
            }
            if (newValue.getVersion() < oldValue.getVersion()) {
                // don't update to an older version (same version is ok)
                return false;
            }
            return true;
        });

        if (taskMismatch.get()) {
            // This will also set the task status to FAILED state directly.
            // Additionally, this will issue a DELETE for the task to the worker.
            // While sending the DELETE is not required, it is preferred because a task was created by the previous request.
            onFail.accept(new TrinoException(REMOTE_TASK_MISMATCH, format("%s (%s)", REMOTE_TASK_MISMATCH_ERROR, HostAddress.fromUri(getTaskStatus().getSelf()))));
        }

        dynamicFiltersFetcher.updateDynamicFiltersVersionAndFetchIfNecessary(newValue.getDynamicFiltersVersion());
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateMachine.StateChangeListener<TaskStatus> stateChangeListener)
    {
        taskStatus.addStateChangeListener(stateChangeListener);
    }

    private void updateStats(long currentRequestStartNanos)
    {
        stats.statusRoundTripMillis(nanosSince(currentRequestStartNanos).toMillis());
    }
}
