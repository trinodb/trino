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
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.SpanBuilder;
import io.trino.execution.StateMachine;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.SpoolingOutputStats;
import io.trino.operator.RetryPolicy;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.operator.RetryPolicy.TASK;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TaskInfoFetcher
{
    private static final Logger log = Logger.get(TaskInfoFetcher.class);

    private static final SpoolingOutputStats.Snapshot ALREADY_RETRIEVED_MARKER = new SpoolingOutputStats.Snapshot(Slices.EMPTY_SLICE, 0);

    private final TaskId taskId;
    private final Consumer<Throwable> onFail;
    private final ContinuousTaskStatusFetcher taskStatusFetcher;
    private final StateMachine<TaskInfo> taskInfo;
    private final StateMachine<Optional<TaskInfo>> finalTaskInfo;
    private final JsonCodec<TaskInfo> taskInfoCodec;

    private final long updateIntervalMillis;
    private final AtomicLong lastUpdateNanos = new AtomicLong();
    private final ScheduledExecutorService updateScheduledExecutor;

    private final Executor executor;
    private final HttpClient httpClient;
    private final Supplier<SpanBuilder> spanBuilderFactory;
    private final RequestErrorTracker errorTracker;

    private final boolean summarizeTaskInfo;
    private final RemoteTaskStats stats;
    private final Optional<DataSize> estimatedMemory;

    private final AtomicReference<SpoolingOutputStats.Snapshot> spoolingOutputStats = new AtomicReference<>();

    private final RetryPolicy retryPolicy;

    @GuardedBy("this")
    private boolean running;

    @GuardedBy("this")
    private ScheduledFuture<?> scheduledFuture;

    @GuardedBy("this")
    private ListenableFuture<FullJsonResponseHandler.JsonResponse<TaskInfo>> future;

    public TaskInfoFetcher(
            Consumer<Throwable> onFail,
            ContinuousTaskStatusFetcher taskStatusFetcher,
            TaskInfo initialTask,
            HttpClient httpClient,
            Supplier<SpanBuilder> spanBuilderFactory,
            Duration updateInterval,
            JsonCodec<TaskInfo> taskInfoCodec,
            Duration maxErrorDuration,
            boolean summarizeTaskInfo,
            Executor executor,
            ScheduledExecutorService updateScheduledExecutor,
            ScheduledExecutorService errorScheduledExecutor,
            RemoteTaskStats stats,
            Optional<DataSize> estimatedMemory,
            RetryPolicy retryPolicy)
    {
        requireNonNull(initialTask, "initialTask is null");
        requireNonNull(errorScheduledExecutor, "errorScheduledExecutor is null");

        this.taskId = initialTask.taskStatus().getTaskId();
        this.onFail = requireNonNull(onFail, "onFail is null");
        this.taskStatusFetcher = requireNonNull(taskStatusFetcher, "taskStatusFetcher is null");
        this.taskInfo = new StateMachine<>("task " + taskId, executor, initialTask);
        this.finalTaskInfo = new StateMachine<>("task-" + taskId, executor, Optional.empty());
        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");

        this.updateIntervalMillis = updateInterval.toMillis();
        this.updateScheduledExecutor = requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        this.errorTracker = new RequestErrorTracker(taskId, initialTask.taskStatus().getSelf(), maxErrorDuration, errorScheduledExecutor, "getting info for task");

        this.summarizeTaskInfo = summarizeTaskInfo;

        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.spanBuilderFactory = requireNonNull(spanBuilderFactory, "spanBuilderFactory is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.estimatedMemory = requireNonNull(estimatedMemory, "estimatedMemory is null");
        this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
    }

    public TaskInfo getTaskInfo()
    {
        return taskInfo.get();
    }

    public synchronized void start()
    {
        if (running) {
            // already running
            return;
        }
        running = true;
        scheduleUpdate();
    }

    private synchronized void stop()
    {
        running = false;
        if (future != null) {
            future.cancel(true);
            future = null;
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
    }

    /**
     * Add a listener for the final task info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addFinalTaskInfoListener(StateChangeListener<TaskInfo> stateChangeListener)
    {
        AtomicBoolean done = new AtomicBoolean();
        StateChangeListener<Optional<TaskInfo>> fireOnceStateChangeListener = finalTaskInfo -> {
            if (finalTaskInfo.isPresent() && done.compareAndSet(false, true)) {
                stateChangeListener.stateChanged(finalTaskInfo.get());
            }
        };
        finalTaskInfo.addStateChangeListener(fireOnceStateChangeListener);
        fireOnceStateChangeListener.stateChanged(finalTaskInfo.get());
    }

    public Optional<SpoolingOutputStats.Snapshot> retrieveAndDropSpoolingOutputStats()
    {
        Optional<TaskInfo> finalTaskInfo = this.finalTaskInfo.get();
        checkState(finalTaskInfo.isPresent(), "finalTaskInfo must be present");
        TaskState taskState = finalTaskInfo.get().taskStatus().getState();
        checkState(taskState == TaskState.FINISHED, "task must be FINISHED, got: %s", taskState);
        SpoolingOutputStats.Snapshot result = spoolingOutputStats.getAndSet(ALREADY_RETRIEVED_MARKER);
        checkState(result != ALREADY_RETRIEVED_MARKER, "spooling output stats were already retrieved");
        return Optional.ofNullable(result);
    }

    private synchronized void scheduleUpdate()
    {
        scheduledFuture = updateScheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                synchronized (this) {
                    // if the previous request still running, don't schedule a new request
                    if (future != null && !future.isDone()) {
                        return;
                    }
                }
                if (nanosSince(lastUpdateNanos.get()).toMillis() >= updateIntervalMillis) {
                    sendNextRequest();
                }
            }
            catch (Throwable e) {
                // ignore to avoid getting unscheduled
                log.error(e, "Unexpected error while getting task info");
            }
        }, 0, 100, MILLISECONDS);
    }

    private synchronized void sendNextRequest()
    {
        TaskStatus taskStatus = getTaskInfo().taskStatus();

        if (!running) {
            return;
        }

        // we already have the final task info
        if (isDone(getTaskInfo())) {
            stop();
            return;
        }

        // if we have an outstanding request
        if (future != null && !future.isDone()) {
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<Void> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::sendNextRequest, executor);
            return;
        }

        HttpUriBuilder httpUriBuilder = uriBuilderFrom(taskStatus.getSelf());
        URI uri = summarizeTaskInfo ? httpUriBuilder.addParameter("summarize").build() : httpUriBuilder.build();
        Request request = prepareGet()
                .setUri(uri)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setSpanBuilder(spanBuilderFactory.get())
                .build();

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskInfoCodec));
        Futures.addCallback(future, new SimpleHttpResponseHandler<>(new TaskInfoResponseCallback(), request.getUri(), stats), executor);
    }

    synchronized void updateTaskInfo(TaskInfo newTaskInfo)
    {
        TaskStatus localTaskStatus = taskStatusFetcher.getTaskStatus();
        TaskStatus newRemoteTaskStatus = newTaskInfo.taskStatus();

        if (!newRemoteTaskStatus.getTaskId().equals(taskId)) {
            log.debug("Task ID mismatch on remote task status. Member task ID is %s, but remote task ID is %s. This will confuse finalTaskInfo listeners.", taskId, newRemoteTaskStatus.getTaskId());
        }

        if (localTaskStatus.getState().isDone() && newRemoteTaskStatus.getState().isDone() && localTaskStatus.getState() != newRemoteTaskStatus.getState()) {
            // prefer local
            newTaskInfo = newTaskInfo.withTaskStatus(localTaskStatus);
            if (!localTaskStatus.getTaskId().equals(taskId)) {
                log.debug("Task ID mismatch on local task status. Member task ID is %s, but status-fetcher ID is %s. This will confuse finalTaskInfo listeners.", taskId, newRemoteTaskStatus.getTaskId());
            }
        }

        if (estimatedMemory.isPresent()) {
            newTaskInfo = newTaskInfo.withEstimatedMemory(estimatedMemory.get());
        }

        boolean missingSpoolingOutputStats = false;
        if (newTaskInfo.taskStatus().getState().isDone()) {
            boolean wasSet = spoolingOutputStats.compareAndSet(null, newTaskInfo.outputBuffers().getSpoolingOutputStats().orElse(null));
            if (newTaskInfo.taskStatus().getState() == TaskState.FINISHED && retryPolicy == TASK && wasSet && spoolingOutputStats.get() == null) {
                missingSpoolingOutputStats = true;
                if (log.isDebugEnabled()) {
                    log.debug("Task %s was updated to null spoolingOutputStats. Future calls to retrieveAndDropSpoolingOutputStats will fail; taskInfo=%s", taskId, taskInfoCodec.toJson(newTaskInfo));
                }
            }
            newTaskInfo = newTaskInfo.pruneSpoolingOutputStats();
        }

        TaskInfo newValue = newTaskInfo;
        boolean updated = taskInfo.setIf(newValue, oldValue -> {
            TaskStatus oldTaskStatus = oldValue.taskStatus();
            TaskStatus newTaskStatus = newValue.taskStatus();
            if (oldTaskStatus.getState().isDone()) {
                // never update if the task has reached a terminal state
                return false;
            }
            // don't update to an older version (same version is ok)
            return newTaskStatus.getVersion() >= oldTaskStatus.getVersion();
        });

        if (updated && newValue.taskStatus().getState().isDone()) {
            taskStatusFetcher.updateTaskStatus(newTaskInfo.taskStatus());
            boolean finalTaskInfoUpdated = finalTaskInfo.compareAndSet(Optional.empty(), Optional.of(newValue));
            if (missingSpoolingOutputStats && finalTaskInfoUpdated) {
                log.debug("Updated finalTaskInfo for task %s to one with missing spoolingOutputStats", taskId);
            }
            stop();
        }
    }

    private class TaskInfoResponseCallback
            implements SimpleHttpResponseCallback<TaskInfo>
    {
        private final long requestStartNanos = System.nanoTime();

        @Override
        public void success(TaskInfo newValue)
        {
            try (SetThreadName _ = new SetThreadName("TaskInfoFetcher-" + taskId)) {
                lastUpdateNanos.set(System.nanoTime());

                updateStats(requestStartNanos);
                errorTracker.requestSucceeded();
                updateTaskInfo(newValue);
            }
            finally {
                cleanupRequest();
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            try (SetThreadName _ = new SetThreadName("TaskInfoFetcher-" + taskId)) {
                lastUpdateNanos.set(System.nanoTime());

                // if task not already done, record error
                if (!isDone(getTaskInfo())) {
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
                cleanupRequest();
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName _ = new SetThreadName("TaskInfoFetcher-" + taskId)) {
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

    private void updateStats(long currentRequestStartNanos)
    {
        stats.infoRoundTripMillis(nanosSince(currentRequestStartNanos).toMillis());
    }

    private static boolean isDone(TaskInfo taskInfo)
    {
        return taskInfo.taskStatus().getState().isDone();
    }
}
