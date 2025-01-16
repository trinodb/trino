package io.trino.server.remotetask;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.log.Logger;
import io.opentelemetry.api.trace.SpanBuilder;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.preparePost;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RemoteTaskCleaner
{
    private static final Logger log = Logger.get(RemoteTaskCleaner.class);

    private static final long TASK_CLEANUP_DELAY_VARIANCE_MILLIS = 10_000;

    private final TaskId taskId;
    private final URI taskUri;
    private final HttpClient httpClient;
    private final ScheduledExecutorService executor;
    private final Supplier<SpanBuilder> spanBuilderFactory;

    @GuardedBy("this")
    private boolean taskStatusFetcherStopped;

    @GuardedBy("this")
    private boolean taskInfoFetcherStopped;

    @GuardedBy("this")
    private boolean dynamidFilterFetcherStopped;

    @GuardedBy("this")
    private TaskState taskState;

    public RemoteTaskCleaner(TaskId taskId, URI taskUri, HttpClient httpClient, ScheduledExecutorService executor, Supplier<SpanBuilder> spanBuilderFactory)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskUri = requireNonNull(taskUri, "taskUri is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.spanBuilderFactory = requireNonNull(spanBuilderFactory, "spanBuilderFactory is null");
    }

    public synchronized void markTaskStatusFetcherStopped(TaskState taskState)
    {
        if (taskStatusFetcherStopped) {
            return;
        }
        taskStatusFetcherStopped = true;
        this.taskState = taskState;
        cleanupIfReady();
    }

    public synchronized void markTaskInfoFetcherStopped()
    {
        if (taskInfoFetcherStopped) {
            return;
        }
        taskInfoFetcherStopped = true;
        cleanupIfReady();
    }

    public synchronized void markDynamidFilterFetcherStopped()
    {
        if (dynamidFilterFetcherStopped) {
            return;
        }
        dynamidFilterFetcherStopped = true;
        cleanupIfReady();
    }

    @GuardedBy("this")
    private void cleanupIfReady()
    {
        if (taskState != TaskState.FINISHED) {
            // we do not perform early cleanup if task did not finish successfully.
            // other workers may still reach out for the results; and we have no control over that.
            return;
        }
        if (taskStatusFetcherStopped && taskInfoFetcherStopped && dynamidFilterFetcherStopped) {
            scheduleCleanupRequest();
        }
    }

    private void scheduleCleanupRequest()
    {
        executor.schedule(
                () -> {
                    Request request = preparePost()
                            .setUri(uriBuilderFrom(taskUri)
                                    .appendPath("/cleanup")
                                    .build())
                            .setSpanBuilder(spanBuilderFactory.get())
                            .build();

                    StatusResponseHandler.StatusResponse response = httpClient.execute(request, StatusResponseHandler.createStatusResponseHandler());
                    if (response.getStatusCode() != 200) {
                        log.warn("Failed to cleanup task %s: %s", taskId, response.getStatusCode());
                    }
                    return null;
                },
                ThreadLocalRandom.current().nextLong(TASK_CLEANUP_DELAY_VARIANCE_MILLIS),
                MILLISECONDS);
    }
}
