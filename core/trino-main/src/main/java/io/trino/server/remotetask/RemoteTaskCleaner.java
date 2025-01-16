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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.log.Logger;
import io.opentelemetry.api.trace.SpanBuilder;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;

import java.net.URI;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.util.Objects.requireNonNull;

public class RemoteTaskCleaner
{
    private static final Logger log = Logger.get(RemoteTaskCleaner.class);

    private final TaskId taskId;
    private final URI taskUri;
    private final HttpClient httpClient;
    private final Executor executor;
    private final Supplier<SpanBuilder> spanBuilderFactory;

    @GuardedBy("this")
    private boolean taskStatusFetcherStopped;

    @GuardedBy("this")
    private boolean taskInfoFetcherStopped;

    @GuardedBy("this")
    private boolean dynamicFilterFetcherStopped;

    @GuardedBy("this")
    private TaskState taskState;

    public RemoteTaskCleaner(TaskId taskId, URI taskUri, HttpClient httpClient, Executor executor, Supplier<SpanBuilder> spanBuilderFactory)
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

    public synchronized void markDynamicFilterFetcherStopped()
    {
        if (dynamicFilterFetcherStopped) {
            return;
        }
        dynamicFilterFetcherStopped = true;
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
        if (taskStatusFetcherStopped && taskInfoFetcherStopped && dynamicFilterFetcherStopped) {
            scheduleCleanupRequest();
        }
    }

    private void scheduleCleanupRequest()
    {
        executor.execute(
                () -> {
                    Request request = preparePost()
                            .setUri(uriBuilderFrom(taskUri)
                                    .appendPath("/cleanup")
                                    .build())
                            .setSpanBuilder(spanBuilderFactory.get())
                            .build();

                    StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
                    if (response.getStatusCode() / 100 != 2) {
                        log.warn("Failed to cleanup task %s: %s", taskId, response.getStatusCode());
                    }
                });
    }
}
