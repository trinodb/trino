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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.stats.TimeStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.FailureInjector;
import io.trino.execution.FailureInjector.InjectedFailure;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskManager;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.OutputBuffers.OutputBufferId;
import io.trino.metadata.SessionPropertyManager;
import io.trino.server.security.ResourceSecurity;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.CompletionCallback;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.trino.TrinoMediaTypes.TRINO_PAGES;
import static io.trino.server.InternalHeaders.TRINO_BUFFER_COMPLETE;
import static io.trino.server.InternalHeaders.TRINO_CURRENT_VERSION;
import static io.trino.server.InternalHeaders.TRINO_MAX_SIZE;
import static io.trino.server.InternalHeaders.TRINO_MAX_WAIT;
import static io.trino.server.InternalHeaders.TRINO_PAGE_NEXT_TOKEN;
import static io.trino.server.InternalHeaders.TRINO_PAGE_TOKEN;
import static io.trino.server.InternalHeaders.TRINO_TASK_FAILED;
import static io.trino.server.InternalHeaders.TRINO_TASK_INSTANCE_ID;
import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Manages tasks on this worker node
 */
@Path("/v1/task")
public class TaskResource
{
    private static final Logger log = Logger.get(TaskResource.class);

    private static final Duration ADDITIONAL_WAIT_TIME = new Duration(5, SECONDS);
    private static final Duration DEFAULT_MAX_WAIT_TIME = new Duration(2, SECONDS);

    private final TaskManager taskManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final FailureInjector failureInjector;
    private final TimeStat readFromOutputBufferTime = new TimeStat();
    private final TimeStat resultsRequestTime = new TimeStat();

    @Inject
    public TaskResource(
            TaskManager taskManager,
            SessionPropertyManager sessionPropertyManager,
            @ForAsyncHttp BoundedExecutor responseExecutor,
            @ForAsyncHttp ScheduledExecutorService timeoutExecutor,
            FailureInjector failureInjector)
    {
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.failureInjector = requireNonNull(failureInjector, "failureInjector is null");
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<TaskInfo> getAllTaskInfo(@Context UriInfo uriInfo)
    {
        List<TaskInfo> allTaskInfo = taskManager.getAllTaskInfo();
        if (shouldSummarize(uriInfo)) {
            allTaskInfo = ImmutableList.copyOf(transform(allTaskInfo, TaskInfo::summarize));
        }
        return allTaskInfo;
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Path("{taskId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public void createOrUpdateTask(
            @PathParam("taskId") TaskId taskId,
            TaskUpdateRequest taskUpdateRequest,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(taskUpdateRequest, "taskUpdateRequest is null");

        Session session = taskUpdateRequest.getSession().toSession(sessionPropertyManager, taskUpdateRequest.getExtraCredentials());

        if (injectFailure(session.getTraceToken(), taskId, RequestType.CREATE_OR_UPDATE_TASK, asyncResponse)) {
            return;
        }

        TaskInfo taskInfo = taskManager.updateTask(session,
                taskId,
                taskUpdateRequest.getFragment(),
                taskUpdateRequest.getSources(),
                taskUpdateRequest.getOutputIds(),
                taskUpdateRequest.getDynamicFilterDomains());

        if (shouldSummarize(uriInfo)) {
            taskInfo = taskInfo.summarize();
        }

        asyncResponse.resume(Response.ok().entity(taskInfo).build());
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @GET
    @Path("{taskId}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getTaskInfo(
            @PathParam("taskId") TaskId taskId,
            @HeaderParam(TRINO_CURRENT_VERSION) Long currentVersion,
            @HeaderParam(TRINO_MAX_WAIT) Duration maxWait,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");

        if (injectFailure(taskManager.getTraceToken(taskId), taskId, RequestType.GET_TASK_INFO, asyncResponse)) {
            return;
        }

        if (currentVersion == null || maxWait == null) {
            TaskInfo taskInfo = taskManager.getTaskInfo(taskId);
            if (shouldSummarize(uriInfo)) {
                taskInfo = taskInfo.summarize();
            }
            asyncResponse.resume(taskInfo);
            return;
        }

        Duration waitTime = randomizeWaitTime(maxWait);
        ListenableFuture<TaskInfo> futureTaskInfo = addTimeout(
                taskManager.getTaskInfo(taskId, currentVersion),
                () -> taskManager.getTaskInfo(taskId),
                waitTime,
                timeoutExecutor);

        if (shouldSummarize(uriInfo)) {
            futureTaskInfo = Futures.transform(futureTaskInfo, TaskInfo::summarize, directExecutor());
        }

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindAsyncResponse(asyncResponse, futureTaskInfo, responseExecutor)
                .withTimeout(timeout);
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @GET
    @Path("{taskId}/status")
    @Produces(MediaType.APPLICATION_JSON)
    public void getTaskStatus(
            @PathParam("taskId") TaskId taskId,
            @HeaderParam(TRINO_CURRENT_VERSION) Long currentVersion,
            @HeaderParam(TRINO_MAX_WAIT) Duration maxWait,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");

        if (injectFailure(taskManager.getTraceToken(taskId), taskId, RequestType.GET_TASK_STATUS, asyncResponse)) {
            return;
        }

        if (currentVersion == null || maxWait == null) {
            TaskStatus taskStatus = taskManager.getTaskStatus(taskId);
            asyncResponse.resume(taskStatus);
            return;
        }

        Duration waitTime = randomizeWaitTime(maxWait);
        // TODO: With current implementation, a newly completed driver group won't trigger immediate HTTP response,
        // leading to a slight delay of approx 1 second, which is not a major issue for any query that are heavy weight enough
        // to justify group-by-group execution. In order to fix this, REST endpoint /v1/{task}/status will need change.
        ListenableFuture<TaskStatus> futureTaskStatus = addTimeout(
                taskManager.getTaskStatus(taskId, currentVersion),
                () -> taskManager.getTaskStatus(taskId),
                waitTime,
                timeoutExecutor);

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindAsyncResponse(asyncResponse, futureTaskStatus, responseExecutor)
                .withTimeout(timeout);
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @GET
    @Path("{taskId}/dynamicfilters")
    @Produces(MediaType.APPLICATION_JSON)
    public void acknowledgeAndGetNewDynamicFilterDomains(
            @PathParam("taskId") TaskId taskId,
            @HeaderParam(TRINO_CURRENT_VERSION) Long currentDynamicFiltersVersion,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(currentDynamicFiltersVersion, "currentDynamicFiltersVersion is null");

        if (injectFailure(taskManager.getTraceToken(taskId), taskId, RequestType.ACKNOWLEDGE_AND_GET_NEW_DYNAMIC_FILTER_DOMAINS, asyncResponse)) {
            return;
        }

        asyncResponse.resume(taskManager.acknowledgeAndGetNewDynamicFilterDomains(taskId, currentDynamicFiltersVersion));
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @DELETE
    @Path("{taskId}")
    @Produces(MediaType.APPLICATION_JSON)
    public TaskInfo deleteTask(
            @PathParam("taskId") TaskId taskId,
            @QueryParam("abort") @DefaultValue("true") boolean abort,
            @Context UriInfo uriInfo)
    {
        requireNonNull(taskId, "taskId is null");
        TaskInfo taskInfo;

        if (abort) {
            taskInfo = taskManager.abortTask(taskId);
        }
        else {
            taskInfo = taskManager.cancelTask(taskId);
        }

        if (shouldSummarize(uriInfo)) {
            taskInfo = taskInfo.summarize();
        }
        return taskInfo;
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @GET
    @Path("{taskId}/results/{bufferId}/{token}")
    @Produces(TRINO_PAGES)
    public void getResults(
            @PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") OutputBufferId bufferId,
            @PathParam("token") long token,
            @HeaderParam(TRINO_MAX_SIZE) DataSize maxSize,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        if (injectFailure(taskManager.getTraceToken(taskId), taskId, RequestType.GET_RESULTS, asyncResponse)) {
            return;
        }

        TaskState state = taskManager.getTaskStatus(taskId).getState();
        boolean taskFailed = state == TaskState.ABORTED || state == TaskState.FAILED;

        long start = System.nanoTime();
        ListenableFuture<BufferResult> bufferResultFuture = taskManager.getTaskResults(taskId, bufferId, token, maxSize);
        Duration waitTime = randomizeWaitTime(DEFAULT_MAX_WAIT_TIME);
        bufferResultFuture = addTimeout(
                bufferResultFuture,
                () -> BufferResult.emptyResults(taskManager.getTaskInstanceId(taskId), token, false),
                waitTime,
                timeoutExecutor);

        ListenableFuture<Response> responseFuture = Futures.transform(bufferResultFuture, result -> {
            List<Slice> serializedPages = result.getSerializedPages();

            GenericEntity<?> entity = null;
            Status status;
            if (serializedPages.isEmpty()) {
                status = Status.NO_CONTENT;
            }
            else {
                entity = new GenericEntity<>(serializedPages, new TypeToken<List<Slice>>() {}.getType());
                status = Status.OK;
            }

            return Response.status(status)
                    .entity(entity)
                    .header(TRINO_TASK_INSTANCE_ID, result.getTaskInstanceId())
                    .header(TRINO_PAGE_TOKEN, result.getToken())
                    .header(TRINO_PAGE_NEXT_TOKEN, result.getNextToken())
                    .header(TRINO_BUFFER_COMPLETE, result.isBufferComplete())
                    .header(TRINO_TASK_FAILED, taskFailed)
                    .build();
        }, directExecutor());

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindAsyncResponse(asyncResponse, responseFuture, responseExecutor)
                .withTimeout(timeout,
                        Response.status(Status.NO_CONTENT)
                                .header(TRINO_TASK_INSTANCE_ID, taskManager.getTaskInstanceId(taskId))
                                .header(TRINO_PAGE_TOKEN, token)
                                .header(TRINO_PAGE_NEXT_TOKEN, token)
                                .header(TRINO_BUFFER_COMPLETE, false)
                                .header(TRINO_TASK_FAILED, taskFailed)
                                .build());

        responseFuture.addListener(() -> readFromOutputBufferTime.add(Duration.nanosSince(start)), directExecutor());
        asyncResponse.register((CompletionCallback) throwable -> resultsRequestTime.add(Duration.nanosSince(start)));
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @GET
    @Path("{taskId}/results/{bufferId}/{token}/acknowledge")
    public void acknowledgeResults(
            @PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") OutputBufferId bufferId,
            @PathParam("token") long token)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        taskManager.acknowledgeTaskResults(taskId, bufferId, token);
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @DELETE
    @Path("{taskId}/results/{bufferId}")
    public void abortResults(
            @PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") OutputBufferId bufferId,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        if (injectFailure(taskManager.getTraceToken(taskId), taskId, RequestType.ABORT_RESULTS, asyncResponse)) {
            return;
        }

        taskManager.abortTaskResults(taskId, bufferId);
        asyncResponse.resume(Response.noContent().build());
    }

    private boolean injectFailure(
            Optional<String> traceToken,
            TaskId taskId,
            RequestType requestType,
            AsyncResponse asyncResponse)
    {
        if (traceToken.isEmpty()) {
            return false;
        }

        Optional<InjectedFailure> injectedFailure = failureInjector.getInjectedFailure(
                traceToken.get(),
                taskId.getStageId().getId(),
                taskId.getPartitionId(),
                taskId.getAttemptId());

        if (injectedFailure.isEmpty()) {
            return false;
        }

        InjectedFailure failure = injectedFailure.get();
        Duration timeout = failureInjector.getRequestTimeout();
        switch (failure.getInjectedFailureType()) {
            case TASK_MANAGEMENT_REQUEST_FAILURE:
                if (requestType.isTaskManagement()) {
                    log.info("Failing %s request for task %s", requestType, taskId);
                    asyncResponse.resume(Response.serverError().build());
                    return true;
                }
                break;
            case TASK_MANAGEMENT_REQUEST_TIMEOUT:
                if (requestType.isTaskManagement()) {
                    log.info("Timing out %s request for task %s", requestType, taskId);
                    asyncResponse.setTimeout(timeout.toMillis(), MILLISECONDS);
                    return true;
                }
                break;
            case TASK_GET_RESULTS_REQUEST_FAILURE:
                if (!requestType.isTaskManagement()) {
                    log.info("Failing %s request for task %s", requestType, taskId);
                    asyncResponse.resume(Response.serverError().build());
                    return true;
                }
                break;
            case TASK_GET_RESULTS_REQUEST_TIMEOUT:
                if (!requestType.isTaskManagement()) {
                    log.info("Timing out %s request for task %s", requestType, taskId);
                    asyncResponse.setTimeout(timeout.toMillis(), MILLISECONDS);
                    return true;
                }
                break;
            case TASK_FAILURE:
                log.info("Injecting failure for task %s at %s", taskId, requestType);
                taskManager.failTask(taskId, injectedFailure.get().getTaskFailureException());
                break;
            default:
                throw new IllegalArgumentException("unexpected failure type: " + failure.getInjectedFailureType());
        }

        return false;
    }

    private enum RequestType
    {
        CREATE_OR_UPDATE_TASK(true),
        GET_TASK_INFO(true),
        GET_TASK_STATUS(true),
        ACKNOWLEDGE_AND_GET_NEW_DYNAMIC_FILTER_DOMAINS(true),
        GET_RESULTS(false),
        ABORT_RESULTS(false);

        private final boolean taskManagement;

        RequestType(boolean taskManagement)
        {
            this.taskManagement = taskManagement;
        }

        public boolean isTaskManagement()
        {
            return taskManagement;
        }
    }

    @Managed
    @Nested
    public TimeStat getReadFromOutputBufferTime()
    {
        return readFromOutputBufferTime;
    }

    @Managed
    @Nested
    public TimeStat getResultsRequestTime()
    {
        return resultsRequestTime;
    }

    private static boolean shouldSummarize(UriInfo uriInfo)
    {
        return uriInfo.getQueryParameters().containsKey("summarize");
    }

    private static Duration randomizeWaitTime(Duration waitTime)
    {
        // Randomize in [T/2, T], so wait is not near zero and the client-supplied max wait time is respected
        long halfWaitMillis = waitTime.toMillis() / 2;
        return new Duration(halfWaitMillis + ThreadLocalRandom.current().nextLong(halfWaitMillis), MILLISECONDS);
    }
}
