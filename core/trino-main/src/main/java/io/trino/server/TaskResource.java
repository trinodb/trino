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
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.stats.TimeStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.FailureInjector;
import io.trino.execution.FailureInjector.InjectedFailure;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.SqlTaskManager.SqlTaskWithResults;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.metadata.SessionPropertyManager;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.connector.CatalogHandle;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.InternalServerErrorException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.GenericEntity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.UriInfo;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.trino.TrinoMediaTypes.TRINO_PAGES;
import static io.trino.execution.buffer.BufferResult.emptyResults;
import static io.trino.server.DisconnectionAwareAsyncResponse.bindDisconnectionAwareAsyncResponse;
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

    private final StartupStatus startupStatus;
    private final SqlTaskManager taskManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final FailureInjector failureInjector;
    private final TimeStat readFromOutputBufferTime = new TimeStat();
    private final TimeStat resultsRequestTime = new TimeStat();

    @Inject
    public TaskResource(
            StartupStatus startupStatus,
            SqlTaskManager taskManager,
            SessionPropertyManager sessionPropertyManager,
            @ForAsyncHttp BoundedExecutor responseExecutor,
            @ForAsyncHttp ScheduledExecutorService timeoutExecutor,
            FailureInjector failureInjector)
    {
        this.startupStatus = requireNonNull(startupStatus, "startupStatus is null");
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
            @Suspended @BeanParam DisconnectionAwareAsyncResponse asyncResponse)
    {
        requireNonNull(taskUpdateRequest, "taskUpdateRequest is null");
        if (failRequestIfInvalid(asyncResponse)) {
            return;
        }

        Session session = taskUpdateRequest.session().toSession(sessionPropertyManager, taskUpdateRequest.extraCredentials(), taskUpdateRequest.exchangeEncryptionKey());

        if (injectFailure(session.getTraceToken(), taskId, RequestType.CREATE_OR_UPDATE_TASK, asyncResponse)) {
            return;
        }

        TaskInfo taskInfo = taskManager.updateTask(
                session,
                taskId,
                taskUpdateRequest.stageSpan(),
                taskUpdateRequest.fragment(),
                taskUpdateRequest.splitAssignments(),
                taskUpdateRequest.outputIds(),
                taskUpdateRequest.dynamicFilterDomains(),
                taskUpdateRequest.speculative());

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
            @Suspended @BeanParam DisconnectionAwareAsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");
        if (failRequestIfInvalid(asyncResponse)) {
            return;
        }

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
        ListenableFuture<TaskInfo> futureTaskInfo = taskManager.getTaskInfo(taskId, currentVersion);
        if (!futureTaskInfo.isDone()) {
            futureTaskInfo = addTimeout(
                    futureTaskInfo,
                    () -> taskManager.getTaskInfo(taskId),
                    waitTime,
                    timeoutExecutor);
        }

        if (shouldSummarize(uriInfo)) {
            futureTaskInfo = Futures.transform(futureTaskInfo, TaskInfo::summarize, directExecutor());
        }

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindDisconnectionAwareAsyncResponse(asyncResponse, futureTaskInfo, responseExecutor)
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
            @Suspended @BeanParam DisconnectionAwareAsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");
        if (failRequestIfInvalid(asyncResponse)) {
            return;
        }

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
        ListenableFuture<TaskStatus> futureTaskStatus = taskManager.getTaskStatus(taskId, currentVersion);
        if (!futureTaskStatus.isDone()) {
            futureTaskStatus = addTimeout(
                    futureTaskStatus,
                    () -> taskManager.getTaskStatus(taskId),
                    waitTime,
                    timeoutExecutor);
        }

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindDisconnectionAwareAsyncResponse(asyncResponse, futureTaskStatus, responseExecutor)
                .withTimeout(timeout);
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @GET
    @Path("{taskId}/dynamicfilters")
    @Produces(MediaType.APPLICATION_JSON)
    public void acknowledgeAndGetNewDynamicFilterDomains(
            @PathParam("taskId") TaskId taskId,
            @HeaderParam(TRINO_CURRENT_VERSION) Long currentDynamicFiltersVersion,
            @Suspended @BeanParam DisconnectionAwareAsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(currentDynamicFiltersVersion, "currentDynamicFiltersVersion is null");
        if (failRequestIfInvalid(asyncResponse)) {
            return;
        }

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
    @POST
    @Path("{taskId}/fail")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public TaskInfo failTask(
            @PathParam("taskId") TaskId taskId,
            FailTaskRequest failTaskRequest)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(failTaskRequest, "failTaskRequest is null");
        return taskManager.failTask(taskId, failTaskRequest.getFailureInfo().toException());
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @GET
    @Path("{taskId}/results/{bufferId}/{token}")
    @Produces(TRINO_PAGES)
    public void getResults(
            @PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") PipelinedOutputBuffers.OutputBufferId bufferId,
            @PathParam("token") long token,
            @HeaderParam(TRINO_MAX_SIZE) DataSize maxSize,
            @Suspended @BeanParam DisconnectionAwareAsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        if (injectFailure(taskManager.getTraceToken(taskId), taskId, RequestType.GET_RESULTS, asyncResponse)) {
            return;
        }

        long start = System.nanoTime();
        SqlTaskWithResults taskWithResults = taskManager.getTaskResults(taskId, bufferId, token, maxSize);
        ListenableFuture<BufferResult> bufferResultFuture = taskWithResults.getResultsFuture();
        BufferResult emptyBufferResults = emptyResults(taskWithResults.getTaskInstanceId(), token, false);

        Duration waitTime = randomizeWaitTime(DEFAULT_MAX_WAIT_TIME);
        if (!bufferResultFuture.isDone()) {
            bufferResultFuture = addTimeout(
                    bufferResultFuture,
                    () -> emptyBufferResults,
                    waitTime,
                    timeoutExecutor);
        }

        ListenableFuture<Response> responseFuture = Futures.transform(bufferResultFuture, results -> createBufferResultResponse(taskWithResults, results), directExecutor());

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindDisconnectionAwareAsyncResponse(asyncResponse, responseFuture, responseExecutor)
                .withTimeout(timeout, () -> createBufferResultResponse(taskWithResults, emptyBufferResults));

        responseFuture.addListener(() -> readFromOutputBufferTime.add(Duration.nanosSince(start)), directExecutor());
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @GET
    @Path("{taskId}/results/{bufferId}/{token}/acknowledge")
    public void acknowledgeResults(
            @PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") PipelinedOutputBuffers.OutputBufferId bufferId,
            @PathParam("token") long token)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        taskManager.acknowledgeTaskResults(taskId, bufferId, token);
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @DELETE
    @Path("{taskId}/results/{bufferId}")
    public void destroyTaskResults(
            @PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") PipelinedOutputBuffers.OutputBufferId bufferId,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        if (injectFailure(taskManager.getTraceToken(taskId), taskId, RequestType.DESTROY_RESULTS, asyncResponse)) {
            return;
        }

        taskManager.destroyTaskResults(taskId, bufferId);
        asyncResponse.resume(Response.noContent().build());
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @POST
    @Path("pruneCatalogs")
    @Consumes(MediaType.APPLICATION_JSON)
    public void pruneCatalogs(Set<CatalogHandle> catalogHandles)
    {
        taskManager.pruneCatalogs(catalogHandles);
    }

    private boolean failRequestIfInvalid(AsyncResponse asyncResponse)
    {
        if (!startupStatus.isStartupComplete()) {
            // When worker node is restarted after a crash, coordinator may be still unaware of the situation and may attempt to schedule tasks on it.
            // Ideally the coordinator should not schedule tasks on worker that is not ready, but in pipelined execution there is currently no way to move a task.
            // Accepting a request too early will likely lead to some failure and HTTP 500 (INTERNAL_SERVER_ERROR) response. The coordinator won't retry on this.
            // Send 503 (SERVICE_UNAVAILABLE) so that request is retried.
            asyncResponse.resume(new ServiceUnavailableException("The server is not fully started yet"));
            return true;
        }
        return false;
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
                    asyncResponse.resume(new InternalServerErrorException("Task %s failed".formatted(taskId)));
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
                    asyncResponse.resume(new InternalServerErrorException("Task %s failed".formatted(taskId)));
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
        DESTROY_RESULTS(false);

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

    private static Response createBufferResultResponse(SqlTaskWithResults taskWithResults, BufferResult result)
    {
        // This response may have been created as the result of a timeout, so refresh the task heartbeat
        taskWithResults.recordHeartbeat();

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
                // check for task failure after getting the result to ensure it's consistent with isBufferComplete()
                .header(TRINO_TASK_FAILED, taskWithResults.isTaskFailedOrFailing())
                .build();
    }
}
