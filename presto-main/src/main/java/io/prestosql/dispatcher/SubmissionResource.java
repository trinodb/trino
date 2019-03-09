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
package io.prestosql.dispatcher;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.ForQueryExecution;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.QueryPreparer;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.ForStatementResource;
import io.prestosql.server.GracefulShutdownHandler;
import io.prestosql.server.protocol.QuerySubmissionManager;
import io.prestosql.spi.NodeState;
import io.prestosql.transaction.TransactionId;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static io.prestosql.dispatcher.QueryAnalysisResponse.analysisFailed;
import static io.prestosql.dispatcher.QueryAnalysisResponse.analysisPassed;
import static io.prestosql.dispatcher.QuerySubmissionResponse.Status.FAILED;
import static io.prestosql.dispatcher.QuerySubmissionResponse.Status.SUCCESS;
import static io.prestosql.spi.NodeState.ACTIVE;
import static io.prestosql.spi.NodeState.SHUTTING_DOWN;
import static io.prestosql.util.Failures.toFailure;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/coordinator")
public class SubmissionResource
{
    private static final Duration MAX_ANALYSIS_TIME = new Duration(3, TimeUnit.MINUTES);

    private final QueryManager queryManager;
    private final GracefulShutdownHandler shutdownHandler;
    private final QuerySubmissionManager querySubmissionManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final QueryPreparer queryPreparer;
    private final ListeningExecutorService executor;
    private final ScheduledExecutorService timeoutExecutor;

    @Inject
    public SubmissionResource(
            QueryManager queryManager,
            GracefulShutdownHandler shutdownHandler,
            QuerySubmissionManager querySubmissionManager,
            SessionPropertyManager sessionPropertyManager,
            QueryPreparer queryPreparer,
            @ForQueryExecution ExecutorService executor,
            @ForStatementResource ScheduledExecutorService timeoutExecutor)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.shutdownHandler = requireNonNull(shutdownHandler, "shutdownHandler is null");
        this.querySubmissionManager = requireNonNull(querySubmissionManager, "querySubmissionManager is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");
        this.executor = listeningDecorator(requireNonNull(executor, "executor is null"));
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
    }

    @GET
    @Path("status")
    @Produces(APPLICATION_JSON)
    public CoordinatorStatus getCoordinatorStatus()
    {
        NodeState nodeState = getNodeState();
        List<BasicQueryInfo> queries = queryManager.getQueries();
        return new CoordinatorStatus(nodeState, queries);
    }

    private NodeState getNodeState()
    {
        if (shutdownHandler.isShutdownRequested()) {
            return SHUTTING_DOWN;
        }
        else {
            return ACTIVE;
        }
    }

    // note queryId is passed only for debugging purposes
    @POST
    @Path("submit/{queryId}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public QuerySubmissionResponse submitQuery(@PathParam("queryId") QueryId queryId, QuerySubmission querySubmission)
    {
        try {
            Session session = querySubmission.getSession(sessionPropertyManager);
            querySubmissionManager.submitQuery(
                    session,
                    querySubmission.getQuery(),
                    querySubmission.getSlug(),
                    querySubmission.getQueryElapsedTime(),
                    querySubmission.getQueryQueueDuration(),
                    queryPreparer.prepareQuery(session, querySubmission.getQuery()),
                    querySubmission.getResourceGroupId());

            return new QuerySubmissionResponse(SUCCESS, Optional.empty());
        }
        catch (Exception e) {
            return new QuerySubmissionResponse(FAILED, Optional.of(toFailure(e)));
        }
    }

    // note queryId is passed only for debugging purposes
    @POST
    @Path("analysis/{queryId}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void analyzeQuery(@PathParam("queryId") QueryId queryId, QueryAnalysis queryAnalysis, @Suspended AsyncResponse asyncResponse)
    {
        ListenableFuture<QueryAnalysisResponse> analysisFuture = executor.submit(() -> analyzeQueryInternal(queryAnalysis));
        ListenableFuture<QueryAnalysisResponse> timeoutFuture = addTimeout(
                analysisFuture,
                QueryAnalysisResponse::analysisUnknown,
                MAX_ANALYSIS_TIME,
                timeoutExecutor);
        bindAsyncResponse(asyncResponse, timeoutFuture, executor);
    }

    private QueryAnalysisResponse analyzeQueryInternal(QueryAnalysis queryAnalysis)
    {
        try {
            Session session = queryAnalysis.getSession(sessionPropertyManager);
            PreparedQuery preparedQuery = queryPreparer.prepareQuery(session, queryAnalysis.getQuery());

            querySubmissionManager.analyzeQuery(session, preparedQuery);
            return analysisPassed();
        }
        catch (Exception e) {
            return analysisFailed(toFailure(e));
        }
    }
    @POST
    @Path("transaction/resetInactiveTimeout")
    @Consumes(APPLICATION_JSON)
    public Response resetInactiveTimeout(List<TransactionId> transactionIds)
    {
        querySubmissionManager.resetInactiveTimeout(transactionIds);
        return Response.noContent().build();
    }
}
