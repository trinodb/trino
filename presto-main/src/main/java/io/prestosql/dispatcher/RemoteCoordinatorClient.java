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
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.prestosql.spi.QueryId;
import io.prestosql.transaction.TransactionId;

import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.prestosql.dispatcher.RetryHttpClient.createJsonResponseHandler;
import static io.prestosql.dispatcher.RetryHttpClient.createStatusResponseHandler;
import static java.util.Objects.requireNonNull;

public class RemoteCoordinatorClient
{
    private final CoordinatorLocation coordinatorLocation;

    private final JsonCodec<QuerySubmission> querySubmissionCodec;
    private final JsonCodec<QuerySubmissionResponse> querySubmissionResponseCodec;
    private final JsonCodec<QueryAnalysis> queryAnalysisCodec;
    private final JsonCodec<QueryAnalysisResponse> queryAnalysisResponseCodec;
    private final JsonCodec<CoordinatorStatus> coordinatorStatusCodec;
    private final RetryHttpClient retryHttpClient;

    public RemoteCoordinatorClient(
            CoordinatorLocation coordinatorLocation,
            HttpClient httpClient,
            Executor callbackExecutor,
            ScheduledExecutorService scheduledExecutor,
            JsonCodec<QuerySubmission> querySubmissionCodec,
            JsonCodec<QuerySubmissionResponse> querySubmissionResponseCodec,
            JsonCodec<QueryAnalysis> queryAnalysisCodec,
            JsonCodec<QueryAnalysisResponse> queryAnalysisResponseCodec,
            JsonCodec<CoordinatorStatus> coordinatorStatusCodec)
    {
        this.coordinatorLocation = requireNonNull(coordinatorLocation, "coordinatorLocation is null");

        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(callbackExecutor, "callbackExecutor is null");
        requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        this.retryHttpClient = new RetryHttpClient(httpClient, callbackExecutor, scheduledExecutor);

        this.querySubmissionCodec = requireNonNull(querySubmissionCodec, "querySubmissionCodec is null");
        this.querySubmissionResponseCodec = requireNonNull(querySubmissionResponseCodec, "querySubmissionResponseCodec is null");
        this.queryAnalysisCodec = requireNonNull(queryAnalysisCodec, "queryAnalysisCodec is null");
        this.queryAnalysisResponseCodec = requireNonNull(queryAnalysisResponseCodec, "queryAnalysisResponseCodec is null");
        this.coordinatorStatusCodec = requireNonNull(coordinatorStatusCodec, "coordinatorStatusCodec is null");
    }

    public CoordinatorLocation getCoordinatorLocation()
    {
        return coordinatorLocation;
    }

    public ListenableFuture<QueryAnalysisResponse> analyseQuery(QueryAnalysis queryAnalysis, Duration maxRequestTime)
    {
        Request request = preparePost()
                .setUri(UriBuilder.fromUri(coordinatorLocation.getUri("https"))
                        .replacePath("/v1/coordinator/analysis/" + queryAnalysis.getSession().getQueryId())
                        .build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(queryAnalysisCodec, queryAnalysis))
                .build();

        return retryHttpClient.execute(
                String.format("analysis query %s", queryAnalysis.getSession().getQueryId()),
                () -> request,
                createJsonResponseHandler(String.format("analysis query %s", queryAnalysis.getSession().getQueryId()), queryAnalysisResponseCodec, 200),
                maxRequestTime);
    }

    public ListenableFuture<QuerySubmissionResponse> submitQuery(Supplier<QuerySubmission> querySubmissionSupplier, Duration maxRequestTime)
    {
        return retryHttpClient.execute(
                String.format("submit query %s", querySubmissionSupplier.get().getSession().getQueryId()),
                () -> toSubmitRequest(querySubmissionSupplier.get()),
                createJsonResponseHandler(String.format("submit query %s", querySubmissionSupplier.get().getSession().getQueryId()), querySubmissionResponseCodec, 200),
                maxRequestTime);
    }

    private Request toSubmitRequest(QuerySubmission querySubmission)
    {
        URI location = UriBuilder.fromUri(coordinatorLocation.getUri("https"))
                .replacePath("/v1/coordinator/submit/" + querySubmission.getSession().getQueryId())
                .build();
        return preparePost()
                .setUri(location)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(querySubmissionCodec, querySubmission))
                .build();
    }

    public void cancelQuery(QueryId queryId, Duration maxRequestTime)
    {
        Request request = prepareDelete()
                .setUri(UriBuilder.fromUri(coordinatorLocation.getUri("https"))
                        .replacePath("/v1/coordinator/query/" + queryId)
                        .build())
                .build();

        retryHttpClient.execute(
                String.format("cancel query %s", queryId),
                () -> request,
                createStatusResponseHandler(String.format("cancel query %s", queryId), 204),
                maxRequestTime);
    }

    public void resetInactiveTimeout(Collection<TransactionId> transactionIds, Duration maxRequestTime)
    {
        Request request = prepareDelete()
                .setUri(UriBuilder.fromUri(coordinatorLocation.getUri("https"))
                        .replacePath("/v1/coordinator/transaction/resetInactiveTimeout")
                        .build())
                .build();

        retryHttpClient.execute(
                "reset inactive timeout",
                () -> request,
                createStatusResponseHandler("reset inactive timeout", 204),
                maxRequestTime);
    }

    public ListenableFuture<CoordinatorStatus> getStatus(Duration maxRequestTime)
    {
        Request request = prepareGet()
                .setUri(UriBuilder.fromUri(coordinatorLocation.getUri("https"))
                        .replacePath("/v1/coordinator/status")
                        .build())
                .build();

        return retryHttpClient.execute(
                "get coordinator status",
                () -> request,
                createJsonResponseHandler("get coordinator status", coordinatorStatusCodec, 200),
                maxRequestTime);
    }
}
