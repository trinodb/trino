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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.prestosql.execution.ExecutionFailureInfo;
import io.prestosql.execution.StateMachine;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.spi.PrestoException;
import io.prestosql.transaction.TransactionId;

import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.prestosql.spi.NodeState.INACTIVE;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RemoteCoordinator
{
    private static final Duration MAX_FAILURE_INTERVAL = new Duration(5, MINUTES);

    private final CoordinatorLocation coordinatorLocation;

    private final Executor callbackExecutor;

    private final RemoteCoordinatorStatusTracker remoteCoordinatorStatusTracker;
    private final RemoteCoordinatorClient remoteCoordinatorClient;
    private final StateMachine<CoordinatorStatus> status;

    public static RemoteCoordinator createRemoteCoordinator(
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
        RemoteCoordinator coordinator = new RemoteCoordinator(
                coordinatorLocation,
                httpClient,
                callbackExecutor,
                scheduledExecutor,
                querySubmissionCodec,
                querySubmissionResponseCodec,
                queryAnalysisCodec,
                queryAnalysisResponseCodec,
                coordinatorStatusCodec);
        coordinator.start();
        return coordinator;
    }

    private RemoteCoordinator(
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
        this.coordinatorLocation = coordinatorLocation;
        this.callbackExecutor = callbackExecutor;

        this.remoteCoordinatorClient = new RemoteCoordinatorClient(
                coordinatorLocation,
                httpClient,
                callbackExecutor,
                scheduledExecutor,
                querySubmissionCodec,
                querySubmissionResponseCodec,
                queryAnalysisCodec,
                queryAnalysisResponseCodec,
                coordinatorStatusCodec);

        status = new StateMachine<>("coordinator status", callbackExecutor, new CoordinatorStatus(INACTIVE, ImmutableList.of()));
        this.remoteCoordinatorStatusTracker = new RemoteCoordinatorStatusTracker(
                remoteCoordinatorClient,
                this::updateStatus,
                new Duration(3, SECONDS),
                httpClient,
                callbackExecutor,
                scheduledExecutor,
                coordinatorStatusCodec);
    }

    private void start()
    {
        remoteCoordinatorStatusTracker.start();
    }

    public void destroy()
    {
        remoteCoordinatorStatusTracker.stop();
    }

    public CoordinatorStatus getStatus()
    {
        return status.get();
    }

    private void updateStatus(CoordinatorStatus status)
    {
        this.status.set(status);
    }

    public void addStatusChangeListener(StateChangeListener<CoordinatorStatus> stateChangeListener)
    {
        status.addStateChangeListener(stateChangeListener);
    }

    public ListenableFuture<QueryAnalysisResponse> analyzeQuery(QueuedQueryStateMachine stateMachine)
    {
        QueryAnalysis queryAnalysis = new QueryAnalysis(
                stateMachine.getSession().toSessionRepresentation(),
                stateMachine.getSession().getIdentity().getExtraCredentials(),
                stateMachine.getQuery());
        return remoteCoordinatorClient.analyseQuery(queryAnalysis, MAX_FAILURE_INTERVAL);
    }

    public ListenableFuture<?> dispatchQuery(QueuedQueryStateMachine stateMachine)
    {
        try {
            ListenableFuture<QuerySubmissionResponse> future = remoteCoordinatorClient.submitQuery(() -> toQuerySubmission(stateMachine), MAX_FAILURE_INTERVAL);
            Futures.addCallback(
                    future,
                    new FutureCallback<QuerySubmissionResponse>()
                    {
                        @Override
                        public void onSuccess(QuerySubmissionResponse submissionResponse)
                        {
                            try {
                                switch (submissionResponse.getStatus()) {
                                    case SUCCESS:
                                        stateMachine.transitionToDispatching(coordinatorLocation);
                                        break;
                                    case FAILED:
                                        Exception exception = submissionResponse.getFailureInfo()
                                                .map(ExecutionFailureInfo::toException)
                                                .orElseGet(() -> new PrestoException(GENERIC_INTERNAL_ERROR, "Query submission failed for an unknown reason"));
                                        stateMachine.transitionToFailed(exception);
                                        break;
                                    case SHUTTING_DOWN:
                                        stateMachine.transitionToFailed(new PrestoException(SERVER_SHUTTING_DOWN, "Coordinator is shutting down"));
                                        break;
                                    default:
                                        stateMachine.transitionToFailed(new PrestoException(GENERIC_INTERNAL_ERROR, "Query submission failed for an unknown reason"));
                                }
                            }
                            catch (Throwable e) {
                                stateMachine.transitionToFailed(e);
                            }
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            stateMachine.transitionToFailed(t);
                        }
                    },
                    callbackExecutor);
            return future;
        }
        catch (Exception e) {
            stateMachine.transitionToFailed(e);
            return immediateFailedFuture(e);
        }
    }

    private static QuerySubmission toQuerySubmission(QueuedQueryStateMachine stateMachine)
    {
        return new QuerySubmission(
                    stateMachine.getSession().toSessionRepresentation(),
                    stateMachine.getSession().getIdentity().getExtraCredentials(),
                    stateMachine.getSlug(),
                    stateMachine.getQuery(),
                    stateMachine.getElapsedTime(),
                    stateMachine.getQueuedTime(),
                    stateMachine.getResourceGroup());
    }

    public void resetInactiveTimeout(Collection<TransactionId> transactionIds)
    {
        remoteCoordinatorClient.resetInactiveTimeout(transactionIds, MAX_FAILURE_INTERVAL);
    }
}
