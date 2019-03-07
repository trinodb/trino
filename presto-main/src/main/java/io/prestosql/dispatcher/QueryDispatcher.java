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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.execution.QueryTracker;
import io.prestosql.execution.StateMachine;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.Node;
import io.prestosql.spi.PrestoException;
import io.prestosql.transaction.TransactionId;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.dispatcher.QueryAnalysisResponse.analysisUnknown;
import static io.prestosql.spi.NodeState.ACTIVE;
import static io.prestosql.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static java.util.Objects.requireNonNull;

public class QueryDispatcher
{
    private static final Logger log = Logger.get(QueryDispatcher.class);
    private final HttpClient httpClient;
    private final JsonCodec<QuerySubmission> querySubmissionCodec;
    private final JsonCodec<QuerySubmissionResponse> querySubmissionResponseCodec;
    private final JsonCodec<QueryAnalysis> queryAnalysisCodec;
    private final JsonCodec<QueryAnalysisResponse> queryAnalysisResponseCodec;
    private final JsonCodec<CoordinatorStatus> coordinatorStatusCodec;
    private final Executor executor;
    private final ScheduledExecutorService scheduledExecutor;

    private final QueryTracker<DispatchQuery> dispatchQueryTracker;
    private final RemoteCoordinatorMonitor remoteCoordinatorMonitor;
    private final SessionPropertyManager sessionPropertyManager;

    @GuardedBy("this")
    private boolean running;

    @GuardedBy("this")
    private final List<Runnable> coordinatorsUpdateListeners = new ArrayList<>();

    private final StateMachine<Map<String, RemoteCoordinator>> remoteCoordinators;
    private final Consumer<Set<Node>> coordinatorNodesListener = this::updateCoordinators;

    @Inject
    public QueryDispatcher(
            QueryTracker<DispatchQuery> dispatchQueryTracker,
            RemoteCoordinatorMonitor remoteCoordinatorMonitor,
            SessionPropertyManager sessionPropertyManager,
            @ForDispatcher HttpClient httpClient,
            DispatchExecutor dispatchExecutor,
            JsonCodec<QuerySubmission> querySubmissionCodec,
            JsonCodec<QuerySubmissionResponse> querySubmissionResponseCodec,
            JsonCodec<QueryAnalysis> queryAnalysisCodec,
            JsonCodec<QueryAnalysisResponse> queryAnalysisResponseCodec,
            JsonCodec<CoordinatorStatus> coordinatorStatusCodec)
    {
        this.dispatchQueryTracker = requireNonNull(dispatchQueryTracker, "dispatchQueryTracker is null");
        this.remoteCoordinatorMonitor = requireNonNull(remoteCoordinatorMonitor, "remoteCoordinatorMonitor is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.querySubmissionCodec = requireNonNull(querySubmissionCodec, "querySubmissionCodec is null");
        this.querySubmissionResponseCodec = requireNonNull(querySubmissionResponseCodec, "querySubmissionResponseCodec is null");
        this.queryAnalysisCodec = requireNonNull(queryAnalysisCodec, "queryAnalysisCodec is null");
        this.queryAnalysisResponseCodec = requireNonNull(queryAnalysisResponseCodec, "queryAnalysisResponseCodec is null");
        this.coordinatorStatusCodec = requireNonNull(coordinatorStatusCodec, "coordinatorStatusCodec is null");

        requireNonNull(dispatchExecutor, "dispatchExecutor is null");
        this.executor = dispatchExecutor.getExecutor();
        this.scheduledExecutor = dispatchExecutor.getScheduledExecutor();
        this.remoteCoordinators = new StateMachine<>("coordinators", executor, ImmutableMap.of());
    }

    @PostConstruct
    public synchronized void start()
    {
        if (running) {
            return;
        }
        running = true;
        remoteCoordinatorMonitor.addCoordinatorsChangeListener(coordinatorNodesListener);
    }

    @PreDestroy
    public void stop()
    {
        Map<String, RemoteCoordinator> activeCoordinators;
        synchronized (this) {
            if (!running) {
                return;
            }
            running = false;
            remoteCoordinatorMonitor.removeCoordinatorsChangeListener(coordinatorNodesListener);
            activeCoordinators = remoteCoordinators.set(ImmutableMap.of());
        }
        for (RemoteCoordinator activeCoordinator : activeCoordinators.values()) {
            activeCoordinator.destroy();
        }
    }

    public ListenableFuture<QueryAnalysisResponse> analyzeQuery(QueuedQueryStateMachine stateMachine)
    {
        ListenableFuture<RemoteCoordinator> coordinatorFuture = getCoordinator(stateMachine);
        return Futures.transformAsync(coordinatorFuture, remoteCoordinator -> {
            requireNonNull(remoteCoordinator, "remoteCoordinator is null");
            if (stateMachine.getQueryState().isDone()) {
                return immediateFuture(analysisUnknown());
            }
            return remoteCoordinator.analyzeQuery(stateMachine);
        }, executor);
    }

    public ListenableFuture<?> dispatchQuery(QueuedQueryStateMachine stateMachine)
    {
        ListenableFuture<RemoteCoordinator> remoteCoordinatorListenableFuture = getCoordinator(stateMachine);
        return Futures.transformAsync(remoteCoordinatorListenableFuture, remoteCoordinator -> {
            requireNonNull(remoteCoordinator, "remoteCoordinator is null");
            if (stateMachine.getQueryState().isDone()) {
                return immediateFuture(null);
            }
            return remoteCoordinator.dispatchQuery(stateMachine);
        }, executor);
    }

    private Optional<DispatchQuery> getDispatchQuery(BasicQueryInfo queryInfo, CoordinatorLocation coordinatorLocation)
    {
        Optional<DispatchQuery> dispatchQuery = dispatchQueryTracker.tryGetQuery(queryInfo.getQueryId());
        if (dispatchQuery.isPresent()) {
            return dispatchQuery;
        }

        // new queries can be "discovered" when the dispatcher crashes, and comes back
        // up with existing queries running on the coordinators

        // if the discovered query is already finished, just skip it
        if (queryInfo.getState().isDone()) {
            return Optional.empty();
        }

        try {
            DiscoveredDispatchQuery discoveredDispatchQuery = new DiscoveredDispatchQuery(queryInfo, coordinatorLocation, sessionPropertyManager, executor);
            if (dispatchQueryTracker.addQuery(discoveredDispatchQuery)) {
                return Optional.of(discoveredDispatchQuery);
            }
        }
        catch (Exception ignored) {
            // failed to create discovered query, just skip it for now
        }

        // it is likely there was a race, just skip this time, if necessary this will be addressed in the next update
        return Optional.empty();
    }

    private ListenableFuture<RemoteCoordinator> getCoordinator(QueuedQueryStateMachine stateMachine)
    {
        Optional<TransactionId> transactionId = stateMachine.getSession().getTransactionId();
        ListenableFuture<RemoteCoordinator> coordinatorFuture;
        if (transactionId.isPresent()) {
            coordinatorFuture = getCoordinatorForTransaction(transactionId.get());
        }
        else {
            coordinatorFuture = selectRandomCoordinator();
        }
        return coordinatorFuture;
    }

    private ListenableFuture<RemoteCoordinator> getCoordinatorForTransaction(TransactionId transactionId)
    {
        // this code does not use the listener mechanism because the query can only be placed on the
        // coordinator managing the transaction, so we ignore all other systems and send the query
        // as soon as the coordinator is registered
        Map<String, RemoteCoordinator> coordinators = remoteCoordinators.get();

        RemoteCoordinator coordinator = coordinators.get(transactionId.getNodeId());
        if (coordinator == null) {
            // wait for there the coordinator to register
            return Futures.transformAsync(remoteCoordinators.getStateChange(coordinators), ignored -> getCoordinatorForTransaction(transactionId), executor);
        }

        return immediateFuture(coordinator);
    }

    private ListenableFuture<RemoteCoordinator> selectRandomCoordinator()
    {
        CoordinatorFuture coordinatorFuture = new CoordinatorFuture();
        if (!coordinatorFuture.isDone()) {
            synchronized (this) {
                coordinatorsUpdateListeners.add(coordinatorFuture);
            }
            executor.execute(coordinatorFuture);
        }
        return coordinatorFuture;
    }

    private void updateRemoteQueries(CoordinatorLocation coordinatorLocation, Collection<BasicQueryInfo> queryInfos)
    {
        for (BasicQueryInfo queryInfo : queryInfos) {
            getDispatchQuery(queryInfo, coordinatorLocation)
                    .ifPresent(query -> query.updateRemoteQueryInfo(queryInfo));
        }
    }

    private synchronized void updateCoordinators(Set<Node> activeCoordinatorNodes)
    {
        Set<String> activeCoordinatorIds = activeCoordinatorNodes.stream()
                .map(Node::getNodeIdentifier)
                .collect(toImmutableSet());

        // get a reference to the current coordinators
        Map<String, RemoteCoordinator> oldCoordinators = remoteCoordinators.get();
        if (oldCoordinators.keySet().equals(activeCoordinatorIds)) {
            return;
        }

        // build new coordinators list, reusing existing coordinator objects if possible
        ImmutableMap.Builder<String, RemoteCoordinator> newCoordinatorsBuilder = ImmutableMap.builder();
        for (Node coordinatorNode : activeCoordinatorNodes) {
            RemoteCoordinator remoteCoordinator = oldCoordinators.get(coordinatorNode.getNodeIdentifier());
            if (remoteCoordinator == null) {
                CoordinatorLocation coordinatorLocation = new CoordinatorLocation(Optional.of(coordinatorNode.getHttpUri()), Optional.of(coordinatorNode.getHttpUri()));
                remoteCoordinator = createRemoteCoordinator(coordinatorLocation);
            }
            newCoordinatorsBuilder.put(coordinatorNode.getNodeIdentifier(), remoteCoordinator);
        }

        // update the coordinators list
        ImmutableMap<String, RemoteCoordinator> newCoordinators = newCoordinatorsBuilder.build();
        remoteCoordinators.set(newCoordinators);

        // destroy and remote coordinator no longer active
        // this is thread safe as destroy only stops the active monitoring
        for (Entry<String, RemoteCoordinator> entry : oldCoordinators.entrySet()) {
            if (!newCoordinators.containsKey(entry.getKey())) {
                entry.getValue().destroy();
            }
        }

        if (!oldCoordinators.keySet().containsAll(activeCoordinatorIds)) {
            coordinatorsUpdated();
        }
    }

    private void updateCoordinatorStatus(CoordinatorLocation coordinatorLocation, CoordinatorStatus coordinatorStatus)
    {
        updateRemoteQueries(coordinatorLocation, coordinatorStatus.getQueries());
        if (coordinatorStatus.getNodeState() == ACTIVE) {
            coordinatorsUpdated();
        }
    }

    private void coordinatorsUpdated()
    {
        List<Runnable> listeners;
        synchronized (this) {
            listeners = ImmutableList.copyOf(coordinatorsUpdateListeners);
        }

        for (Runnable listener : listeners) {
            try {
                executor.execute(listener);
            }
            catch (RejectedExecutionException e) {
                if ((executor instanceof ExecutorService) && ((ExecutorService) executor).isShutdown()) {
                    throw new PrestoException(SERVER_SHUTTING_DOWN, "Server is shutting down", e);
                }
                log.error(e, "Error notifying coordinators update listener");
            }
        }
    }

    private RemoteCoordinator createRemoteCoordinator(CoordinatorLocation coordinatorLocation)
    {
        RemoteCoordinator remoteCoordinator = RemoteCoordinator.createRemoteCoordinator(
                coordinatorLocation,
                httpClient,
                executor,
                scheduledExecutor,
                querySubmissionCodec,
                querySubmissionResponseCodec,
                queryAnalysisCodec,
                queryAnalysisResponseCodec,
                coordinatorStatusCodec);
        remoteCoordinator.addStatusChangeListener(status -> updateCoordinatorStatus(coordinatorLocation, status));
        return remoteCoordinator;
    }

    private class CoordinatorFuture
            extends AbstractFuture<RemoteCoordinator>
            implements Runnable
    {
        @Override
        public void run()
        {
            //
            // For now, we just select a random active coordinator.  A better algorithm
            // should be used in the future.
            //

            List<RemoteCoordinator> candidates = remoteCoordinators.get().values().stream()
                    .filter(coordinator -> coordinator.getStatus().getNodeState() == ACTIVE)
                    .collect(toImmutableList());

            if (candidates.isEmpty()) {
                // wait for there the be at least one active coordinator
                return;
            }

            set(candidates.get(ThreadLocalRandom.current().nextInt(candidates.size())));
            synchronized (QueryDispatcher.this) {
                coordinatorsUpdateListeners.remove(this);
            }
        }
    }
}
