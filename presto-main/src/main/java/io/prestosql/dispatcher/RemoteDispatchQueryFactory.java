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

import io.prestosql.Session;
import io.prestosql.event.QueryMonitor;
import io.prestosql.execution.ExecutionFailureInfo;
import io.prestosql.execution.LocationFactory;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.concurrent.Executor;

import static io.prestosql.execution.QueryState.FAILED;
import static io.prestosql.util.Failures.toFailure;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RemoteDispatchQueryFactory
        implements DispatchQueryFactory
{
    private final TransactionManager transactionManager;
    private final QueryMonitor queryMonitor;
    private final LocationFactory locationFactory;
    private final QueryDispatcher queryDispatcher;
    private final Executor executor;

    @Inject
    public RemoteDispatchQueryFactory(
            TransactionManager transactionManager,
            QueryMonitor queryMonitor,
            LocationFactory locationFactory,
            QueryDispatcher queryDispatcher,
            DispatchExecutor dispatchExecutor)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.queryDispatcher = requireNonNull(queryDispatcher, "queryDispatcher is null");
        this.executor = requireNonNull(dispatchExecutor, "dispatchExecutor is null").getExecutor();
    }

    @Override
    public DispatchQuery createDispatchQuery(
            Session session,
            String query,
            PreparedQuery preparedQuery,
            String slug,
            ResourceGroupId resourceGroup)
    {
        QueuedQueryStateMachine stateMachine = QueuedQueryStateMachine.begin(
                session,
                query,
                slug,
                locationFactory.createQueryLocation(session.getQueryId()),
                resourceGroup,
                queryMonitor,
                transactionManager,
                executor);

        queryMonitor.queryCreatedEvent(stateMachine.getBasicQueryInfo());
        stateMachine.addStateChangeListener(queryState -> {
            if (queryState == FAILED) {
                ExecutionFailureInfo cause = stateMachine.getFailureCause()
                        .orElseGet(() -> toFailure(new RuntimeException(format("Query %s FAILED for an unknown reason", session.getQueryId()))));
                queryMonitor.queryImmediateFailureEvent(stateMachine.getBasicQueryInfo(), cause);
            }
        });

        return new RemoteDispatchQuery(stateMachine, queryDispatcher, executor);
    }
}
