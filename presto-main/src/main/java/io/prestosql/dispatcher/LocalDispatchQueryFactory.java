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
import io.prestosql.Session;
import io.prestosql.event.QueryMonitor;
import io.prestosql.execution.ClusterSizeMonitor;
import io.prestosql.execution.LocationFactory;
import io.prestosql.execution.QueryExecution;
import io.prestosql.execution.QueryExecution.QueryExecutionFactory;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.execution.QueryStateMachine;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.execution.warnings.WarningCollectorFactory;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.sql.tree.Statement;
import io.prestosql.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.util.StatementUtils.isTransactionControlStatement;
import static java.util.Objects.requireNonNull;

public class LocalDispatchQueryFactory
        implements DispatchQueryFactory
{
    private final QueryManager queryManager;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;
    private final QueryMonitor queryMonitor;
    private final LocationFactory locationFactory;

    private final ClusterSizeMonitor clusterSizeMonitor;

    private final Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories;
    private final WarningCollectorFactory warningCollectorFactory;
    private final ListeningExecutorService executor;

    @Inject
    public LocalDispatchQueryFactory(
            QueryManager queryManager,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Metadata metadata,
            QueryMonitor queryMonitor,
            LocationFactory locationFactory,
            Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories,
            WarningCollectorFactory warningCollectorFactory,
            ClusterSizeMonitor clusterSizeMonitor,
            DispatchExecutor dispatchExecutor)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.executionFactories = requireNonNull(executionFactories, "executionFactories is null");
        this.warningCollectorFactory = requireNonNull(warningCollectorFactory, "warningCollectorFactory is null");

        this.clusterSizeMonitor = requireNonNull(clusterSizeMonitor, "clusterSizeMonitor is null");

        this.executor = requireNonNull(dispatchExecutor, "executorService is null").getExecutor();
    }

    @Override
    public DispatchQuery createDispatchQuery(
            Session session,
            String query,
            PreparedQuery preparedQuery,
            String slug,
            ResourceGroupId resourceGroup)
    {
        WarningCollector warningCollector = warningCollectorFactory.create();
        QueryStateMachine stateMachine = QueryStateMachine.begin(
                query,
                preparedQuery.getPrepareSql(),
                session,
                locationFactory.createQueryLocation(session.getQueryId()),
                resourceGroup,
                isTransactionControlStatement(preparedQuery.getStatement()),
                transactionManager,
                accessControl,
                executor,
                metadata,
                warningCollector);

        queryMonitor.queryCreatedEvent(stateMachine.getBasicQueryInfo(Optional.empty()));

        ListenableFuture<QueryExecution> queryExecutionFuture = executor.submit(() -> {
            QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(preparedQuery.getStatement().getClass());
            if (queryExecutionFactory == null) {
                throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type: " + preparedQuery.getStatement().getClass().getSimpleName());
            }

            return queryExecutionFactory.createQueryExecution(preparedQuery, stateMachine, slug, warningCollector);
        });

        return new LocalDispatchQuery(
                stateMachine,
                queryExecutionFuture,
                clusterSizeMonitor,
                executor,
                queryManager::createQuery);
    }
}
