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
package io.trino.dispatcher;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.event.QueryMonitor;
import io.trino.execution.ClusterSizeMonitor;
import io.trino.execution.LocationFactory;
import io.trino.execution.QueryExecution;
import io.trino.execution.QueryExecution.QueryExecutionFactory;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.execution.warnings.WarningCollectorFactory;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.server.protocol.Slug;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.SessionPropertyResolver;
import io.trino.sql.tree.Statement;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.util.StatementUtils.getQueryType;
import static io.trino.util.StatementUtils.isTransactionControlStatement;
import static java.util.Objects.requireNonNull;

public class LocalDispatchQueryFactory
        implements DispatchQueryFactory
{
    private static final Logger log = Logger.get(LocalDispatchQueryFactory.class);

    private final QueryManager queryManager;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;
    private final SessionPropertyResolver sessionPropertyResolver;
    private final QueryMonitor queryMonitor;
    private final LocationFactory locationFactory;

    private final ClusterSizeMonitor clusterSizeMonitor;

    private final Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories;
    private final WarningCollectorFactory warningCollectorFactory;
    private final ListeningExecutorService executor;
    private final int maxStateMachineThreadsPerQuery;
    private final int queryReportedRuleStatsLimit;
    private final boolean faultTolerantExecutionExchangeEncryptionEnabled;
    private final NodeVersion version;

    @Inject
    public LocalDispatchQueryFactory(
            QueryManager queryManager,
            QueryManagerConfig queryManagerConfig,
            TransactionManager transactionManager,
            SessionPropertyResolver sessionPropertyResolver,
            AccessControl accessControl,
            Metadata metadata,
            QueryMonitor queryMonitor,
            LocationFactory locationFactory,
            Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories,
            WarningCollectorFactory warningCollectorFactory,
            ClusterSizeMonitor clusterSizeMonitor,
            DispatchExecutor dispatchExecutor,
            FeaturesConfig featuresConfig,
            NodeVersion version)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sessionPropertyResolver = requireNonNull(sessionPropertyResolver, "sessionPropertyInterpreter is null");
        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.executionFactories = requireNonNull(executionFactories, "executionFactories is null");
        this.warningCollectorFactory = requireNonNull(warningCollectorFactory, "warningCollectorFactory is null");
        this.clusterSizeMonitor = requireNonNull(clusterSizeMonitor, "clusterSizeMonitor is null");
        this.executor = dispatchExecutor.getExecutor();
        this.maxStateMachineThreadsPerQuery = queryManagerConfig.getMaxStateMachineCallbackThreads();
        this.queryReportedRuleStatsLimit = queryManagerConfig.getQueryReportedRuleStatsLimit();
        this.faultTolerantExecutionExchangeEncryptionEnabled = requireNonNull(featuresConfig, "featuresConfig is null").isFaultTolerantExecutionExchangeEncryptionEnabled();
        this.version = requireNonNull(version, "version is null");
    }

    @Override
    public DispatchQuery createDispatchQuery(
            Session session,
            Optional<TransactionId> existingTransactionId,
            String query,
            PreparedQuery preparedQuery,
            Slug slug,
            ResourceGroupId resourceGroup)
    {
        WarningCollector warningCollector = warningCollectorFactory.create();
        PlanOptimizersStatsCollector planOptimizersStatsCollector = new PlanOptimizersStatsCollector(queryReportedRuleStatsLimit);
        QueryStateMachine stateMachine = QueryStateMachine.begin(
                existingTransactionId,
                query,
                preparedQuery.getPrepareSql(),
                session,
                locationFactory.createQueryLocation(session.getQueryId()),
                resourceGroup,
                isTransactionControlStatement(preparedQuery.getStatement()),
                transactionManager,
                accessControl,
                // limit the number of state change listener callback threads for each query
                new BoundedExecutor(executor, maxStateMachineThreadsPerQuery),
                metadata,
                warningCollector,
                planOptimizersStatsCollector,
                getQueryType(preparedQuery.getStatement()),
                faultTolerantExecutionExchangeEncryptionEnabled,
                Optional.of(sessionPropertyResolver.getSessionPropertiesApplier(preparedQuery)),
                version);

        // It is important that `queryCreatedEvent` is called here. Moving it past the `executor.submit` below
        // can result in delivering query-created event after query analysis has already started.
        // That can result in misbehaviour of plugins called during analysis phase (e.g. access control auditing)
        // which depend on the contract that event was already delivered.
        //
        // Note that for immediate and in-order delivery of query events we depend on synchronous nature of
        // QueryMonitor and EventListenerManager.
        queryMonitor.queryCreatedEvent(stateMachine.getBasicQueryInfo(Optional.empty()));

        ListenableFuture<QueryExecution> queryExecutionFuture = executor.submit(() -> {
            QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(preparedQuery.getStatement().getClass());
            if (queryExecutionFactory == null) {
                throw new TrinoException(NOT_SUPPORTED, "Unsupported statement type: " + preparedQuery.getStatement().getClass().getSimpleName());
            }

            try {
                return queryExecutionFactory.createQueryExecution(preparedQuery, stateMachine, slug, warningCollector, planOptimizersStatsCollector);
            }
            catch (Throwable e) {
                if (e instanceof Error) {
                    if (e instanceof StackOverflowError) {
                        log.error(e, "Unhandled StackOverFlowError; should be handled earlier; to investigate full stacktrace you may need to enable -XX:MaxJavaStackTraceDepth=0 JVM flag");
                    }
                    else {
                        log.error(e, "Unhandled Error");
                    }
                    // wrapping as RuntimeException to guard us from problem that code downstream which investigates queryExecutionFuture may not necessarily handle
                    // Error subclass of Throwable well.
                    RuntimeException wrappedError = new RuntimeException(e);
                    stateMachine.transitionToFailed(wrappedError);
                    throw wrappedError;
                }
                stateMachine.transitionToFailed(e);
                throw e;
            }
        });

        return new LocalDispatchQuery(
                stateMachine,
                queryExecutionFuture,
                queryMonitor,
                clusterSizeMonitor,
                executor,
                queryManager::createQuery);
    }
}
