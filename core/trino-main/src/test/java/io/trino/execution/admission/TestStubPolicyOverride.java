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
package io.trino.execution.admission;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.trino.connector.CatalogHandle;
import io.trino.connector.ConnectorCatalogServiceProvider;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.dispatcher.LocalDispatchQuery;
import io.trino.event.QueryMonitor;
import io.trino.event.QueryMonitorConfig;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.exchange.ExchangeMetricsCollector;
import io.trino.execution.ClusterSizeMonitor;
import io.trino.execution.DataDefinitionExecution;
import io.trino.execution.DataDefinitionTask;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.QueryPreparer;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.StagesInfo;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.LanguageFunctionProvider;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.node.InternalNodeManager;
import io.trino.node.TestingInternalNodeManager;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.server.protocol.Slug;
import io.trino.spi.NodeVersion;
import io.trino.spi.admission.AdmissionPolicy;
import io.trino.spi.admission.WaitDecision;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.opentelemetry.api.OpenTelemetry.noop;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.metadata.TestingMetadataManager.createTestingMetadataManager;
import static io.trino.sql.tree.SaveMode.FAIL;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that binding a stub {@link AdmissionPolicy} returning
 * {@link WaitDecision.ProceedNow} causes queries to proceed without invoking
 * {@link ClusterSizeMonitor#waitForMinimumWorkers(int, Duration)}.
 *
 * <p>Validates: Requirements 4.1, 4.2 / Design: §LocalDispatchQuery integration,
 * §Correctness Properties — Property 3 (stub-policy override).
 */
public class TestStubPolicyOverride
{
    @Test
    @DisplayName("Feature: admission-policy-spi, stub policy override → ClusterSizeMonitor.waitForMinimumWorkers is never called")
    public void testProceedNowSkipsWaitForMinimumWorkers()
            throws InterruptedException
    {
        // No-op stub policy: always proceeds.
        AdmissionPolicy stubPolicy = _ -> new WaitDecision.ProceedNow("stub-proceed");

        Executor executor = newCachedThreadPool(daemonThreadsNamed("admission-stub-test-%s"));
        Metadata metadata = createTestingMetadataManager();
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControl = new AccessControlManager(
                NodeVersion.UNKNOWN,
                transactionManager,
                emptyEventListenerManager(),
                new AccessControlConfig(),
                noop(),
                new SecretsResolver(ImmutableMap.of()),
                DefaultSystemAccessControl.NAME);
        accessControl.setSystemAccessControls(List.of(AllowAllSystemAccessControl.INSTANCE));

        QueryStateMachine queryStateMachine = QueryStateMachine.begin(
                Optional.empty(),
                "sql",
                Optional.empty(),
                TEST_SESSION,
                URI.create("fake://stub-policy-override"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                accessControl,
                executor,
                metadata,
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                new ExchangeMetricsCollector(ImmutableList::of, java.time.Duration.ofMillis(1)),
                Optional.of(QueryType.DATA_DEFINITION),
                true,
                Optional.empty(),
                new NodeVersion("test"));

        QueryMonitor queryMonitor = new QueryMonitor(
                jsonCodec(StagesInfo.class),
                jsonCodec(OperatorStats.class),
                jsonCodec(ExecutionFailureInfo.class),
                jsonCodec(StatsAndCosts.class),
                new EventListenerManager(new EventListenerConfig(), new SecretsResolver(ImmutableMap.of()), noop(), noopTracer(), new NodeVersion("test")),
                new NodeInfo("node"),
                new NodeVersion("version"),
                new SessionPropertyManager(),
                metadata,
                new FunctionManager(
                        new ConnectorCatalogServiceProvider<>("function provider", new NoConnectorServicesProvider(), ConnectorServices::getFunctionProvider),
                        new GlobalFunctionCatalog(
                                () -> { throw new UnsupportedOperationException(); },
                                () -> { throw new UnsupportedOperationException(); },
                                () -> { throw new UnsupportedOperationException(); }),
                        LanguageFunctionProvider.DISABLED),
                new QueryMonitorConfig());

        CreateTable createTable = new CreateTable(new NodeLocation(1, 1), QualifiedName.of("table"), ImmutableList.of(), FAIL, ImmutableList.of(), Optional.empty());
        QueryPreparer.PreparedQuery preparedQuery = new QueryPreparer.PreparedQuery(createTable, ImmutableList.of(), Optional.empty());
        DataDefinitionExecution.DataDefinitionExecutionFactory dataDefinitionExecutionFactory =
                new DataDefinitionExecution.DataDefinitionExecutionFactory(
                        ImmutableMap.<Class<? extends Statement>, DataDefinitionTask<?>>of(CreateTable.class, new ImmediateCreateTableTask()));
        DataDefinitionExecution<?> dataDefinitionExecution = dataDefinitionExecutionFactory.createQueryExecution(
                preparedQuery,
                queryStateMachine,
                Slug.createNew(),
                WarningCollector.NOOP,
                null);

        CountingClusterSizeMonitor clusterSizeMonitor = new CountingClusterSizeMonitor(
                TestingInternalNodeManager.createDefault(),
                new NodeSchedulerConfig());

        CountDownLatch dispatchedLatch = new CountDownLatch(1);
        queryStateMachine.addStateChangeListener(state -> {
            if (state.ordinal() >= QueryState.DISPATCHING.ordinal()) {
                dispatchedLatch.countDown();
            }
        });

        LocalDispatchQuery localDispatchQuery = new LocalDispatchQuery(
                queryStateMachine,
                Futures.immediateFuture(dataDefinitionExecution),
                queryMonitor,
                clusterSizeMonitor,
                stubPolicy,
                executor,
                _ -> dataDefinitionExecution.start());

        localDispatchQuery.startWaitingForResources();

        boolean dispatched = dispatchedLatch.await(30, TimeUnit.SECONDS);
        assertThat(dispatched)
                .as("query must transition to DISPATCHING (or beyond) when the stub policy returns ProceedNow")
                .isTrue();
        assertThat(clusterSizeMonitor.waitCallCount.get())
                .as("ClusterSizeMonitor.waitForMinimumWorkers must NOT be invoked when policy returns ProceedNow")
                .isZero();
        assertThat(queryStateMachine.getQueryState())
                .as("query must not be FAILED")
                .isNotEqualTo(QueryState.FAILED);
    }

    /**
     * Stub {@link ClusterSizeMonitor} that flags any invocation of
     * {@link #waitForMinimumWorkers(int, Duration)}.
     */
    private static final class CountingClusterSizeMonitor
            extends ClusterSizeMonitor
    {
        final AtomicInteger waitCallCount = new AtomicInteger();

        CountingClusterSizeMonitor(InternalNodeManager nodeManager, NodeSchedulerConfig nodeSchedulerConfig)
        {
            super(nodeManager, nodeSchedulerConfig);
        }

        @Override
        public synchronized ListenableFuture<Void> waitForMinimumWorkers(int executionMinCount, Duration executionMaxWait)
        {
            waitCallCount.incrementAndGet();
            return immediateVoidFuture();
        }
    }

    private static final class NoConnectorServicesProvider
            implements ConnectorServicesProvider
    {
        @Override
        public void loadInitialCatalogs() {}

        @Override
        public void ensureCatalogsLoaded(List<CatalogProperties> catalogs) {}

        @Override
        public PrunableState getPrunableState()
        {
            return PrunableState.empty();
        }

        @Override
        public void pruneCatalogs(PrunableState prunableState, Set<CatalogHandle> catalogsInUse)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static final class ImmediateCreateTableTask
            implements DataDefinitionTask<CreateTable>
    {
        @Override
        public String getName()
        {
            return "test";
        }

        @Override
        public ListenableFuture<Void> execute(
                CreateTable statement,
                QueryStateMachine stateMachine,
                List<Expression> parameters,
                WarningCollector warningCollector)
        {
            return immediateVoidFuture();
        }
    }
}
