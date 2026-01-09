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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.json.JsonCodec;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogHandle;
import io.trino.connector.ConnectorCatalogServiceProvider;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.event.QueryMonitor;
import io.trino.event.QueryMonitorConfig;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
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
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.opentelemetry.api.OpenTelemetry.noop;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.metadata.TestMetadataManager.createTestMetadataManager;
import static io.trino.sql.tree.SaveMode.FAIL;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLocalDispatchQuery
{
    private CountDownLatch countDownLatch;

    @Test
    public void testSubmittedForDispatchedQuery()
            throws InterruptedException
    {
        countDownLatch = new CountDownLatch(1);
        Executor executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        Metadata metadata = createTestMetadataManager();
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
                URI.create("fake://fake-query"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                accessControl,
                executor,
                metadata,
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                Optional.of(QueryType.DATA_DEFINITION),
                true,
                Optional.empty(),
                new NodeVersion("test"));
        QueryMonitor queryMonitor = new QueryMonitor(
                JsonCodec.jsonCodec(StagesInfo.class),
                JsonCodec.jsonCodec(OperatorStats.class),
                JsonCodec.jsonCodec(ExecutionFailureInfo.class),
                JsonCodec.jsonCodec(StatsAndCosts.class),
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
        DataDefinitionExecution.DataDefinitionExecutionFactory dataDefinitionExecutionFactory = new DataDefinitionExecution.DataDefinitionExecutionFactory(
                ImmutableMap.<Class<? extends Statement>, DataDefinitionTask<?>>of(CreateTable.class, new TestCreateTableTask()));
        DataDefinitionExecution dataDefinitionExecution = dataDefinitionExecutionFactory.createQueryExecution(
                preparedQuery,
                queryStateMachine,
                Slug.createNew(),
                WarningCollector.NOOP,
                null);
        LocalDispatchQuery localDispatchQuery = new LocalDispatchQuery(
                queryStateMachine,
                Futures.immediateFuture(dataDefinitionExecution),
                queryMonitor,
                new TestClusterSizeMonitor(TestingInternalNodeManager.createDefault(), new NodeSchedulerConfig()),
                executor,
                queryExecution -> dataDefinitionExecution.start());
        queryStateMachine.addStateChangeListener(state -> {
            if (state.ordinal() >= QueryState.PLANNING.ordinal()) {
                countDownLatch.countDown();
            }
        });
        localDispatchQuery.startWaitingForResources();
        countDownLatch.await();
        assertThat(localDispatchQuery.getDispatchInfo().getCoordinatorLocation()).isPresent();
    }

    /**
     * Tests that fail() does not block when the queryExecutionFuture is still running,
     * and verifies that the original failure cause is preserved (not replaced by CancellationException).
     * This reproduces the scenario where:
     * 1. Query analysis is in progress (e.g., blocked on HMS call)
     * 2. Resource group rejects the query and calls fail()
     * 3. fail() should not block waiting for the analysis to complete
     * 4. The original failure cause should be preserved in the query state
     */
    @Test
    public void testFailDoesNotBlockAndPreservesFailureCause()
            throws Exception
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-fail-test-%s"));
        try {
            Metadata metadata = createTestMetadataManager();
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
                    URI.create("fake://fake-query"),
                    new ResourceGroupId("test"),
                    false,
                    transactionManager,
                    accessControl,
                    executor,
                    metadata,
                    WarningCollector.NOOP,
                    createPlanOptimizersStatsCollector(),
                    Optional.of(QueryType.DATA_DEFINITION),
                    true,
                    Optional.empty(),
                    new NodeVersion("test"));

            QueryMonitor queryMonitor = new QueryMonitor(
                    JsonCodec.jsonCodec(StagesInfo.class),
                    JsonCodec.jsonCodec(OperatorStats.class),
                    JsonCodec.jsonCodec(ExecutionFailureInfo.class),
                    JsonCodec.jsonCodec(StatsAndCosts.class),
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

            // Latch to signal when the "analysis" has started
            CountDownLatch analysisStarted = new CountDownLatch(1);
            // Latch to control when the "analysis" completes
            CountDownLatch allowAnalysisToComplete = new CountDownLatch(1);
            // Track any exceptions from fail()
            AtomicReference<Throwable> failException = new AtomicReference<>();

            // Submit the "analysis" work to the executor
            ListenableFuture<Object> queryExecutionFuture = Futures.submit(() -> {
                analysisStarted.countDown();
                // Wait until test allows completion (interruptible is fine for this test)
                allowAnalysisToComplete.await();
                return null;
            }, executor);

            // Create LocalDispatchQuery with the future
            @SuppressWarnings("unchecked")
            LocalDispatchQuery localDispatchQuery = new LocalDispatchQuery(
                    queryStateMachine,
                    (ListenableFuture) queryExecutionFuture,
                    queryMonitor,
                    new TestClusterSizeMonitor(TestingInternalNodeManager.createDefault(), new NodeSchedulerConfig()),
                    executor,
                    _ -> {});

            // Wait for analysis to start
            assertThat(analysisStarted.await(5, TimeUnit.SECONDS)).isTrue();

            // Call fail() - this should NOT block
            String expectedErrorMessage = "Query queue full - test error";
            CountDownLatch failCompleted = new CountDownLatch(1);
            executor.submit(() -> {
                try {
                    localDispatchQuery.fail(new RuntimeException(expectedErrorMessage));
                }
                catch (Throwable t) {
                    failException.set(t);
                }
                finally {
                    failCompleted.countDown();
                }
            });

            // fail() should complete quickly (not block on the running future)
            assertThat(failCompleted.await(2, TimeUnit.SECONDS))
                    .describedAs("fail() should not block when future is running")
                    .isTrue();

            // Check no exception was thrown from fail()
            assertThat(failException.get()).isNull();

            // Allow the analysis to complete (in case it's still running)
            allowAnalysisToComplete.countDown();

            // Wait for the state to transition to FAILED
            CountDownLatch stateChangedToFailed = new CountDownLatch(1);
            queryStateMachine.addStateChangeListener(state -> {
                if (state == QueryState.FAILED) {
                    stateChangedToFailed.countDown();
                }
            });

            assertThat(stateChangedToFailed.await(5, TimeUnit.SECONDS))
                    .describedAs("Query should transition to FAILED")
                    .isTrue();

            // Verify the query is in FAILED state
            assertThat(queryStateMachine.getQueryState()).isEqualTo(QueryState.FAILED);

            // Verify the original failure cause is preserved (not CancellationException)
            Optional<ExecutionFailureInfo> failureInfo = queryStateMachine.getFailureInfo();
            assertThat(failureInfo).isPresent();
            assertThat(failureInfo.get().getMessage()).contains(expectedErrorMessage);
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static class NoConnectorServicesProvider
            implements ConnectorServicesProvider
    {
        @Override
        public void loadInitialCatalogs() {}

        @Override
        public void ensureCatalogsLoaded(List<CatalogProperties> catalogs) {}

        @Override
        public void pruneCatalogs(Set<CatalogHandle> catalogsInUse)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestCreateTableTask
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
            while (true) {
                try {
                    Thread.sleep(10_000L);
                }
                catch (InterruptedException e) {
                    break;
                }
            }
            return null;
        }
    }

    private static class TestClusterSizeMonitor
            extends ClusterSizeMonitor
    {
        public TestClusterSizeMonitor(InternalNodeManager nodeManager, NodeSchedulerConfig nodeSchedulerConfig)
        {
            super(nodeManager, nodeSchedulerConfig);
        }

        @Override
        public synchronized ListenableFuture<Void> waitForMinimumWorkers(int executionMinCount, Duration executionMaxWait)
        {
            return immediateVoidFuture();
        }
    }
}
