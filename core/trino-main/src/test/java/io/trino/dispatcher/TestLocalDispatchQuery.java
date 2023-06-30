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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogProperties;
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
import io.trino.execution.StageInfo;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.server.protocol.Slug;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertTrue;

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
                transactionManager,
                emptyEventListenerManager(),
                new AccessControlConfig(),
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
                new NodeVersion("test"));
        QueryMonitor queryMonitor = new QueryMonitor(
                JsonCodec.jsonCodec(StageInfo.class),
                JsonCodec.jsonCodec(OperatorStats.class),
                JsonCodec.jsonCodec(ExecutionFailureInfo.class),
                JsonCodec.jsonCodec(StatsAndCosts.class),
                new EventListenerManager(new EventListenerConfig()),
                new NodeInfo("node"),
                new NodeVersion("version"),
                new SessionPropertyManager(),
                metadata,
                new FunctionManager(
                        new ConnectorCatalogServiceProvider<>("function provider", new NoConnectorServicesProvider(), ConnectorServices::getFunctionProvider),
                        new GlobalFunctionCatalog()),
                new QueryMonitorConfig());
        CreateTable createTable = new CreateTable(QualifiedName.of("table"), ImmutableList.of(), false, ImmutableList.of(), Optional.empty());
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
                new TestClusterSizeMonitor(new InMemoryNodeManager(ImmutableSet.of()), new NodeSchedulerConfig()),
                executor,
                (queryExecution -> dataDefinitionExecution.start()));
        queryStateMachine.addStateChangeListener(state -> {
            if (state.ordinal() >= QueryState.PLANNING.ordinal()) {
                countDownLatch.countDown();
            }
        });
        localDispatchQuery.startWaitingForResources();
        countDownLatch.await();
        assertTrue(localDispatchQuery.getDispatchInfo().getCoordinatorLocation().isPresent());
    }

    private static class NoConnectorServicesProvider
            implements ConnectorServicesProvider
    {
        @Override
        public void loadInitialCatalogs() {}

        @Override
        public void ensureCatalogsLoaded(Session session, List<CatalogProperties> catalogs) {}

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
