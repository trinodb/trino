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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.node.NodeInfo;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.tracing.Tracing;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.connector.CatalogConnector;
import io.trino.connector.CatalogFactory;
import io.trino.connector.CatalogProperties;
import io.trino.connector.CatalogPruneTask;
import io.trino.connector.CatalogPruneTaskConfig;
import io.trino.connector.ConnectorName;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.TestingLocalCatalogPruneTask;
import io.trino.connector.WorkerDynamicCatalogManager;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.execution.executor.RunningSplitInfo;
import io.trino.execution.executor.TaskExecutor;
import io.trino.execution.executor.TaskHandle;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.WorkerLanguageFunctionProvider;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spiller.LocalSpillManager;
import io.trino.spiller.NodeSpillConfig;
import io.trino.sql.planner.PlanFragment;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingSession;
import io.trino.transaction.NoOpTransactionManager;
import io.trino.transaction.TransactionInfo;
import io.trino.util.EmbedVersion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;
import java.util.function.Predicate;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.execution.BaseTestSqlTaskManager.OUT;
import static io.trino.execution.TaskTestUtils.PLAN_FRAGMENT;
import static io.trino.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static io.trino.execution.TaskTestUtils.createTestSplitMonitor;
import static io.trino.execution.TaskTestUtils.createTestingPlanner;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.PARTITIONED;
import static io.trino.metadata.CatalogManager.NO_CATALOGS;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestSqlTaskManagerRaceWithCatalogPrune
{
    private static final int NUM_TASKS = 20000;
    private static final ConnectorServicesProvider NOOP_CONNECTOR_SERVICES_PROVIDER = new ConnectorServicesProvider()
    {
        @Override
        public void loadInitialCatalogs() {}

        @Override
        public void ensureCatalogsLoaded(Session session, List<CatalogProperties> catalogs) {}

        @Override
        public void pruneCatalogs(Set<CatalogHandle> catalogsInUse) {}

        @Override
        public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
        {
            return null;
        }
    };
    private static final CatalogFactory MOCK_CATALOG_FACTORY = new CatalogFactory()
    {
        @Override
        public void addConnectorFactory(ConnectorFactory connectorFactory) {}

        @Override
        public CatalogConnector createCatalog(CatalogProperties catalogProperties)
        {
            Connector connector = MockConnectorFactory.create().create(catalogProperties.getCatalogHandle().getCatalogName(), catalogProperties.getProperties(), new TestingConnectorContext());
            ConnectorServices noOpConnectorService = new ConnectorServices(
                    Tracing.noopTracer(),
                    catalogProperties.getCatalogHandle(),
                    connector);
            return new CatalogConnector(
                    catalogProperties.getCatalogHandle(),
                    new ConnectorName("mock"),
                    noOpConnectorService,
                    noOpConnectorService,
                    noOpConnectorService,
                    Optional.of(catalogProperties));
        }

        @Override
        public CatalogConnector createCatalog(CatalogHandle catalogHandle, ConnectorName connectorName, Connector connector)
        {
            throw new UnsupportedOperationException("Only implement what is needed by worker catalog manager");
        }
    };
    private static final TaskExecutor NOOP_TASK_EXECUTOR = new TaskExecutor() {
        @Override
        public TaskHandle addTask(TaskId taskId, DoubleSupplier utilizationSupplier, int initialSplitConcurrency, Duration splitConcurrencyAdjustFrequency, OptionalInt maxDriversPerTask)
        {
            return new TaskHandle() {
                @Override
                public boolean isDestroyed()
                {
                    return false;
                }
            };
        }

        @Override
        public void removeTask(TaskHandle taskHandle) {}

        @Override
        public List<ListenableFuture<Void>> enqueueSplits(TaskHandle taskHandle, boolean intermediate, List<? extends SplitRunner> taskSplits)
        {
            return ImmutableList.of();
        }

        @Override
        public Set<TaskId> getStuckSplitTaskIds(Duration processingDurationThreshold, Predicate<RunningSplitInfo> filter)
        {
            return ImmutableSet.of();
        }

        @Override
        public void start() {}

        @Override
        public void stop() {}
    };
    private final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2, 2, 10, TimeUnit.MINUTES, new LinkedBlockingDeque<>());
    private final AtomicInteger sequence = new AtomicInteger(1);

    @AfterAll
    public void cleanup()
    {
        threadPoolExecutor.shutdown();
    }

    @Test
    public void testMultipleTaskUpdatesWithMultipleCatalogPrunes()
    {
        ConnectorServicesProvider workerConnectorServiceProvider = new WorkerDynamicCatalogManager(MOCK_CATALOG_FACTORY);
        SqlTaskManager workerTaskManager = getWorkerTaskManagerWithConnectorServiceProvider(workerConnectorServiceProvider);

        CatalogPruneTask catalogPruneTask = new TestingLocalCatalogPruneTask(
                new NoInfoTransactionManager(),
                NO_CATALOGS,
                NOOP_CONNECTOR_SERVICES_PROVIDER,
                new NodeInfo("testversion"),
                new CatalogPruneTaskConfig(),
                workerTaskManager);

        Future<Void> catalogTaskFuture = Futures.submit(() ->
        {
            for (int i = 0; i < NUM_TASKS; i++) {
                String catalogName = "catalog_" + i;
                CatalogHandle catalogHandle = createRootCatalogHandle(catalogName, new CatalogHandle.CatalogVersion(UUID.randomUUID().toString()));
                TaskId taskId = newTaskId();
                workerTaskManager.updateTask(
                        TestingSession.testSession(),
                        taskId,
                        Span.getInvalid(),
                        Optional.of(fragmentWithCatalog(catalogHandle)),
                        ImmutableList.of(new SplitAssignment(TABLE_SCAN_NODE_ID, ImmutableSet.of(), true)),
                        PipelinedOutputBuffers.createInitial(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds(),
                        ImmutableMap.of(),
                        false);
                try {
                    Thread.sleep(0, ThreadLocalRandom.current().nextInt(25, 75));
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                assertDoesNotThrow(() -> workerConnectorServiceProvider.getConnectorServices(catalogHandle));
                workerTaskManager.cancelTask(taskId);
                if ((i & 63) == 0) {
                    workerTaskManager.removeOldTasks();
                }
            }
        }, threadPoolExecutor);

        Future<Void> pruneCatalogsFuture = Futures.submit(() ->
        {
            for (int i = 0; i < NUM_TASKS; i++) {
                catalogPruneTask.pruneWorkerCatalogs();
                try {
                    Thread.sleep(0, ThreadLocalRandom.current().nextInt(25, 75));
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }, threadPoolExecutor);

        assertDoesNotThrow(() -> catalogTaskFuture.get(2, TimeUnit.MINUTES));
        assertDoesNotThrow(() -> pruneCatalogsFuture.get(2, TimeUnit.MINUTES));
    }

    private TaskId newTaskId()
    {
        return new TaskId(new StageId("query" + sequence.incrementAndGet(), 0), 1, 0);
    }

    private static SqlTaskManager getWorkerTaskManagerWithConnectorServiceProvider(ConnectorServicesProvider workerConnectorServiceProvider)
    {
        return new SqlTaskManager(
                new EmbedVersion("testversion"),
                workerConnectorServiceProvider,
                createTestingPlanner(),
                new WorkerLanguageFunctionProvider(),
                new BaseTestSqlTaskManager.MockLocationFactory(),
                NOOP_TASK_EXECUTOR,
                createTestSplitMonitor(),
                new NodeInfo("testversion"),
                new LocalMemoryManager(new NodeMemoryConfig()),
                new TaskManagementExecutor(),
                new TaskManagerConfig().setInfoMaxAge(Duration.ZERO),
                new NodeMemoryConfig(),
                new LocalSpillManager(new NodeSpillConfig()),
                new NodeSpillConfig(),
                new TestingGcMonitor(),
                noopTracer(),
                new ExchangeManagerRegistry(OpenTelemetry.noop(), Tracing.noopTracer()),
                ignore -> true);
    }

    private static PlanFragment fragmentWithCatalog(CatalogHandle catalogHandle)
    {
        return PLAN_FRAGMENT.withActiveCatalogs(ImmutableList.of(
                new CatalogProperties(
                        catalogHandle,
                        new ConnectorName("mock"),
                        ImmutableMap.of())));
    }

    private static class NoInfoTransactionManager
            extends NoOpTransactionManager
    {
        @Override
        public List<TransactionInfo> getAllTransactionInfos()
        {
            return ImmutableList.of();
        }
    }
}
