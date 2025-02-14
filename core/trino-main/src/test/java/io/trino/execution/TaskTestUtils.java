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
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.json.ObjectMapperProvider;
import io.opentelemetry.api.trace.Span;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogServiceProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.event.SplitMonitor;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.BaseTestSqlTaskManager.MockDirectExchangeClientSupplier;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.Split;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.PagesIndex;
import io.trino.operator.index.IndexJoinLookupStats;
import io.trino.operator.index.IndexManager;
import io.trino.server.protocol.spooling.QueryDataEncoders;
import io.trino.server.protocol.spooling.SpoolingEnabledConfig;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spiller.GenericSpillerFactory;
import io.trino.split.PageSinkManager;
import io.trino.split.PageSourceManager;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.gen.columnar.ColumnarFilterCompiler;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.planner.LocalExecutionPlanner;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.testing.TestingSplit;
import io.trino.type.BlockTypeOperators;
import io.trino.util.FinalizerService;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.opentelemetry.api.OpenTelemetry.noop;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;

public final class TaskTestUtils
{
    private TaskTestUtils() {}

    public static final PlanNodeId TABLE_SCAN_NODE_ID = new PlanNodeId("tableScan");

    private static final CatalogHandle CATALOG_HANDLE = TEST_TABLE_HANDLE.catalogHandle();

    public static final ScheduledSplit SPLIT = new ScheduledSplit(0, TABLE_SCAN_NODE_ID, new Split(CATALOG_HANDLE, TestingSplit.createLocalSplit()));

    public static final ImmutableList<SplitAssignment> EMPTY_SPLIT_ASSIGNMENTS = ImmutableList.of();

    public static final Symbol SYMBOL = new Symbol(BIGINT, "column");

    public static final PlanFragment PLAN_FRAGMENT = new PlanFragment(
            new PlanFragmentId("fragment"),
            new TableScanNode(
                    TABLE_SCAN_NODE_ID,
                    TEST_TABLE_HANDLE,
                    ImmutableList.of(SYMBOL),
                    ImmutableMap.of(SYMBOL, new TestingColumnHandle("column", 0, BIGINT)),
                    TupleDomain.all(),
                    Optional.empty(),
                    false,
                    Optional.empty()),
            ImmutableSet.of(SYMBOL),
            SOURCE_DISTRIBUTION,
            Optional.empty(),
            ImmutableList.of(TABLE_SCAN_NODE_ID),
            new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(SYMBOL))
                    .withBucketToPartition(Optional.of(new int[1])),
            StatsAndCosts.empty(),
            ImmutableList.of(),
            ImmutableMap.of(),
            Optional.empty());

    public static final DynamicFilterId DYNAMIC_FILTER_SOURCE_ID = new DynamicFilterId("filter");

    public static final PlanFragment PLAN_FRAGMENT_WITH_DYNAMIC_FILTER_SOURCE = new PlanFragment(
            new PlanFragmentId("fragment"),
            new DynamicFilterSourceNode(
                    new PlanNodeId("dynamicFilterSource"),
                    new TableScanNode(
                            TABLE_SCAN_NODE_ID,
                            TEST_TABLE_HANDLE,
                            ImmutableList.of(SYMBOL),
                            ImmutableMap.of(SYMBOL, new TestingColumnHandle("column", 0, BIGINT)),
                            TupleDomain.all(),
                            Optional.empty(),
                            false,
                            Optional.empty()),
                    ImmutableMap.of(DYNAMIC_FILTER_SOURCE_ID, SYMBOL)),
            ImmutableSet.of(SYMBOL),
            SOURCE_DISTRIBUTION,
            Optional.empty(),
            ImmutableList.of(TABLE_SCAN_NODE_ID),
            new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(SYMBOL))
                    .withBucketToPartition(Optional.of(new int[1])),
            StatsAndCosts.empty(),
            ImmutableList.of(),
            ImmutableMap.of(),
            Optional.empty());

    public static LocalExecutionPlanner createTestingPlanner()
    {
        PageSourceManager pageSourceManager = new PageSourceManager(CatalogServiceProvider.singleton(CATALOG_HANDLE, new TestingPageSourceProvider()));

        // we don't start the finalizer so nothing will be collected, which is ok for a test
        FinalizerService finalizerService = new FinalizerService();

        BlockTypeOperators blockTypeOperators = new BlockTypeOperators(PLANNER_CONTEXT.getTypeOperators());
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(
                new InMemoryNodeManager(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(finalizerService)));
        NodePartitioningManager nodePartitioningManager = new NodePartitioningManager(
                nodeScheduler,
                PLANNER_CONTEXT.getTypeOperators(),
                CatalogServiceProvider.fail());

        PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(PLANNER_CONTEXT.getFunctionManager(), 0);
        ColumnarFilterCompiler columnarFilterCompiler = new ColumnarFilterCompiler(PLANNER_CONTEXT.getFunctionManager(), 0);
        return new LocalExecutionPlanner(
                PLANNER_CONTEXT,
                Optional.empty(),
                pageSourceManager,
                new IndexManager(CatalogServiceProvider.fail()),
                nodePartitioningManager,
                new PageSinkManager(CatalogServiceProvider.fail()),
                new MockDirectExchangeClientSupplier(),
                new ExpressionCompiler(PLANNER_CONTEXT.getFunctionManager(), pageFunctionCompiler, columnarFilterCompiler),
                pageFunctionCompiler,
                new JoinFilterFunctionCompiler(PLANNER_CONTEXT.getFunctionManager()),
                new IndexJoinLookupStats(),
                new TaskManagerConfig(),
                new GenericSpillerFactory((types, spillContext, memoryContext) -> {
                    throw new UnsupportedOperationException();
                }),
                new QueryDataEncoders(new SpoolingEnabledConfig(), Set.of()),
                Optional.empty(),
                (types, spillContext, memoryContext) -> {
                    throw new UnsupportedOperationException();
                },
                (types, partitionFunction, spillContext, memoryContext) -> {
                    throw new UnsupportedOperationException();
                },
                new PagesIndex.TestingFactory(false),
                new JoinCompiler(PLANNER_CONTEXT.getTypeOperators()),
                new FlatHashStrategyCompiler(PLANNER_CONTEXT.getTypeOperators()),
                new OrderingCompiler(PLANNER_CONTEXT.getTypeOperators()),
                new DynamicFilterConfig(),
                blockTypeOperators,
                PLANNER_CONTEXT.getTypeOperators(),
                new TableExecuteContextManager(),
                new ExchangeManagerRegistry(noop(), noopTracer(), new SecretsResolver(ImmutableMap.of())),
                new NodeVersion("test"),
                new CompilerConfig());
    }

    public static TaskInfo updateTask(SqlTask sqlTask, List<SplitAssignment> splitAssignments, OutputBuffers outputBuffers)
    {
        return sqlTask.updateTask(TEST_SESSION, Span.getInvalid(), Optional.of(PLAN_FRAGMENT), splitAssignments, outputBuffers, ImmutableMap.of(), false);
    }

    public static SplitMonitor createTestSplitMonitor()
    {
        return new SplitMonitor(
                new EventListenerManager(new EventListenerConfig(), new SecretsResolver(ImmutableMap.of()), noop(), noopTracer(), new NodeVersion("test")),
                new ObjectMapperProvider().get());
    }
}
