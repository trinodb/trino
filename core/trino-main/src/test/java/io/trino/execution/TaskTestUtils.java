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
import io.airlift.json.ObjectMapperProvider;
import io.trino.connector.CatalogName;
import io.trino.cost.StatsAndCosts;
import io.trino.event.SplitMonitor;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.TestSqlTaskManager.MockDirectExchangeClientSupplier;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.index.IndexManager;
import io.trino.metadata.ExchangeHandleResolver;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.Split;
import io.trino.operator.PagesIndex;
import io.trino.operator.TrinoOperatorFactories;
import io.trino.operator.index.IndexJoinLookupStats;
import io.trino.spiller.GenericSpillerFactory;
import io.trino.split.PageSinkManager;
import io.trino.split.PageSourceManager;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.planner.LocalExecutionPlanner;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.testing.TestingSplit;
import io.trino.type.BlockTypeOperators;
import io.trino.util.FinalizerService;

import java.util.List;
import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.GroupByHashFactoryTestUtils.createGroupByHashFactory;
import static io.trino.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;

public final class TaskTestUtils
{
    private TaskTestUtils() {}

    public static final PlanNodeId TABLE_SCAN_NODE_ID = new PlanNodeId("tableScan");

    private static final CatalogName CONNECTOR_ID = TEST_TABLE_HANDLE.getCatalogName();

    public static final ScheduledSplit SPLIT = new ScheduledSplit(0, TABLE_SCAN_NODE_ID, new Split(CONNECTOR_ID, TestingSplit.createLocalSplit(), Lifespan.taskWide()));

    public static final ImmutableList<SplitAssignment> EMPTY_SPLIT_ASSIGNMENTS = ImmutableList.of();

    public static final Symbol SYMBOL = new Symbol("column");

    public static final PlanFragment PLAN_FRAGMENT = new PlanFragment(
            new PlanFragmentId("fragment"),
            TableScanNode.newInstance(
                    TABLE_SCAN_NODE_ID,
                    TEST_TABLE_HANDLE,
                    ImmutableList.of(SYMBOL),
                    ImmutableMap.of(SYMBOL, new TestingColumnHandle("column", 0, BIGINT)),
                    false,
                    Optional.empty()),
            ImmutableMap.of(SYMBOL, VARCHAR),
            SOURCE_DISTRIBUTION,
            ImmutableList.of(TABLE_SCAN_NODE_ID),
            new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(SYMBOL))
                    .withBucketToPartition(Optional.of(new int[1])),
            ungroupedExecution(),
            StatsAndCosts.empty(),
            Optional.empty());

    public static LocalExecutionPlanner createTestingPlanner()
    {
        PageSourceManager pageSourceManager = new PageSourceManager();
        pageSourceManager.addConnectorPageSourceProvider(CONNECTOR_ID, new TestingPageSourceProvider());

        // we don't start the finalizer so nothing will be collected, which is ok for a test
        FinalizerService finalizerService = new FinalizerService();

        BlockTypeOperators blockTypeOperators = new BlockTypeOperators(PLANNER_CONTEXT.getTypeOperators());
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(
                new InMemoryNodeManager(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(finalizerService)));
        NodePartitioningManager nodePartitioningManager = new NodePartitioningManager(nodeScheduler, blockTypeOperators);

        PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(PLANNER_CONTEXT.getFunctionManager(), 0);
        JoinCompiler joinCompiler = new JoinCompiler(PLANNER_CONTEXT.getTypeOperators());
        return new LocalExecutionPlanner(
                PLANNER_CONTEXT,
                createTestingTypeAnalyzer(PLANNER_CONTEXT),
                Optional.empty(),
                pageSourceManager,
                new IndexManager(),
                nodePartitioningManager,
                new PageSinkManager(),
                new MockDirectExchangeClientSupplier(),
                new ExpressionCompiler(PLANNER_CONTEXT.getFunctionManager(), pageFunctionCompiler),
                pageFunctionCompiler,
                new JoinFilterFunctionCompiler(PLANNER_CONTEXT.getFunctionManager()),
                new IndexJoinLookupStats(),
                new TaskManagerConfig(),
                new GenericSpillerFactory((types, spillContext, memoryContext) -> {
                    throw new UnsupportedOperationException();
                }),
                (types, spillContext, memoryContext) -> {
                    throw new UnsupportedOperationException();
                },
                (types, partitionFunction, spillContext, memoryContext) -> {
                    throw new UnsupportedOperationException();
                },
                new PagesIndex.TestingFactory(false),
                joinCompiler,
                createGroupByHashFactory(joinCompiler, blockTypeOperators),
                new TrinoOperatorFactories(),
                new OrderingCompiler(PLANNER_CONTEXT.getTypeOperators()),
                new DynamicFilterConfig(),
                blockTypeOperators,
                new TableExecuteContextManager(),
                new ExchangeManagerRegistry(new ExchangeHandleResolver()));
    }

    public static TaskInfo updateTask(SqlTask sqlTask, List<SplitAssignment> splitAssignments, OutputBuffers outputBuffers)
    {
        return sqlTask.updateTask(TEST_SESSION, Optional.of(PLAN_FRAGMENT), splitAssignments, outputBuffers, ImmutableMap.of());
    }

    public static SplitMonitor createTestSplitMonitor()
    {
        return new SplitMonitor(
                new EventListenerManager(new EventListenerConfig()),
                new ObjectMapperProvider().get());
    }
}
