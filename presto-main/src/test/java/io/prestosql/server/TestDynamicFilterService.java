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
package io.prestosql.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.execution.StageId;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.StageState;
import io.prestosql.execution.StageStats;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.execution.TaskStatus;
import io.prestosql.operator.TaskStats;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.TestingColumnHandle;
import io.prestosql.spi.eventlistener.StageGcStatistics;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.Expression;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.execution.TaskInfo.createInitialTask;
import static io.prestosql.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.prestosql.spi.predicate.Domain.multipleValues;
import static io.prestosql.spi.predicate.Domain.none;
import static io.prestosql.spi.predicate.Domain.singleValue;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.testing.TestingHandles.TEST_TABLE_HANDLE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDynamicFilterService
{
    @Test
    public void testDynamicFilterSummaryCompletion()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(new TaskManagerConfig());
        String filterId = "df";
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 0);
        List<TaskId> taskIds = ImmutableList.of(new TaskId(stageId, 0), new TaskId(stageId, 1), new TaskId(stageId, 2));

        assertFalse(dynamicFilterService.getSummary(queryId, filterId).isPresent());
        TestDynamicFiltersStageSupplier dynamicFiltersStageSupplier = new TestDynamicFiltersStageSupplier();
        dynamicFiltersStageSupplier.addDynamicFilter(filterId, taskIds, "probeColumn");
        dynamicFilterService.registerQuery(queryId, dynamicFiltersStageSupplier);
        assertFalse(dynamicFilterService.getSummary(queryId, filterId).isPresent());

        dynamicFiltersStageSupplier.storeSummary(
                filterId,
                new TaskId(stageId, 0),
                singleValue(INTEGER, 1L));
        dynamicFilterService.collectDynamicFilters();
        assertFalse(dynamicFilterService.getSummary(queryId, filterId).isPresent());

        dynamicFiltersStageSupplier.storeSummary(
                filterId,
                new TaskId(stageId, 1),
                singleValue(INTEGER, 2L));
        dynamicFilterService.collectDynamicFilters();
        assertFalse(dynamicFilterService.getSummary(queryId, filterId).isPresent());

        dynamicFiltersStageSupplier.storeSummary(
                filterId,
                new TaskId(stageId, 2),
                singleValue(INTEGER, 3L));
        dynamicFilterService.collectDynamicFilters();
        Optional<Domain> summary = dynamicFilterService.getSummary(queryId, filterId);
        assertTrue(summary.isPresent());
        assertEquals(summary.get(), multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)));
    }

    @Test
    public void testDynamicFilterSupplier()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(new TaskManagerConfig());
        String filterId1 = "df1";
        String filterId2 = "df2";
        String filterId3 = "df3";
        Expression df1 = expression("DF_SYMBOL1");
        Expression df2 = expression("DF_SYMBOL2");
        Expression df3 = expression("DF_SYMBOL3");
        QueryId queryId = new QueryId("query");
        StageId stageId1 = new StageId(queryId, 1);
        StageId stageId2 = new StageId(queryId, 2);
        StageId stageId3 = new StageId(queryId, 3);

        Supplier<TupleDomain<ColumnHandle>> dynamicFilterSupplier = dynamicFilterService.createDynamicFilterSupplier(
                queryId,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, df1),
                        new DynamicFilters.Descriptor(filterId2, df2),
                        new DynamicFilters.Descriptor(filterId3, df3)),
                ImmutableMap.of(
                        Symbol.from(df1), new TestingColumnHandle("probeColumnA"),
                        Symbol.from(df2), new TestingColumnHandle("probeColumnA"),
                        Symbol.from(df3), new TestingColumnHandle("probeColumnB")));

        assertTrue(dynamicFilterSupplier.get().isAll());
        TestDynamicFiltersStageSupplier dynamicFiltersStageSupplier = new TestDynamicFiltersStageSupplier();

        List<TaskId> taskIds = ImmutableList.of(new TaskId(stageId1, 0), new TaskId(stageId1, 1));
        dynamicFiltersStageSupplier.addDynamicFilter(filterId1, taskIds, "probeColumnA");

        taskIds = ImmutableList.of(new TaskId(stageId2, 0), new TaskId(stageId2, 1));
        dynamicFiltersStageSupplier.addDynamicFilter(filterId2, taskIds, "probeColumnA");

        taskIds = ImmutableList.of(new TaskId(stageId3, 0), new TaskId(stageId3, 1));
        dynamicFiltersStageSupplier.addDynamicFilter(filterId3, taskIds, "probeColumnB");

        dynamicFilterService.registerQuery(queryId, dynamicFiltersStageSupplier);
        assertTrue(dynamicFilterSupplier.get().isAll());

        dynamicFiltersStageSupplier.storeSummary(
                filterId1,
                new TaskId(stageId1, 0),
                singleValue(INTEGER, 1L));
        dynamicFilterService.collectDynamicFilters();
        assertTrue(dynamicFilterSupplier.get().isAll());

        dynamicFiltersStageSupplier.storeSummary(
                filterId1,
                new TaskId(stageId1, 1),
                singleValue(INTEGER, 2L));
        dynamicFilterService.collectDynamicFilters();
        assertEquals(dynamicFilterSupplier.get(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                multipleValues(INTEGER, ImmutableList.of(1L, 2L)))));

        dynamicFiltersStageSupplier.storeSummary(
                filterId2,
                new TaskId(stageId2, 0),
                singleValue(INTEGER, 2L));
        dynamicFilterService.collectDynamicFilters();
        assertEquals(dynamicFilterSupplier.get(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                multipleValues(INTEGER, ImmutableList.of(1L, 2L)))));

        dynamicFiltersStageSupplier.storeSummary(
                filterId2,
                new TaskId(stageId2, 1),
                singleValue(INTEGER, 3L));
        dynamicFilterService.collectDynamicFilters();
        assertEquals(dynamicFilterSupplier.get(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 2L))));

        dynamicFiltersStageSupplier.storeSummary(
                filterId3,
                new TaskId(stageId3, 0),
                none(INTEGER));
        dynamicFilterService.collectDynamicFilters();
        assertEquals(dynamicFilterSupplier.get(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 2L))));

        dynamicFiltersStageSupplier.storeSummary(
                filterId3,
                new TaskId(stageId3, 1),
                none(INTEGER));
        dynamicFilterService.collectDynamicFilters();
        assertEquals(dynamicFilterSupplier.get(), TupleDomain.none());
    }

    private static class TestDynamicFiltersStageSupplier
            implements Supplier<List<StageInfo>>
    {
        private static final StageStats TEST_STAGE_STATS = new StageStats(
                new DateTime(0),

                new Distribution(0).snapshot(),

                4,
                5,
                6,

                7,
                8,
                10,
                26,
                11,

                12.0,
                DataSize.of(13, BYTE),
                DataSize.of(14, BYTE),
                DataSize.of(15, BYTE),
                DataSize.of(16, BYTE),
                DataSize.of(17, BYTE),

                new Duration(15, NANOSECONDS),
                new Duration(16, NANOSECONDS),
                new Duration(18, NANOSECONDS),
                false,
                ImmutableSet.of(),

                DataSize.of(191, BYTE),
                201,
                new Duration(19, NANOSECONDS),

                DataSize.of(192, BYTE),
                202,

                DataSize.of(19, BYTE),
                20,

                DataSize.of(21, BYTE),
                22,

                DataSize.of(23, BYTE),
                DataSize.of(24, BYTE),
                25,

                DataSize.of(26, BYTE),

                new StageGcStatistics(
                        101,
                        102,
                        103,
                        104,
                        105,
                        106,
                        107),

                ImmutableList.of());

        private final Map<Symbol, TableScanNode> probes = new HashMap<>();
        private final Map<StageId, StageInfo> stagesInfo = new HashMap<>();

        void addDynamicFilter(String filterId, List<TaskId> taskIds, String probeColumnName)
        {
            String colName = "column" + filterId;
            Symbol buildSymbol = new Symbol(colName);
            TableScanNode build = TableScanNode.newInstance(
                    new PlanNodeId("build" + filterId),
                    TEST_TABLE_HANDLE,
                    ImmutableList.of(buildSymbol),
                    ImmutableMap.of(buildSymbol, new TestingColumnHandle(colName)));

            Symbol probeSymbol = new Symbol(probeColumnName);
            TableScanNode probe = probes.computeIfAbsent(
                    probeSymbol,
                    symbol -> TableScanNode.newInstance(
                            new PlanNodeId("probe" + filterId),
                            TEST_TABLE_HANDLE,
                            ImmutableList.of(symbol),
                            ImmutableMap.of(symbol, new TestingColumnHandle(symbol.getName()))));

            PlanFragment testFragment = new PlanFragment(
                    new PlanFragmentId("plan_id" + filterId),
                    new JoinNode(
                            new PlanNodeId("join_id" + filterId),
                            INNER,
                            build,
                            probe,
                            ImmutableList.of(),
                            build.getOutputSymbols(),
                            probe.getOutputSymbols(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            ImmutableMap.of(filterId, probeSymbol),
                            Optional.empty()),
                    ImmutableMap.of(probeSymbol, VARCHAR),
                    SOURCE_DISTRIBUTION,
                    ImmutableList.of(),
                    new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(probeSymbol)),
                    ungroupedExecution(),
                    StatsAndCosts.empty(),
                    Optional.empty());

            StageId stageId = taskIds.stream().findFirst().get().getStageId();
            List<TaskInfo> tasks = taskIds.stream()
                    .map(taskId -> createInitialTask(
                            taskId, URI.create(""), "", ImmutableList.of(), new TaskStats(DateTime.now(), DateTime.now())))
                    .collect(toImmutableList());

            stagesInfo.put(stageId, new StageInfo(
                    stageId,
                    StageState.RUNNING,
                    testFragment,
                    ImmutableList.of(),
                    TEST_STAGE_STATS,
                    tasks,
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    null));
        }

        void storeSummary(String filterId, TaskId taskId, Domain domain)
        {
            StageId stageId = taskId.getStageId();
            ImmutableList.Builder<TaskInfo> updatedTasks = ImmutableList.builder();
            StageInfo stageInfo = stagesInfo.get(stageId);
            for (TaskInfo task : stageInfo.getTasks()) {
                if (task.getTaskStatus().getTaskId().equals(taskId)) {
                    TaskStatus taskStatus = task.getTaskStatus();
                    updatedTasks.add(new TaskInfo(
                            new TaskStatus(
                                    taskStatus.getTaskId(),
                                    taskStatus.getTaskInstanceId(),
                                    taskStatus.getVersion(),
                                    taskStatus.getState(),
                                    taskStatus.getSelf(),
                                    taskStatus.getNodeId(),
                                    taskStatus.getCompletedDriverGroups(),
                                    taskStatus.getFailures(),
                                    taskStatus.getQueuedPartitionedDrivers(),
                                    taskStatus.getRunningPartitionedDrivers(),
                                    taskStatus.isOutputBufferOverutilized(),
                                    taskStatus.getPhysicalWrittenDataSize(),
                                    taskStatus.getMemoryReservation(),
                                    taskStatus.getSystemMemoryReservation(),
                                    taskStatus.getRevocableMemoryReservation(),
                                    taskStatus.getFullGcCount(),
                                    taskStatus.getFullGcTime(),
                                    ImmutableMap.of(filterId, domain)),
                            task.getLastHeartbeat(),
                            task.getOutputBuffers(),
                            task.getNoMoreSplits(),
                            task.getStats(),
                            task.isNeedsPlan()));
                }
                else {
                    updatedTasks.add(task);
                }
            }
            stagesInfo.put(stageId, new StageInfo(
                    stageInfo.getStageId(),
                    stageInfo.getState(),
                    stageInfo.getPlan(),
                    stageInfo.getTypes(),
                    TEST_STAGE_STATS,
                    updatedTasks.build(),
                    stageInfo.getSubStages(),
                    stageInfo.getTables(),
                    null));
        }

        @Override
        public List<StageInfo> get()
        {
            return ImmutableList.copyOf(stagesInfo.values());
        }
    }
}
