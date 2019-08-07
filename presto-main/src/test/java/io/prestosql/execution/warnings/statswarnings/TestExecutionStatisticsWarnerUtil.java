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

package io.prestosql.execution.warnings.statswarnings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.connector.CatalogName;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryStats;
import io.prestosql.execution.StageId;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.StageState;
import io.prestosql.execution.StageStats;
import io.prestosql.execution.TableInfo;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskStatus;
import io.prestosql.execution.buffer.OutputBufferInfo;
import io.prestosql.metadata.TableHandle;
import io.prestosql.operator.BlockedReason;
import io.prestosql.operator.OperatorStats;
import io.prestosql.operator.SplitOperatorInfo;
import io.prestosql.operator.StageExecutionDescriptor;
import io.prestosql.operator.TaskStats;
import io.prestosql.operator.TestPipelineStats;
import io.prestosql.plugin.tpch.TpchTableHandle;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.eventlistener.StageGcStatistics;
import io.prestosql.spi.memory.MemoryPoolId;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningHandle;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TestingConnectorTransactionHandle;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.execution.QueryState.RUNNING;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class TestExecutionStatisticsWarnerUtil
{
    private TestExecutionStatisticsWarnerUtil()
    {
    }

    public static QueryInfo getQueryInfo1()
    {
        URI uri = null;
        try {
            uri = new URI("111");
        }
        catch (Exception e) {
        }

        StageStats stageStats = new StageStats(
                new DateTime(0),
                getTestDistribution(1),
                4,
                5,
                6,
                7,
                8,
                10,
                26,
                11,
                12.0,
                new DataSize(13, BYTE),
                new DataSize(14, BYTE),
                new DataSize(15, BYTE),
                new DataSize(16, BYTE),
                new DataSize(17, BYTE),
                new Duration(15, NANOSECONDS),
                new Duration(16, NANOSECONDS),
                new Duration(18, NANOSECONDS),
                false,
                ImmutableSet.of(),
                new DataSize(191, BYTE),
                201,
                new DataSize(192, BYTE),
                202,
                new DataSize(19, BYTE),
                20,
                new DataSize(21, BYTE),
                22,
                new DataSize(23, BYTE),
                new DataSize(24, BYTE),
                25,
                new DataSize(26, BYTE),
                new StageGcStatistics(
                    101,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107),
                ImmutableList.of(createOperatorStats("1", 1), createOperatorStats("2", 10000)));

        StageInfo stageInfo2 = new StageInfo(
                new StageId("2", 2),
                StageState.FINISHED,
                uri,
                createPlanFragment(createTableSubPlan()),
                null,
                stageStats,
                Arrays.asList(createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(12.0, 12.0, uri)),
                new ArrayList<>(),
                new HashMap<PlanNodeId, TableInfo>(),
                null);

        StageInfo stageInfo1 = new StageInfo(
                new StageId("1", 1),
                StageState.FINISHED,
                uri,
                createPlanFragment(createPlan1()),
                null,
                stageStats,
                Arrays.asList(createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(12.0, 12.0, uri)),
                Arrays.asList(stageInfo2),
                new HashMap<PlanNodeId, TableInfo>(),
                null);

        return new QueryInfo(
                new QueryId("0"),
                TEST_SESSION.toSessionRepresentation(),
                RUNNING,
                new MemoryPoolId("reserved"),
                false,
                URI.create("1"),
                ImmutableList.of("2", "3"),
                "SELECT 4",
                Optional.of("SELECT 4"),
                new QueryStats(
                        DateTime.parse("1991-09-06T05:00-05:30"),
                        DateTime.parse("1991-09-06T05:01-05:30"),
                        DateTime.parse("1991-09-06T05:02-05:30"),
                        DateTime.parse("1991-09-06T06:00-05:30"),
                        Duration.valueOf("8m"),
                        Duration.valueOf("7m"),
                        Duration.valueOf("34m"),
                        Duration.valueOf("35m"),
                        Duration.valueOf("44m"),
                        Duration.valueOf("9m"),
                        Duration.valueOf("10m"),
                        Duration.valueOf("11m"),
                        Duration.valueOf("12m"),
                        13,
                        14,
                        15,
                        16,
                        17,
                        18,
                        34,
                        19,
                        20.0,
                        DataSize.valueOf("19GB"),
                        DataSize.valueOf("22GB"),
                        DataSize.valueOf("23GB"),
                        DataSize.valueOf("24GB"),
                        DataSize.valueOf("25GB"),
                        DataSize.valueOf("26GB"),
                        DataSize.valueOf("27GB"),
                        DataSize.valueOf("28GB"),
                        DataSize.valueOf("29GB"),
                        true,
                        Duration.valueOf("800000000d"),
                        Duration.valueOf("800000000d"),
                        Duration.valueOf("26m"),
                        true,
                        ImmutableSet.of(BlockedReason.WAITING_FOR_MEMORY),
                        DataSize.valueOf("271GB"),
                        281,
                        DataSize.valueOf("272GB"),
                        282,
                        DataSize.valueOf("27GB"),
                        28,
                        DataSize.valueOf("29GB"),
                        30,
                        DataSize.valueOf("31GB"),
                        32,
                        DataSize.valueOf("32GB"),
                        ImmutableList.of(new StageGcStatistics(
                            101,
                            102,
                            103,
                            104,
                            105,
                            106,
                            107)),
                        ImmutableList.of()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                "33",
                Optional.of(stageInfo1),
                null,
                StandardErrorCode.ABANDONED_QUERY.toErrorCode(),
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                Optional.empty());
    }

    public static QueryInfo getQueryInfo2()
    {
        URI uri = null;
        try {
            uri = new URI("111");
        }
        catch (Exception e) {
        }

        StageStats stageStats = new StageStats(
                new DateTime(0),
                getTestDistribution(1),
                4,
                5,
                6,
                7,
                8,
                10,
                26,
                11,
                12.0,
                new DataSize(13, BYTE),
                new DataSize(14, BYTE),
                new DataSize(15, BYTE),
                new DataSize(16, BYTE),
                new DataSize(17, BYTE),
                new Duration(15, NANOSECONDS),
                new Duration(16, NANOSECONDS),
                new Duration(18, NANOSECONDS),
                false,
                ImmutableSet.of(),
                new DataSize(191, BYTE),
                201,
                new DataSize(192, BYTE),
                202,
                new DataSize(19, BYTE),
                20,
                new DataSize(21, BYTE),
                22,
                new DataSize(23, BYTE),
                new DataSize(24, BYTE),
                25,
                new DataSize(26, BYTE),
                new StageGcStatistics(
                    101,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107),
                ImmutableList.of(createOperatorStats("1", 1), createOperatorStats("2", 10000)));

        StageInfo stageInfo3 = new StageInfo(
                new StageId("2", 2),
                StageState.FINISHED,
                uri,
                createPlanFragment(createTableSubPlan()),
                null,
                stageStats,
                Arrays.asList(createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri)),
                new ArrayList<>(),
                new HashMap<PlanNodeId, TableInfo>(),
                null);

        StageInfo stageInfo2 = new StageInfo(
                new StageId("2", 2),
                StageState.FINISHED,
                uri,
                createPlanFragment(createTableSubPlan()),
                null,
                stageStats,
                Arrays.asList(createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri)),
                Arrays.asList(stageInfo3),
                new HashMap<PlanNodeId, TableInfo>(),
                null);

        StageInfo stageInfo1 = new StageInfo(
                new StageId("1", 1),
                StageState.FINISHED,
                uri,
                createPlanFragment(createPlan2()),
                null,
                stageStats,
                Arrays.asList(createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri),
                      createTaskInfo(1.0, 1.0, uri)),
                Arrays.asList(stageInfo2),
                new HashMap<PlanNodeId, TableInfo>(),
                null);

        return new QueryInfo(
                new QueryId("1"),
                TEST_SESSION.toSessionRepresentation(),
                RUNNING,
                new MemoryPoolId("reserved"),
                false,
                URI.create("1"),
                ImmutableList.of("2", "3"),
                "SELECT 4",
                Optional.of("SELECT 4"),
                new QueryStats(
                        DateTime.parse("1991-09-06T05:00-05:30"),
                        DateTime.parse("1991-09-06T05:01-05:30"),
                        DateTime.parse("1991-09-06T05:02-05:30"),
                        DateTime.parse("1991-09-06T06:00-05:30"),
                        Duration.valueOf("8m"),
                        Duration.valueOf("7m"),
                        Duration.valueOf("34m"),
                        Duration.valueOf("35m"),
                        Duration.valueOf("44m"),
                        Duration.valueOf("9m"),
                        Duration.valueOf("10m"),
                        Duration.valueOf("11m"),
                        Duration.valueOf("12m"),
                        13,
                        14,
                        15,
                        16,
                        17,
                        18,
                        34,
                        19,
                        20.0,
                        DataSize.valueOf("1GB"),
                        DataSize.valueOf("22GB"),
                        DataSize.valueOf("23GB"),
                        DataSize.valueOf("1GB"),
                        DataSize.valueOf("25GB"),
                        DataSize.valueOf("26GB"),
                        DataSize.valueOf("27GB"),
                        DataSize.valueOf("28GB"),
                        DataSize.valueOf("29GB"),
                        true,
                        Duration.valueOf("1d"),
                        Duration.valueOf("1d"),
                        Duration.valueOf("26m"),
                        true,
                        ImmutableSet.of(BlockedReason.WAITING_FOR_MEMORY),
                        DataSize.valueOf("271GB"),
                        281,
                        DataSize.valueOf("272GB"),
                        282,
                        DataSize.valueOf("27GB"),
                        28,
                        DataSize.valueOf("29GB"),
                        30,
                        DataSize.valueOf("31GB"),
                        32,
                        DataSize.valueOf("32GB"),
                        ImmutableList.of(new StageGcStatistics(
                            101,
                            102,
                            103,
                            104,
                            105,
                            106,
                            107)),
                        ImmutableList.of()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                "33",
                Optional.of(stageInfo1),
                null,
                StandardErrorCode.ABANDONED_QUERY.toErrorCode(),
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                Optional.empty());
    }

    private static Distribution.DistributionSnapshot getTestDistribution(int count)
    {
        Distribution distribution = new Distribution();
        for (int i = 0; i < count; i++) {
            distribution.add(i);
        }
        return distribution.snapshot();
    }

    private static TaskInfo createTaskInfo(double totalScheduledTime, double totalCPUTime, URI uri)
    {
        OutputBufferInfo outputBufferInfo = new OutputBufferInfo(null, null, true, true, 1, 1, 1, 1, new ArrayList<>());
        TaskStatus taskStatus = TaskStatus.initialTaskStatus(new TaskId("1", 1, 1), uri, "1");
        return new TaskInfo(taskStatus, DateTime.now(), outputBufferInfo, new HashSet<>(), createTaskStats(totalScheduledTime, totalCPUTime), false);
    }

    private static TaskStats createTaskStats(double totalScheduledTime, double totalCPUTime)
    {
        TaskStats taskStats = new TaskStats(
                new DateTime(1),
                new DateTime(2),
                new DateTime(100),
                new DateTime(101),
                new DateTime(3),
                new Duration(4, NANOSECONDS),
                new Duration(5, NANOSECONDS),
                6,
                7,
                5,
                8,
                6,
                24,
                10,
                11.0,
                new DataSize(12, BYTE),
                new DataSize(13, BYTE),
                new DataSize(14, BYTE),
                new Duration(totalScheduledTime, NANOSECONDS),
                new Duration(totalCPUTime, NANOSECONDS),
                new Duration(18, NANOSECONDS),
                false,
                ImmutableSet.of(),
                new DataSize(191, BYTE),
                201,
                new DataSize(192, BYTE),
                202,
                new DataSize(19, BYTE),
                20,
                new DataSize(21, BYTE),
                22,
                new DataSize(23, BYTE),
                24,
                new DataSize(25, BYTE),
                26,
                new Duration(27, NANOSECONDS),
                ImmutableList.of(TestPipelineStats.EXPECTED));
        return taskStats;
    }

    private static PlanFragment createPlanFragment(PlanNode planNode)
    {
        PlanFragmentId planFragmentId = new PlanFragmentId("1");
        Map<Symbol, Type> symbols = new HashMap();
        ConnectorPartitioningHandle connectorPartitioningHandle = new ConnectorPartitioningHandle() {
                @Override
                public boolean isSingleNode()
                {
                    return false;
                }
        };
        PartitioningHandle partitioningHandle = new PartitioningHandle(Optional.empty(), Optional.empty(), connectorPartitioningHandle);
        List<PlanNodeId> partitionedSources = new ArrayList<>();
        PartitioningScheme partitioningScheme = new PartitioningScheme(Partitioning.create(partitioningHandle,
                new ArrayList<>()), new ArrayList<>(), Optional.empty(), false, Optional.empty());
        StageExecutionDescriptor stageExecutionDescriptor = StageExecutionDescriptor.ungroupedExecution();
        StatsAndCosts statsAndCosts = StatsAndCosts.empty();
        Optional<String> jsonRepresentation = Optional.empty();
        PlanFragment planFragment = new PlanFragment(planFragmentId, planNode, symbols,
                partitioningHandle, partitionedSources, partitioningScheme, stageExecutionDescriptor, statsAndCosts, jsonRepresentation);
        return planFragment;
    }

    private static PlanNode createPlan1()
    {
        CatalogName catalogName = new CatalogName("hive");
        ConnectorTableHandle connectorTableHandle = new TpchTableHandle("table", 1.0, TupleDomain.none());
        ConnectorTransactionHandle connectorTransactionHandle = TestingConnectorTransactionHandle.INSTANCE;
        TableHandle tableHandle = new TableHandle(catalogName, connectorTableHandle, connectorTransactionHandle, Optional.empty());
        TableScanNode tableScanNode = new TableScanNode(new PlanNodeId("1"), tableHandle, new ArrayList<>(), new HashMap<>());
        RemoteSourceNode remoteSourceNode = new RemoteSourceNode(new PlanNodeId("2"), Arrays.asList(new PlanFragmentId("2")), new ArrayList<>(), Optional.empty(),
                ExchangeNode.Type.GATHER);

        JoinNode joinNode = new JoinNode(new PlanNodeId("3"), JoinNode.Type.INNER, tableScanNode, remoteSourceNode, new ArrayList<>(), new ArrayList<>(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), new HashMap<>());
        OutputNode outputNode = new OutputNode(new PlanNodeId("4"), joinNode, new ArrayList<>(), new ArrayList<>());
        return outputNode;
    }

    private static PlanNode createPlan2()
    {
        RemoteSourceNode remoteSourceNode = new RemoteSourceNode(new PlanNodeId("1"), Arrays.asList(new PlanFragmentId("1")), new ArrayList<>(), Optional.empty(),
                ExchangeNode.Type.GATHER);
        RemoteSourceNode remoteSourceNode2 = new RemoteSourceNode(new PlanNodeId("2"), Arrays.asList(new PlanFragmentId("2")), new ArrayList<>(), Optional.empty(),
                ExchangeNode.Type.GATHER);

        JoinNode joinNode = new JoinNode(new PlanNodeId("3"), JoinNode.Type.INNER, remoteSourceNode, remoteSourceNode2, new ArrayList<>(), new ArrayList<>(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), new HashMap<>());
        OutputNode outputNode = new OutputNode(new PlanNodeId("4"), joinNode, new ArrayList<>(), new ArrayList<>());
        return outputNode;
    }

    private static PlanNode createTableSubPlan()
    {
        CatalogName catalogName = new CatalogName("hive");
        ConnectorTableHandle connectorTableHandle = new TpchTableHandle("table", 1.0, TupleDomain.none());
        ConnectorTransactionHandle connectorTransactionHandle = TestingConnectorTransactionHandle.INSTANCE;
        TableHandle tableHandle = new TableHandle(catalogName, connectorTableHandle, connectorTransactionHandle, Optional.empty());
        TableScanNode tableScanNode = new TableScanNode(new PlanNodeId("4"), tableHandle, new ArrayList<>(), new HashMap<>());
        ExchangeNode exchangeNode = ExchangeNode.gatheringExchange(new PlanNodeId("4"), ExchangeNode.Scope.REMOTE, tableScanNode);
        return exchangeNode;
    }

    private static OperatorStats createOperatorStats(String planNodeId, int outputPosition)
    {
        OperatorStats operatorStats = new OperatorStats(
                0,
                1,
                41,
                new PlanNodeId(planNodeId),
                "test",
                1,
                2,
                new Duration(3, NANOSECONDS),
                new Duration(4, NANOSECONDS),
                new DataSize(51, BYTE),
                511,
                new DataSize(52, BYTE),
                522,
                new DataSize(5, BYTE),
                new DataSize(6, BYTE),
                7,
                8d,
                9,
                new Duration(10, NANOSECONDS),
                new Duration(11, NANOSECONDS),
                new DataSize(12, BYTE),
                outputPosition,
                new DataSize(14, BYTE),
                new Duration(15, NANOSECONDS),
                16,
                new Duration(17, NANOSECONDS),
                new Duration(18, NANOSECONDS),
                new DataSize(19, BYTE),
                new DataSize(20, BYTE),
                new DataSize(21, BYTE),
                new DataSize(22, BYTE),
                new DataSize(23, BYTE),
                new DataSize(24, BYTE),
                new DataSize(25, BYTE),
                new DataSize(26, BYTE),
                Optional.empty(),
                new SplitOperatorInfo("some_info"));
        return operatorStats;
    }
}
