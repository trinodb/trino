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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.connector.alternatives.MockPlanAlternativeChooser.PlanAlternativePageSource;
import io.trino.connector.alternatives.MockPlanAlternativeTableHandle;
import io.trino.operator.BlockedReason;
import io.trino.operator.OperatorInfo;
import io.trino.operator.OperatorStats;
import io.trino.operator.dynamicfiltering.DynamicRowFilteringPageSource;
import io.trino.spi.Mergeable;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.metrics.Count;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;

// Make sure plan alternatives don't interfere with dynamic row filtering
public class TestHiveDynamicRowFilteringWithPlanAlternatives
        extends TestHiveDynamicRowFiltering
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .withPlanAlternatives()
                .setHiveProperties(ImmutableMap.of(
                        "hive.max-initial-split-size", "10kB", // so that we have multiple splits to utilize alternatives
                        "hive.max-split-size", "10kB")) // so that we have multiple splits to utilize alternatives
                .build();
    }

    @Override
    @ParameterizedTest
    @MethodSource("joinDistributionTypes")
    @Timeout(30)
    public void testRowFilteringWithCharStrings(JoinDistributionType joinDistributionType)
    {
        assertRowFiltering(
                "SELECT o1.clerk, o1.custkey, CAST(o1.orderstatus AS VARCHAR(1)) FROM orders o1, orders o2 WHERE o1.clerk = o2.clerk AND o2.custkey < 10",
                joinDistributionType,
                "orders");

        assertNoRowFiltering(
                "SELECT COUNT(*) FROM orders o1, orders o2 WHERE o1.orderstatus = o2.orderstatus AND o2.custkey < 20",
                joinDistributionType,
                "orders");
    }

    @Override
    protected SchemaTableName getSchemaTableName(ConnectorTableHandle connectorHandle)
    {
        ConnectorTableHandle table = connectorHandle instanceof MockPlanAlternativeTableHandle handle ? handle.delegate() : connectorHandle;
        return ((HiveTableHandle) table).getSchemaTableName();
    }

    @ParameterizedTest
    @MethodSource("joinDistributionTypes")
    @Timeout(30)
    public void testRowFilteringWithStringsAndPlanAlternativesOnBothSides(JoinDistributionType joinDistributionType)
    {
        assertRowFiltering(
                "SELECT * FROM customer c1, customer c2 WHERE c1.mktsegment = c2.mktsegment AND c1.nationkey = 14 AND c2.custkey = 1",
                joinDistributionType,
                "customer");
    }

    @Override
    @ParameterizedTest
    @MethodSource("joinDistributionTypes")
    @Timeout(30)
    public void testRowFilteringWithStrings(JoinDistributionType joinDistributionType)
    {
        assertRowFiltering("SELECT * FROM customer c1, customer c2 WHERE c1.name = c2.name AND c2.acctbal > 9000", joinDistributionType);
        assertRowFiltering("SELECT * FROM customer c1, customer c2 WHERE c1.mktsegment = c2.mktsegment AND c2.custkey = 1", joinDistributionType);
        assertNoRowFiltering("SELECT * FROM customer c1, customer c2 WHERE c1.mktsegment = c2.mktsegment AND c2.custkey < 10", joinDistributionType, "customer");
    }

    @Override
    protected OperatorStats getScanFilterAndProjectOperatorStats(QueryId queryId, String tableName)
    {
        Plan plan = getDistributedQueryRunner().getQueryPlan(queryId);
        // we need to sum multiple table scan operators in case of sub-plan alternatives
        Set<PlanNodeId> nodeIds = PlanNodeSearcher.searchFrom(plan.getRoot())
                .where(node -> {
                    // project -> filter -> scan can be split by ChooseAlternative. In that case there is no ScanFilterAndProjectOperator but rather TableScanOperator
                    PlanNode nodeToCheck = node;
                    if (nodeToCheck instanceof ProjectNode projectNode) {
                        nodeToCheck = projectNode.getSource();
                    }
                    if (nodeToCheck instanceof FilterNode filterNode) {
                        nodeToCheck = filterNode.getSource();
                    }
                    if (nodeToCheck instanceof TableScanNode tableScanNode) {
                        return getSchemaTableName(tableScanNode.getTable().getConnectorHandle())
                                .equals(new SchemaTableName("tpch", tableName));
                    }
                    return false;
                })
                .findAll()
                .stream()
                .map(PlanNode::getId)
                .collect(toImmutableSet());

        return extractOperatorStatsForNodeIds(getDistributedQueryRunner(), queryId, nodeIds);
    }

    @Override
    protected void assertRowFiltering(@Language("SQL") String sql, JoinDistributionType joinDistributionType, String tableName)
    {
        MaterializedResultWithPlan rowFilteringResultWithQueryId = getDistributedQueryRunner().executeWithPlan(
                dynamicRowFiltering(joinDistributionType),
                sql);

        MaterializedResultWithPlan noRowFilteringResultWithQueryId = getDistributedQueryRunner().executeWithPlan(
                noDynamicRowFiltering(joinDistributionType),
                sql);

        // ensure results are correct
        MaterializedResult expected = computeExpected(sql, rowFilteringResultWithQueryId.result().getTypes());
        assertEqualsIgnoreOrder(rowFilteringResultWithQueryId.result(), expected, "For query: \n " + sql);
        assertEqualsIgnoreOrder(noRowFilteringResultWithQueryId.result(), expected, "For query: \n " + sql);

        OperatorStats rowFilteringProbeStats = getScanFilterAndProjectOperatorStats(
                rowFilteringResultWithQueryId.queryId(),
                tableName);
        // input positions is smaller than physical input positions due to row filtering
        assertThat(rowFilteringProbeStats.getInputPositions())
                .isLessThan(rowFilteringProbeStats.getPhysicalInputPositions());

        OperatorStats noRowFilteringProbeStats = getScanFilterAndProjectOperatorStats(
                noRowFilteringResultWithQueryId.queryId(),
                tableName);

        Map<String, Metric<?>> noRowFilteringMetrics = noRowFilteringProbeStats.getConnectorMetrics().getMetrics();
        long filteredOutPositions = noRowFilteringMetrics.containsKey(PlanAlternativePageSource.FILTERED_OUT_POSITIONS) ?
                ((Count<?>) noRowFilteringMetrics.get(PlanAlternativePageSource.FILTERED_OUT_POSITIONS)).getTotal() :
                0;

        // input positions + number of positions filtered by the connector = physical input positions
        assertThat(noRowFilteringProbeStats.getInputPositions() + filteredOutPositions)
                .isEqualTo(noRowFilteringProbeStats.getPhysicalInputPositions());
        // input positions is smaller in row filtering case than in no row filtering case
        assertThat(rowFilteringProbeStats.getInputPositions())
                .isLessThan(noRowFilteringProbeStats.getInputPositions());

        Map<String, Metric<?>> rowFilteringMetrics = rowFilteringProbeStats.getConnectorMetrics().getMetrics();
        long filterOutputPositions = ((Count<?>) rowFilteringMetrics.get(DynamicRowFilteringPageSource.FILTER_OUTPUT_POSITIONS)).getTotal();
        long filterInputPositions = ((Count<?>) rowFilteringMetrics.get(DynamicRowFilteringPageSource.FILTER_INPUT_POSITIONS)).getTotal();
        assertThat(filterOutputPositions).isLessThan(filterInputPositions);
        assertThat(filterOutputPositions).isLessThan(rowFilteringProbeStats.getPhysicalInputPositions());
        assertThat(((Count<?>) rowFilteringMetrics.get(DynamicRowFilteringPageSource.ROW_FILTERING_TIME_MILLIS)).getTotal()).isGreaterThanOrEqualTo(0);
    }

    @Override
    protected void assertNoRowFiltering(@Language("SQL") String sql, JoinDistributionType joinDistributionType, String tableName)
    {
        MaterializedResultWithPlan rowFilteringResultWithQueryId = getDistributedQueryRunner().executeWithPlan(
                dynamicRowFiltering(joinDistributionType),
                sql);

        // ensure results are correct
        MaterializedResult expected = computeExpected(sql, rowFilteringResultWithQueryId.result().getTypes());
        assertEqualsIgnoreOrder(rowFilteringResultWithQueryId.result(), expected, "For query: \n " + sql);

        OperatorStats rowFilteringProbeStats = getScanFilterAndProjectOperatorStats(
                rowFilteringResultWithQueryId.queryId(),
                tableName);

        Map<String, Metric<?>> metrics = rowFilteringProbeStats.getConnectorMetrics().getMetrics();
        long filteredOutPositions = metrics.containsKey(PlanAlternativePageSource.FILTERED_OUT_POSITIONS) ?
                ((Count<?>) metrics.get(PlanAlternativePageSource.FILTERED_OUT_POSITIONS)).getTotal() :
                0;

        // input positions + rows filtered by the connector is equal to physical input positions
        assertThat(rowFilteringProbeStats.getInputPositions() + filteredOutPositions)
                .isEqualTo(rowFilteringProbeStats.getPhysicalInputPositions());

        Count<?> filterOutputPositions = (Count<?>) metrics.get(DynamicRowFilteringPageSource.FILTER_OUTPUT_POSITIONS);
        Count<?> filterInputPositions = (Count<?>) metrics.get(DynamicRowFilteringPageSource.FILTER_INPUT_POSITIONS);
        assertThat(filterOutputPositions).isEqualTo(filterInputPositions);
        assertThat(((Count<?>) metrics.get(DynamicRowFilteringPageSource.ROW_FILTERING_TIME_MILLIS)).getTotal()).isGreaterThanOrEqualTo(0);
    }

    private static OperatorStats extractOperatorStatsForNodeIds(DistributedQueryRunner queryRunner, QueryId queryId, Set<PlanNodeId> nodeIds)
    {
        return queryRunner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> nodeIds.contains(summary.getPlanNodeId()) &&
                        (summary.getOperatorType().equals("ScanFilterAndProjectOperator") || summary.getOperatorType().equals("TableScanOperator")))
                .reduce(TestHiveDynamicRowFilteringWithPlanAlternatives::addOperatorStats)
                .orElseThrow(() -> new NoSuchElementException("table scan operator not found in " + queryId + " nodeIds: " + nodeIds));
    }

    // like OperatorStats::add but does not throw on different operator ids and different operator types
    public static OperatorStats addOperatorStats(OperatorStats left, OperatorStats operator)
    {
        long totalDrivers = left.getTotalDrivers() + operator.getTotalDrivers();
        long addInputCalls = left.getAddInputCalls() + operator.getAddInputCalls();
        long addInputWall = left.getAddInputWall().roundTo(NANOSECONDS) + operator.getAddInputWall().roundTo(NANOSECONDS);
        long addInputCpu = left.getAddInputCpu().roundTo(NANOSECONDS) + operator.getAddInputCpu().roundTo(NANOSECONDS);
        long physicalInputDataSize = left.getPhysicalInputDataSize().toBytes() + operator.getPhysicalInputDataSize().toBytes();
        long physicalInputPositions = left.getPhysicalInputPositions() + operator.getPhysicalInputPositions();
        long physicalInputReadTimeNanos = left.getPhysicalInputReadTime().roundTo(NANOSECONDS) + operator.getPhysicalInputReadTime().roundTo(NANOSECONDS);
        long internalNetworkInputDataSize = left.getInternalNetworkInputDataSize().toBytes() + operator.getInternalNetworkInputDataSize().toBytes();
        long internalNetworkInputPositions = left.getInternalNetworkInputPositions() + operator.getInternalNetworkInputPositions();
        long rawInputDataSize = left.getRawInputDataSize().toBytes() + operator.getRawInputDataSize().toBytes();
        long inputDataSize = left.getInputDataSize().toBytes() + operator.getInputDataSize().toBytes();
        long inputPositions = left.getInputPositions() + operator.getInputPositions();
        double sumSquaredInputPositions = left.getSumSquaredInputPositions() + operator.getSumSquaredInputPositions();

        long getOutputCalls = left.getGetOutputCalls() + operator.getGetOutputCalls();
        long getOutputWall = left.getGetOutputWall().roundTo(NANOSECONDS) + operator.getGetOutputWall().roundTo(NANOSECONDS);
        long getOutputCpu = left.getGetOutputCpu().roundTo(NANOSECONDS) + operator.getGetOutputCpu().roundTo(NANOSECONDS);
        long outputDataSize = left.getOutputDataSize().toBytes() + operator.getOutputDataSize().toBytes();
        long outputPositions = left.getOutputPositions() + operator.getOutputPositions();

        long dynamicFilterSplitsProcessed = left.getDynamicFilterSplitsProcessed() + operator.getDynamicFilterSplitsProcessed();
        Metrics.Accumulator metricsAccumulator = Metrics.accumulator().add(left.getMetrics()).add(operator.getMetrics());
        Metrics.Accumulator connectorMetricsAccumulator = Metrics.accumulator().add(left.getConnectorMetrics()).add(operator.getConnectorMetrics());

        long physicalWrittenDataSize = left.getPhysicalWrittenDataSize().toBytes();

        long finishCalls = left.getFinishCalls() + operator.getFinishCalls();
        long finishWall = left.getFinishWall().roundTo(NANOSECONDS) + operator.getFinishWall().roundTo(NANOSECONDS);
        long finishCpu = left.getFinishCpu().roundTo(NANOSECONDS) + operator.getFinishCpu().roundTo(NANOSECONDS);

        long blockedWall = left.getBlockedWall().roundTo(NANOSECONDS) + operator.getBlockedWall().roundTo(NANOSECONDS);

        long memoryReservation = left.getUserMemoryReservation().toBytes() + operator.getUserMemoryReservation().toBytes();
        long revocableMemoryReservation = left.getRevocableMemoryReservation().toBytes() + operator.getRevocableMemoryReservation().toBytes();

        long peakUserMemory = max(left.getPeakUserMemoryReservation().toBytes(), operator.getPeakUserMemoryReservation().toBytes());
        long peakRevocableMemory = max(left.getPeakRevocableMemoryReservation().toBytes(), operator.getPeakRevocableMemoryReservation().toBytes());
        long peakTotalMemory = max(left.getPeakTotalMemoryReservation().toBytes(), operator.getPeakTotalMemoryReservation().toBytes());

        long spilledDataSize = left.getSpilledDataSize().toBytes() + operator.getSpilledDataSize().toBytes();

        Optional<BlockedReason> blockedReason = left.getBlockedReason();
        if (operator.getBlockedReason().isPresent()) {
            blockedReason = operator.getBlockedReason();
        }

        Mergeable<OperatorInfo> info = getMergeableInfoOrNull(left.getInfo());
        OperatorInfo rightInfo = operator.getInfo();
        if (info != null && rightInfo != null) {
            verify(info.getClass() == rightInfo.getClass(), "Cannot merge operator infos: %s and %s", info, rightInfo);
            info = mergeInfo(info, rightInfo);
        }

        return new OperatorStats(
                left.getStageId(),
                left.getPipelineId(),
                0,
                left.getOperatorId(),
                left.getPlanNodeId(),
                left.getOperatorType(),

                totalDrivers,

                addInputCalls,
                new Duration(addInputWall, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(addInputCpu, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                DataSize.ofBytes(physicalInputDataSize),
                physicalInputPositions,
                new Duration(physicalInputReadTimeNanos, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                DataSize.ofBytes(internalNetworkInputDataSize),
                internalNetworkInputPositions,
                DataSize.ofBytes(rawInputDataSize),
                DataSize.ofBytes(inputDataSize),
                inputPositions,
                sumSquaredInputPositions,

                getOutputCalls,
                new Duration(getOutputWall, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(getOutputCpu, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                DataSize.ofBytes(outputDataSize),
                outputPositions,

                dynamicFilterSplitsProcessed,
                metricsAccumulator.get(),
                connectorMetricsAccumulator.get(),

                DataSize.ofBytes(physicalWrittenDataSize),

                new Duration(blockedWall, NANOSECONDS).convertToMostSuccinctTimeUnit(),

                finishCalls,
                new Duration(finishWall, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(finishCpu, NANOSECONDS).convertToMostSuccinctTimeUnit(),

                DataSize.ofBytes(memoryReservation),
                DataSize.ofBytes(revocableMemoryReservation),
                DataSize.ofBytes(peakUserMemory),
                DataSize.ofBytes(peakRevocableMemory),
                DataSize.ofBytes(peakTotalMemory),

                DataSize.ofBytes(spilledDataSize),

                blockedReason,

                (OperatorInfo) info);
    }

    private static Mergeable<OperatorInfo> getMergeableInfoOrNull(OperatorInfo info)
    {
        Mergeable<OperatorInfo> base = null;
        if (info instanceof Mergeable) {
            base = (Mergeable<OperatorInfo>) info;
        }
        return base;
    }

    private static <T> Mergeable<T> mergeInfo(Mergeable<T> base, T other)
    {
        return (Mergeable<T>) base.mergeWith(other);
    }
}
