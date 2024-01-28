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
import io.trino.metadata.QualifiedObjectName;
import io.trino.operator.BlockedReason;
import io.trino.operator.OperatorInfo;
import io.trino.operator.OperatorStats;
import io.trino.server.DynamicFilterService;
import io.trino.spi.Mergeable;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.metrics.Metrics;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHiveDynamicPartitionPruningTestWithPlanAlternatives
        extends TestHiveDynamicPartitionPruningTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setExtraProperties(EXTRA_PROPERTIES)
                .setHiveProperties(ImmutableMap.of("hive.dynamic-filtering.wait-timeout", "1h"))
                .setInitialTables(REQUIRED_TABLES)
                .withPlanAlternatives()
                .build();
    }

    @Test
    @Timeout(30)
    public void testJoinWithSelectiveBuildSideAndAlternativesOnBothSides()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey " +
                "AND supplier.name = 'Supplier#000000001' AND partitioned_lineitem.partkey > 880";
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.result(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.queryId(), getQualifiedTableName(PARTITIONED_LINEITEM));
        // Probe-side is partially scanned
        assertEquals(probeStats.getInputPositions(), 369L);

        DynamicFilterService.DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.queryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterService.DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), singleValue(BIGINT, 1L).toString(getSession().toConnectorSession()));
    }

    @Override
    @Test
    protected OperatorStats searchScanFilterAndProjectOperatorStats(QueryId queryId, QualifiedObjectName catalogSchemaTableName)
    {
        DistributedQueryRunner runner = getDistributedQueryRunner();
        Plan plan = runner.getQueryPlan(queryId);
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
                        CatalogSchemaTableName tableName = getTableName(tableScanNode.getTable());
                        return tableName.equals(catalogSchemaTableName.asCatalogSchemaTableName());
                    }
                    return false;
                })
                .findAll()
                .stream()
                .map(PlanNode::getId)
                .collect(toImmutableSet());

        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> nodeIds.contains(summary.getPlanNodeId())
                        && (summary.getOperatorType().equals("ScanFilterAndProjectOperator") || summary.getOperatorType().equals("TableScanOperator")))
                .reduce(TestHiveDynamicPartitionPruningTestWithPlanAlternatives::addOperatorStats)
                .orElseThrow(() -> new NoSuchElementException("table scan operator not found for " + catalogSchemaTableName + " in " + queryId + " nodeIds: " + nodeIds));
    }

    // like OperatorStats::add but does not throw on different operator ids
    private static OperatorStats addOperatorStats(OperatorStats left, OperatorStats operator)
    {
        checkArgument(operator.getOperatorType().equals(left.getOperatorType()), "Expected operatorType to be %s but was %s", left.getOperatorType(), operator.getOperatorType());

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
