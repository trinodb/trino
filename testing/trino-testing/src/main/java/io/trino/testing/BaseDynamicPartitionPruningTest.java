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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.operator.OperatorStats;
import io.trino.server.DynamicFilterService.DynamicFilterDomainStats;
import io.trino.server.DynamicFilterService.DynamicFiltersStats;
import io.trino.spi.QueryId;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.ENABLE_LARGE_DYNAMIC_FILTERS;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.spi.predicate.Domain.none;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.PARTITIONED;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.SUPPLIER;
import static io.trino.util.DynamicFiltersTestUtil.getSimplifiedDomainString;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class BaseDynamicPartitionPruningTest
        extends AbstractTestQueryFramework
{
    private static final String PARTITIONED_LINEITEM = "partitioned_lineitem";
    private static final long LINEITEM_COUNT = 60175;
    protected static final Set<TpchTable<?>> REQUIRED_TABLES = ImmutableSet.of(LINE_ITEM, ORDERS, SUPPLIER);
    protected static final Map<String, String> EXTRA_PROPERTIES = ImmutableMap.of(
            // Reduced partitioned join limit for large DF to enable DF min/max collection with ENABLE_LARGE_DYNAMIC_FILTERS
            "dynamic-filtering.large-partitioned.max-distinct-values-per-driver", "100",
            "dynamic-filtering.large-partitioned.range-row-limit-per-driver", "100000",
            // disable semi join to inner join rewrite to test semi join operators explicitly
            "optimizer.rewrite-filtering-semi-join-to-inner-join", "false");

    @BeforeClass
    public void initTables()
            throws Exception
    {
        // setup partitioned fact table for dynamic partition pruning
        createLineitemTable(PARTITIONED_LINEITEM, ImmutableList.of("orderkey", "partkey", "suppkey"), ImmutableList.of("suppkey"));
    }

    protected abstract void createLineitemTable(String tableName, List<String> columns, List<String> partitionColumns);

    protected abstract void createPartitionedTable(String tableName, List<String> columns, List<String> partitionColumns);

    protected abstract void createPartitionedAndBucketedTable(String tableName, List<String> columns, List<String> partitionColumns, List<String> bucketColumns);

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name()) // Avoid node local DF
                // Enabled large dynamic filters to verify min/max DF collection in testJoinLargeBuildSideRangeDynamicFiltering
                .setSystemProperty(ENABLE_LARGE_DYNAMIC_FILTERS, "true")
                .build();
    }

    @Test(timeOut = 30_000)
    public void testJoinWithEmptyBuildSide()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey AND supplier.name = 'abc'";
        MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.getResult(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), getQualifiedTableName(PARTITIONED_LINEITEM));
        assertEquals(probeStats.getInputPositions(), 0L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), none(BIGINT).toString(getSession().toConnectorSession()));
        assertTrue(domainStats.getCollectionDuration().isPresent());
    }

    @Test(timeOut = 30_000)
    public void testJoinWithSelectiveBuildSide()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey " +
                "AND supplier.name = 'Supplier#000000001'";
        MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.getResult(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), getQualifiedTableName(PARTITIONED_LINEITEM));
        // Probe-side is partially scanned
        assertEquals(probeStats.getInputPositions(), 615L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), singleValue(BIGINT, 1L).toString(getSession().toConnectorSession()));
    }

    @Test(timeOut = 30_000)
    public void testJoinWithNonSelectiveBuildSide()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey";
        MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.getResult(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), getQualifiedTableName(PARTITIONED_LINEITEM));
        // Probe-side is fully scanned
        assertEquals(probeStats.getInputPositions(), LINEITEM_COUNT);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertThat(domainStats.getSimplifiedDomain())
                .isEqualTo(getSimplifiedDomainString(1L, 100L, 100, BIGINT));
    }

    @Test(timeOut = 30_000)
    public void testJoinLargeBuildSideRangeDynamicFiltering()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem JOIN orders ON partitioned_lineitem.orderkey = orders.orderkey";
        MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.getResult(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), getQualifiedTableName(PARTITIONED_LINEITEM));
        // Probe-side is fully scanned because the build-side is too large for dynamic filtering
        assertEquals(probeStats.getInputPositions(), LINEITEM_COUNT);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(
                domainStats.getSimplifiedDomain(),
                Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 60000L, true)), false)
                        .toString(getSession().toConnectorSession()));
    }

    @Test(timeOut = 30_000)
    public void testJoinWithMultipleDynamicFiltersOnProbe()
    {
        // supplier names Supplier#000000001 and Supplier#000000002 match suppkey 1 and 2
        @Language("SQL") String selectQuery = "SELECT * FROM (" +
                "SELECT supplier.suppkey FROM " +
                "partitioned_lineitem JOIN tpch.tiny.supplier ON partitioned_lineitem.suppkey = supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')" +
                ") t JOIN supplier ON t.suppkey = supplier.suppkey AND supplier.suppkey IN (2, 3)";
        MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.getResult(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), getQualifiedTableName(PARTITIONED_LINEITEM));
        // Probe-side is partially scanned
        assertEquals(probeStats.getInputPositions(), 558L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 2L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 2L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 2);

        List<DynamicFilterDomainStats> domainStats = dynamicFiltersStats.getDynamicFilterDomainStats();
        assertThat(domainStats).map(DynamicFilterDomainStats::getSimplifiedDomain)
                .containsExactlyInAnyOrder(
                        getSimplifiedDomainString(2L, 3L, 2, BIGINT),
                        getSimplifiedDomainString(2L, 2L, 1, BIGINT));
    }

    @Test(timeOut = 30_000)
    public void testJoinWithImplicitCoercion()
    {
        // setup partitioned fact table with integer suppkey
        createLineitemTable("partitioned_lineitem_int", ImmutableList.of("orderkey", "CAST(suppkey as int) suppkey_int"), ImmutableList.of("suppkey_int"));
        assertQuery(
                "SELECT count(*) FROM partitioned_lineitem_int",
                "VALUES " + LINEITEM_COUNT);

        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem_int l JOIN supplier s ON l.suppkey_int = s.suppkey AND s.name = 'Supplier#000000001'";
        MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.getResult(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), getQualifiedTableName("partitioned_lineitem_int"));
        // Probe-side is partially scanned
        assertEquals(probeStats.getInputPositions(), 615L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);
        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), singleValue(BIGINT, 1L).toString(getSession().toConnectorSession()));
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithEmptyBuildSide()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem WHERE suppkey IN (SELECT suppkey FROM supplier WHERE name = 'abc')";
        MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.getResult(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), getQualifiedTableName(PARTITIONED_LINEITEM));
        assertEquals(probeStats.getInputPositions(), 0L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), none(BIGINT).toString(getSession().toConnectorSession()));
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithSelectiveBuildSide()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem WHERE suppkey IN (SELECT suppkey FROM supplier WHERE name = 'Supplier#000000001')";
        MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.getResult(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), getQualifiedTableName(PARTITIONED_LINEITEM));
        // Probe-side is partially scanned
        assertEquals(probeStats.getInputPositions(), 615L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), singleValue(BIGINT, 1L).toString(getSession().toConnectorSession()));
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithNonSelectiveBuildSide()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem WHERE suppkey IN (SELECT suppkey FROM supplier)";
        MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.getResult(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), getQualifiedTableName(PARTITIONED_LINEITEM));
        // Probe-side is fully scanned
        assertEquals(probeStats.getInputPositions(), LINEITEM_COUNT);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertThat(domainStats.getSimplifiedDomain())
                .isEqualTo(getSimplifiedDomainString(1L, 100L, 100, BIGINT));
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinLargeBuildSideRangeDynamicFiltering()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem WHERE orderkey IN (SELECT orderkey FROM orders)";
        MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.getResult(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), getQualifiedTableName(PARTITIONED_LINEITEM));
        // Probe-side is fully scanned because the build-side is too large for dynamic filtering
        assertEquals(probeStats.getInputPositions(), LINEITEM_COUNT);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(
                domainStats.getSimplifiedDomain(),
                Domain.create(ValueSet.ofRanges(range(BIGINT, 1L, true, 60000L, true)), false)
                        .toString(getSession().toConnectorSession()));
    }

    @Test(timeOut = 30_000)
    public void testRightJoinWithEmptyBuildSide()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem l RIGHT JOIN supplier s ON l.suppkey = s.suppkey WHERE name = 'abc'";
        MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.getResult(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), getQualifiedTableName(PARTITIONED_LINEITEM));
        assertEquals(probeStats.getInputPositions(), 0L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), none(BIGINT).toString(getSession().toConnectorSession()));
    }

    @Test(timeOut = 30_000)
    public void testRightJoinWithSelectiveBuildSide()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem l RIGHT JOIN supplier s ON l.suppkey = s.suppkey WHERE name = 'Supplier#000000001'";
        MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.getResult(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), getQualifiedTableName(PARTITIONED_LINEITEM));
        // Probe-side is partially scanned
        assertEquals(probeStats.getInputPositions(), 615L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), singleValue(BIGINT, 1L).toString(getSession().toConnectorSession()));
    }

    @Test(timeOut = 30_000)
    public void testRightJoinWithNonSelectiveBuildSide()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem l RIGHT JOIN supplier s ON l.suppkey = s.suppkey";
        MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.getResult(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), getQualifiedTableName(PARTITIONED_LINEITEM));
        // Probe-side is fully scanned
        assertEquals(probeStats.getInputPositions(), LINEITEM_COUNT);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertThat(domainStats.getSimplifiedDomain())
                .isEqualTo(getSimplifiedDomainString(1L, 100L, 100, BIGINT));
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testJoinDynamicFilteringMultiJoinOnPartitionedTables(JoinDistributionType joinDistributionType)
    {
        assertUpdate("DROP TABLE IF EXISTS t0_part");
        assertUpdate("DROP TABLE IF EXISTS t1_part");
        assertUpdate("DROP TABLE IF EXISTS t2_part");
        createPartitionedTable("t0_part", ImmutableList.of("v0 real", "k0 integer"), ImmutableList.of("k0"));
        createPartitionedTable("t1_part", ImmutableList.of("v1 real", "i1 integer"), ImmutableList.of());
        createPartitionedTable("t2_part", ImmutableList.of("v2 real", "i2 integer", "k2 integer"), ImmutableList.of("k2"));
        assertUpdate("INSERT INTO t0_part VALUES (1.0, 1), (1.0, 2)", 2);
        assertUpdate("INSERT INTO t1_part VALUES (2.0, 10), (2.0, 20)", 2);
        assertUpdate("INSERT INTO t2_part VALUES (3.0, 1, 1), (3.0, 2, 2)", 2);
        testJoinDynamicFilteringMultiJoin(joinDistributionType, "t0_part", "t1_part", "t2_part");
    }

    // TODO: use joinDistributionTypeProvider when https://github.com/trinodb/trino/issues/4713 is done as currently waiting for BROADCAST DFs doesn't work for bucketed tables
    @Test(timeOut = 30_000)
    public void testJoinDynamicFilteringMultiJoinOnBucketedTables()
    {
        assertUpdate("DROP TABLE IF EXISTS t0_bucketed");
        assertUpdate("DROP TABLE IF EXISTS t1_bucketed");
        assertUpdate("DROP TABLE IF EXISTS t2_bucketed");
        createPartitionedAndBucketedTable("t0_bucketed", ImmutableList.of("v0 real", "k0 integer"), ImmutableList.of("k0"), ImmutableList.of("v0"));
        createPartitionedAndBucketedTable("t1_bucketed", ImmutableList.of("v1 real", "i1 integer"), ImmutableList.of(), ImmutableList.of("v1"));
        createPartitionedAndBucketedTable("t2_bucketed", ImmutableList.of("v2 real", "i2 integer", "k2 integer"), ImmutableList.of("k2"), ImmutableList.of("v2"));
        assertUpdate("INSERT INTO t0_bucketed VALUES (1.0, 1), (1.0, 2)", 2);
        assertUpdate("INSERT INTO t1_bucketed VALUES (2.0, 10), (2.0, 20)", 2);
        assertUpdate("INSERT INTO t2_bucketed VALUES (3.0, 1, 1), (3.0, 2, 2)", 2);
        testJoinDynamicFilteringMultiJoin(PARTITIONED, "t0_bucketed", "t1_bucketed", "t2_bucketed");
    }

    private void testJoinDynamicFilteringMultiJoin(JoinDistributionType joinDistributionType, String t0, String t1, String t2)
    {
        // queries should not deadlock

        // t0 table scan depends on DFs from t1 and t2
        assertDynamicFilters(
                noJoinReordering(joinDistributionType),
                format("SELECT v0, v1, v2 FROM (%s JOIN %s ON k0 = i2) JOIN %s ON k0 = i1", t0, t2, t1),
                0);

        // DF evaluation order is: t1 => t2 => t0
        assertDynamicFilters(
                noJoinReordering(joinDistributionType),
                format("SELECT v0, v1, v2 FROM (%s JOIN %s ON k0 = i2) JOIN %s ON k2 = i1", t0, t2, t1),
                0);

        // t2 table scan depends on t1 DFs, but t0 <-> t2 join is blocked on t2 data
        // "(k0 * -1) + 2 = i2)" prevents DF to be used on t0
        assertDynamicFilters(
                noJoinReordering(joinDistributionType),
                format("SELECT v0, v1, v2 FROM (%s JOIN %s ON (k0 * -1) + 2 = i2) JOIN %s ON k2 = i1", t0, t2, t1),
                0);
    }

    private void assertDynamicFilters(Session session, @Language("SQL") String query, int expectedRowCount)
    {
        long filteredInputPositions = getQueryInputPositions(session, query, expectedRowCount);
        long unfilteredInputPositions = getQueryInputPositions(withDynamicFilteringDisabled(session), query, 0);

        assertThat(filteredInputPositions)
                .as("filtered input positions")
                .isLessThan(unfilteredInputPositions);
    }

    private long getQueryInputPositions(Session session, @Language("SQL") String sql, int expectedRowCount)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        MaterializedResultWithQueryId result = runner.executeWithQueryId(session, sql);
        assertThat(result.getResult().getRowCount()).isEqualTo(expectedRowCount);
        QueryId queryId = result.getQueryId();
        QueryStats stats = runner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats();
        return stats.getPhysicalInputPositions();
    }

    @DataProvider
    public Object[][] joinDistributionTypes()
    {
        return Stream.of(JoinDistributionType.values())
                .collect(toDataProvider());
    }

    private Session withDynamicFilteringDisabled()
    {
        return withDynamicFilteringDisabled(getSession());
    }

    private Session withDynamicFilteringDisabled(Session session)
    {
        return Session.builder(session)
                .setSystemProperty("enable_dynamic_filtering", "false")
                .build();
    }
}
