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

import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.operator.OperatorStats;
import io.trino.spi.QueryId;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.analyzer.FeaturesConfig;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.trino.SystemSessionProperties.ENABLE_LARGE_DYNAMIC_FILTERS;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.server.DynamicFilterService.DynamicFilterDomainStats;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static io.trino.spi.predicate.Domain.none;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static io.trino.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Generic test for connectors exercising connector's dynamic filter capabilities.
 *
 * @see AbstractDynamicFilteringIntegrationSmokeTest
 */
public abstract class AbstractDynamicFilteringIntegrationSmokeTest
        extends AbstractTestQueries
{
    private static final int LINEITEM_COUNT = 60175;
    private static final int ORDERS_COUNT = 15000;
    private static final int PART_COUNT = 2000;
    private static final int CUSTOMER_COUNT = 1500;

    protected boolean supportsNodeLocalDynamicFiltering()
    {
        return true;
    }

    protected boolean supportsCoordinatorDynamicFiltering()
    {
        return true;
    }

    @Test(timeOut = 30_000)
    public void testJoinWithEmptyBuildSide()
    {
        skipTestUnless(supportsCoordinatorDynamicFiltering());

        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey AND supplier.name = 'abc'");
        assertEquals(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/trinodb/trino/commit/1feaa0f928a02f577c8ac9ef6cc0c8ec2008a46d
        // after https://github.com/trinodb/trino/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), none(INTEGER).toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 0);
        assertTrue(domainStats.getCollectionDuration().isPresent());
    }

    @Test
    public void testJoinWithEmptyBuildSideWithBroadcastJoin()
    {
        skipTestUnless(supportsCoordinatorDynamicFiltering());

        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.BROADCAST.name())
                .build();
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                session,
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice = 123.4567");
        assertEquals(result.getResult().getRowCount(), 0);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:lineitem");
        // Probe-side is not scanned at all, due to dynamic filtering:
        assertEquals(probeStats.getInputPositions(), 0L);
        assertEquals(probeStats.getDynamicFilterSplitsProcessed(), probeStats.getTotalDrivers());
    }

    @Test(timeOut = 30_000)
    public void testJoinWithSelectiveBuildSide()
    {
        skipTestUnless(supportsCoordinatorDynamicFiltering());

        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey " +
                        "AND supplier.name = 'Supplier#000000001'");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/prestosql/presto/commit/1feaa0f928a02f577c8ac9ef6cc0c8ec2008a46d
        // after https://github.com/prestosql/presto/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), singleValue(INTEGER, 1L).toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 1);
    }

    @Test(timeOut = 30_000)
    public void testJoinWithNonSelectiveBuildSide()
    {
        skipTestUnless(supportsCoordinatorDynamicFiltering());

        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey");
        assertEquals(result.getResult().getRowCount(), LINEITEM_COUNT);

        // TODO bring back OperatorStats assertions from https://github.com/prestosql/presto/commit/1feaa0f928a02f577c8ac9ef6cc0c8ec2008a46d
        // after https://github.com/prestosql/presto/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), Domain.create(ValueSet.ofRanges(
                range(INTEGER, 1L, true, 100L, true)), false)
                .toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 100);
    }

    @Test(timeOut = 30_000)
    public void testJoinLargeBuildSideRangeDynamicFiltering()
    {
        skipTestUnless(supportsCoordinatorDynamicFiltering());

        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem JOIN orders ON partitioned_lineitem.orderkey = orders.orderkey");
        assertEquals(result.getResult().getRowCount(), LINEITEM_COUNT);

        // TODO bring back OperatorStats assertions from https://github.com/prestosql/presto/commit/1feaa0f928a02f577c8ac9ef6cc0c8ec2008a46d
        // after https://github.com/prestosql/presto/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(
                domainStats.getSimplifiedDomain(),
                Domain.create(
                        ValueSet.ofRanges(range(INTEGER, 1L, true, 60000L, true)), false)
                        .toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 1);
    }

    @Test(timeOut = 30_000)
    public void testJoinWithMultipleDynamicFiltersOnProbe()
    {
        skipTestUnless(supportsCoordinatorDynamicFiltering());

        // supplier names Supplier#000000001 and Supplier#000000002 match suppkey 1 and 2
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM (" +
                        "SELECT supplier.suppkey FROM " +
                        "partitioned_lineitem JOIN tpch.tiny.supplier ON partitioned_lineitem.suppkey = supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')" +
                        ") t JOIN supplier ON t.suppkey = supplier.suppkey AND supplier.suppkey IN (2, 3)");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/prestosql/presto/commit/1feaa0f928a02f577c8ac9ef6cc0c8ec2008a46d
        // after https://github.com/prestosql/presto/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 2L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 2L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 2);

        List<DynamicFilterDomainStats> domainStats = dynamicFiltersStats.getDynamicFilterDomainStats();
        assertEquals(domainStats.size(), 2);
        domainStats.forEach(stats -> {
            assertGreaterThanOrEqual(stats.getRangeCount(), 1);
            assertEquals(stats.getDiscreteValuesCount(), 0);
        });
    }

    @Test(timeOut = 30_000)
    public void testJoinWithImplicitCoercion()
    {
        skipTestUnless(supportsCoordinatorDynamicFiltering());

        // setup partitioned fact table with integer suppkey
        assertUpdate(
                "CREATE TABLE partitioned_lineitem_int " +
                        "WITH (format = 'TEXTFILE', partitioned_by=array['suppkey_int']) AS " +
                        "SELECT orderkey, CAST(suppkey as int) suppkey_int FROM tpch.tiny.lineitem",
                LINEITEM_COUNT);

        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem_int l JOIN supplier s ON l.suppkey_int = s.suppkey AND s.name = 'Supplier#000000001'");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);
        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), singleValue(BIGINT, 1L).toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 1);
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithEmptyBuildSide()
    {
        skipTestUnless(supportsCoordinatorDynamicFiltering());

        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem WHERE suppkey IN (SELECT suppkey FROM supplier WHERE name = 'abc')");
        assertEquals(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/prestosql/presto/commit/0fb16ab9d9c990e58fad63d4dab3dbbe482a077d
        // after https://github.com/prestosql/presto/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), none(INTEGER).toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 0);
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithSelectiveBuildSide()
    {
        skipTestUnless(supportsCoordinatorDynamicFiltering());

        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem WHERE suppkey IN (SELECT suppkey FROM supplier WHERE name = 'Supplier#000000001')");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/prestosql/presto/commit/0fb16ab9d9c990e58fad63d4dab3dbbe482a077d
        // after https://github.com/prestosql/presto/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), singleValue(INTEGER, 1L).toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 1);
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithNonSelectiveBuildSide()
    {
        skipTestUnless(supportsCoordinatorDynamicFiltering());

        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem WHERE suppkey IN (SELECT suppkey FROM supplier)");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/prestosql/presto/commit/0fb16ab9d9c990e58fad63d4dab3dbbe482a077d
        // after https://github.com/prestosql/presto/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), Domain.create(ValueSet.ofRanges(
                range(INTEGER, 1L, true, 100L, true)), false)
                .toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 100);
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinLargeBuildSideRangeDynamicFiltering()
    {
        skipTestUnless(supportsCoordinatorDynamicFiltering());

        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem WHERE orderkey IN (SELECT orderkey FROM orders)");
        assertEquals(result.getResult().getRowCount(), LINEITEM_COUNT);

        // TODO bring back OperatorStats assertions from https://github.com/prestosql/presto/commit/0fb16ab9d9c990e58fad63d4dab3dbbe482a077d
        // after https://github.com/prestosql/presto/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(
                domainStats.getSimplifiedDomain(),
                Domain.create(
                        ValueSet.ofRanges(range(INTEGER, 1L, true, 60000L, true)), false)
                        .toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 1);
    }

    @Test(timeOut = 30_000)
    public void testRightJoinWithEmptyBuildSide()
    {
        skipTestUnless(supportsCoordinatorDynamicFiltering());

        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem l RIGHT JOIN supplier s ON l.suppkey = s.suppkey WHERE name = 'abc'");
        assertEquals(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/prestosql/presto/commit/0fb16ab9d9c990e58fad63d4dab3dbbe482a077d
        // after https://github.com/prestosql/presto/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), none(BIGINT).toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 0);
    }

    @Test(timeOut = 30_000)
    public void testRightJoinWithSelectiveBuildSide()
    {
        skipTestUnless(supportsCoordinatorDynamicFiltering());

        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem l RIGHT JOIN supplier s ON l.suppkey = s.suppkey WHERE name = 'Supplier#000000001'");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/prestosql/presto/commit/0fb16ab9d9c990e58fad63d4dab3dbbe482a077d
        // after https://github.com/prestosql/presto/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), singleValue(BIGINT, 1L).toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 1);
    }

    @Test(timeOut = 30_000)
    public void testRightJoinWithNonSelectiveBuildSide()
    {
        skipTestUnless(supportsCoordinatorDynamicFiltering());

        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem l RIGHT JOIN supplier s ON l.suppkey = s.suppkey");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/prestosql/presto/commit/0fb16ab9d9c990e58fad63d4dab3dbbe482a077d
        // after https://github.com/prestosql/presto/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), Domain.create(ValueSet.ofRanges(
                range(BIGINT, 1L, true, 100L, true)), false)
                .toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 100);
    }

    @Test
    public void testJoinDynamicFilteringNone()
    {
        skipTestUnless(supportsNodeLocalDynamicFiltering());

        // Probe-side is not scanned at all, due to dynamic filtering:
        assertDynamicFiltering(
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice < 0",
                withBroadcastJoin(),
                0,
                0, ORDERS_COUNT);
    }

    @Test
    public void testJoinLargeBuildSideDynamicFiltering()
    {
        skipTestUnless(supportsNodeLocalDynamicFiltering());

        @Language("SQL") String sql = "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey and orders.custkey BETWEEN 300 AND 700";
        int expectedRowCount = 15793;
        // Probe-side is fully scanned because the build-side is too large for dynamic filtering:
        assertDynamicFiltering(
                sql,
                withBroadcastJoin(),
                expectedRowCount,
                LINEITEM_COUNT, ORDERS_COUNT);
        // Probe-side is partially scanned because we extract min/max from large build-side for dynamic filtering
        assertDynamicFiltering(
                sql,
                withLargeDynamicFilters(),
                expectedRowCount,
                60139, ORDERS_COUNT);
    }

    @Test
    public void testPartitionedJoinNoDynamicFiltering()
    {
        skipTestUnless(supportsNodeLocalDynamicFiltering());

        // Probe-side is fully scanned, because local dynamic filtering does not work for partitioned joins:
        assertDynamicFiltering(
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice < 0",
                withPartitionedJoin(),
                0,
                LINEITEM_COUNT, ORDERS_COUNT);
    }

    @Test
    public void testJoinDynamicFilteringSingleValue()
    {
        skipTestUnless(supportsNodeLocalDynamicFiltering());

        assertQueryResult("SELECT orderkey FROM orders WHERE comment = 'nstructions sleep furiously among '", 1L);
        assertQueryResult("SELECT COUNT() FROM lineitem WHERE orderkey = 1", 6L);

        assertQueryResult("SELECT partkey FROM part WHERE comment = 'onic deposits'", 1552L);
        assertQueryResult("SELECT COUNT() FROM lineitem WHERE partkey = 1552", 39L);

        // Join lineitem with a single row of orders
        assertDynamicFiltering(
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment = 'nstructions sleep furiously among '",
                withBroadcastJoin(),
                6,
                6, ORDERS_COUNT);

        // Join lineitem with a single row of part
        assertDynamicFiltering(
                "SELECT l.comment FROM  lineitem l, part p WHERE p.partkey = l.partkey AND p.comment = 'onic deposits'",
                withBroadcastJoin(),
                39,
                39, PART_COUNT);
    }

    @Test
    public void testJoinDynamicFilteringImplicitCoercion()
    {
        skipTestUnless(supportsNodeLocalDynamicFiltering());

        assertUpdate("CREATE TABLE coerce_test AS SELECT CAST(orderkey as INT) orderkey_int FROM tpch.tiny.lineitem", "SELECT count(*) FROM lineitem");
        // Probe-side is partially scanned, dynamic filters from build side are coerced to the probe column type
        assertDynamicFiltering(
                "SELECT * FROM coerce_test l JOIN orders o ON l.orderkey_int = o.orderkey AND o.comment = 'nstructions sleep furiously among '",
                withBroadcastJoin(),
                6,
                6, ORDERS_COUNT);
    }

    @Test
    public void testJoinDynamicFilteringBlockProbeSide()
    {
        skipTestUnless(supportsNodeLocalDynamicFiltering());

        // Wait for both build sides to finish before starting the scan of 'lineitem' table (should be very selective given the dynamic filters).
        assertDynamicFiltering(
                "SELECT l.comment" +
                        " FROM  lineitem l, part p, orders o" +
                        " WHERE l.orderkey = o.orderkey AND o.comment = 'nstructions sleep furiously among '" +
                        " AND p.partkey = l.partkey AND p.comment = 'onic deposits'",
                withBroadcastJoinNonReordering(),
                1,
                1, PART_COUNT, ORDERS_COUNT);
    }

    @Test
    public void testSemiJoinDynamicFilteringNone()
    {
        skipTestUnless(supportsNodeLocalDynamicFiltering());

        // Probe-side is not scanned at all, due to dynamic filtering:
        assertDynamicFiltering(
                "SELECT * FROM lineitem WHERE lineitem.orderkey IN (SELECT orders.orderkey FROM orders WHERE orders.totalprice < 0)",
                withBroadcastJoin(),
                0,
                0, ORDERS_COUNT);
    }

    @Test
    public void testSemiJoinLargeBuildSideDynamicFiltering()
    {
        skipTestUnless(supportsNodeLocalDynamicFiltering());

        // Probe-side is fully scanned because the build-side is too large for dynamic filtering:
        @Language("SQL") String sql = "SELECT * FROM lineitem WHERE lineitem.orderkey IN " +
                "(SELECT orders.orderkey FROM orders WHERE orders.custkey BETWEEN 300 AND 700)";
        int expectedRowCount = 15793;
        // Probe-side is fully scanned because the build-side is too large for dynamic filtering:
        assertDynamicFiltering(
                sql,
                withBroadcastJoin(),
                expectedRowCount,
                LINEITEM_COUNT, ORDERS_COUNT);
        // Probe-side is partially scanned because we extract min/max from large build-side for dynamic filtering
        assertDynamicFiltering(
                sql,
                withLargeDynamicFilters(),
                expectedRowCount,
                60139, ORDERS_COUNT);
    }

    @Test
    public void testPartitionedSemiJoinNoDynamicFiltering()
    {
        skipTestUnless(supportsNodeLocalDynamicFiltering());

        // Probe-side is fully scanned, because local dynamic filtering does not work for partitioned joins:
        assertDynamicFiltering(
                "SELECT * FROM lineitem WHERE lineitem.orderkey IN (SELECT orders.orderkey FROM orders WHERE orders.totalprice < 0)",
                withPartitionedJoin(),
                0,
                LINEITEM_COUNT, ORDERS_COUNT);
    }

    @Test
    public void testSemiJoinDynamicFilteringSingleValue()
    {
        skipTestUnless(supportsNodeLocalDynamicFiltering());

        // Join lineitem with a single row of orders
        assertDynamicFiltering(
                "SELECT * FROM lineitem WHERE lineitem.orderkey IN (SELECT orders.orderkey FROM orders WHERE orders.comment = 'nstructions sleep furiously among ')",
                withBroadcastJoin(),
                6,
                6, ORDERS_COUNT);

        // Join lineitem with a single row of part
        assertDynamicFiltering(
                "SELECT l.comment FROM lineitem l WHERE l.partkey IN (SELECT p.partkey FROM part p WHERE p.comment = 'onic deposits')",
                withBroadcastJoin(),
                39,
                39, PART_COUNT);
    }

    @Test
    public void testSemiJoinDynamicFilteringBlockProbeSide()
    {
        skipTestUnless(supportsNodeLocalDynamicFiltering());

        // Wait for both build sides to finish before starting the scan of 'lineitem' table (should be very selective given the dynamic filters).
        assertDynamicFiltering(
                "SELECT t.comment FROM " +
                        "(SELECT * FROM lineitem l WHERE l.orderkey IN (SELECT o.orderkey FROM orders o WHERE o.comment = 'nstructions sleep furiously among ')) t " +
                        "WHERE t.partkey IN (SELECT p.partkey FROM part p WHERE p.comment = 'onic deposits')",
                withBroadcastJoinNonReordering(),
                1,
                1, ORDERS_COUNT, PART_COUNT);
    }

    @Test
    public void testCrossJoinDynamicFiltering()
    {
        skipTestUnless(supportsNodeLocalDynamicFiltering());

        assertUpdate("CREATE TABLE probe (k VARCHAR, v INTEGER)");
        assertUpdate("CREATE TABLE build (vmin INTEGER, vmax INTEGER)");
        assertUpdate("INSERT INTO probe VALUES ('a', 0), ('b', 1), ('c', 2), ('d', 3)", 4);
        assertUpdate("INSERT INTO build VALUES (1, 2)", 1);

        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v >= vmin", withBroadcastJoin(), 3, 3, 1);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v > vmin", withBroadcastJoin(), 2, 2, 1);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v <= vmax", withBroadcastJoin(), 3, 3, 1);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v < vmax", withBroadcastJoin(), 2, 2, 1);

        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v >= vmin AND v < vmax", withBroadcastJoin(), 1, 1, 1);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v > vmin AND v <= vmax", withBroadcastJoin(), 1, 1, 1);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v > vmin AND v < vmax", withBroadcastJoin(), 0, 0, 1);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v > vmin AND vmax < 0", withBroadcastJoin(), 0, 0, 1);

        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v BETWEEN vmin AND vmax", withBroadcastJoin(), 2, 2, 1);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v >= vmin AND v <= vmax", withBroadcastJoin(), 2, 2, 1);
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v BETWEEN vmin AND vmax", withBroadcastJoin(), 2, 2, 1);
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v >= vmin AND v <= vmax", withBroadcastJoin(), 2, 2, 1);

        // TODO: support complex inequality join clauses: https://github.com/prestosql/presto/issues/5755
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v BETWEEN vmin AND vmax - 1", withBroadcastJoin(), 1, 3, 1);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v BETWEEN vmin + 1 AND vmax", withBroadcastJoin(), 1, 3, 1);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v BETWEEN vmin + 1 AND vmax - 1", withBroadcastJoin(), 0, 4, 1);
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v BETWEEN vmin AND vmax - 1", withBroadcastJoin(), 1, 3, 1);
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v BETWEEN vmin + 1 AND vmax", withBroadcastJoin(), 1, 3, 1);
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v BETWEEN vmin + 1 AND vmax - 1", withBroadcastJoin(), 0, 4, 1);

        // TODO: make sure it works after https://github.com/prestosql/presto/issues/5777 is fixed
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v >= vmin AND v <= vmax - 1", withBroadcastJoin(), 1, 1, 1);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v >= vmin + 1 AND v <= vmax", withBroadcastJoin(), 1, 1, 1);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v >= vmin + 1 AND v <= vmax - 1", withBroadcastJoin(), 0, 0, 1);

        // TODO: support complex inequality join clauses: https://github.com/prestosql/presto/issues/5755
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v >= vmin AND v <= vmax - 1", withBroadcastJoin(), 1, 3, 1);
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v >= vmin + 1 AND v <= vmax", withBroadcastJoin(), 1, 3, 1);
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v >= vmin + 1 AND v <= vmax - 1", withBroadcastJoin(), 0, 4, 1);

        assertDynamicFiltering("SELECT * FROM probe WHERE v <= (SELECT max(vmax) FROM build)", withBroadcastJoin(), 3, 3, 1);
    }

    @Test
    public void testCrossJoinLargeBuildSideDynamicFiltering()
    {
        skipTestUnless(supportsNodeLocalDynamicFiltering());

        // Probe-side is fully scanned because the build-side is too large for dynamic filtering:
        assertDynamicFiltering(
                "SELECT * FROM orders o, customer c WHERE o.custkey < c.custkey AND c.name < 'Customer#000001000' AND o.custkey > 1000",
                withBroadcastJoin(),
                0,
                ORDERS_COUNT, CUSTOMER_COUNT);
    }

    @Test
    public void testJoinDynamicFilteringMultiJoin()
    {
        assertUpdate("CREATE TABLE t0 (k0 integer, v0 real)");
        assertUpdate("CREATE TABLE t1 (k1 integer, v1 real)");
        assertUpdate("CREATE TABLE t2 (k2 integer, v2 real)");
        assertUpdate("INSERT INTO t0 VALUES (1, 1.0)", 1);
        assertUpdate("INSERT INTO t1 VALUES (1, 2.0)", 1);
        assertUpdate("INSERT INTO t2 VALUES (1, 3.0)", 1);

        String query = "SELECT k0, k1, k2 FROM t0, t1, t2 WHERE (k0 = k1) AND (k0 = k2) AND (v0 + v1 = v2)";
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.BROADCAST.name())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, FeaturesConfig.JoinReorderingStrategy.NONE.name())
                .build();
        assertQuery(session, query, "SELECT 1, 1, 1");
    }

    private void assertQueryResult(@Language("SQL") String sql, Object... expected)
    {
        MaterializedResult rows = computeActual(sql);
        assertEquals(rows.getRowCount(), expected.length);

        for (int i = 0; i < expected.length; i++) {
            MaterializedRow materializedRow = rows.getMaterializedRows().get(i);
            int fieldCount = materializedRow.getFieldCount();
            assertEquals(fieldCount, 1, format("Expected only one column, but got '%d'", fieldCount));
            Object value = materializedRow.getField(0);
            assertEquals(value, expected[i]);
            assertEquals(materializedRow.getFieldCount(), 1);
        }
    }

    private void assertDynamicFiltering(@Language("SQL") String selectQuery, Session session, int expectedRowCount, int... expectedOperatorRowsRead)
    {
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(session, selectQuery);

        assertEquals(result.getResult().getRowCount(), expectedRowCount);
        assertEquals(getOperatorRowsRead(getDistributedQueryRunner(), result.getQueryId()).toArray(), expectedOperatorRowsRead);
    }

    private Session withBroadcastJoin()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .build();
    }

    private Session withLargeDynamicFilters()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .setSystemProperty(ENABLE_LARGE_DYNAMIC_FILTERS, "true")
                .build();
    }

    private Session withBroadcastJoinNonReordering()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .build();
    }

    private Session withPartitionedJoin()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                .build();
    }

    private static List<Integer> getOperatorRowsRead(DistributedQueryRunner runner, QueryId queryId)
    {
        QueryStats stats = runner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats();
        return stats.getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().contains("Scan"))
                .map(OperatorStats::getInputPositions)
                .map(Math::toIntExact)
                .collect(toImmutableList());
    }

    private DynamicFiltersStats getDynamicFilteringStats(QueryId queryId)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getDynamicFiltersStats();
    }
}
