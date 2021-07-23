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
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.spi.QueryId;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.SystemSessionProperties.ENABLE_LARGE_DYNAMIC_FILTERS;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.server.DynamicFilterService.DynamicFilterDomainStats;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static io.trino.spi.predicate.Domain.none;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static io.trino.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static io.trino.tpch.TpchTable.getTables;
import static io.trino.util.DynamicFiltersTestUtil.getSimplifiedDomainString;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveDynamicPartitionPruning
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestHiveDynamicPartitionPruning.class);
    private static final String PARTITIONED_LINEITEM = "partitioned_lineitem";
    private static final long LINEITEM_COUNT = 60175;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                // Reduced partitioned join limit for large DF to enable DF min/max collection with ENABLE_LARGE_DYNAMIC_FILTERS
                .addExtraProperty("dynamic-filtering.large-partitioned.max-distinct-values-per-driver", "100")
                .addExtraProperty("dynamic-filtering.large-partitioned.range-row-limit-per-driver", "100000")
                .setHiveProperties(ImmutableMap.of("hive.dynamic-filtering-probe-blocking-timeout", "1h"))
                .setInitialTables(getTables())
                .build();
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        super.init();
        // setup partitioned fact table for dynamic partition pruning
        @Language("SQL") String sql = format("CREATE TABLE %s WITH (format = 'TEXTFILE', partitioned_by=array['suppkey']) AS " +
                "SELECT orderkey, partkey, suppkey FROM %s", PARTITIONED_LINEITEM, "tpch.tiny.lineitem");
        long start = System.nanoTime();
        long rows = (Long) getQueryRunner().execute(sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, PARTITIONED_LINEITEM, nanosSince(start).convertToMostSuccinctTimeUnit());
    }

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
        assertEquals(domainStats.getSimplifiedDomain(), none(BIGINT).toString(getSession().toConnectorSession()));
        assertTrue(domainStats.getCollectionDuration().isPresent());
    }

    @Test(timeOut = 30_000)
    public void testJoinWithSelectiveBuildSide()
    {
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey " +
                        "AND supplier.name = 'Supplier#000000001'");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/trinodb/trino/commit/1feaa0f928a02f577c8ac9ef6cc0c8ec2008a46d
        // after https://github.com/trinodb/trino/issues/5120 is fixed

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
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey");
        assertEquals(result.getResult().getRowCount(), LINEITEM_COUNT);

        // TODO bring back OperatorStats assertions from https://github.com/trinodb/trino/commit/1feaa0f928a02f577c8ac9ef6cc0c8ec2008a46d
        // after https://github.com/trinodb/trino/issues/5120 is fixed

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
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem JOIN orders ON partitioned_lineitem.orderkey = orders.orderkey");
        assertEquals(result.getResult().getRowCount(), LINEITEM_COUNT);

        // TODO bring back OperatorStats assertions from https://github.com/trinodb/trino/commit/1feaa0f928a02f577c8ac9ef6cc0c8ec2008a46d
        // after https://github.com/trinodb/trino/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(
                domainStats.getSimplifiedDomain(),
                Domain.create(
                        ValueSet.ofRanges(range(BIGINT, 1L, true, 60000L, true)), false)
                        .toString(getSession().toConnectorSession()));
    }

    @Test(timeOut = 30_000)
    public void testJoinWithMultipleDynamicFiltersOnProbe()
    {
        // supplier names Supplier#000000001 and Supplier#000000002 match suppkey 1 and 2
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM (" +
                        "SELECT supplier.suppkey FROM " +
                        "partitioned_lineitem JOIN tpch.tiny.supplier ON partitioned_lineitem.suppkey = supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')" +
                        ") t JOIN supplier ON t.suppkey = supplier.suppkey AND supplier.suppkey IN (2, 3)");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/trinodb/trino/commit/1feaa0f928a02f577c8ac9ef6cc0c8ec2008a46d
        // after https://github.com/trinodb/trino/issues/5120 is fixed

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
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithEmptyBuildSide()
    {
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem WHERE suppkey IN (SELECT suppkey FROM supplier WHERE name = 'abc')");
        assertEquals(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/trinodb/trino/commit/0fb16ab9d9c990e58fad63d4dab3dbbe482a077d
        // after https://github.com/trinodb/trino/issues/5120 is fixed

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
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem WHERE suppkey IN (SELECT suppkey FROM supplier WHERE name = 'Supplier#000000001')");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/trinodb/trino/commit/0fb16ab9d9c990e58fad63d4dab3dbbe482a077d
        // after https://github.com/trinodb/trino/issues/5120 is fixed

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
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem WHERE suppkey IN (SELECT suppkey FROM supplier)");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/trinodb/trino/commit/0fb16ab9d9c990e58fad63d4dab3dbbe482a077d
        // after https://github.com/trinodb/trino/issues/5120 is fixed

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
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem WHERE orderkey IN (SELECT orderkey FROM orders)");
        assertEquals(result.getResult().getRowCount(), LINEITEM_COUNT);

        // TODO bring back OperatorStats assertions from https://github.com/trinodb/trino/commit/0fb16ab9d9c990e58fad63d4dab3dbbe482a077d
        // after https://github.com/trinodb/trino/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(
                domainStats.getSimplifiedDomain(),
                Domain.create(
                        ValueSet.ofRanges(range(BIGINT, 1L, true, 60000L, true)), false)
                        .toString(getSession().toConnectorSession()));
    }

    @Test(timeOut = 30_000)
    public void testRightJoinWithEmptyBuildSide()
    {
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem l RIGHT JOIN supplier s ON l.suppkey = s.suppkey WHERE name = 'abc'");
        assertEquals(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/trinodb/trino/commit/0fb16ab9d9c990e58fad63d4dab3dbbe482a077d
        // after https://github.com/trinodb/trino/issues/5120 is fixed

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
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem l RIGHT JOIN supplier s ON l.suppkey = s.suppkey WHERE name = 'Supplier#000000001'");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/trinodb/trino/commit/0fb16ab9d9c990e58fad63d4dab3dbbe482a077d
        // after https://github.com/trinodb/trino/issues/5120 is fixed

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
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem l RIGHT JOIN supplier s ON l.suppkey = s.suppkey");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        // TODO bring back OperatorStats assertions from https://github.com/trinodb/trino/commit/0fb16ab9d9c990e58fad63d4dab3dbbe482a077d
        // after https://github.com/trinodb/trino/issues/5120 is fixed

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertThat(domainStats.getSimplifiedDomain())
                .isEqualTo(getSimplifiedDomainString(1L, 100L, 100, BIGINT));
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
