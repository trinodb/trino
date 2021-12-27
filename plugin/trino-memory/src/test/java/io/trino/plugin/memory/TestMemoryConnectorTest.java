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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.QueryId;
import io.trino.spi.metrics.Count;
import io.trino.spi.metrics.Metrics;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import io.trino.testng.services.Flaky;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.FeaturesConfig.JoinDistributionType;
import static io.trino.FeaturesConfig.JoinDistributionType.BROADCAST;
import static io.trino.SystemSessionProperties.ENABLE_LARGE_DYNAMIC_FILTERS;
import static io.trino.plugin.memory.MemoryQueryRunner.createMemoryQueryRunner;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMemoryConnectorTest
        extends BaseConnectorTest
{
    private static final int LINEITEM_COUNT = 60175;
    private static final int ORDERS_COUNT = 15000;
    private static final int PART_COUNT = 2000;
    private static final int CUSTOMER_COUNT = 1500;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createMemoryQueryRunner(
                // Adjust DF limits to test edge cases
                ImmutableMap.<String, String>builder()
                        .put("dynamic-filtering.small-broadcast.max-distinct-values-per-driver", "100")
                        .put("dynamic-filtering.small-broadcast.range-row-limit-per-driver", "100")
                        .put("dynamic-filtering.large-broadcast.max-distinct-values-per-driver", "100")
                        .put("dynamic-filtering.large-broadcast.range-row-limit-per-driver", "100000")
                        .put("dynamic-filtering.large-partitioned.max-distinct-values-per-driver", "100")
                        .put("dynamic-filtering.large-partitioned.range-row-limit-per-driver", "100000")
                        // disable semi join to inner join rewrite to test semi join operators explicitly
                        .put("optimizer.rewrite-filtering-semi-join-to-inner-join", "false")
                        .build(),
                ImmutableSet.<TpchTable<?>>builder()
                        .addAll(REQUIRED_TPCH_TABLES)
                        .add(TpchTable.PART)
                        .add(TpchTable.LINE_ITEM)
                        .build());
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN:
            case SUPPORTS_LIMIT_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_AGGREGATION_PUSHDOWN:
                return false;

            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_DROP_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_RENAME_SCHEMA:
                return false;

            case SUPPORTS_CREATE_VIEW:
                return true;

            case SUPPORTS_NOT_NULL_CONSTRAINT:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Memory connector does not support column default values");
    }

    @Test
    public void testCreateTableWhenTableIsAlreadyCreated()
    {
        @Language("SQL") String createTableSql = "CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation";

        // it has to be RuntimeException as FailureInfo$FailureException is private
        assertThatThrownBy(() -> assertUpdate(createTableSql))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("line 1:1: Destination table 'memory.default.nation' already exists");
    }

    @Test
    public void testSelect()
    {
        assertUpdate("CREATE TABLE test_select AS SELECT * FROM tpch.tiny.nation", "SELECT count(*) FROM nation");

        assertQuery("SELECT * FROM test_select ORDER BY nationkey", "SELECT * FROM nation ORDER BY nationkey");

        assertQueryResult("INSERT INTO test_select SELECT * FROM tpch.tiny.nation", 25L);

        assertQueryResult("INSERT INTO test_select SELECT * FROM tpch.tiny.nation", 25L);

        assertQueryResult("SELECT count(*) FROM test_select", 75L);
    }

    @Test
    // TODO (https://github.com/trinodb/trino/issues/8691) fix the test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/8691", match = "ComparisonFailure: expected:<LongCount\\{total=\\[\\d+]}> but was:<(LongCount\\{total=\\[\\d+]}|null)>")
    public void testCustomMetricsScanFilter()
    {
        Metrics metrics = collectCustomMetrics("SELECT partkey FROM part WHERE partkey % 1000 > 0");
        assertThat(metrics.getMetrics().get("rows")).isEqualTo(new LongCount(PART_COUNT));
        assertThat(metrics.getMetrics().get("started")).isEqualTo(metrics.getMetrics().get("finished"));
        assertThat(((Count<?>) metrics.getMetrics().get("finished")).getTotal()).isGreaterThan(0);
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/8691", match = "ComparisonFailure: expected:<LongCount\\{total=\\[\\d+]}> but was:<(LongCount\\{total=\\[\\d+]}|null)>")
    public void testCustomMetricsScanOnly()
    {
        Metrics metrics = collectCustomMetrics("SELECT partkey FROM part");
        assertThat(metrics.getMetrics().get("rows")).isEqualTo(new LongCount(PART_COUNT));
        assertThat(metrics.getMetrics().get("started")).isEqualTo(metrics.getMetrics().get("finished"));
        assertThat(((Count<?>) metrics.getMetrics().get("finished")).getTotal()).isGreaterThan(0);
    }

    @Test
    public void testExplainCustomMetricsScanOnly()
    {
        assertExplainAnalyze(
                "EXPLAIN ANALYZE VERBOSE SELECT partkey FROM part",
                "'rows' = LongCount\\{total=2000}");
    }

    @Test
    public void testExplainCustomMetricsScanFilter()
    {
        assertExplainAnalyze(
                "EXPLAIN ANALYZE VERBOSE SELECT partkey FROM part WHERE partkey % 1000 > 0",
                "'rows' = LongCount\\{total=2000}");
    }

    private Metrics collectCustomMetrics(String sql)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(getSession(), sql);
        return runner
                .getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.getQueryId())
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .map(OperatorStats::getConnectorMetrics)
                .reduce(Metrics.EMPTY, Metrics::mergeWith);
    }

    @Test(timeOut = 30_000)
    public void testPhysicalInputPositions()
    {
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(
                getSession(),
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey " +
                        "AND supplier.name = 'Supplier#000000001'");
        assertEquals(result.getResult().getRowCount(), 615);

        OperatorStats probeStats = getScanOperatorStats(getDistributedQueryRunner(), result.getQueryId()).stream()
                .findFirst().orElseThrow();
        assertEquals(probeStats.getInputPositions(), 615);
        assertEquals(probeStats.getPhysicalInputPositions(), LINEITEM_COUNT);
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testJoinDynamicFilteringNone(JoinDistributionType joinDistributionType)
    {
        // Probe-side is not scanned at all, due to dynamic filtering:
        assertDynamicFiltering(
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice < 0",
                noJoinReordering(joinDistributionType),
                0,
                0, ORDERS_COUNT);
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testJoinLargeBuildSideDynamicFiltering(JoinDistributionType joinDistributionType)
    {
        @Language("SQL") String sql = "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey and orders.custkey BETWEEN 300 AND 700";
        int expectedRowCount = 15793;
        // Probe-side is fully scanned because the build-side is too large for dynamic filtering:
        assertDynamicFiltering(
                sql,
                noJoinReordering(joinDistributionType),
                expectedRowCount,
                LINEITEM_COUNT, ORDERS_COUNT);
        // Probe-side is partially scanned because we extract min/max from large build-side for dynamic filtering
        assertDynamicFiltering(
                sql,
                withLargeDynamicFilters(joinDistributionType),
                expectedRowCount,
                60139, ORDERS_COUNT);
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testJoinDynamicFilteringSingleValue(JoinDistributionType joinDistributionType)
    {
        assertQueryResult("SELECT orderkey FROM orders WHERE comment = 'nstructions sleep furiously among '", 1L);
        assertQueryResult("SELECT COUNT() FROM lineitem WHERE orderkey = 1", 6L);

        assertQueryResult("SELECT partkey FROM part WHERE comment = 'onic deposits'", 1552L);
        assertQueryResult("SELECT COUNT() FROM lineitem WHERE partkey = 1552", 39L);

        // Join lineitem with a single row of orders
        assertDynamicFiltering(
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment = 'nstructions sleep furiously among '",
                noJoinReordering(joinDistributionType),
                6,
                6, ORDERS_COUNT);

        // Join lineitem with a single row of part
        assertDynamicFiltering(
                "SELECT l.comment FROM  lineitem l, part p WHERE p.partkey = l.partkey AND p.comment = 'onic deposits'",
                noJoinReordering(joinDistributionType),
                39,
                39, PART_COUNT);
    }

    @Test
    public void testJoinDynamicFilteringImplicitCoercion()
    {
        assertUpdate("CREATE TABLE coerce_test AS SELECT CAST(orderkey as INT) orderkey_int FROM tpch.tiny.lineitem", "SELECT count(*) FROM lineitem");
        // Probe-side is partially scanned, dynamic filters from build side are coerced to the probe column type
        assertDynamicFiltering(
                "SELECT * FROM coerce_test l JOIN orders o ON l.orderkey_int = o.orderkey AND o.comment = 'nstructions sleep furiously among '",
                noJoinReordering(BROADCAST),
                6,
                6, ORDERS_COUNT);
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testJoinDynamicFilteringBlockProbeSide(JoinDistributionType joinDistributionType)
    {
        // Wait for both build sides to finish before starting the scan of 'lineitem' table (should be very selective given the dynamic filters).
        assertDynamicFiltering(
                "SELECT l.comment" +
                        " FROM  lineitem l, part p, orders o" +
                        " WHERE l.orderkey = o.orderkey AND o.comment = 'nstructions sleep furiously among '" +
                        " AND p.partkey = l.partkey AND p.comment = 'onic deposits'",
                noJoinReordering(joinDistributionType),
                1,
                1, PART_COUNT, ORDERS_COUNT);
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testSemiJoinDynamicFilteringNone(JoinDistributionType joinDistributionType)
    {
        // Probe-side is not scanned at all, due to dynamic filtering:
        assertDynamicFiltering(
                "SELECT * FROM lineitem WHERE lineitem.orderkey IN (SELECT orders.orderkey FROM orders WHERE orders.totalprice < 0)",
                noJoinReordering(joinDistributionType),
                0,
                0, ORDERS_COUNT);
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testSemiJoinLargeBuildSideDynamicFiltering(JoinDistributionType joinDistributionType)
    {
        // Probe-side is fully scanned because the build-side is too large for dynamic filtering:
        @Language("SQL") String sql = "SELECT * FROM lineitem WHERE lineitem.orderkey IN " +
                "(SELECT orders.orderkey FROM orders WHERE orders.custkey BETWEEN 300 AND 700)";
        int expectedRowCount = 15793;
        // Probe-side is fully scanned because the build-side is too large for dynamic filtering:
        assertDynamicFiltering(
                sql,
                noJoinReordering(joinDistributionType),
                expectedRowCount,
                LINEITEM_COUNT, ORDERS_COUNT);
        // Probe-side is partially scanned because we extract min/max from large build-side for dynamic filtering
        assertDynamicFiltering(
                sql,
                withLargeDynamicFilters(joinDistributionType),
                expectedRowCount,
                60139, ORDERS_COUNT);
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testSemiJoinDynamicFilteringSingleValue(JoinDistributionType joinDistributionType)
    {
        // Join lineitem with a single row of orders
        assertDynamicFiltering(
                "SELECT * FROM lineitem WHERE lineitem.orderkey IN (SELECT orders.orderkey FROM orders WHERE orders.comment = 'nstructions sleep furiously among ')",
                noJoinReordering(joinDistributionType),
                6,
                6, ORDERS_COUNT);

        // Join lineitem with a single row of part
        assertDynamicFiltering(
                "SELECT l.comment FROM lineitem l WHERE l.partkey IN (SELECT p.partkey FROM part p WHERE p.comment = 'onic deposits')",
                noJoinReordering(joinDistributionType),
                39,
                39, PART_COUNT);
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testSemiJoinDynamicFilteringBlockProbeSide(JoinDistributionType joinDistributionType)
    {
        // Wait for both build sides to finish before starting the scan of 'lineitem' table (should be very selective given the dynamic filters).
        assertDynamicFiltering(
                "SELECT t.comment FROM " +
                        "(SELECT * FROM lineitem l WHERE l.orderkey IN (SELECT o.orderkey FROM orders o WHERE o.comment = 'nstructions sleep furiously among ')) t " +
                        "WHERE t.partkey IN (SELECT p.partkey FROM part p WHERE p.comment = 'onic deposits')",
                noJoinReordering(joinDistributionType),
                1,
                1, ORDERS_COUNT, PART_COUNT);
    }

    @Test
    public void testCrossJoinDynamicFiltering()
    {
        assertUpdate("DROP TABLE IF EXISTS probe");
        assertUpdate("CREATE TABLE probe (k VARCHAR, v INTEGER)");
        assertUpdate("INSERT INTO probe VALUES ('a', 0), ('b', 1), ('c', 2), ('d', 3), ('e', NULL)", 5);

        assertUpdate("DROP TABLE IF EXISTS build");
        assertUpdate("CREATE TABLE build (vmin INTEGER, vmax INTEGER)");
        assertUpdate("INSERT INTO build VALUES (1, 2), (NULL, NULL)", 2);

        Session session = noJoinReordering(BROADCAST);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v >= vmin", session, 3, 3, 2);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v > vmin", session, 2, 2, 2);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v <= vmax", session, 3, 3, 2);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v < vmax", session, 2, 2, 2);

        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v >= vmin AND v < vmax", session, 1, 1, 2);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v > vmin AND v <= vmax", session, 1, 1, 2);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v > vmin AND v < vmax", session, 0, 0, 2);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v > vmin AND vmax < 0", session, 0, 0, 2);

        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v BETWEEN vmin AND vmax", session, 2, 2, 2);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v >= vmin AND v <= vmax", session, 2, 2, 2);
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v BETWEEN vmin AND vmax", session, 2, 2, 2);
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v >= vmin AND v <= vmax", session, 2, 2, 2);

        // TODO: support complex inequality join clauses: https://github.com/trinodb/trino/issues/5755
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v BETWEEN vmin AND vmax - 1", session, 1, 3, 2);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v BETWEEN vmin + 1 AND vmax", session, 1, 3, 2);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v BETWEEN vmin + 1 AND vmax - 1", session, 0, 5, 2);
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v BETWEEN vmin AND vmax - 1", session, 1, 3, 2);
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v BETWEEN vmin + 1 AND vmax", session, 1, 3, 2);
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v BETWEEN vmin + 1 AND vmax - 1", session, 0, 5, 2);

        // TODO: make sure it works after https://github.com/trinodb/trino/issues/5777 is fixed
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v >= vmin AND v <= vmax - 1", session, 1, 1, 2);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v >= vmin + 1 AND v <= vmax", session, 1, 1, 2);
        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v >= vmin + 1 AND v <= vmax - 1", session, 0, 0, 2);

        // TODO: support complex inequality join clauses: https://github.com/trinodb/trino/issues/5755
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v >= vmin AND v <= vmax - 1", session, 1, 3, 2);
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v >= vmin + 1 AND v <= vmax", session, 1, 3, 2);
        assertDynamicFiltering("SELECT * FROM probe, build WHERE v >= vmin + 1 AND v <= vmax - 1", session, 0, 5, 2);

        assertDynamicFiltering("SELECT * FROM probe WHERE v <= (SELECT max(vmax) FROM build)", session, 3, 3, 2);

        assertDynamicFiltering("SELECT * FROM probe JOIN build ON v IS NOT DISTINCT FROM vmin", session, 2, 2, 2);
    }

    @Test
    public void testIsNotDistinctFromNaN()
    {
        assertUpdate("DROP TABLE IF EXISTS probe_nan");
        assertUpdate("CREATE TABLE probe_nan (v DOUBLE)");
        assertUpdate("INSERT INTO probe_nan VALUES 0, 1, 2, NULL, nan()", 5);

        assertUpdate("DROP TABLE IF EXISTS build_nan");
        assertUpdate("CREATE TABLE build_nan (v DOUBLE)");
        assertUpdate("INSERT INTO build_nan VALUES 1, NULL, nan()", 3);

        Session session = noJoinReordering(BROADCAST);
        assertDynamicFiltering("SELECT * FROM probe_nan p JOIN build_nan b ON p.v IS NOT DISTINCT FROM b.v", session, 3, 5, 3);
        assertDynamicFiltering("SELECT * FROM probe_nan p JOIN build_nan b ON p.v = b.v", session, 1, 1, 3);
    }

    @Test
    public void testCrossJoinLargeBuildSideDynamicFiltering()
    {
        // Probe-side is fully scanned because the build-side is too large for dynamic filtering:
        assertDynamicFiltering(
                "SELECT * FROM orders o, customer c WHERE o.custkey < c.custkey AND c.name < 'Customer#000001000' AND o.custkey > 1000",
                noJoinReordering(BROADCAST),
                0,
                ORDERS_COUNT, CUSTOMER_COUNT);
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testJoinDynamicFilteringMultiJoin(JoinDistributionType joinDistributionType)
    {
        assertUpdate("DROP TABLE IF EXISTS t0");
        assertUpdate("DROP TABLE IF EXISTS t1");
        assertUpdate("DROP TABLE IF EXISTS t2");
        assertUpdate("CREATE TABLE t0 (k0 integer, v0 real)");
        assertUpdate("CREATE TABLE t1 (k1 integer, v1 real)");
        assertUpdate("CREATE TABLE t2 (k2 integer, v2 real)");
        assertUpdate("INSERT INTO t0 VALUES (1, 1.0)", 1);
        assertUpdate("INSERT INTO t1 VALUES (1, 2.0)", 1);
        assertUpdate("INSERT INTO t2 VALUES (1, 3.0)", 1);

        assertQuery(
                noJoinReordering(joinDistributionType),
                "SELECT k0, k1, k2 FROM t0, t1, t2 WHERE (k0 = k1) AND (k0 = k2) AND (v0 + v1 = v2)",
                "SELECT 1, 1, 1");
    }

    private void assertDynamicFiltering(@Language("SQL") String selectQuery, Session session, int expectedRowCount, int... expectedOperatorRowsRead)
    {
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(session, selectQuery);

        assertEquals(result.getResult().getRowCount(), expectedRowCount);
        assertEquals(getOperatorRowsRead(getDistributedQueryRunner(), result.getQueryId()), Ints.asList(expectedOperatorRowsRead));
    }

    private Session withLargeDynamicFilters(JoinDistributionType joinDistributionType)
    {
        return Session.builder(noJoinReordering(joinDistributionType))
                .setSystemProperty(ENABLE_LARGE_DYNAMIC_FILTERS, "true")
                .build();
    }

    private static List<Integer> getOperatorRowsRead(DistributedQueryRunner runner, QueryId queryId)
    {
        return getScanOperatorStats(runner, queryId).stream()
                .map(OperatorStats::getInputPositions)
                .map(Math::toIntExact)
                .collect(toImmutableList());
    }

    private static List<OperatorStats> getScanOperatorStats(DistributedQueryRunner runner, QueryId queryId)
    {
        QueryStats stats = runner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats();
        return stats.getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().contains("Scan"))
                .collect(toImmutableList());
    }

    @Test
    public void testCreateTableWithNoData()
    {
        assertUpdate("CREATE TABLE test_empty (a BIGINT)");
        assertQueryResult("SELECT count(*) FROM test_empty", 0L);
        assertQueryResult("INSERT INTO test_empty SELECT nationkey FROM tpch.tiny.nation", 25L);
        assertQueryResult("SELECT count(*) FROM test_empty", 25L);
    }

    @Test
    public void testCreateFilteredOutTable()
    {
        assertUpdate("CREATE TABLE filtered_out AS SELECT nationkey FROM tpch.tiny.nation WHERE nationkey < 0", "SELECT count(nationkey) FROM nation WHERE nationkey < 0");
        assertQueryResult("SELECT count(*) FROM filtered_out", 0L);
        assertQueryResult("INSERT INTO filtered_out SELECT nationkey FROM tpch.tiny.nation", 25L);
        assertQueryResult("SELECT count(*) FROM filtered_out", 25L);
    }

    @Test
    public void testSelectFromEmptyTable()
    {
        assertUpdate("CREATE TABLE test_select_empty AS SELECT * FROM tpch.tiny.nation WHERE nationkey > 1000", "SELECT count(*) FROM nation WHERE nationkey > 1000");

        assertQueryResult("SELECT count(*) FROM test_select_empty", 0L);
    }

    @Test
    public void testSelectSingleRow()
    {
        assertQuery("SELECT * FROM tpch.tiny.nation WHERE nationkey = 1", "SELECT * FROM nation WHERE nationkey = 1");
    }

    @Test
    public void testSelectColumnsSubset()
    {
        assertQuery("SELECT nationkey, regionkey FROM tpch.tiny.nation ORDER BY nationkey", "SELECT nationkey, regionkey FROM nation ORDER BY nationkey");
    }

    @Test
    public void testCreateTableInNonDefaultSchema()
    {
        assertUpdate("CREATE SCHEMA schema1");
        assertUpdate("CREATE SCHEMA schema2");

        assertQueryResult("SHOW SCHEMAS", "default", "information_schema", "schema1", "schema2");
        assertUpdate("CREATE TABLE schema1.nation AS SELECT * FROM tpch.tiny.nation WHERE nationkey % 2 = 0", "SELECT count(*) FROM nation WHERE MOD(nationkey, 2) = 0");
        assertUpdate("CREATE TABLE schema2.nation AS SELECT * FROM tpch.tiny.nation WHERE nationkey % 2 = 1", "SELECT count(*) FROM nation WHERE MOD(nationkey, 2) = 1");

        assertQueryResult("SELECT count(*) FROM schema1.nation", 13L);
        assertQueryResult("SELECT count(*) FROM schema2.nation", 12L);
    }

    @Test
    public void testCreateTableAndViewInNotExistSchema()
    {
        assertQueryFails("CREATE TABLE schema3.test_table3 (x date)", "Schema schema3 not found");
        assertFalse(getQueryRunner().tableExists(getSession(), "schema3.test_table3"));
        assertQueryFails("CREATE VIEW schema4.test_view4 AS SELECT 123 x", "Schema schema4 not found");
        assertFalse(getQueryRunner().tableExists(getSession(), "schema4.test_view4"));
        assertQueryFails("CREATE OR REPLACE VIEW schema5.test_view5 AS SELECT 123 x", "Schema schema5 not found");
        assertFalse(getQueryRunner().tableExists(getSession(), "schema5.test_view5"));
    }

    @Test
    public void testViews()
    {
        @Language("SQL") String query = "SELECT orderkey, orderstatus, totalprice / 2 half FROM orders";

        assertUpdate("CREATE VIEW test_view AS SELECT 123 x");
        assertUpdate("CREATE OR REPLACE VIEW test_view AS " + query);

        assertQueryFails("CREATE TABLE test_view (x date)", "View \\[default.test_view] already exists");
        assertQueryFails("CREATE VIEW test_view AS SELECT 123 x", ".*View already exists: 'memory.default.test_view'");

        assertQuery("SELECT * FROM test_view", query);

        assertTrue(computeActual("SHOW TABLES").getOnlyColumnAsSet().contains("test_view"));

        assertUpdate("DROP VIEW test_view");
        assertQueryFails("DROP VIEW test_view", "line 1:1: View 'memory.default.test_view' does not exist");
    }

    @Test
    public void testRenameView()
    {
        @Language("SQL") String query = "SELECT orderkey, orderstatus, totalprice / 2 half FROM orders";

        assertUpdate("CREATE VIEW test_view_to_be_renamed AS " + query);
        assertQueryFails("ALTER VIEW test_view_to_be_renamed RENAME TO memory.test_schema_not_exist.test_view_renamed", "Schema test_schema_not_exist not found");
        assertUpdate("ALTER VIEW test_view_to_be_renamed RENAME TO test_view_renamed");
        assertQuery("SELECT * FROM test_view_renamed", query);

        assertUpdate("CREATE SCHEMA test_different_schema");
        assertUpdate("ALTER VIEW test_view_renamed RENAME TO test_different_schema.test_view_renamed");
        assertQuery("SELECT * FROM test_different_schema.test_view_renamed", query);

        assertUpdate("DROP VIEW test_different_schema.test_view_renamed");
        assertUpdate("DROP SCHEMA test_different_schema");
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
}
