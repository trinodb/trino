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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.planprinter.PlanPrinter;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.spi.NodeVersion.UNKNOWN;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers https://github.com/trinodb/trino/issues/14.
 * <p>
 * A {@code UNION ALL} of global aggregations over one table is planned as one scan.
 * Result checks on the inline {@code sales} values, and the plan checks for grouped
 * aggregations, distinct aggregates, and other shapes, stay on the original plan.
 */
public class TestUnionAllSimilarAggregations
        extends BasePlanTest
{
    private static final String SALES =
            """
            WITH sales(region, amount) AS (
                VALUES
                    (1, 10),
                    (1, 20),
                    (1, 30),
                    (2, 5),
                    (2, 15),
                    (3, 100),
                    (NULL, 7),
                    (1, NULL))
            """;

    private final QueryAssertions assertions;

    public TestUnionAllSimilarAggregations()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new TpchPlugin());
        assertions.getQueryRunner().createCatalog("tpch", "tpch");
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testUnionAllOfSimilarAggregationsScansTableOnce()
    {
        assertTableScanCount(
                """
                SELECT 1 AS bucket, count(*) AS rows, sum(nationkey) AS total
                FROM nation
                WHERE regionkey = 1
                UNION ALL
                SELECT 2, count(*), sum(nationkey)
                FROM nation
                WHERE regionkey = 2
                """,
                "nation",
                1);
        assertUnionMatchesBranches(
                """
                SELECT 1, count(*), sum(nationkey)
                FROM tpch.tiny.nation
                WHERE regionkey = 1
                """,
                """
                SELECT 2, count(*), sum(nationkey)
                FROM tpch.tiny.nation
                WHERE regionkey = 2
                """);
    }

    @Test
    public void testThreeBranchesScanOnce()
    {
        assertTableScanCount(
                """
                SELECT 1, count(*)
                FROM nation
                WHERE regionkey = 1
                UNION ALL
                SELECT 2, count(*)
                FROM nation
                WHERE regionkey = 2
                UNION ALL
                SELECT 3, count(*)
                FROM nation
                WHERE regionkey = 3
                """,
                "nation",
                1);
    }

    @Test
    public void testNestedUnionAllScansOnce()
    {
        assertTableScanCount(
                """
                SELECT 1, count(*)
                FROM nation
                WHERE regionkey = 1
                UNION ALL
                (
                    SELECT 2, count(*)
                    FROM nation
                    WHERE regionkey = 2
                    UNION ALL
                    SELECT 3, count(*)
                    FROM nation
                    WHERE regionkey = 3
                )
                """,
                "nation",
                1);
    }

    @Test
    public void testConstantAfterAggregateScansOnce()
    {
        assertTableScanCount(
                """
                SELECT count(*), 1
                FROM nation
                WHERE regionkey = 1
                UNION ALL
                SELECT count(*), 2
                FROM nation
                WHERE regionkey = 2
                """,
                "nation",
                1);
    }

    @Test
    public void testWhereFalseBranchMatchesSeparateScan()
    {
        assertUnionMatchesBranches(
                """
                SELECT 1, count(*), sum(nationkey)
                FROM tpch.tiny.nation
                WHERE regionkey = 1
                """,
                """
                SELECT 2, count(*), sum(nationkey)
                FROM tpch.tiny.nation
                WHERE false
                """);
    }

    @Test
    public void testOverlappingFiltersMatchSeparateScans()
    {
        assertUnionMatchesBranches(
                """
                SELECT 1, count(*), sum(nationkey)
                FROM tpch.tiny.nation
                WHERE regionkey = 1
                """,
                """
                SELECT 2, count(*), sum(nationkey)
                FROM tpch.tiny.nation
                WHERE regionkey IN (1, 2)
                """);
    }

    @Test
    public void testUnfilteredBranchMatchesSeparateScan()
    {
        assertUnionMatchesBranches(
                """
                SELECT 1, count(*), sum(nationkey)
                FROM tpch.tiny.nation
                WHERE regionkey = 1
                """,
                """
                SELECT 2, count(*), sum(nationkey)
                FROM tpch.tiny.nation
                """);
    }

    @Test
    public void testDifferentTablesStaySeparateScans()
    {
        String sql =
                """
                SELECT 1, count(*)
                FROM nation
                UNION ALL
                SELECT 2, count(*)
                FROM region
                """;

        assertTableScanCount(sql, "nation", 1);
        assertTableScanCount(sql, "region", 1);
    }

    @Test
    public void testGroupedAggregationsStaySeparateScans()
    {
        assertTableScanCount(
                """
                SELECT regionkey, count(*)
                FROM nation
                WHERE regionkey < 3
                GROUP BY regionkey
                UNION ALL
                SELECT regionkey, count(*)
                FROM nation
                WHERE regionkey >= 3
                GROUP BY regionkey
                """,
                "nation",
                2);
    }

    @Test
    public void testAggregateBesidePlainSelectStaysSeparateScans()
    {
        assertTableScanCount(
                """
                SELECT 1, count(*), sum(nationkey)
                FROM nation
                WHERE regionkey = 1
                UNION ALL
                SELECT regionkey, nationkey, nationkey
                FROM nation
                WHERE regionkey = 2
                """,
                "nation",
                2);
    }

    @Test
    public void testDistinctAggregationStaysSeparateScans()
    {
        assertTableScanCount(
                """
                SELECT 1, count(DISTINCT nationkey)
                FROM nation
                WHERE regionkey = 1
                UNION ALL
                SELECT 2, count(DISTINCT nationkey)
                FROM nation
                WHERE regionkey = 2
                """,
                "nation",
                2);
    }

    @Test
    public void testLimitedBranchIsNotASecondAggregateRow()
    {
        assertTableScanCount(
                """
                SELECT 1, count(*), sum(nationkey)
                FROM nation
                WHERE regionkey = 1
                UNION ALL
                (
                    SELECT 2, count(*), sum(nationkey)
                    FROM nation
                    WHERE regionkey = 2
                    LIMIT 0
                )
                """,
                "nation",
                1);

        assertResults(
                """
                SELECT 1, count(*), sum(amount)
                FROM sales
                WHERE region = 1
                UNION ALL
                (
                    SELECT 2, count(*), sum(amount)
                    FROM sales
                    WHERE region = 2
                    LIMIT 0
                )
                ORDER BY 1
                """,
                "VALUES (1, BIGINT '4', BIGINT '60')");
    }

    @Test
    public void testNonDeterministicFilterStaysSeparateScans()
    {
        assertTableScanCount(
                """
                SELECT 1, count(*)
                FROM nation
                WHERE regionkey = 1 AND random() >= 0
                UNION ALL
                SELECT 2, count(*)
                FROM nation
                WHERE regionkey = 2 AND random() >= 0
                """,
                "nation",
                2);
    }

    @Test
    public void testEmptyInputStillReturnsBothBranches()
    {
        assertResults(
                """
                SELECT 1, count(*), sum(amount)
                FROM sales
                WHERE false
                UNION ALL
                SELECT 2, count(*), sum(amount)
                FROM sales
                WHERE false
                ORDER BY 1
                """,
                "VALUES (1, BIGINT '0', CAST(NULL AS bigint)), (2, BIGINT '0', CAST(NULL AS bigint))");
    }

    @Test
    public void testWhereFalseBranchReturnsZeroCount()
    {
        assertResults(
                """
                SELECT 1, count(*), sum(amount)
                FROM sales
                WHERE region = 1
                UNION ALL
                SELECT 2, count(*), sum(amount)
                FROM sales
                WHERE false
                ORDER BY 1
                """,
                "VALUES (1, BIGINT '4', BIGINT '60'), (2, BIGINT '0', CAST(NULL AS bigint))");
    }

    @Test
    public void testNullRegionIsExcludedAndNullAmountIsIgnoredBySum()
    {
        assertResults(
                """
                SELECT 1, count(*), sum(amount)
                FROM sales
                WHERE region = 1
                UNION ALL
                SELECT 2, count(*), sum(amount)
                FROM sales
                WHERE region IS NULL
                ORDER BY 1
                """,
                "VALUES (1, BIGINT '4', BIGINT '60'), (2, BIGINT '1', BIGINT '7')");
    }

    @Test
    public void testOverlappingFiltersCountSharedRowsTwice()
    {
        assertResults(
                """
                SELECT 1, count(*), sum(amount)
                FROM sales
                WHERE region = 1
                UNION ALL
                SELECT 2, count(*), sum(amount)
                FROM sales
                WHERE region IN (1, 2)
                ORDER BY 1
                """,
                "VALUES (1, BIGINT '4', BIGINT '60'), (2, BIGINT '6', BIGINT '80')");
    }

    @Test
    public void testUnfilteredBranchReadsEveryRow()
    {
        assertResults(
                """
                SELECT 1, count(*), sum(amount)
                FROM sales
                WHERE region = 1
                UNION ALL
                SELECT 2, count(*), sum(amount)
                FROM sales
                ORDER BY 1
                """,
                "VALUES (1, BIGINT '4', BIGINT '60'), (2, BIGINT '8', BIGINT '187')");
    }

    @Test
    public void testThreeBranchesKeepTheirResults()
    {
        assertResults(
                """
                SELECT 1 AS bucket, count(*) AS rows
                FROM sales
                WHERE region = 1
                UNION ALL
                SELECT 2, count(*)
                FROM sales
                WHERE region = 2
                UNION ALL
                SELECT 3, count(*)
                FROM sales
                WHERE region = 3
                ORDER BY bucket
                """,
                "VALUES (1, BIGINT '4'), (2, BIGINT '2'), (3, BIGINT '1')");
    }

    private void assertResults(String query, String expected)
    {
        assertThat(assertions.query(SALES + query))
                .ordered()
                .matches(expected);
    }

    private void assertUnionMatchesBranches(String... branches)
    {
        MaterializedResult actual = assertions.execute(String.join("\nUNION ALL\n", branches) + "\nORDER BY 1");
        ImmutableList.Builder<MaterializedRow> expected = ImmutableList.builder();
        for (String branch : branches) {
            expected.addAll(assertions.execute(branch).getMaterializedRows());
        }
        assertThat(actual.getMaterializedRows()).isEqualTo(expected.build());
    }

    private void assertTableScanCount(String sql, String tableName, int expectedScanCount)
    {
        String explain = distributedExplain(sql);
        String table = "table = test_catalog:tiny:" + tableName;
        long scans = explain.lines()
                .filter(line -> line.contains(table + ",") || line.contains(table + "]"))
                .count();
        assertThat(scans)
                .as("distributed explain plan:\n%s", explain)
                .isEqualTo(expectedScanCount);
    }

    private String distributedExplain(String sql)
    {
        return getPlanTester().inTransaction(getPlanTester().getDefaultSession(), session -> {
            Plan plan = getPlanTester().createPlan(
                    session,
                    sql,
                    getPlanTester().getPlanOptimizers(false),
                    OPTIMIZED_AND_VALIDATED,
                    NOOP,
                    createPlanOptimizersStatsCollector());
            SubPlan subPlan = getPlanTester().createSubPlans(session, plan, false);
            return PlanPrinter.textDistributedPlan(
                    subPlan,
                    getPlanTester().getPlannerContext().getMetadata(),
                    getPlanTester().getPlannerContext().getFunctionManager(),
                    session,
                    false,
                    UNKNOWN);
        });
    }
}
