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
package io.trino.plugin.jdbc;

import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Locale;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseJdbcTableStatisticsTest
        extends AbstractTestQueryFramework
{
    // Currently this class serves as a common "interface" to define cases that should be covered.
    //  TODO extend it to provide reusable blocks to reduce boiler-plate.

    @BeforeClass
    public void setUpTables()
    {
        setUpTableFromTpch("region");
        setUpTableFromTpch("nation");
    }

    private void setUpTableFromTpch(String tableName)
    {
        // Create the table. Some subclasses use shared resources, so the table may actually exist.
        computeActual("CREATE TABLE IF NOT EXISTS " + tableName + " AS TABLE tpch.tiny." + tableName);
        // Sanity check on state of the  table in case it already existed.
        assertThat(query("SELECT count(*) FROM " + tableName))
                .matches("SELECT count(*) FROM tpch.tiny." + tableName);

        try {
            gatherStats(tableName);
        }
        catch (Exception e) {
            // gatherStats does not have to be idempotent, so we need to ignore certain errors
            if (getStackTraceAsString(e).toLowerCase(Locale.ENGLISH).contains(
                    // wording comes from Synapse
                    "there are already statistics on table")) {
                // ignore
            }
            else {
                throw e;
            }
        }
    }

    @Test
    public abstract void testNotAnalyzed();

    @Test
    public abstract void testBasic();

    @Test
    public void testEmptyTable()
    {
        String tableName = "test_stats_table_empty_" + randomNameSuffix();
        computeActual(format("CREATE TABLE %s AS SELECT orderkey, custkey, orderpriority, comment FROM tpch.tiny.orders WHERE false", tableName));
        try {
            gatherStats(tableName);
            checkEmptyTableStats(tableName);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    protected void checkEmptyTableStats(String tableName)
    {
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('orderkey', 0, 0, 1, null, null, null)," +
                        "('custkey', 0, 0, 1, null, null, null)," +
                        "('orderpriority', 0, 0, 1, null, null, null)," +
                        "('comment', 0, 0, 1, null, null, null)," +
                        "(null, null, null, null, 0, null, null)");
    }

    @Test
    public abstract void testAllNulls();

    @Test
    public abstract void testNullsFraction();

    @Test
    public abstract void testAverageColumnLength();

    @Test
    public abstract void testPartitionedTable();

    @Test
    public abstract void testView();

    @Test
    public abstract void testMaterializedView();

    @Test(dataProvider = "testCaseColumnNamesDataProvider")
    public abstract void testCaseColumnNames(String tableName);

    @DataProvider
    public Object[][] testCaseColumnNamesDataProvider()
    {
        return new Object[][] {
                {"TEST_STATS_MIXED_UNQUOTED_UPPER"},
                {"test_stats_mixed_unquoted_lower"},
                {"test_stats_mixed_uNQuoTeD_miXED"},
                {"\"TEST_STATS_MIXED_QUOTED_UPPER\""},
                {"\"test_stats_mixed_quoted_lower\""},
                {"\"test_stats_mixed_QuoTeD_miXED\""},
        };
    }

    @Test
    public abstract void testNumericCornerCases();

    @Test
    public void testStatsWithPredicatePushdown()
    {
        // Predicate on a numeric column. Should be eligible for pushdown.
        String query = "SELECT * FROM nation WHERE regionkey = 1";

        // Verify query can be pushed down, that's the situation we want to test for.
        assertThat(query(query)).isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .exceptColumns("data_size", "low_value", "high_value")
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('nationkey', 5e0, 0e0, null)," +
                        "('name', 5e0, 0e0, null)," +
                        "('regionkey', 1e0, 0e0, null)," +
                        "('comment', 5e0, 0e0, null)," +
                        "(null, null, null, 5e0)");
    }

    @Test
    public void testStatsWithVarcharPredicatePushdown()
    {
        // Predicate on a varchar column. May or may not be pushed down, may or may not be subsumed.
        assertThat(query("SHOW STATS FOR (SELECT * FROM nation WHERE name = 'PERU')"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .exceptColumns("data_size", "low_value", "high_value")
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('nationkey', 1e0, 0e0, null)," +
                        "('name', 1e0, 0e0, null)," +
                        "('regionkey', 1e0, 0e0, null)," +
                        "('comment', 1e0, 0e0, null)," +
                        "(null, null, null, 1e0)");

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "varchar_duplicates",
                // each letter A-E repeated 5 times
                " AS SELECT nationkey, chr(codepoint('A') + nationkey / 5) fl FROM  tpch.tiny.nation")) {
            gatherStats(table.getName());

            assertThat(query("SHOW STATS FOR (SELECT * FROM " + table.getName() + " WHERE fl = 'B')"))
                    .exceptColumns("data_size", "low_value", "high_value")
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "('nationkey', 5e0, 0e0, null)," +
                            "('fl', 1e0, 0e0, null)," +
                            "(null, null, null, 5e0)");
        }
    }

    /**
     * Verify that when {@value SystemSessionProperties#STATISTICS_PRECALCULATION_FOR_PUSHDOWN_ENABLED} is disabled,
     * the connector still returns reasonable statistics.
     */
    @Test
    public void testStatsWithPredicatePushdownWithStatsPrecalculationDisabled()
    {
        // Predicate on a numeric column. Should be eligible for pushdown.
        String query = "SELECT * FROM nation WHERE regionkey = 1";
        Session session = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.STATISTICS_PRECALCULATION_FOR_PUSHDOWN_ENABLED, "false")
                .build();

        // Verify query can be pushed down, that's the situation we want to test for.
        assertThat(query(session, query)).isFullyPushedDown();

        assertThat(query(session, "SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .exceptColumns("data_size", "low_value", "high_value")
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('nationkey', 25e0, 0e0, null)," +
                        "('name', 25e0, 0e0, null)," +
                        "('regionkey', 5e0, 0e0, null)," +
                        "('comment', 25e0, 0e0, null)," +
                        "(null, null, null, 25e0)");
    }

    @Test
    public void testStatsWithLimitPushdown()
    {
        // Just limit, should be eligible for pushdown.
        String query = "SELECT regionkey, nationkey FROM nation LIMIT 2";

        // Verify query can be pushed down, that's the situation we want to test for.
        // it's important that we test with LIMIT value smaller than table row count, hence need to skip results check
        assertThat(query(query)).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .exceptColumns("data_size", "low_value", "high_value")
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('regionkey', 2e0, 0e0, null)," +
                        "('nationkey', 2e0, 0e0, null)," +
                        "(null, null, null, 2e0)");
    }

    @Test
    public void testStatsWithTopNPushdown()
    {
        // TopN on a numeric column, should be eligible for pushdown.
        String query = "SELECT regionkey, nationkey FROM nation ORDER BY regionkey LIMIT 2";

        // Verify query can be pushed down, that's the situation we want to test for.
        // it's important that we test with LIMIT value smaller than table row count and we intentionally sort on a non-unique regionkey, hence need to skip results check.
        assertThat(query(query)).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .exceptColumns("data_size", "low_value", "high_value")
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('regionkey', 2e0, 0e0, null)," +
                        "('nationkey', 2e0, 0e0, null)," +
                        "(null, null, null, 2e0)");
    }

    @Test
    public void testStatsWithDistinctPushdown()
    {
        // Just distinct, should be eligible for pushdown.
        String query = "SELECT DISTINCT regionkey FROM nation";

        // Verify query can be pushed down, that's the situation we want to test for.
        assertThat(query(query)).isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .exceptColumns("data_size", "low_value", "high_value")
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('regionkey', 5e0, 0e0, null)," +
                        "(null, null, null, 5e0)");
    }

    @Test
    public void testStatsWithDistinctLimitPushdown()
    {
        // Distinct with limit (DistinctLimitNode), should be eligible for pushdown.
        String query = "SELECT DISTINCT regionkey FROM nation LIMIT 3";

        // Verify query can be pushed down, that's the situation we want to test for.
        // it's important that we test with LIMIT value smaller than count(DISTINCT regionkey), hence need to skip results check
        assertThat(query(query)).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .exceptColumns("data_size", "low_value", "high_value")
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('regionkey', 3e0, 0e0, null)," +
                        "(null, null, null, 3e0)");
    }

    @Test
    public void testStatsWithAggregationPushdown()
    {
        // Simple aggregation, should be eligible for pushdown.
        String query = "SELECT regionkey, max(nationkey) max_nationkey, count(*) c FROM nation GROUP BY regionkey";

        // Verify query can be pushed down, that's the situation we want to test for.
        assertThat(query(query)).isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .exceptColumns("data_size", "low_value", "high_value")
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('regionkey', 5e0, 0e0, null)," +
                        "('max_nationkey', null, null, null)," +
                        "('c', null, null, null)," +
                        "(null, null, null, 5e0)");
    }

    @Test
    public void testStatsWithSimpleJoinPushdown()
    {
        // Simple filtering join with no predicates, should be eligible for pushdown.
        String query = "SELECT n.name n_name FROM nation n JOIN region r ON n.nationkey = r.regionkey";

        // Verify query can be pushed down, that's the situation we want to test for.
        assertThat(query(query)).isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .exceptColumns("data_size", "low_value", "high_value")
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('n_name', 5e0, 0e0, null)," +
                        "(null, null, null, 5e0)");
    }

    @Test
    public void testStatsWithJoinPushdown()
    {
        // Simple join with heavily filtered side, should be eligible for pushdown.
        String query = "SELECT r.regionkey regionkey, r.name r_name, n.name n_name FROM region r JOIN nation n ON r.regionkey = n.regionkey WHERE n.nationkey = 5";

        // Verify query can be pushed down, that's the situation we want to test for.
        assertThat(query(query)).isFullyPushedDown();

        assertThat(query("SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .exceptColumns("data_size", "low_value", "high_value")
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('regionkey', 1e0, 0e0, null)," +
                        "('r_name', 1e0, 0e0, null)," +
                        "('n_name', 1e0, 0e0, null)," +
                        "(null, null, null, 1e0)");
    }

    protected abstract void gatherStats(String tableName);
}
