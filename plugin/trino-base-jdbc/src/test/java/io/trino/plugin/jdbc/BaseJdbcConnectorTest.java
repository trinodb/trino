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

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.spi.QueryId;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.query.QueryAssertions.QueryAssert;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SystemSessionProperties.USE_MARK_DISTINCT;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.JOIN_PUSHDOWN_ENABLED;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.CANCELLED;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.RUNNING;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CANCELLATION;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_LIMIT_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN_WITH_LIKE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_LEVEL_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseJdbcConnectorTest
        extends BaseConnectorTest
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getName()));

    protected abstract SqlExecutor onRemoteDatabase();

    @AfterClass(alwaysRun = true)
    public void afterClass()
    {
        executor.shutdownNow();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_DELETE:
            case SUPPORTS_TRUNCATE:
                return true;

            case SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN:
                // TODO support pushdown of complex expressions in predicates
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    public void testInsertInPresenceOfNotSupportedColumn()
    {
        try (TestTable testTable = createTableWithUnsupportedColumn()) {
            String unqualifiedTableName = testTable.getName().replaceAll("^\\w+\\.", "");
            // Check that column 'two' is not supported.
            assertQuery("SELECT column_name FROM information_schema.columns WHERE table_name = '" + unqualifiedTableName + "'", "VALUES 'one', 'three'");
            assertUpdate("INSERT INTO " + testTable.getName() + " (one, three) VALUES (123, 'test')", 1);
            assertQuery("SELECT one, three FROM " + testTable.getName(), "SELECT 123, 'test'");
        }
    }

    /**
     * Creates a table with columns {@code one, two, three} where the middle one is of an unsupported (unmapped) type.
     * The first column should be numeric, and third should be varchar.
     */
    protected TestTable createTableWithUnsupportedColumn()
    {
        // TODO throw new UnsupportedOperationException();
        throw new SkipException("Not implemented");
    }

    // TODO move common tests from connector-specific classes here

    @Test
    public void testAggregationPushdown()
    {
        if (!hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN)) {
            assertThat(query("SELECT count(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
            return;
        }

        // TODO support aggregation pushdown with GROUPING SETS
        // TODO support aggregation over expressions

        // count()
        assertThat(query("SELECT count(*) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(1) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count() FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, count(1) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        try (TestTable emptyTable = createAggregationTestTable(getSession().getSchema().orElseThrow() + ".empty_table", ImmutableList.of())) {
            assertThat(query("SELECT count(*) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT count(a_bigint) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT count(1) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT count() FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT a_bigint, count(1) FROM " + emptyTable.getName() + " GROUP BY a_bigint")).isFullyPushedDown();
        }

        // GROUP BY
        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        try (TestTable emptyTable = createAggregationTestTable(getSession().getSchema().orElseThrow() + ".empty_table", ImmutableList.of())) {
            assertThat(query("SELECT t_double, min(a_bigint) FROM " + emptyTable.getName() + " GROUP BY t_double")).isFullyPushedDown();
            assertThat(query("SELECT t_double, max(a_bigint) FROM " + emptyTable.getName() + " GROUP BY t_double")).isFullyPushedDown();
            assertThat(query("SELECT t_double, sum(a_bigint) FROM " + emptyTable.getName() + " GROUP BY t_double")).isFullyPushedDown();
            assertThat(query("SELECT t_double, avg(a_bigint) FROM " + emptyTable.getName() + " GROUP BY t_double")).isFullyPushedDown();
        }

        // GROUP BY and WHERE on bigint column
        // GROUP BY and WHERE on aggregation key
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 GROUP BY regionkey")).isFullyPushedDown();

        // GROUP BY and WHERE on varchar column
        // GROUP BY and WHERE on "other" (not aggregation key, not aggregation input)
        assertConditionallyPushedDown(
                getSession(),
                "SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 AND name > 'AAA' GROUP BY regionkey",
                hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY),
                node(FilterNode.class, node(TableScanNode.class)));
        // GROUP BY above WHERE and LIMIT
        assertConditionallyPushedDown(
                getSession(),
                "SELECT regionkey, sum(nationkey) FROM (SELECT * FROM nation WHERE regionkey < 2 LIMIT 11) GROUP BY regionkey",
                hasBehavior(SUPPORTS_LIMIT_PUSHDOWN),
                node(LimitNode.class, anyTree(node(TableScanNode.class))));
        // GROUP BY above TopN
        assertConditionallyPushedDown(
                getSession(),
                "SELECT custkey, sum(totalprice) FROM (SELECT custkey, totalprice FROM orders ORDER BY orderdate ASC, totalprice ASC LIMIT 10) GROUP BY custkey",
                hasBehavior(SUPPORTS_TOPN_PUSHDOWN),
                node(TopNNode.class, anyTree(node(TableScanNode.class))));
        // GROUP BY with JOIN
        assertConditionallyPushedDown(
                joinPushdownEnabled(getSession()),
                "SELECT n.regionkey, sum(c.acctbal) acctbals FROM nation n LEFT JOIN customer c USING (nationkey) GROUP BY 1",
                hasBehavior(SUPPORTS_JOIN_PUSHDOWN),
                node(JoinNode.class, anyTree(node(TableScanNode.class)), anyTree(node(TableScanNode.class))));
        // GROUP BY with WHERE on neither grouping nor aggregation column
        assertConditionallyPushedDown(
                getSession(),
                "SELECT nationkey, min(regionkey) FROM nation WHERE name = 'ARGENTINA' GROUP BY nationkey",
                hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY),
                node(FilterNode.class, node(TableScanNode.class)));
        // GROUP BY with WHERE complex predicate
        assertConditionallyPushedDown(
                getSession(),
                "SELECT regionkey, sum(nationkey) FROM nation WHERE name LIKE '%N%' GROUP BY regionkey",
                hasBehavior(SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN_WITH_LIKE),
                node(FilterNode.class, node(TableScanNode.class)));
        // aggregation on varchar column
        assertThat(query("SELECT count(name) FROM nation")).isFullyPushedDown();
        // aggregation on varchar column with GROUPING
        assertThat(query("SELECT nationkey, count(name) FROM nation GROUP BY nationkey")).isFullyPushedDown();
        // aggregation on varchar column with WHERE
        assertConditionallyPushedDown(
                getSession(),
                "SELECT count(name) FROM nation WHERE name = 'ARGENTINA'",
                hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY),
                node(FilterNode.class, node(TableScanNode.class)));
    }

    @Test
    public void testCaseSensitiveAggregationPushdown()
    {
        if (!hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN)) {
            // Covered by testAggregationPushdown
            return;
        }

        boolean supportsPushdownWithVarcharInequality = hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY);
        boolean supportsCountDistinctPushdown = hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT);

        PlanMatchPattern aggregationOverTableScan = node(AggregationNode.class, node(TableScanNode.class));
        PlanMatchPattern groupingAggregationOverTableScan = node(AggregationNode.class, node(ProjectNode.class, node(TableScanNode.class)));
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_case_sensitive_aggregation_pushdown",
                "(a_string varchar(1), a_char char(1), a_bigint bigint)",
                ImmutableList.of(
                        "'A', 'A', 1",
                        "'B', 'B', 2",
                        "'a', 'a', 3",
                        "'b', 'b', 4"))) {
            // case-sensitive functions prevent pushdown
            assertConditionallyPushedDown(
                    getSession(),
                    "SELECT max(a_string), min(a_string), max(a_char), min(a_char) FROM " + table.getName(),
                    supportsPushdownWithVarcharInequality,
                    aggregationOverTableScan)
                    .skippingTypesCheck()
                    .matches("VALUES ('b', 'A', 'b', 'A')");
            // distinct over case-sensitive column prevents pushdown
            assertConditionallyPushedDown(
                    getSession(),
                    "SELECT distinct a_string FROM " + table.getName(),
                    supportsPushdownWithVarcharInequality,
                    groupingAggregationOverTableScan)
                    .skippingTypesCheck()
                    .matches("VALUES 'A', 'B', 'a', 'b'");
            assertConditionallyPushedDown(
                    getSession(),
                    "SELECT distinct a_char FROM " + table.getName(),
                    supportsPushdownWithVarcharInequality,
                    groupingAggregationOverTableScan)
                    .skippingTypesCheck()
                    .matches("VALUES 'A', 'B', 'a', 'b'");
            // case-sensitive grouping sets prevent pushdown
            assertConditionallyPushedDown(getSession(),
                    "SELECT a_string, count(*) FROM " + table.getName() + " GROUP BY a_string",
                    supportsPushdownWithVarcharInequality,
                    groupingAggregationOverTableScan)
                    .skippingTypesCheck()
                    .matches("VALUES ('A', BIGINT '1'), ('a', BIGINT '1'), ('b', BIGINT '1'), ('B', BIGINT '1')");
            assertConditionallyPushedDown(getSession(),
                    "SELECT a_char, count(*) FROM " + table.getName() + " GROUP BY a_char",
                    supportsPushdownWithVarcharInequality,
                    groupingAggregationOverTableScan)
                    .skippingTypesCheck()
                    .matches("VALUES ('A', BIGINT '1'), ('B', BIGINT '1'), ('a', BIGINT '1'), ('b', BIGINT '1')");

            // case-insensitive functions can still be pushed down as long as grouping sets are not case-sensitive
            assertThat(query("SELECT count(a_string), count(a_char) FROM " + table.getName())).isFullyPushedDown();
            assertThat(query("SELECT count(a_string), count(a_char) FROM " + table.getName() + " GROUP BY a_bigint")).isFullyPushedDown();

            // DISTINCT over case-sensitive columns prevents pushdown
            assertConditionallyPushedDown(getSession(),
                    "SELECT count(DISTINCT a_string) FROM " + table.getName(),
                    supportsPushdownWithVarcharInequality,
                    groupingAggregationOverTableScan)
                    .skippingTypesCheck()
                    .matches("VALUES BIGINT '4'");
            assertConditionallyPushedDown(getSession(),
                    "SELECT count(DISTINCT a_char) FROM " + table.getName(),
                    supportsPushdownWithVarcharInequality,
                    groupingAggregationOverTableScan)
                    .skippingTypesCheck()
                    .matches("VALUES BIGINT '4'");

            assertConditionallyPushedDown(getSession(),
                    "SELECT count(DISTINCT a_string), count(DISTINCT a_bigint) FROM " + table.getName(),
                    supportsPushdownWithVarcharInequality && supportsCountDistinctPushdown,
                    node(ExchangeNode.class, node(AggregationNode.class, anyTree(node(TableScanNode.class)))))
                    .skippingTypesCheck()
                    .matches("VALUES (BIGINT '4', BIGINT '4')");

            assertConditionallyPushedDown(getSession(),
                    "SELECT count(DISTINCT a_char), count(DISTINCT a_bigint) FROM " + table.getName(),
                    supportsPushdownWithVarcharInequality && supportsCountDistinctPushdown,
                    node(ExchangeNode.class, node(AggregationNode.class, anyTree(node(TableScanNode.class)))))
                    .skippingTypesCheck()
                    .matches("VALUES (BIGINT '4', BIGINT '4')");
        }
    }

    @Test
    public void testAggregationWithUnsupportedResultType()
    {
        // TODO array_agg returns array, so it could be supported
        assertThat(query("SELECT array_agg(nationkey) FROM nation"))
                .skipResultsCorrectnessCheckForPushdown() // array_agg doesn't have a deterministic order of elements in result array
                .isNotFullyPushedDown(AggregationNode.class);
        // histogram returns map, which is not supported
        assertThat(query("SELECT histogram(regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
        // multimap_agg returns multimap, which is not supported
        assertThat(query("SELECT multimap_agg(regionkey, nationkey) FROM nation"))
                .skipResultsCorrectnessCheckForPushdown() // multimap_agg doesn't have a deterministic order of values for a key
                .isNotFullyPushedDown(AggregationNode.class);
        // approx_set returns HyperLogLog, which is not supported
        assertThat(query("SELECT approx_set(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
    }

    @Test
    public void testDistinctAggregationPushdown()
    {
        if (!hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN)) {
            assertThat(query("SELECT count(DISTINCT regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
            return;
        }

        // SELECT DISTINCT
        assertThat(query("SELECT DISTINCT regionkey FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT min(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT DISTINCT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        try (TestTable emptyTable = createAggregationTestTable(getSession().getSchema().orElseThrow() + ".empty_table", ImmutableList.of())) {
            assertThat(query("SELECT DISTINCT a_bigint FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT min(DISTINCT a_bigint) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT DISTINCT t_double, min(a_bigint) FROM " + emptyTable.getName() + " GROUP BY t_double")).isFullyPushedDown();
        }

        Session withMarkDistinct = Session.builder(getSession())
                .setSystemProperty(USE_MARK_DISTINCT, "true")
                .build();
        // distinct aggregation
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertConditionallyPushedDown(
                withMarkDistinct,
                "SELECT count(DISTINCT comment) FROM nation",
                hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY),
                node(AggregationNode.class, node(ProjectNode.class, node(TableScanNode.class))));
        // two distinct aggregations
        assertConditionallyPushedDown(
                withMarkDistinct,
                "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation",
                hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT),
                node(MarkDistinctNode.class, node(ExchangeNode.class, node(ExchangeNode.class, node(ProjectNode.class, node(TableScanNode.class))))));
        // distinct aggregation and a non-distinct aggregation
        assertConditionallyPushedDown(
                withMarkDistinct,
                "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation",
                hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT),
                node(MarkDistinctNode.class, node(ExchangeNode.class, node(ExchangeNode.class, node(ProjectNode.class, node(TableScanNode.class))))));

        Session withoutMarkDistinct = Session.builder(getSession())
                .setSystemProperty(USE_MARK_DISTINCT, "false")
                .build();
        // distinct aggregation
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertConditionallyPushedDown(
                withoutMarkDistinct,
                "SELECT count(DISTINCT comment) FROM nation",
                hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY),
                node(AggregationNode.class, node(ProjectNode.class, node(TableScanNode.class))));
        // two distinct aggregations
        assertConditionallyPushedDown(
                withoutMarkDistinct,
                "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation",
                hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT),
                node(AggregationNode.class, node(ExchangeNode.class, node(ExchangeNode.class, node(TableScanNode.class)))));
        // distinct aggregation and a non-distinct aggregation
        assertConditionallyPushedDown(
                withoutMarkDistinct,
                "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation",
                hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT),
                node(AggregationNode.class, node(ExchangeNode.class, node(ExchangeNode.class, node(TableScanNode.class)))));
    }

    @Test
    public void testNumericAggregationPushdown()
    {
        if (!hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN)) {
            assertThat(query("SELECT min(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
            assertThat(query("SELECT max(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
            assertThat(query("SELECT sum(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
            assertThat(query("SELECT avg(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
            return;
        }

        String schemaName = getSession().getSchema().orElseThrow();
        // empty table
        try (TestTable emptyTable = createAggregationTestTable(schemaName + ".test_aggregation_pushdown", ImmutableList.of())) {
            assertThat(query("SELECT min(short_decimal), min(long_decimal), min(a_bigint), min(t_double) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal), max(a_bigint), max(t_double) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal), sum(a_bigint), sum(t_double) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal), avg(a_bigint), avg(t_double) FROM " + emptyTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = createAggregationTestTable(schemaName + ".test_aggregation_pushdown",
                ImmutableList.of("100.000, 100000000.000000000, 100.000, 100000000", "123.321, 123456789.987654321, 123.321, 123456789"))) {
            assertThat(query("SELECT min(short_decimal), min(long_decimal), min(a_bigint), min(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal), max(a_bigint), max(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal), sum(a_bigint), sum(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal), avg(a_bigint), avg(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            // smoke testing of more complex cases
            // WHERE on aggregation column
            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 AND long_decimal < 124")).isFullyPushedDown();
            // WHERE on non-aggregation column
            assertThat(query("SELECT min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110")).isFullyPushedDown();
            // GROUP BY
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on both grouping and aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 AND long_decimal < 124 GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on grouping column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE long_decimal < 124 GROUP BY short_decimal")).isFullyPushedDown();
        }
    }

    @Test
    public void testCountDistinctWithStringTypes()
    {
        if (!(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_INSERT))) {
            throw new SkipException("Unable to CREATE TABLE to test count distinct");
        }

        List<String> rows = Stream.of("a", "b", "A", "B", " a ", "a", "b", " b ", "ą")
                .map(value -> format("'%1$s', '%1$s'", value))
                .collect(toImmutableList());

        try (TestTable testTable = new TestTable(getQueryRunner()::execute, "distinct_strings", "(t_char CHAR(5), t_varchar VARCHAR(5))", rows)) {
            if (!(hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN) && hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY))) {
                // disabling hash generation to prevent extra projections in the plan which make it hard to write matchers for isNotFullyPushedDown
                Session optimizeHashGenerationDisabled = Session.builder(getSession())
                        .setSystemProperty("optimize_hash_generation", "false")
                        .build();

                // It is not captured in the `isNotFullyPushedDown` calls (can't do that) but depending on the connector in use some aggregations
                // still can be pushed down to connector.
                // If `SUPPORTS_AGGREGATION_PUSHDOWN == false` but `SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY == true` the DISTINCT part of aggregation
                // will still be pushed down to connector as `GROUP BY`. Only the `count` part will remain on the Trino side.
                // If `SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY == false` both parts of aggregation will be executed on Trino side.

                assertThat(query(optimizeHashGenerationDisabled, "SELECT count(DISTINCT t_varchar) FROM " + testTable.getName()))
                        .matches("VALUES BIGINT '7'")
                        .isNotFullyPushedDown(AggregationNode.class);
                assertThat(query(optimizeHashGenerationDisabled, "SELECT count(DISTINCT t_char) FROM " + testTable.getName()))
                        .matches("VALUES BIGINT '7'")
                        .isNotFullyPushedDown(AggregationNode.class);

                assertThat(query("SELECT count(DISTINCT t_char), count(DISTINCT t_varchar) FROM " + testTable.getName()))
                        .matches("VALUES (BIGINT '7', BIGINT '7')")
                        .isNotFullyPushedDown(MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
            }
            else {
                // Single count(DISTINCT ...) can be pushed even down even if SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT == false as GROUP BY
                assertThat(query("SELECT count(DISTINCT t_varchar) FROM " + testTable.getName()))
                        .matches("VALUES BIGINT '7'")
                        .isFullyPushedDown();

                // Single count(DISTINCT ...) can be pushed down even if SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT == false as GROUP BY
                assertThat(query("SELECT count(DISTINCT t_char) FROM " + testTable.getName()))
                        .matches("VALUES BIGINT '7'")
                        .isFullyPushedDown();

                assertConditionallyPushedDown(
                        getSession(),
                        "SELECT count(DISTINCT t_char), count(DISTINCT t_varchar) FROM " + testTable.getName(),
                        hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT),
                        node(MarkDistinctNode.class, node(ExchangeNode.class, node(ExchangeNode.class, node(ProjectNode.class, node(TableScanNode.class))))));
            }
        }
    }

    /**
     * Creates a table with columns {@code short_decimal decimal(9, 3), long_decimal decimal(30, 10), t_double double, a_bigint bigint} populated
     * with the provided rows.
     */
    protected TestTable createAggregationTestTable(String name, List<String> rows)
    {
        return new TestTable(onRemoteDatabase(), name, "(short_decimal decimal(9, 3), long_decimal decimal(30, 10), t_double double precision, a_bigint bigint)", rows);
    }

    @Test
    public void testStddevAggregationPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        if (!hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV)) {
            if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
                throw new SkipException("Unable to CREATE TABLE to test aggregation pushdown");
            }

            try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_stddev_pushdown", ImmutableList.of())) {
                assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
                assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
                assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
                return;
            }
        }

        try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_stddev_pushdown", ImmutableList.of())) {
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            // with non-lowercase name on input
            assertThat(query("SELECT StdDEv(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT StdDEv_SaMP(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            // with delimited, non-lowercase name on input
            assertThat(query("SELECT \"StdDEv\"(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT \"StdDEv_SaMP\"(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            onRemoteDatabase().execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (1)");
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            onRemoteDatabase().execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (3)");
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            onRemoteDatabase().execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (5)");
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_stddev_pushdown",
                ImmutableList.of("1, 1, 1, 1", "2, 2, 2, 2", "4, 4, 4, 4", "5, 5, 5, 5"))) {
            // Test non-whole number results
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testVarianceAggregationPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        if (!hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE)) {
            if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
                throw new SkipException("Unable to CREATE TABLE to test aggregation pushdown");
            }

            try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_var_pushdown", ImmutableList.of())) {
                assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
                assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
                assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
                return;
            }
        }

        try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_var_pushdown", ImmutableList.of())) {
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            onRemoteDatabase().execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (1)");
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            onRemoteDatabase().execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (3)");
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            onRemoteDatabase().execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (5)");
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_var_pushdown",
                ImmutableList.of("1, 1, 1, 1", "2, 2, 2, 2", "4, 4, 4, 4", "5, 5, 5, 5"))) {
            // Test non-whole number results
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testCovarianceAggregationPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        if (!hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE)) {
            if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
                throw new SkipException("Unable to CREATE TABLE to test aggregation pushdown");
            }

            try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_covar_pushdown", ImmutableList.of())) {
                assertThat(query("SELECT covar_pop(t_double, u_double), covar_pop(v_real, w_real) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
                assertThat(query("SELECT covar_samp(t_double, u_double), covar_samp(v_real, w_real) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
                return;
            }
        }

        // empty table
        try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_covariance_pushdown", ImmutableList.of())) {
            assertThat(query("SELECT covar_pop(t_double, u_double), covar_pop(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT covar_samp(t_double, u_double), covar_samp(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // test some values for which the aggregate functions return whole numbers
        try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_covariance_pushdown",
                ImmutableList.of("2, 2, 2, 2", "4, 4, 4, 4"))) {
            assertThat(query("SELECT covar_pop(t_double, u_double), covar_pop(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT covar_samp(t_double, u_double), covar_samp(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // non-whole number results
        try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_covariance_pushdown",
                ImmutableList.of("1, 2, 1, 2", "100000000.123456, 4, 100000000.123456, 4", "123456789.987654, 8, 123456789.987654, 8"))) {
            assertThat(query("SELECT covar_pop(t_double, u_double), covar_pop(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT covar_samp(t_double, u_double), covar_samp(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testCorrAggregationPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        if (!hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION)) {
            if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
                throw new SkipException("Unable to CREATE TABLE to test aggregation pushdown");
            }

            try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_corr_pushdown", ImmutableList.of())) {
                assertThat(query("SELECT corr(t_double, u_double), corr(v_real, w_real) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
                return;
            }
        }

        // empty table
        try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_corr_pushdown", ImmutableList.of())) {
            assertThat(query("SELECT corr(t_double, u_double), corr(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // test some values for which the aggregate functions return whole numbers
        try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_corr_pushdown",
                ImmutableList.of("2, 2, 2, 2", "4, 4, 4, 4"))) {
            assertThat(query("SELECT corr(t_double, u_double), corr(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // non-whole number results
        try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_corr_pushdown",
                ImmutableList.of("1, 2, 1, 2", "100000000.123456, 4, 100000000.123456, 4", "123456789.987654, 8, 123456789.987654, 8"))) {
            assertThat(query("SELECT corr(t_double, u_double), corr(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testRegrAggregationPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        if (!hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION)) {
            if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
                throw new SkipException("Unable to CREATE TABLE to test aggregation pushdown");
            }

            try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_regr_pushdown", ImmutableList.of())) {
                assertThat(query("SELECT regr_intercept(t_double, u_double), regr_intercept(v_real, w_real) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
                assertThat(query("SELECT regr_slope(t_double, u_double), regr_slope(v_real, w_real) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
                return;
            }
        }

        // empty table
        try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_regr_pushdown", ImmutableList.of())) {
            assertThat(query("SELECT regr_intercept(t_double, u_double), regr_intercept(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT regr_slope(t_double, u_double), regr_slope(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // test some values for which the aggregate functions return whole numbers
        try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_regr_pushdown",
                ImmutableList.of("2, 2, 2, 2", "4, 4, 4, 4"))) {
            assertThat(query("SELECT regr_intercept(t_double, u_double), regr_intercept(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT regr_slope(t_double, u_double), regr_slope(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
        }

        // non-whole number results
        try (TestTable testTable = createTableWithDoubleAndRealColumns(schemaName + ".test_regr_pushdown",
                ImmutableList.of("1, 2, 1, 2", "100000000.123456, 4, 100000000.123456, 4", "123456789.987654, 8, 123456789.987654, 8"))) {
            assertThat(query("SELECT regr_intercept(t_double, u_double), regr_intercept(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT regr_slope(t_double, u_double), regr_slope(v_real, w_real) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    /**
     * Creates a table with columns {@code t_double double, u_double double, v_real real, w_real real} populated with the provided rows.
     */
    protected TestTable createTableWithDoubleAndRealColumns(String name, List<String> rows)
    {
        return new TestTable(onRemoteDatabase(), name, "(t_double double precision, u_double double precision, v_real real, w_real real)", rows);
    }

    @Test
    public void testLimitPushdown()
    {
        if (!hasBehavior(SUPPORTS_LIMIT_PUSHDOWN)) {
            assertThat(query("SELECT name FROM nation LIMIT 30")).isNotFullyPushedDown(LimitNode.class); // Use high limit for result determinism
            return;
        }

        assertThat(query("SELECT name FROM nation LIMIT 30")).isFullyPushedDown(); // Use high limit for result determinism
        assertThat(query("SELECT name FROM nation LIMIT 3")).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isFullyPushedDown();

        // with filter over varchar column
        PlanMatchPattern filterOverTableScan = node(FilterNode.class, node(TableScanNode.class));
        assertConditionallyPushedDown(
                getSession(),
                "SELECT name FROM nation WHERE name < 'EEE' LIMIT 5",
                hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY),
                filterOverTableScan);

        // with aggregation
        PlanMatchPattern aggregationOverTableScan = node(AggregationNode.class, anyTree(node(TableScanNode.class)));
        assertConditionallyPushedDown(
                getSession(),
                "SELECT max(regionkey) FROM nation LIMIT 5", // global aggregation, LIMIT removed
                hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN),
                aggregationOverTableScan);
        assertConditionallyPushedDown(
                getSession(),
                "SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey LIMIT 5",
                hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN),
                aggregationOverTableScan);

        // distinct limit can be pushed down even without aggregation pushdown
        assertThat(query("SELECT DISTINCT regionkey FROM nation LIMIT 5")).isFullyPushedDown();

        // with aggregation and filter over numeric column
        assertConditionallyPushedDown(
                getSession(),
                "SELECT regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey LIMIT 3",
                hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN),
                aggregationOverTableScan);
        // with aggregation and filter over varchar column
        if (hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY)) {
            assertConditionallyPushedDown(
                    getSession(),
                    "SELECT regionkey, count(*) FROM nation WHERE name < 'EGYPT' GROUP BY regionkey LIMIT 3",
                    hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN),
                    aggregationOverTableScan);
        }

        // with TopN over numeric column
        PlanMatchPattern topnOverTableScan = node(TopNNode.class, anyTree(node(TableScanNode.class)));
        assertConditionallyPushedDown(
                getSession(),
                "SELECT * FROM (SELECT regionkey FROM nation ORDER BY nationkey ASC LIMIT 10) LIMIT 5",
                hasBehavior(SUPPORTS_TOPN_PUSHDOWN),
                topnOverTableScan);
        // with TopN over varchar column
        assertConditionallyPushedDown(
                getSession(),
                "SELECT * FROM (SELECT regionkey FROM nation ORDER BY name ASC LIMIT 10) LIMIT 5",
                hasBehavior(SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR),
                topnOverTableScan);

        // with join
        PlanMatchPattern joinOverTableScans = node(JoinNode.class,
                anyTree(node(TableScanNode.class)),
                anyTree(node(TableScanNode.class)));
        assertConditionallyPushedDown(
                joinPushdownEnabled(getSession()),
                "SELECT n.name, r.name " +
                        "FROM nation n " +
                        "LEFT JOIN region r USING (regionkey) " +
                        "LIMIT 30",
                hasBehavior(SUPPORTS_JOIN_PUSHDOWN),
                joinOverTableScans);
    }

    @Test
    public void testTopNPushdownDisabled()
    {
        Session topNPushdownDisabled = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "topn_pushdown_enabled", "false")
                .build();
        assertThat(query(topNPushdownDisabled, "SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10"))
                .ordered()
                .isNotFullyPushedDown(TopNNode.class);
    }

    @Test
    public void testTopNPushdown()
    {
        if (!hasBehavior(SUPPORTS_TOPN_PUSHDOWN)) {
            assertThat(query("SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10"))
                    .ordered()
                    .isNotFullyPushedDown(TopNNode.class);
            return;
        }

        assertThat(query("SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10"))
                .ordered()
                .isFullyPushedDown();

        assertThat(query("SELECT orderkey FROM orders ORDER BY orderkey DESC LIMIT 10"))
                .ordered()
                .isFullyPushedDown();

        // multiple sort columns with different orders
        assertThat(query("SELECT * FROM orders ORDER BY shippriority DESC, totalprice ASC LIMIT 10"))
                .ordered()
                .isFullyPushedDown();

        // TopN over aggregation column
        if (hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN)) {
            assertThat(query("SELECT sum(totalprice) AS total FROM orders GROUP BY custkey ORDER BY total DESC LIMIT 10"))
                    .ordered()
                    .isFullyPushedDown();
        }

        // TopN over TopN
        assertThat(query("SELECT orderkey, totalprice FROM (SELECT orderkey, totalprice FROM orders ORDER BY 1, 2 LIMIT 10) ORDER BY 2, 1 LIMIT 5"))
                .ordered()
                .isFullyPushedDown();

        assertThat(query("" +
                "SELECT orderkey, totalprice " +
                "FROM (SELECT orderkey, totalprice FROM (SELECT orderkey, totalprice FROM orders ORDER BY 1, 2 LIMIT 10) " +
                "ORDER BY 2, 1 LIMIT 5) ORDER BY 1, 2 LIMIT 3"))
                .ordered()
                .isFullyPushedDown();

        // TopN over limit - use high limit for deterministic result
        assertThat(query("SELECT orderkey, totalprice FROM (SELECT orderkey, totalprice FROM orders LIMIT 15000) ORDER BY totalprice ASC LIMIT 5"))
                .ordered()
                .isFullyPushedDown();

        // TopN over limit with filter
        assertThat(query("" +
                "SELECT orderkey, totalprice " +
                "FROM (SELECT orderkey, totalprice FROM orders WHERE orderdate = DATE '1995-09-16' LIMIT 20) " +
                "ORDER BY totalprice ASC LIMIT 5"))
                .ordered()
                .isFullyPushedDown();

        // TopN over aggregation with filter
        if (hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN)) {
            assertThat(query("" +
                    "SELECT * " +
                    "FROM (SELECT SUM(totalprice) as sum, custkey AS total FROM orders GROUP BY custkey HAVING COUNT(*) > 3) " +
                    "ORDER BY sum DESC LIMIT 10"))
                    .ordered()
                    .isFullyPushedDown();
        }

        // TopN over LEFT join (enforces SINGLE TopN cannot be pushed below OUTER side of join)
        // We expect PARTIAL TopN on the LEFT side of join to be pushed down.
        assertThat(query(
                // Explicitly disable join pushdown. The purpose of the this test is to verify partial TopN gets pushed down when Join is not.
                Session.builder(getSession())
                        .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), JOIN_PUSHDOWN_ENABLED, "false")
                        .build(),
                "SELECT * FROM nation n LEFT JOIN region r ON n.regionkey = r.regionkey " +
                        "ORDER BY n.nationkey LIMIT 3"))
                .ordered()
                .isNotFullyPushedDown(
                        node(TopNNode.class, // FINAL TopN
                                anyTree(node(JoinNode.class,
                                        node(ExchangeNode.class, node(ProjectNode.class, node(TableScanNode.class))), // no PARTIAL TopN
                                        anyTree(node(TableScanNode.class))))));
    }

    @Test
    public void testNullSensitiveTopNPushdown()
    {
        if (!hasBehavior(SUPPORTS_TOPN_PUSHDOWN)) {
            // Covered by testTopNPushdown
            return;
        }

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_null_sensitive_topn_pushdown",
                "(name varchar(10), a bigint)",
                List.of(
                        "'small', 42",
                        "'big', 134134",
                        "'negative', -15",
                        "'null', NULL"))) {
            verify(SortOrder.values().length == 4, "The test needs to be updated when new options are added");
            assertThat(query("SELECT name FROM " + testTable.getName() + " ORDER BY a ASC NULLS FIRST LIMIT 5"))
                    .ordered()
                    .isFullyPushedDown();
            assertThat(query("SELECT name FROM " + testTable.getName() + " ORDER BY a ASC NULLS LAST LIMIT 5"))
                    .ordered()
                    .isFullyPushedDown();
            assertThat(query("SELECT name FROM " + testTable.getName() + " ORDER BY a DESC NULLS FIRST LIMIT 5"))
                    .ordered()
                    .isFullyPushedDown();
            assertThat(query("SELECT name FROM " + testTable.getName() + " ORDER BY a DESC NULLS LAST LIMIT 5"))
                    .ordered()
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testCaseSensitiveTopNPushdown()
    {
        if (!hasBehavior(SUPPORTS_TOPN_PUSHDOWN)) {
            // Covered by testTopNPushdown
            return;
        }

        // topN over varchar/char columns should only be pushed down if the remote systems's sort order matches Trino
        boolean expectTopNPushdown = hasBehavior(SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR);
        PlanMatchPattern topNOverTableScan = node(TopNNode.class, anyTree(node(TableScanNode.class)));

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_case_sensitive_topn_pushdown",
                "(a_string varchar(10), a_char char(10), a_bigint bigint)",
                List.of(
                        "'A', 'A', 1",
                        "'B', 'B', 2",
                        "'a', 'a', 3",
                        "'b', 'b', 4"))) {
            assertConditionallyOrderedPushedDown(
                    getSession(),
                    "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_string ASC LIMIT 2",
                    expectTopNPushdown,
                    topNOverTableScan);
            assertConditionallyOrderedPushedDown(
                    getSession(),
                    "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_string DESC LIMIT 2",
                    expectTopNPushdown,
                    topNOverTableScan);
            assertConditionallyOrderedPushedDown(
                    getSession(),
                    "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_char ASC LIMIT 2",
                    expectTopNPushdown,
                    topNOverTableScan);
            assertConditionallyOrderedPushedDown(
                    getSession(),
                    "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_char DESC LIMIT 2",
                    expectTopNPushdown,
                    topNOverTableScan);

            // multiple sort columns with at-least one case-sensitive column
            assertConditionallyOrderedPushedDown(
                    getSession(),
                    "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_bigint, a_char LIMIT 2",
                    expectTopNPushdown,
                    topNOverTableScan);
            assertConditionallyOrderedPushedDown(
                    getSession(),
                    "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_bigint, a_string DESC LIMIT 2",
                    expectTopNPushdown,
                    topNOverTableScan);
        }
    }

    @Test
    public void testCaseInsensitivePredicate()
    {
        assertThat(query("SELECT address FROM customer WHERE address IN ('01bR7OOM6zPqo29DpAq', 'BJYZYJQk4yD5B', 'a6M1wdC44LW')"))
                .skippingTypesCheck()
                .matches("VALUES 'BJYZYJQk4yD5B', 'a6M1wdC44LW', '01bR7OOM6zPqo29DpAq'");

        assertThat(query("SELECT COUNT(*) FROM customer WHERE address >= '01bR7OOM6zPqo29DpAq' AND address <= 'a6M1wdC44LW'"))
                .matches("VALUES BIGINT '858'");

        assertThat(query(
                // Force conversion to a range predicate which would exclude the row corresponding to 'BJYZYJQk4yD5B'
                // if the range predicate were pushed into a case insensitive connector
                Session.builder(getSession())
                        .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "domain_compaction_threshold", "1")
                        .build(),
                "SELECT address FROM customer WHERE address IN ('01bR7OOM6zPqo29DpAq', 'BJYZYJQk4yD5B', 'a6M1wdC44LW')"))
                .skippingTypesCheck()
                .matches("VALUES 'BJYZYJQk4yD5B', 'a6M1wdC44LW', '01bR7OOM6zPqo29DpAq'");
    }

    @Test
    public void testJoinPushdownDisabled()
    {
        Session noJoinPushdown = Session.builder(getSession())
                // Explicitly disable join pushdown
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), JOIN_PUSHDOWN_ENABLED, "false")
                // Disable dynamic filtering so that expected plans in case of no pushdown remain "simple"
                .setSystemProperty("enable_dynamic_filtering", "false")
                // Disable optimized hash generation so that expected plans in case of no pushdown remain "simple"
                .setSystemProperty("optimize_hash_generation", "false")
                .build();

        PlanMatchPattern partitionedJoinOverTableScans = node(JoinNode.class,
                exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.REPARTITION,
                        node(TableScanNode.class)),
                exchange(ExchangeNode.Scope.LOCAL,
                        exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.REPARTITION,
                                node(TableScanNode.class))));

        assertThat(query(noJoinPushdown, "SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey"))
                .isNotFullyPushedDown(partitionedJoinOverTableScans);
    }

    /**
     * Verify !SUPPORTS_JOIN_PUSHDOWN declaration is true.
     */
    @Test
    public void verifySupportsJoinPushdownDeclaration()
    {
        if (hasBehavior(SUPPORTS_JOIN_PUSHDOWN)) {
            // Covered by testJoinPushdown
            return;
        }

        assertThat(query(joinPushdownEnabled(getSession()), "SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey"))
                .isNotFullyPushedDown(
                        node(JoinNode.class,
                                anyTree(node(TableScanNode.class)),
                                anyTree(node(TableScanNode.class))));
    }

    @Test
    public void testJoinPushdown()
    {
        PlanMatchPattern joinOverTableScans =
                node(JoinNode.class,
                        anyTree(node(TableScanNode.class)),
                        anyTree(node(TableScanNode.class)));

        PlanMatchPattern broadcastJoinOverTableScans =
                node(JoinNode.class,
                        node(TableScanNode.class),
                        exchange(ExchangeNode.Scope.LOCAL,
                                exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.REPLICATE,
                                        node(TableScanNode.class))));

        Session session = joinPushdownEnabled(getSession());

        if (!hasBehavior(SUPPORTS_JOIN_PUSHDOWN)) {
            assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey"))
                    .isNotFullyPushedDown(joinOverTableScans);
            return;
        }

        // Disable DF here for the sake of negative test cases' expected plan. With DF enabled, some operators return in DF's FilterNode and some do not.
        Session withoutDynamicFiltering = Session.builder(session)
                .setSystemProperty("enable_dynamic_filtering", "false")
                .build();

        String notDistinctOperator = "IS NOT DISTINCT FROM";
        List<String> nonEqualities = Stream.concat(
                        Stream.of(JoinCondition.Operator.values())
                                .filter(operator -> operator != JoinCondition.Operator.EQUAL)
                                .map(JoinCondition.Operator::getValue),
                        Stream.of(notDistinctOperator))
                .collect(toImmutableList());

        try (TestTable nationLowercaseTable = new TestTable(
                // If a connector supports Join pushdown, but does not allow CTAS, we need to make the table creation here overridable.
                getQueryRunner()::execute,
                "nation_lowercase",
                "AS SELECT nationkey, lower(name) name, regionkey FROM nation")) {
            // basic case
            assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey")).isFullyPushedDown();

            // join over different columns
            assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.nationkey = r.regionkey")).isFullyPushedDown();

            // pushdown when using USING
            assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r USING(regionkey)")).isFullyPushedDown();

            // varchar equality predicate
            assertConditionallyPushedDown(
                    session,
                    "SELECT n.name, n2.regionkey FROM nation n JOIN nation n2 ON n.name = n2.name",
                    hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY),
                    joinOverTableScans);
            assertConditionallyPushedDown(
                    session,
                    format("SELECT n.name, nl.regionkey FROM nation n JOIN %s nl ON n.name = nl.name", nationLowercaseTable.getName()),
                    hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY),
                    joinOverTableScans);

            // multiple bigint predicates
            assertThat(query(session, "SELECT n.name, c.name FROM nation n JOIN customer c ON n.nationkey = c.nationkey and n.regionkey = c.custkey"))
                    .isFullyPushedDown();

            // inequality
            for (String operator : nonEqualities) {
                // bigint inequality predicate
                assertThat(query(withoutDynamicFiltering, format("SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey %s r.regionkey", operator)))
                        // Currently no pushdown as inequality predicate is removed from Join to maintain Cross Join and Filter as separate nodes
                        .isNotFullyPushedDown(broadcastJoinOverTableScans);

                // varchar inequality predicate
                assertThat(query(withoutDynamicFiltering, format("SELECT n.name, nl.name FROM nation n JOIN %s nl ON n.name %s nl.name", nationLowercaseTable.getName(), operator)))
                        // Currently no pushdown as inequality predicate is removed from Join to maintain Cross Join and Filter as separate nodes
                        .isNotFullyPushedDown(broadcastJoinOverTableScans);
            }

            // inequality along with an equality, which constitutes an equi-condition and allows filter to remain as part of the Join
            for (String operator : nonEqualities) {
                assertConditionallyPushedDown(
                        session,
                        format("SELECT n.name, c.name FROM nation n JOIN customer c ON n.nationkey = c.nationkey AND n.regionkey %s c.custkey", operator),
                        expectJoinPushdown(operator),
                        joinOverTableScans);
            }

            // varchar inequality along with an equality, which constitutes an equi-condition and allows filter to remain as part of the Join
            for (String operator : nonEqualities) {
                assertConditionallyPushedDown(
                        session,
                        format("SELECT n.name, nl.name FROM nation n JOIN %s nl ON n.regionkey = nl.regionkey AND n.name %s nl.name", nationLowercaseTable.getName(), operator),
                        expectVarcharJoinPushdown(operator),
                        joinOverTableScans);
            }

            // LEFT JOIN
            assertThat(query(session, "SELECT r.name, n.name FROM nation n LEFT JOIN region r ON n.nationkey = r.regionkey")).isFullyPushedDown();
            assertThat(query(session, "SELECT r.name, n.name FROM region r LEFT JOIN nation n ON n.nationkey = r.regionkey")).isFullyPushedDown();

            // RIGHT JOIN
            assertThat(query(session, "SELECT r.name, n.name FROM nation n RIGHT JOIN region r ON n.nationkey = r.regionkey")).isFullyPushedDown();
            assertThat(query(session, "SELECT r.name, n.name FROM region r RIGHT JOIN nation n ON n.nationkey = r.regionkey")).isFullyPushedDown();

            // FULL JOIN
            assertConditionallyPushedDown(
                    session,
                    "SELECT r.name, n.name FROM nation n FULL JOIN region r ON n.nationkey = r.regionkey",
                    hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN),
                    joinOverTableScans);

            // Join over a (double) predicate
            assertThat(query(session, "" +
                    "SELECT c.name, n.name " +
                    "FROM (SELECT * FROM customer WHERE acctbal > 8000) c " +
                    "JOIN nation n ON c.custkey = n.nationkey"))
                    .isFullyPushedDown();

            // Join over a varchar equality predicate
            assertConditionallyPushedDown(
                    session,
                    "SELECT c.name, n.name FROM (SELECT * FROM customer WHERE address = 'TcGe5gaZNgVePxU5kRrvXBfkasDTea') c " +
                            "JOIN nation n ON c.custkey = n.nationkey",
                    hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY),
                    joinOverTableScans);

            // Join over a varchar inequality predicate
            assertConditionallyPushedDown(
                    session,
                    "SELECT c.name, n.name FROM (SELECT * FROM customer WHERE address < 'TcGe5gaZNgVePxU5kRrvXBfkasDTea') c " +
                            "JOIN nation n ON c.custkey = n.nationkey",
                    hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY),
                    joinOverTableScans);

            // join over aggregation
            assertConditionallyPushedDown(
                    session,
                    "SELECT * FROM (SELECT regionkey rk, count(nationkey) c FROM nation GROUP BY regionkey) n " +
                            "JOIN region r ON n.rk = r.regionkey",
                    hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN),
                    joinOverTableScans);

            // join over LIMIT
            assertConditionallyPushedDown(
                    session,
                    "SELECT * FROM (SELECT nationkey FROM nation LIMIT 30) n " +
                            "JOIN region r ON n.nationkey = r.regionkey",
                    hasBehavior(SUPPORTS_LIMIT_PUSHDOWN),
                    joinOverTableScans);

            // join over TopN
            assertConditionallyPushedDown(
                    session,
                    "SELECT * FROM (SELECT nationkey FROM nation ORDER BY regionkey LIMIT 5) n " +
                            "JOIN region r ON n.nationkey = r.regionkey",
                    hasBehavior(SUPPORTS_TOPN_PUSHDOWN),
                    joinOverTableScans);

            // join over join
            assertThat(query(session, "SELECT * FROM nation n, region r, customer c WHERE n.regionkey = r.regionkey AND r.regionkey = c.custkey"))
                    .isFullyPushedDown();
        }
    }

    protected QueryAssert assertConditionallyPushedDown(
            Session session,
            @Language("SQL") String query,
            boolean condition,
            PlanMatchPattern otherwiseExpected)
    {
        QueryAssert queryAssert = assertThat(query(session, query));
        if (condition) {
            return queryAssert.isFullyPushedDown();
        }
        else {
            return queryAssert.isNotFullyPushedDown(otherwiseExpected);
        }
    }

    protected void assertConditionallyOrderedPushedDown(
            Session session,
            @Language("SQL") String query,
            boolean condition,
            PlanMatchPattern otherwiseExpected)
    {
        QueryAssert queryAssert = assertThat(query(session, query)).ordered();
        if (condition) {
            queryAssert.isFullyPushedDown();
        }
        else {
            queryAssert.isNotFullyPushedDown(otherwiseExpected);
        }
    }

    protected boolean expectJoinPushdown(String operator)
    {
        if ("IS NOT DISTINCT FROM".equals(operator)) {
            // TODO (https://github.com/trinodb/trino/issues/6967) support join pushdown for IS NOT DISTINCT FROM
            return false;
        }
        switch (toJoinConditionOperator(operator)) {
            case EQUAL:
            case NOT_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return true;
            case IS_DISTINCT_FROM:
                return hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM);
        }
        throw new AssertionError(); // unreachable
    }

    private boolean expectVarcharJoinPushdown(String operator)
    {
        if ("IS NOT DISTINCT FROM".equals(operator)) {
            // TODO (https://github.com/trinodb/trino/issues/6967) support join pushdown for IS NOT DISTINCT FROM
            return false;
        }
        switch (toJoinConditionOperator(operator)) {
            case EQUAL:
            case NOT_EQUAL:
                return hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY);
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY);
            case IS_DISTINCT_FROM:
                return hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM) && hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY);
        }
        throw new AssertionError(); // unreachable
    }

    private JoinCondition.Operator toJoinConditionOperator(String operator)
    {
        return Stream.of(JoinCondition.Operator.values())
                .filter(joinOperator -> joinOperator.getValue().equals(operator))
                .collect(toOptional())
                .orElseThrow(() -> new IllegalArgumentException("Not found: " + operator));
    }

    protected Session joinPushdownEnabled(Session session)
    {
        // If join pushdown gets enabled by default, tests should use default session
        verify(!new JdbcMetadataConfig().isJoinPushdownEnabled());
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_enabled", "true")
                .build();
    }

    @Test(timeOut = 60_000)
    public void testCancellation()
            throws Exception
    {
        if (!hasBehavior(SUPPORTS_CANCELLATION)) {
            throw new SkipException("Cancellation is not supported by given connector");
        }

        try (TestView sleepingView = createSleepingView(new Duration(1, MINUTES))) {
            String query = "SELECT * FROM " + sleepingView.getName();
            Future<?> future = executor.submit(() -> assertQueryFails(query, "Query killed. Message: Killed by test"));
            QueryId queryId = getQueryId(query);

            assertEventually(() -> assertRemoteQueryStatus(sleepingView.getName(), RUNNING));
            assertUpdate(format("CALL system.runtime.kill_query(query_id => '%s', message => '%s')", queryId, "Killed by test"));
            future.get();
            assertEventually(() -> assertRemoteQueryStatus(sleepingView.getName(), CANCELLED));
        }
    }

    private void assertRemoteQueryStatus(String tableNameToScan, RemoteDatabaseEvent.Status status)
    {
        String lowerCasedTableName = tableNameToScan.toLowerCase(ENGLISH);
        assertThat(getRemoteDatabaseEvents())
                .filteredOn(event -> event.getQuery().toLowerCase(ENGLISH).contains(lowerCasedTableName))
                .map(RemoteDatabaseEvent::getStatus)
                .contains(status);
    }

    private QueryId getQueryId(String query)
            throws Exception
    {
        for (int i = 0; i < 100; i++) {
            MaterializedResult queriesResult = getQueryRunner().execute(format(
                    "SELECT query_id FROM system.runtime.queries WHERE query = '%s' AND query NOT LIKE '%%system.runtime.queries%%'",
                    query));
            int rowCount = queriesResult.getRowCount();
            if (rowCount == 0) {
                Thread.sleep(100);
                continue;
            }
            checkState(rowCount == 1, "Too many (%s) query ids were found for: %s", rowCount, query);
            return new QueryId((String) queriesResult.getOnlyValue());
        }
        throw new IllegalStateException("Query id not found for: " + query);
    }

    protected List<RemoteDatabaseEvent> getRemoteDatabaseEvents()
    {
        throw new UnsupportedOperationException();
    }

    protected TestView createSleepingView(Duration minimalSleepDuration)
    {
        throw new UnsupportedOperationException();
    }

    @Test
    public void testDeleteWithBigintEqualityPredicate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_ROW_LEVEL_DELETE));
        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_bigint", "AS SELECT * FROM region")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 1", 1);
            assertQuery(
                    "SELECT regionkey, name FROM " + table.getName(),
                    "VALUES "
                            + "(0, 'AFRICA'),"
                            + "(2, 'ASIA'),"
                            + "(3, 'EUROPE'),"
                            + "(4, 'MIDDLE EAST')");
        }
    }

    @Test
    public void testDeleteWithVarcharEqualityPredicate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_ROW_LEVEL_DELETE));
        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_varchar", "(col varchar(1))", ImmutableList.of("'a'", "'A'", "null"))) {
            if (!hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY)) {
                assertQueryFails("DELETE FROM " + table.getName() + " WHERE col = 'A'", "Unsupported delete");
                return;
            }

            assertUpdate("DELETE FROM " + table.getName() + " WHERE col = 'A'", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 'a', null");
        }
    }

    @Test
    public void testDeleteWithVarcharInequalityPredicate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_ROW_LEVEL_DELETE));
        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_varchar", "(col varchar(1))", ImmutableList.of("'a'", "'A'", "null"))) {
            if (!hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY)) {
                assertQueryFails("DELETE FROM " + table.getName() + " WHERE col != 'A'", "Unsupported delete");
                return;
            }

            assertUpdate("DELETE FROM " + table.getName() + " WHERE col != 'A'", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 'A', null");
        }
    }

    @Test
    public void testDeleteWithVarcharGreaterAndLowerPredicate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_ROW_LEVEL_DELETE));
        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_varchar", "(col varchar(1))", ImmutableList.of("'0'", "'a'", "'A'", "'b'", "null"))) {
            if (!hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY)) {
                assertQueryFails("DELETE FROM " + table.getName() + " WHERE col < 'A'", "Unsupported delete");
                assertQueryFails("DELETE FROM " + table.getName() + " WHERE col > 'A'", "Unsupported delete");
                return;
            }

            assertUpdate("DELETE FROM " + table.getName() + " WHERE col < 'A'", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 'a', 'A', 'b', null");
            assertUpdate("DELETE FROM " + table.getName() + " WHERE col > 'A'", 2);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 'A', null");
        }
    }

    @Override
    public void testDeleteWithComplexPredicate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_ROW_LEVEL_DELETE));
        assertThatThrownBy(super::testDeleteWithComplexPredicate)
                .hasStackTraceContaining("TrinoException: Unsupported delete");
    }

    @Override
    public void testDeleteWithSubquery()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_ROW_LEVEL_DELETE));
        assertThatThrownBy(super::testDeleteWithSubquery)
                .hasStackTraceContaining("TrinoException: Unsupported delete");
    }

    @Override
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_ROW_LEVEL_DELETE));
        assertThatThrownBy(super::testExplainAnalyzeWithDeleteWithSubquery)
                .hasStackTraceContaining("TrinoException: Unsupported delete");
    }

    @Override
    public void testDeleteWithSemiJoin()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_ROW_LEVEL_DELETE));
        assertThatThrownBy(super::testDeleteWithSemiJoin)
                .hasStackTraceContaining("TrinoException: Unsupported delete");
    }

    @Override
    public void testDeleteWithVarcharPredicate()
    {
        throw new SkipException("This is implemented by testDeleteWithVarcharEqualityPredicate");
    }

    @Test
    public void testInsertWithoutTemporaryTable()
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new SkipException("CREATE TABLE is required for testing non-transactional write support");
        }
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "non_transactional_insert", "false")
                .build();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_bypass_temp",
                "(a varchar(36), b bigint)")) {
            String values = String.join(",", buildRowsForInsert(5000));
            assertUpdate(session, "INSERT INTO " + table.getName() + " (a, b) VALUES " + values, 5000);
            assertQuery("SELECT COUNT(*) FROM " + table.getName(), format("VALUES %d", 5000));
        }
    }

    @Test(dataProvider = "batchSizeAndTotalNumberOfRowsToInsertDataProvider")
    public void testWriteBatchSizeSessionProperty(Integer batchSize, Integer numberOfRows)
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new SkipException("CREATE TABLE is required for write_batch_size test but is not supported");
        }
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "write_batch_size", batchSize.toString())
                .build();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "write_batch_size",
                "(a varchar(36), b bigint)")) {
            String values = String.join(",", buildRowsForInsert(numberOfRows));
            assertUpdate(session, "INSERT INTO " + table.getName() + " (a, b) VALUES " + values, numberOfRows);
            assertQuery("SELECT COUNT(*) FROM " + table.getName(), format("VALUES %d", numberOfRows));
        }
    }

    private static List<String> buildRowsForInsert(int numberOfRows)
    {
        List<String> result = new ArrayList<>(numberOfRows);
        for (int i = 0; i < numberOfRows; i++) {
            result.add(format("('%s', %d)", UUID.randomUUID(), ThreadLocalRandom.current().nextLong()));
        }
        return result;
    }

    @DataProvider
    public static Object[][] batchSizeAndTotalNumberOfRowsToInsertDataProvider()
    {
        return new Object[][] {
                {10, 8},  // number of rows < batch size
                {10, 10}, // number of rows = batch size
                {10, 11}, // number of rows > batch size
                {10, 50}, // number of rows = n * batch size
                {10, 52}, // number of rows > n * batch size
        };
    }
}
