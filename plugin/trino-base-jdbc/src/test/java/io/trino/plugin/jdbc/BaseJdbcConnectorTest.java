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
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.query.QueryAssertions.QueryAssert;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.JOIN_PUSHDOWN_ENABLED;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.CANCELLED;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.RUNNING;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CANCELLATION;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_LIMIT_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseJdbcConnectorTest
        extends BaseConnectorTest
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getName()));

    @AfterClass(alwaysRun = true)
    public void afterClass()
    {
        executor.shutdownNow();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_CREATE_VIEW:
                // Not supported by JdbcMetadata
                return false;

            case SUPPORTS_DELETE:
                // Not supported by JdbcMetadata
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
                "SELECT regionkey, max(name) FROM nation GROUP BY regionkey LIMIT 5",
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
                exchange(ExchangeNode.Scope.LOCAL, ExchangeNode.Type.REPARTITION,
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
                        exchange(ExchangeNode.Scope.LOCAL, ExchangeNode.Type.GATHER,
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

    private void assertConditionallyPushedDown(
            Session session,
            @Language("SQL") String query,
            boolean condition,
            PlanMatchPattern otherwiseExpected)
    {
        QueryAssert queryAssert = assertThat(query(session, query));
        if (condition) {
            queryAssert.isFullyPushedDown();
        }
        else {
            queryAssert.isNotFullyPushedDown(otherwiseExpected);
        }
    }

    private void assertConditionallyOrderedPushedDown(
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

    private boolean expectJoinPushdown(String operator)
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
}
