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
package io.trino.plugin.mariadb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.connector.JoinCondition;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.BeforeClass;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.mariadb.MariaDbQueryRunner.createMariaDbQueryRunner;
import static io.trino.plugin.mariadb.TestingMariaDbServer.LATEST_VERSION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMariaDbLatestConnectorTest
        extends BaseMariaDbConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new TestingMariaDbServer(LATEST_VERSION, null));
        return createMariaDbQueryRunner(server, ImmutableMap.of(), ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return server::execute;
    }

    @BeforeClass
    public void setUpTables()
    {
        server.execute("ANALYZE TABLE tpch.nation PERSISTENT FOR ALL;");
        server.execute("ANALYZE TABLE tpch.region PERSISTENT FOR ALL;");
        server.execute("ANALYZE TABLE tpch.orders PERSISTENT FOR ALL;");
        server.execute("ANALYZE TABLE tpch.customer PERSISTENT FOR ALL;");
    }

    @Override
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
            assertThat(query(session, "SELECT c.custkey, o.orderkey FROM customer c JOIN orders o ON c.custkey = o.custkey")).isFullyPushedDown();

            // join over different columns
            assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.nationkey = r.regionkey")).isFullyPushedDown();

            // pushdown when using USING
            assertThat(query(session, "SELECT o.orderkey FROM customer c JOIN orders o USING(custkey)")).isFullyPushedDown();

            // varchar equality predicate
            assertConditionallyPushedDown(
                    session,
                    "SELECT n.name, n2.regionkey FROM nation n JOIN nation n2 ON n.name = n2.name",
                    false,
                    joinOverTableScans);
            assertConditionallyPushedDown(
                    session,
                    format("SELECT n.name, nl.regionkey FROM nation n JOIN %s nl ON n.name = nl.name", nationLowercaseTable.getName()),
                    false,
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
                        format("SELECT n.nationkey FROM nation n JOIN customer c ON n.nationkey = c.nationkey AND n.regionkey %s c.custkey", operator),
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
            assertThat(query(session, "SELECT c.custkey, o.orderkey FROM customer c LEFT JOIN orders o ON c.custkey = o.custkey")).isFullyPushedDown();
            assertThat(query(session, "SELECT c.custkey, o.orderkey FROM orders o LEFT JOIN customer c ON c.custkey = o.custkey")).isFullyPushedDown();

            // RIGHT JOIN
            assertThat(query(session, "SELECT c.custkey, o.orderkey FROM customer c RIGHT JOIN orders o ON c.custkey = o.custkey")).isFullyPushedDown();
            assertThat(query(session, "SELECT c.custkey, o.orderkey FROM orders o RIGHT JOIN customer c ON c.custkey = o.custkey")).isFullyPushedDown();

            // FULL JOIN
            assertConditionallyPushedDown(
                    session,
                    "SELECT r.name, n.name FROM nation n FULL JOIN region r ON n.nationkey = r.regionkey",
                    false,
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
                    false,
                    joinOverTableScans);

            // Join over a varchar inequality predicate
            assertConditionallyPushedDown(
                    session,
                    "SELECT c.name, n.name FROM (SELECT * FROM customer WHERE address < 'TcGe5gaZNgVePxU5kRrvXBfkasDTea') c " +
                            "JOIN nation n ON c.custkey = n.nationkey",
                    false,
                    joinOverTableScans);

            // join over aggregation
            assertConditionallyPushedDown(
                    session,
                    "SELECT * FROM (SELECT regionkey rk, count(nationkey) c FROM nation GROUP BY regionkey) n " +
                            "JOIN region r ON n.rk = r.regionkey",
                    true,
                    joinOverTableScans);

            // join over LIMIT
            assertConditionallyPushedDown(
                    session,
                    "SELECT * FROM (SELECT nationkey FROM nation LIMIT 30) n " +
                            "JOIN region r ON n.nationkey = r.regionkey",
                    true,
                    joinOverTableScans);

            // join over TopN
            assertConditionallyPushedDown(
                    session,
                    "SELECT * FROM (SELECT nationkey FROM nation ORDER BY regionkey LIMIT 5) n " +
                            "JOIN region r ON n.nationkey = r.regionkey",
                    true,
                    joinOverTableScans);

            // join over join
            assertThat(query(session, "SELECT c.custkey, o.orderkey FROM customer c, orders o, nation n WHERE c.custkey = o.custkey AND c.nationkey = n.nationkey"))
                    .isFullyPushedDown();
        }
    }

    @Override
    public void testAggregationPushdown()
    {
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
                false,
                node(FilterNode.class, node(TableScanNode.class)));
        // GROUP BY above WHERE and LIMIT
        assertConditionallyPushedDown(
                getSession(),
                "SELECT regionkey, sum(nationkey) FROM (SELECT * FROM nation WHERE regionkey < 2 LIMIT 11) GROUP BY regionkey",
                true,
                node(LimitNode.class, anyTree(node(TableScanNode.class))));
        // GROUP BY above TopN
        assertConditionallyPushedDown(
                getSession(),
                "SELECT custkey, sum(totalprice) FROM (SELECT custkey, totalprice FROM orders ORDER BY orderdate ASC, totalprice ASC LIMIT 10) GROUP BY custkey",
                true,
                node(TopNNode.class, anyTree(node(TableScanNode.class))));
        // GROUP BY with JOIN
        assertConditionallyPushedDown(
                joinPushdownEnabled(getSession()),
                "SELECT n.regionkey, sum(c.acctbal) acctbals FROM nation n LEFT JOIN customer c USING (nationkey) GROUP BY 1",
                true,
                node(JoinNode.class, anyTree(node(TableScanNode.class)), anyTree(node(TableScanNode.class))));
        // GROUP BY with WHERE on neither grouping nor aggregation column
        assertConditionallyPushedDown(
                getSession(),
                "SELECT nationkey, min(regionkey) FROM nation WHERE name = 'ARGENTINA' GROUP BY nationkey",
                false,
                node(FilterNode.class, node(TableScanNode.class)));
        // GROUP BY with WHERE complex predicate
        assertConditionallyPushedDown(
                getSession(),
                "SELECT regionkey, sum(nationkey) FROM nation WHERE name LIKE '%N%' GROUP BY regionkey",
                false,
                node(FilterNode.class, node(TableScanNode.class)));
        // aggregation on varchar column
        assertThat(query("SELECT count(name) FROM nation")).isFullyPushedDown();
        // aggregation on varchar column with GROUPING
        assertThat(query("SELECT nationkey, count(name) FROM nation GROUP BY nationkey")).isFullyPushedDown();
        // aggregation on varchar column with WHERE
        assertConditionallyPushedDown(
                getSession(),
                "SELECT count(name) FROM nation WHERE name = 'ARGENTINA'",
                false,
                node(FilterNode.class, node(TableScanNode.class)));

        // pruned away aggregation
        assertThat(query("SELECT -13 FROM (SELECT count(*) FROM nation)"))
                .matches("VALUES -13")
                .hasPlan(node(OutputNode.class, node(ValuesNode.class)));
        // aggregation over aggregation
        assertThat(query("SELECT count(*) FROM (SELECT count(*) FROM nation)"))
                .matches("VALUES BIGINT '1'")
                .hasPlan(node(OutputNode.class, node(ValuesNode.class)));
        assertThat(query("SELECT count(*) FROM (SELECT count(*) FROM nation GROUP BY regionkey)"))
                .matches("VALUES BIGINT '5'")
                .isFullyPushedDown();

        // aggregation with UNION ALL and aggregation
        assertThat(query("SELECT count(*) FROM (SELECT name FROM nation UNION ALL SELECT name FROM region)"))
                .matches("VALUES BIGINT '30'")
                // TODO (https://github.com/trinodb/trino/issues/12547): support count(*) over UNION ALL pushdown
                .isNotFullyPushedDown(
                        node(ExchangeNode.class,
                                node(AggregationNode.class, node(TableScanNode.class)),
                                node(AggregationNode.class, node(TableScanNode.class))));

        // aggregation with UNION ALL and aggregation
        assertThat(query("SELECT count(*) FROM (SELECT count(*) FROM nation UNION ALL SELECT count(*) FROM region)"))
                .matches("VALUES BIGINT '2'")
                .hasPlan(
                        // Note: engine could fold this to single ValuesNode
                        node(OutputNode.class,
                                node(AggregationNode.class,
                                        node(ExchangeNode.class,
                                                node(ExchangeNode.class,
                                                        node(AggregationNode.class, node(ValuesNode.class)),
                                                        node(AggregationNode.class, node(ValuesNode.class)))))));
    }

    @Override
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isFullyPushedDown(); // Use high limit for result determinism
        assertThat(query("SELECT name FROM nation LIMIT 3")).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isFullyPushedDown();

        // with filter over varchar column
        PlanMatchPattern filterOverTableScan = node(FilterNode.class, node(TableScanNode.class));
        assertConditionallyPushedDown(
                getSession(),
                "SELECT name FROM nation WHERE name < 'EEE' LIMIT 5",
                false,
                filterOverTableScan);

        // with aggregation
        PlanMatchPattern aggregationOverTableScan = node(AggregationNode.class, anyTree(node(TableScanNode.class)));
        assertConditionallyPushedDown(
                getSession(),
                "SELECT max(regionkey) FROM nation LIMIT 5", // global aggregation, LIMIT removed
                true,
                aggregationOverTableScan);
        assertConditionallyPushedDown(
                getSession(),
                "SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey LIMIT 5",
                true,
                aggregationOverTableScan);

        // distinct limit can be pushed down even without aggregation pushdown
        assertThat(query("SELECT DISTINCT regionkey FROM nation LIMIT 5")).isFullyPushedDown();

        // with aggregation and filter over numeric column
        assertConditionallyPushedDown(
                getSession(),
                "SELECT regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey LIMIT 3",
                true,
                aggregationOverTableScan);
        // with aggregation and filter over varchar column
        if (false) {
            assertConditionallyPushedDown(
                    getSession(),
                    "SELECT regionkey, count(*) FROM nation WHERE name < 'EGYPT' GROUP BY regionkey LIMIT 3",
                    true,
                    aggregationOverTableScan);
        }

        // with TopN over numeric column
        PlanMatchPattern topnOverTableScan = node(TopNNode.class, anyTree(node(TableScanNode.class)));
        assertConditionallyPushedDown(
                getSession(),
                "SELECT * FROM (SELECT regionkey FROM nation ORDER BY nationkey ASC LIMIT 10) LIMIT 5",
                true,
                topnOverTableScan);
        // with TopN over varchar column
        assertConditionallyPushedDown(
                getSession(),
                "SELECT * FROM (SELECT regionkey FROM nation ORDER BY name ASC LIMIT 10) LIMIT 5",
                false,
                topnOverTableScan);

        // with join
        PlanMatchPattern joinOverTableScans = node(JoinNode.class,
                anyTree(node(TableScanNode.class)),
                anyTree(node(TableScanNode.class)));
        assertConditionallyPushedDown(
                joinPushdownEnabled(getSession()),
                //
                "SELECT n.nationkey " +
                        "FROM nation n " +
                        "LEFT JOIN region r USING (regionkey) " +
                        "LIMIT 30",
                true,
                joinOverTableScans);
    }
}
