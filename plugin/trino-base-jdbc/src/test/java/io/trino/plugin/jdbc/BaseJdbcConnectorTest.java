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
import io.trino.spi.connector.JoinCondition;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.query.QueryAssertions.QueryAssert;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_LIMIT_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseJdbcConnectorTest
        extends BaseConnectorTest
{
    // TODO move common tests from connector-specific classes here

    @Test
    public void testJoinPushdownDisabled()
    {
        // If join pushdown gets enabled by default, this test should use a session with join pushdown disabled
        Session noJoinPushdown = Session.builder(getSession())
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

        if (!hasBehavior(SUPPORTS_JOIN_PUSHDOWN)) {
            assertThat(query("SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey"))
                    .isNotFullyPushedDown(joinOverTableScans);
            return;
        }

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
            assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey")).isFullyPushedDown();

            // join over different columns
            assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.nationkey = r.regionkey")).isFullyPushedDown();

            // pushdown when using USING
            assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r USING(regionkey)")).isFullyPushedDown();

            // varchar equality predicate
            assertConditionallyPushedDown(
                    session,
                    "SELECT n.name, n2.regionkey FROM nation n JOIN nation n2 ON n.name = n2.name",
                    hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY),
                    joinOverTableScans);
            assertConditionallyPushedDown(
                    session,
                    format("SELECT n.name, nl.regionkey FROM nation n JOIN %s nl ON n.name = nl.name", nationLowercaseTable.getName()),
                    hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY),
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
                return hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY);
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY);
            case IS_DISTINCT_FROM:
                return hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM) && hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY);
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
}
