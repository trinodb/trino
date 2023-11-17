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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.SMALL_DYNAMIC_FILTER_MAX_ROW_COUNT;
import static io.trino.SystemSessionProperties.getSmallDynamicFilterWaitTimeout;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;

public class TestDeterminePreferredDynamicFilterTimeout
        extends BasePlanTest
{
    private long waitForCascadingDynamicFiltersTimeout;

    public TestDeterminePreferredDynamicFilterTimeout()
    {
        super(ImmutableMap.of(
                ENABLE_DYNAMIC_FILTERING, "true",
                JOIN_REORDERING_STRATEGY, NONE.name(),
                JOIN_DISTRIBUTION_TYPE, BROADCAST.name()));
    }

    @BeforeAll
    public void setup()
    {
        waitForCascadingDynamicFiltersTimeout = getSmallDynamicFilterWaitTimeout(getQueryRunner().getDefaultSession()).toMillis();
    }

    @Test
    public void testNoTimeoutOnTableExceedingSize()
    {
        assertPlan(
                "SELECT part.partkey from (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) " +
                        "JOIN part ON lineitem.orderkey = part.partkey",
                Session.builder(getQueryRunner().getDefaultSession())
                        .setSystemProperty(SMALL_DYNAMIC_FILTER_MAX_ROW_COUNT, "500")
                        .build(),
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("LINEITEM_OK", "PART_PK")
                                .dynamicFilter(
                                        ImmutableList.of(
                                                new PlanMatchPattern.DynamicFilterPattern("LINEITEM_OK", EQUAL, "PART_PK", false),
                                                new PlanMatchPattern.DynamicFilterPattern("ORDERS_OK", EQUAL, "PART_PK", false)))
                                .left(
                                        join(INNER, leftJoinBuilder -> leftJoinBuilder
                                                .equiCriteria("LINEITEM_OK", "ORDERS_OK")
                                                .dynamicFilter("LINEITEM_OK", "ORDERS_OK")
                                                .left(
                                                        node(FilterNode.class,
                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))
                                                .right(
                                                        anyTree(node(FilterNode.class,
                                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))))))
                                .right(
                                        exchange(
                                                tableScan("part", ImmutableMap.of("PART_PK", "partkey")))))));
    }

    @Test
    public void testDependantDynamicFilterTable()
    {
        assertPlan(
                "SELECT part.partkey from (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) " +
                        "JOIN part ON lineitem.orderkey = part.partkey",
                Session.builder(getQueryRunner().getDefaultSession())
                        .setSystemProperty(SMALL_DYNAMIC_FILTER_MAX_ROW_COUNT, "5000")
                        .build(),
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("LINEITEM_OK", "PART_PK")
                                .dynamicFilter(
                                        ImmutableList.of(
                                                new PlanMatchPattern.DynamicFilterPattern("LINEITEM_OK", EQUAL, "PART_PK", false, Optional.of(waitForCascadingDynamicFiltersTimeout)),
                                                new PlanMatchPattern.DynamicFilterPattern("ORDERS_OK", EQUAL, "PART_PK", false, Optional.of(waitForCascadingDynamicFiltersTimeout))))
                                .left(
                                        join(INNER, leftJoinBuilder -> leftJoinBuilder
                                                .equiCriteria("LINEITEM_OK", "ORDERS_OK")
                                                .dynamicFilter("LINEITEM_OK", "ORDERS_OK")
                                                .left(
                                                        node(FilterNode.class,
                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))
                                                .right(
                                                        anyTree(node(FilterNode.class,
                                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))))))
                                .right(
                                        exchange(
                                                tableScan("part", ImmutableMap.of("PART_PK", "partkey")))))));
    }

    @Test
    public void testAddPreferredTimeoutWithExpandingNode()
    {
        assertPlan(
                "SELECT l.suppkey FROM lineitem l JOIN (SELECT suppkey FROM supplier UNION ALL SELECT suppkey FROM supplier) s ON l.suppkey = s.suppkey",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("LINEITEM_SK", "SUPPLIER_SK")
                                .dynamicFilter("LINEITEM_SK", "SUPPLIER_SK", waitForCascadingDynamicFiltersTimeout)
                                .left(
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_SK", "suppkey"))))
                                .right(
                                        exchange(
                                                LOCAL,
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(),
                                                ImmutableSet.of(),
                                                Optional.empty(),
                                                ImmutableList.of("SUPPLIER_SK"),
                                                Optional.empty(),
                                                tableScan("supplier", ImmutableMap.of("SUPPLIER_SK_1", "suppkey")),
                                                tableScan("supplier", ImmutableMap.of("SUPPLIER_SK_2", "suppkey")))))));
    }

    @Test
    public void testNotAddingPreferredTimeoutWithMultiplyingNode()
    {
        assertPlan("""
                        SELECT o.orderkey, o.custkey FROM orders o,lineitem l
                        JOIN partsupp s ON l.partkey = s.partkey AND s.suppkey = l.suppkey
                        WHERE o.orderkey BETWEEN l.orderkey AND l.partkey
                        """,
                anyTree(filter("O_ORDERKEY BETWEEN L_ORDERKEY AND L_PARTKEY",
                        join(INNER, builder -> builder
                                .dynamicFilter(
                                        ImmutableList.of(
                                                new PlanMatchPattern.DynamicFilterPattern("O_ORDERKEY", GREATER_THAN_OR_EQUAL, "L_ORDERKEY", false, Optional.empty()),
                                                new PlanMatchPattern.DynamicFilterPattern("O_ORDERKEY", LESS_THAN_OR_EQUAL, "L_PARTKEY", false, Optional.empty())))
                                .left(
                                        filter(
                                                TRUE_LITERAL,
                                                tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey", "O_CUSTKEY", "custkey"))))
                                .right(
                                        exchange(
                                                LOCAL,
                                                join(INNER, innerJoinBuilder -> innerJoinBuilder
                                                        .equiCriteria(ImmutableList.of(
                                                                equiJoinClause("L_PARTKEY", "P_PARTKEY"),
                                                                equiJoinClause("L_SUPPKEY", "P_SUPPKEY")))
                                                        .dynamicFilter(
                                                                ImmutableList.of(
                                                                        new PlanMatchPattern.DynamicFilterPattern("L_PARTKEY", EQUAL, "P_PARTKEY", false, Optional.of(waitForCascadingDynamicFiltersTimeout)),
                                                                        new PlanMatchPattern.DynamicFilterPattern("L_SUPPKEY", EQUAL, "P_SUPPKEY", false, Optional.of(waitForCascadingDynamicFiltersTimeout))))
                                                        .left(
                                                                anyTree(
                                                                        tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey", "L_PARTKEY", "partkey", "L_SUPPKEY", "suppkey"))))
                                                        .right(
                                                                exchange(
                                                                        LOCAL,
                                                                        tableScan("partsupp", ImmutableMap.of("P_SUPPKEY", "suppkey", "P_PARTKEY", "partkey")))))))))));
    }

    @Test
    public void testSemiJoinWithDynamicFilter()
    {
        assertPlan("SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey = random(5))",
                Session.builder(getQueryRunner().getDefaultSession())
                        .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "false")
                        .build(),
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT", true,
                                filter(TRUE_LITERAL,
                                        tableScan("lineitem", ImmutableMap.of("LINE_ORDER_KEY", "orderkey")))
                                        .with(FilterNode.class, filterNode -> extractDynamicFilters(filterNode.getPredicate())
                                                .getDynamicConjuncts().get(0).getPreferredTimeout()
                                                .equals(Optional.of(waitForCascadingDynamicFiltersTimeout))),
                                node(ExchangeNode.class,
                                        filter("ORDERS_ORDER_KEY = CAST(random(5) AS bigint)",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }
}
