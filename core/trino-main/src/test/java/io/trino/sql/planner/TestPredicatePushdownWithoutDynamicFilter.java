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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.sql.planner.plan.ExchangeNode;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;

public class TestPredicatePushdownWithoutDynamicFilter
        extends AbstractPredicatePushdownTest
{
    public TestPredicatePushdownWithoutDynamicFilter()
    {
        super(false);
    }

    @Test
    @Override
    public void testCoercions()
    {
        // Ensure constant equality predicate is pushed to the other side of the join
        // when type coercions are involved

        // values have the same type (varchar(4)) in both tables
        assertPlan(
                "WITH " +
                        "    t(k, v) AS (SELECT nationkey, CAST(name AS varchar(4)) FROM nation)," +
                        "    u(k, v) AS (SELECT nationkey, CAST(name AS varchar(4)) FROM nation) " +
                        "SELECT 1 " +
                        "FROM t JOIN u ON t.k = u.k AND t.v = u.v " +
                        "WHERE t.v = 'x'",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("t_k", "u_k")
                                .left(
                                        project(
                                                filter(
                                                        "CAST('x' AS varchar(4)) = CAST(t_v AS varchar(4))",
                                                        tableScan("nation", ImmutableMap.of("t_k", "nationkey", "t_v", "name")))))
                                .right(
                                        anyTree(
                                                project(
                                                        filter(
                                                                "CAST('x' AS varchar(4)) = CAST(u_v AS varchar(4))",
                                                                tableScan("nation", ImmutableMap.of("u_k", "nationkey", "u_v", "name")))))))));

        // values have different types (varchar(4) vs varchar(5)) in each table
        assertPlan(
                "WITH " +
                        "    t(k, v) AS (SELECT nationkey, CAST(name AS varchar(4)) FROM nation)," +
                        "    u(k, v) AS (SELECT nationkey, CAST(name AS varchar(5)) FROM nation) " +
                        "SELECT 1 " +
                        "FROM t JOIN u ON t.k = u.k AND t.v = u.v " +
                        "WHERE t.v = 'x'",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("t_k", "u_k")
                                .left(
                                        project(
                                                filter("CAST('x' AS varchar(4)) = CAST(t_v AS varchar(4))",
                                                        tableScan("nation", ImmutableMap.of("t_k", "nationkey", "t_v", "name")))))
                                .right(
                                        anyTree(
                                                project(
                                                        filter(
                                                                "CAST('x' AS varchar(5)) = CAST(u_v AS varchar(5))",
                                                                tableScan("nation", ImmutableMap.of("u_k", "nationkey", "u_v", "name")))))))));
    }

    @Test
    public void testNormalizeOuterJoinToInner()
    {
        Session disableJoinReordering = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .build();

        // one join
        assertPlan(
                "SELECT customer.name, orders.orderdate " +
                        "FROM orders " +
                        "LEFT JOIN customer ON orders.custkey = customer.custkey " +
                        "WHERE customer.name IS NOT NULL",
                disableJoinReordering,
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("o_custkey", "c_custkey")
                                .left(
                                        tableScan("orders", ImmutableMap.of("o_orderdate", "orderdate", "o_custkey", "custkey")))
                                .right(
                                        anyTree(
                                                filter(
                                                        "NOT (c_name IS NULL)",
                                                        tableScan("customer", ImmutableMap.of("c_custkey", "custkey", "c_name", "name"))))))));

        // nested joins
        assertPlan(
                "SELECT customer.name, lineitem.partkey " +
                        "FROM lineitem " +
                        "LEFT JOIN orders ON lineitem.orderkey = orders.orderkey " +
                        "LEFT JOIN customer ON orders.custkey = customer.custkey " +
                        "WHERE customer.name IS NOT NULL",
                disableJoinReordering,
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("o_custkey", "c_custkey")
                                .left(
                                        join(LEFT, // TODO (https://github.com/trinodb/trino/issues/2392) this should be INNER also when dynamic filtering is off
                                                leftJoinBuilder -> leftJoinBuilder
                                                        .equiCriteria("l_orderkey", "o_orderkey")
                                                        .left(
                                                                tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey")))
                                                        .right(
                                                                anyTree(
                                                                        tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey", "o_custkey", "custkey"))))))
                                .right(
                                        anyTree(
                                                filter(
                                                        "NOT (c_name IS NULL)",
                                                        tableScan("customer", ImmutableMap.of("c_custkey", "custkey", "c_name", "name"))))))));
    }

    @Test
    public void testNonDeterministicPredicateDoesNotPropagateFromFilteringSideToSourceSideOfSemiJoin()
    {
        assertPlan("SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey = random(5))",
                noSemiJoinRewrite(),
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT", false,
                                tableScan("lineitem", ImmutableMap.of(
                                        "LINE_ORDER_KEY", "orderkey")),
                                node(ExchangeNode.class,
                                        filter("ORDERS_ORDER_KEY = CAST(random(5) AS bigint)",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testNonStraddlingJoinExpression()
    {
        assertPlan(
                "SELECT * FROM orders JOIN lineitem ON orders.orderkey = lineitem.orderkey AND cast(lineitem.linenumber AS varchar) = '2'",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("LINEITEM_OK", "ORDERS_OK")
                                .left(
                                        filter("cast(LINEITEM_LINENUMBER as varchar) = VARCHAR '2'",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINEITEM_OK", "orderkey",
                                                        "LINEITEM_LINENUMBER", "linenumber"))))
                                .right(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))))));
    }
}
