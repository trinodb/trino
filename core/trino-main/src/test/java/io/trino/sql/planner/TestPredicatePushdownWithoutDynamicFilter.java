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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.plan.ExchangeNode;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
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
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM_INTEGER = FUNCTIONS.resolveFunction("random", fromTypes(INTEGER));

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
                                                        new Comparison(EQUAL, new Constant(createVarcharType(4), Slices.utf8Slice("x")), new Cast(new Reference(createVarcharType(4), "t_v"), createVarcharType(4))),
                                                        tableScan("nation", ImmutableMap.of("t_k", "nationkey", "t_v", "name")))))
                                .right(
                                        anyTree(
                                                project(
                                                        filter(
                                                                new Comparison(EQUAL, new Constant(createVarcharType(4), Slices.utf8Slice("x")), new Cast(new Reference(createVarcharType(5), "u_v"), createVarcharType(4))),
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
                                                filter(
                                                        new Comparison(EQUAL, new Constant(createVarcharType(4), Slices.utf8Slice("x")), new Cast(new Reference(createVarcharType(4), "t_v"), createVarcharType(4))),
                                                        tableScan("nation", ImmutableMap.of("t_k", "nationkey", "t_v", "name")))))
                                .right(
                                        anyTree(
                                                project(
                                                        filter(
                                                                new Comparison(EQUAL, new Constant(createVarcharType(5), Slices.utf8Slice("x")), new Cast(new Reference(createVarcharType(5), "u_v"), createVarcharType(5))),
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
                                                        new Not(new IsNull(new Reference(VARCHAR, "c_name"))),
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
                                                        new Not(new IsNull(new Reference(VARCHAR, "c_name"))),
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
                                        filter(
                                                new Comparison(EQUAL, new Reference(BIGINT, "ORDERS_ORDER_KEY"), new Cast(new Call(RANDOM_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L))), BIGINT)),
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
                                        filter(
                                                new Comparison(EQUAL, new Cast(new Reference(INTEGER, "LINEITEM_LINENUMBER"), VARCHAR), new Constant(VARCHAR, Slices.utf8Slice("2"))),
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINEITEM_OK", "orderkey",
                                                        "LINEITEM_LINENUMBER", "linenumber"))))
                                .right(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))))));
    }
}
