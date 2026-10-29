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
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.SemiJoinDynamicFilterProducer.dynamicFilter;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPredicatePushdown
        extends AbstractPredicatePushdownTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes(INTEGER));

    public TestPredicatePushdown()
    {
        super(true);
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
                                .addDynamicFilter("DF", "u_k")
                                .left(
                                        project(
                                                filter(
                                                        comparison(EQUAL, new Constant(createVarcharType(4), Slices.utf8Slice("x")), new Cast(new Reference(createVarcharType(4), "t_v"), createVarcharType(4))),
                                                        dynamicFilters -> dynamicFilters.addConsumer(consumer -> consumer.alias("DF").expression(BIGINT, "t_k")),
                                                        tableScan("nation", ImmutableMap.of("t_k", "nationkey", "t_v", "name")))))
                                .right(
                                        anyTree(
                                                project(
                                                        filter(
                                                                comparison(EQUAL, new Constant(createVarcharType(4), Slices.utf8Slice("x")), new Cast(new Reference(createVarcharType(4), "u_v"), createVarcharType(4))),
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
                                .addDynamicFilter("DF", "u_k")
                                .left(
                                        project(
                                                filter(
                                                        comparison(EQUAL, new Constant(createVarcharType(4), Slices.utf8Slice("x")), new Cast(new Reference(createVarcharType(4), "t_v"), createVarcharType(4))),
                                                        dynamicFilters -> dynamicFilters.addConsumer(consumer -> consumer.alias("DF").expression(BIGINT, "t_k")),
                                                        tableScan("nation", ImmutableMap.of("t_k", "nationkey", "t_v", "name")))))
                                .right(
                                        anyTree(
                                                project(
                                                        filter(
                                                                comparison(EQUAL, new Constant(createVarcharType(5), Slices.utf8Slice("x")), new Cast(new Reference(createVarcharType(5), "u_v"), createVarcharType(5))),
                                                                tableScan("nation", ImmutableMap.of("u_k", "nationkey", "u_v", "name")))))))));
    }

    @Test
    public void testIsNotDistinctFromJoinUsesEquiCriteria()
    {
        // IS NOT DISTINCT FROM between the join sides plans as equi-join keys (a NULL indicator and
        // a NULL-replaced value), so the join is a hash join rather than a nested loop
        JoinNode join = onlyJoin(plan("SELECT 1 FROM orders o1 JOIN orders o2 ON o1.orderkey IS NOT DISTINCT FROM o2.orderkey"));
        assertThat(join.getCriteria()).hasSize(2);

        join = onlyJoin(plan("SELECT 1 FROM orders o1 LEFT JOIN orders o2 ON o1.clerk IS NOT DISTINCT FROM o2.clerk AND o1.orderkey < o2.orderkey"));
        assertThat(join.getCriteria()).hasSize(2);

        // planning is stable: a later pass reuses the keys instead of adding more
        join = onlyJoin(plan("SELECT 1 FROM orders o1 JOIN orders o2 ON o1.orderkey IS NOT DISTINCT FROM o2.orderkey AND o1.custkey IS NOT DISTINCT FROM o2.custkey"));
        assertThat(join.getCriteria()).hasSize(4);

        // equality differs from IS NOT DISTINCT FROM for NaN and signed zero, so floating-point
        // keys stay in the join filter
        join = onlyJoin(plan("SELECT 1 FROM orders o1 LEFT JOIN orders o2 ON o1.totalprice IS NOT DISTINCT FROM o2.totalprice"));
        assertThat(join.getCriteria()).isEmpty();
        assertThat(join.getFilter()).isPresent();
    }

    private static JoinNode onlyJoin(Plan plan)
    {
        return (JoinNode) getOnlyElement(PlanNodeSearcher.searchFrom(plan.getRoot()).whereIsInstanceOfAny(JoinNode.class).findAll());
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
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("o_orderdate", "orderdate", "o_custkey", "custkey"))))
                                .right(
                                        anyTree(
                                                filter(
                                                        not(getPlanTester().getPlannerContext().getMetadata(), getCharVarcharCoercion(TEST_SESSION), new IsNull(new Reference(VARCHAR, "c_name"))),
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
                                        join(INNER,
                                                leftJoinBuilder -> leftJoinBuilder
                                                        .equiCriteria("l_orderkey", "o_orderkey")
                                                        .left(
                                                                anyTree(
                                                                        tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey"))))
                                                        .right(
                                                                anyTree(
                                                                        tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey", "o_custkey", "custkey"))))))
                                .right(
                                        anyTree(
                                                filter(
                                                        not(getPlanTester().getPlannerContext().getMetadata(), getCharVarcharCoercion(TEST_SESSION), new IsNull(new Reference(VARCHAR, "c_name"))),
                                                        tableScan("customer", ImmutableMap.of("c_custkey", "custkey", "c_name", "name"))))))));
    }

    @Test
    public void testNonDeterministicPredicateDoesNotPropagateFromFilteringSideToSourceSideOfSemiJoin()
    {
        assertPlan("SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey = random(5))",
                noSemiJoinRewrite(),
                anyTree(
                        semiJoin("LINE_ORDER_KEY",
                                "ORDERS_ORDER_KEY",
                                "SEMI_JOIN_RESULT",
                                dynamicFilter("DF"),
                                filter(
                                        TRUE,
                                        dynamicFilters -> dynamicFilters.addConsumer(consumer -> consumer.alias("DF").expression(BIGINT, "LINE_ORDER_KEY")),
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey"))),
                                node(ExchangeNode.class,
                                        filter(
                                                comparison(EQUAL, new Reference(BIGINT, "ORDERS_ORDER_KEY"), new Cast(new Call(RANDOM, ImmutableList.of(new Constant(INTEGER, 5L))), BIGINT)),
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
                                                comparison(EQUAL, new Cast(new Reference(INTEGER, "LINEITEM_LINENUMBER"), VARCHAR), new Constant(VARCHAR, Slices.utf8Slice("2"))),
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINEITEM_OK", "orderkey",
                                                        "LINEITEM_LINENUMBER", "linenumber"))))
                                .right(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))))));
    }
}
