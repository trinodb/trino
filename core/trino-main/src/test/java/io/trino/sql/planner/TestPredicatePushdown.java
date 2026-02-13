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
import io.trino.spi.type.Type;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.PlanMatchPattern.DynamicFilterPattern;
import io.trino.sql.planner.plan.ExchangeNode;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinType.ASOF;
import static io.trino.sql.planner.plan.JoinType.ASOF_LEFT;
import static io.trino.sql.planner.plan.JoinType.INNER;

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
                                .dynamicFilter(BIGINT, "t_k", "u_k")
                                .left(
                                        project(
                                                filter(
                                                        new Comparison(EQUAL, new Constant(createVarcharType(4), Slices.utf8Slice("x")), new Cast(new Reference(createVarcharType(4), "t_v"), createVarcharType(4))),
                                                        tableScan("nation", ImmutableMap.of("t_k", "nationkey", "t_v", "name")))))
                                .right(
                                        anyTree(
                                                project(
                                                        filter(
                                                                new Comparison(EQUAL, new Constant(createVarcharType(4), Slices.utf8Slice("x")), new Cast(new Reference(createVarcharType(4), "u_v"), createVarcharType(4))),
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
                                .dynamicFilter(BIGINT, "t_k", "u_k")
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
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("o_orderdate", "orderdate", "o_custkey", "custkey"))))
                                .right(
                                        anyTree(
                                                filter(
                                                        not(getPlanTester().getPlannerContext().getMetadata(), new IsNull(new Reference(VARCHAR, "c_name"))),
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
                                                        not(getPlanTester().getPlannerContext().getMetadata(), new IsNull(new Reference(VARCHAR, "c_name"))),
                                                        tableScan("customer", ImmutableMap.of("c_custkey", "custkey", "c_name", "name"))))))));
    }

    @Test
    public void testNonDeterministicPredicateDoesNotPropagateFromFilteringSideToSourceSideOfSemiJoin()
    {
        assertPlan("SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey = random(5))",
                noSemiJoinRewrite(),
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT", true,
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey"))),
                                node(ExchangeNode.class,
                                        filter(
                                                new Comparison(EQUAL, new Reference(BIGINT, "ORDERS_ORDER_KEY"), new Cast(new Call(RANDOM, ImmutableList.of(new Constant(INTEGER, 5L))), BIGINT)),
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

    @Test
    public void testAsofJoinInheritedPredicatePushdown()
    {
        // inherited predicate referencing left side: propagate to left and via join equality to right
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey
                        WHERE o1.custkey > 10
                        """,
                output(project(
                        join(ASOF, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                .dynamicFilter(ImmutableList.of(
                                        new DynamicFilterPattern(new Reference(BIGINT, "O1_CUSTKEY"), EQUAL, "O2_CUSTKEY"),
                                        new DynamicFilterPattern(new Reference(BIGINT, "O1_ORDERKEY"), GREATER_THAN_OR_EQUAL, "O2_ORDERKEY")))
                                .left(filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "O1_CUSTKEY"), new Constant(BIGINT, 10L)),
                                        tableScan("orders", ImmutableMap.of(
                                                "O1_ORDERKEY", "orderkey",
                                                "O1_CUSTKEY", "custkey"))))
                                .right(exchange(filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "O2_CUSTKEY"), new Constant(BIGINT, 10L)),
                                        tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey")))))))));
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF LEFT JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey
                        WHERE o1.custkey > 10
                        """,
                output(project(
                        join(ASOF_LEFT, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                .left(filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "O1_CUSTKEY"), new Constant(BIGINT, 10L)),
                                        tableScan("orders", ImmutableMap.of(
                                                "O1_ORDERKEY", "orderkey",
                                                "O1_CUSTKEY", "custkey"))))
                                .right(exchange(filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "O2_CUSTKEY"), new Constant(BIGINT, 10L)),
                                        tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey")))))))));

        // inherited predicate referencing both sides: remains as post-join filter
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey
                        WHERE o1.orderkey = o2.orderkey
                        """,
                output(project(project(
                        filter(
                                new Comparison(EQUAL, new Reference(BIGINT, "O1_ORDERKEY"), new Reference(BIGINT, "O2_ORDERKEY")),
                                join(ASOF, builder -> builder
                                        .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                        .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                        .dynamicFilter(ImmutableList.of(
                                                new DynamicFilterPattern(new Reference(BIGINT, "O1_CUSTKEY"), EQUAL, "O2_CUSTKEY"),
                                                new DynamicFilterPattern(new Reference(BIGINT, "O1_ORDERKEY"), GREATER_THAN_OR_EQUAL, "O2_ORDERKEY")))
                                        .left(filter(TRUE, tableScan("orders", ImmutableMap.of(
                                                "O1_ORDERKEY", "orderkey",
                                                "O1_CUSTKEY", "custkey"))))
                                        .right(exchange(tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey"))))))))));
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF LEFT JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey
                        WHERE o1.orderkey = o2.orderkey OR o2.orderkey IS NULL
                        """,
                output(project(
                        filter(
                                new Logical(OR, ImmutableList.of(
                                        new Comparison(EQUAL, new Reference(BIGINT, "O1_ORDERKEY"), new Reference(BIGINT, "O2_ORDERKEY")),
                                        new IsNull(new Reference(BIGINT, "O2_ORDERKEY")))),
                                join(ASOF_LEFT, builder -> builder
                                        .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                        .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                        .left(tableScan("orders", ImmutableMap.of(
                                                "O1_ORDERKEY", "orderkey",
                                                "O1_CUSTKEY", "custkey")))
                                        .right(exchange(tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey")))))))));

        // inherited predicate referencing right side: remains as post-join filter
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey
                        WHERE o2.custkey = 10
                        """,
                output(project(project(
                        filter(
                                new Comparison(EQUAL, new Reference(BIGINT, "O2_CUSTKEY"), new Constant(BIGINT, 10L)),
                                join(ASOF, builder -> builder
                                        .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                        .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                        .dynamicFilter(ImmutableList.of(
                                                new DynamicFilterPattern(new Reference(BIGINT, "O1_CUSTKEY"), EQUAL, "O2_CUSTKEY"),
                                                new DynamicFilterPattern(new Reference(BIGINT, "O1_ORDERKEY"), GREATER_THAN_OR_EQUAL, "O2_ORDERKEY")))
                                        .left(filter(TRUE, tableScan("orders", ImmutableMap.of(
                                                "O1_ORDERKEY", "orderkey",
                                                "O1_CUSTKEY", "custkey"))))
                                        .right(exchange(tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey"))))))))));
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF LEFT JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey
                        WHERE o2.custkey = 10 OR o2.custkey IS NULL
                        """,
                output(project(
                        filter(
                                new Logical(OR, ImmutableList.of(
                                        new Comparison(EQUAL, new Reference(BIGINT, "O2_CUSTKEY"), new Constant(BIGINT, 10L)),
                                        new IsNull(new Reference(BIGINT, "O2_CUSTKEY")))),
                                join(ASOF_LEFT, builder -> builder
                                        .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                        .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                        .left(tableScan("orders", ImmutableMap.of(
                                                "O1_ORDERKEY", "orderkey",
                                                "O1_CUSTKEY", "custkey")))
                                        .right(exchange(tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey")))))))));
    }

    @Test
    public void testAsofJoinEffectivePredicateTransfers()
    {
        // left effective predicate transferrable to right side via join criteria
        assertPlan(
                """
                        SELECT 1
                        FROM (SELECT * FROM orders o1 WHERE o1.custkey > 10) o1
                        ASOF JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey
                        """,
                output(project(
                        join(ASOF, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                .dynamicFilter(ImmutableList.of(
                                        new DynamicFilterPattern(new Reference(BIGINT, "O1_CUSTKEY"), EQUAL, "O2_CUSTKEY"),
                                        new DynamicFilterPattern(new Reference(BIGINT, "O1_ORDERKEY"), GREATER_THAN_OR_EQUAL, "O2_ORDERKEY")))
                                .left(filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "O1_CUSTKEY"), new Constant(BIGINT, 10L)),
                                        tableScan("orders", ImmutableMap.of(
                                                "O1_ORDERKEY", "orderkey",
                                                "O1_CUSTKEY", "custkey"))))
                                .right(exchange(filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "O2_CUSTKEY"), new Constant(BIGINT, 10L)),
                                        tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey")))))))));
        assertPlan(
                """
                        SELECT 1
                        FROM (SELECT * FROM orders o1 WHERE o1.custkey > 10) o1
                        ASOF LEFT JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey
                        """,
                output(project(
                        join(ASOF_LEFT, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                .left(filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "O1_CUSTKEY"), new Constant(BIGINT, 10L)),
                                        tableScan("orders", ImmutableMap.of(
                                                "O1_ORDERKEY", "orderkey",
                                                "O1_CUSTKEY", "custkey"))))
                                .right(exchange(filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "O2_CUSTKEY"), new Constant(BIGINT, 10L)),
                                        tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey")))))))));

        // right effective predicate transferrable to left side via join criteria (ASOF inner only)
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF JOIN (SELECT * FROM orders o2 WHERE o2.custkey > 10) o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey
                        """,
                output(project(
                        join(ASOF, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                .dynamicFilter(ImmutableList.of(
                                        new DynamicFilterPattern(new Reference(BIGINT, "O1_CUSTKEY"), EQUAL, "O2_CUSTKEY"),
                                        new DynamicFilterPattern(new Reference(BIGINT, "O1_ORDERKEY"), GREATER_THAN_OR_EQUAL, "O2_ORDERKEY")))
                                .left(filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "O1_CUSTKEY"), new Constant(BIGINT, 10L)),
                                        tableScan("orders", ImmutableMap.of(
                                                "O1_ORDERKEY", "orderkey",
                                                "O1_CUSTKEY", "custkey"))))
                                .right(exchange(filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "O2_CUSTKEY"), new Constant(BIGINT, 10L)),
                                        tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey")))))))));
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF LEFT JOIN (SELECT * FROM orders o2 WHERE o2.custkey > 10) o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey
                        """,
                output(project(
                        join(ASOF_LEFT, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                .left(tableScan("orders", ImmutableMap.of(
                                        "O1_ORDERKEY", "orderkey",
                                        "O1_CUSTKEY", "custkey")))
                                .right(exchange(filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "O2_CUSTKEY"), new Constant(BIGINT, 10L)),
                                        tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey")))))))));
    }

    @Test
    public void testAsofJoinPredicatePushdown()
    {
        // left-side predicate specified in the ON clause should be pushed down to the left side (ASOF inner only)
        Type commentType = createVarcharType(79);
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey AND o1.comment = 'F'
                        """,
                output(project(
                        join(ASOF, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                .dynamicFilter(ImmutableList.of(
                                        new DynamicFilterPattern(new Reference(BIGINT, "O1_CUSTKEY"), EQUAL, "O2_CUSTKEY"),
                                        new DynamicFilterPattern(new Reference(BIGINT, "O1_ORDERKEY"), GREATER_THAN_OR_EQUAL, "O2_ORDERKEY")))
                                .left(project(filter(
                                        new Comparison(EQUAL, new Reference(commentType, "O1_COMMENT"), new Constant(commentType, Slices.utf8Slice("F"))),
                                        tableScan("orders", ImmutableMap.of(
                                                "O1_ORDERKEY", "orderkey",
                                                "O1_CUSTKEY", "custkey",
                                                "O1_COMMENT", "comment")))))
                                .right(exchange(
                                        tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey"))))))));

        // BETWEEN form: entire BETWEEN stays on join (no right-side pushdown)
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey BETWEEN o1.orderkey AND 4
                        """,
                output(project(
                        join(ASOF, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Between(new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY"), new Constant(BIGINT, 4L)))
                                .left(filter(TRUE,
                                        tableScan("orders", ImmutableMap.of(
                                                "O1_ORDERKEY", "orderkey",
                                                "O1_CUSTKEY", "custkey"))))
                                .right(exchange(
                                        tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey"))))))));
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF LEFT JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey AND o1.comment = 'F'
                        """,
                output(project(
                        join(ASOF_LEFT, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Logical(AND, ImmutableList.of(
                                        new Comparison(EQUAL, new Reference(commentType, "O1_COMMENT"), new Constant(commentType, Slices.utf8Slice("F"))),
                                        new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))))
                                .left(tableScan("orders", ImmutableMap.of(
                                        "O1_ORDERKEY", "orderkey",
                                        "O1_CUSTKEY", "custkey",
                                        "O1_COMMENT", "comment")))
                                .right(exchange(
                                        tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey"))))))));

        // right-side predicate specified in the ON clause should be pushed down to the right side
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey AND o2.comment = 'F'
                        """,
                output(project(
                        join(ASOF, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                .dynamicFilter(ImmutableList.of(
                                        new DynamicFilterPattern(new Reference(BIGINT, "O1_CUSTKEY"), EQUAL, "O2_CUSTKEY"),
                                        new DynamicFilterPattern(new Reference(BIGINT, "O1_ORDERKEY"), GREATER_THAN_OR_EQUAL, "O2_ORDERKEY")))
                                .left(filter(TRUE,
                                        tableScan("orders", ImmutableMap.of(
                                                "O1_ORDERKEY", "orderkey",
                                                "O1_CUSTKEY", "custkey"))))
                                .right(exchange(project(
                                        filter(new Comparison(EQUAL, new Reference(commentType, "O2_COMMENT"), new Constant(commentType, Slices.utf8Slice("F"))),
                                                tableScan("orders", ImmutableMap.of(
                                                        "O2_ORDERKEY", "orderkey",
                                                        "O2_CUSTKEY", "custkey",
                                                        "O2_COMMENT", "comment"))))))))));
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF LEFT JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey AND o2.comment = 'F'
                        """,
                output(project(
                        join(ASOF_LEFT, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                .left(tableScan("orders", ImmutableMap.of(
                                        "O1_ORDERKEY", "orderkey",
                                        "O1_CUSTKEY", "custkey")))
                                .right(exchange(project(
                                        filter(new Comparison(EQUAL, new Reference(commentType, "O2_COMMENT"), new Constant(commentType, Slices.utf8Slice("F"))),
                                                tableScan("orders", ImmutableMap.of(
                                                        "O2_ORDERKEY", "orderkey",
                                                        "O2_CUSTKEY", "custkey",
                                                        "O2_COMMENT", "comment"))))))))));
    }

    @Test
    public void testAsofJoinOnInequalityCandidateNotPushedRight()
    {
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.custkey
                        """,
                output(project(
                        join(ASOF, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_CUSTKEY")))
                                .dynamicFilter(ImmutableList.of(
                                        new DynamicFilterPattern(new Reference(BIGINT, "O1_CUSTKEY"), EQUAL, "O2_CUSTKEY"),
                                        new DynamicFilterPattern(new Reference(BIGINT, "O1_CUSTKEY"), GREATER_THAN_OR_EQUAL, "O2_ORDERKEY")))
                                .left(filter(TRUE,
                                        tableScan("orders", ImmutableMap.of(
                                                "O1_CUSTKEY", "custkey"))))
                                .right(exchange(
                                        tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey"))))))));
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF LEFT JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.custkey
                        """,
                output(project(
                        join(ASOF_LEFT, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_CUSTKEY")))
                                .left(tableScan("orders", ImmutableMap.of(
                                        "O1_CUSTKEY", "custkey")))
                                .right(exchange(
                                        tableScan("orders", ImmutableMap.of(
                                                "O2_ORDERKEY", "orderkey",
                                                "O2_CUSTKEY", "custkey"))))))));
    }

    @Test
    public void testAsofJoinRightOnlyInequalityInOnIsPushedRight()
    {
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey AND o2.orderkey < 4
                        """,
                output(project(
                        join(ASOF, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                .left(filter(TRUE,
                                        tableScan("orders", ImmutableMap.of(
                                                "O1_ORDERKEY", "orderkey",
                                                "O1_CUSTKEY", "custkey"))))
                                .right(exchange(
                                        filter(new Comparison(LESS_THAN, new Reference(BIGINT, "O2_ORDERKEY"), new Constant(BIGINT, 4L)),
                                                tableScan("orders", ImmutableMap.of(
                                                        "O2_ORDERKEY", "orderkey",
                                                        "O2_CUSTKEY", "custkey")))))))));
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF LEFT JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey AND o2.orderkey < 4
                        """,
                output(project(
                        join(ASOF_LEFT, builder -> builder
                                .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                .left(tableScan("orders", ImmutableMap.of(
                                        "O1_ORDERKEY", "orderkey",
                                        "O1_CUSTKEY", "custkey")))
                                .right(exchange(
                                        filter(new Comparison(LESS_THAN, new Reference(BIGINT, "O2_ORDERKEY"), new Constant(BIGINT, 4L)),
                                                tableScan("orders", ImmutableMap.of(
                                                        "O2_ORDERKEY", "orderkey",
                                                        "O2_CUSTKEY", "custkey")))))))));
    }

    @Test
    public void testAsofJoinInheritedUnsafePredicateStaysAboveJoin()
    {
        // Inherited predicate with potential failure (CAST on comment) must remain above the ASOF join,
        // while safe inherited predicates (o1.custkey > 10) are pushed down to both sides via equality.
        Type commentType = createVarcharType(79);
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey
                        WHERE CAST(o1.comment AS bigint) > 0 AND o1.custkey > 10
                        """,
                output(project(project(
                        filter(
                                new Comparison(GREATER_THAN, new Cast(new Reference(commentType, "O1_COMMENT"), BIGINT), new Constant(BIGINT, 0L)),
                                join(ASOF, builder -> builder
                                        .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                        .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                        .dynamicFilter(ImmutableList.of(
                                                new DynamicFilterPattern(new Reference(BIGINT, "O1_CUSTKEY"), EQUAL, "O2_CUSTKEY"),
                                                new DynamicFilterPattern(new Reference(BIGINT, "O1_ORDERKEY"), GREATER_THAN_OR_EQUAL, "O2_ORDERKEY")))
                                        .left(filter(
                                                new Comparison(GREATER_THAN, new Reference(BIGINT, "O1_CUSTKEY"), new Constant(BIGINT, 10L)),
                                                tableScan("orders", ImmutableMap.of(
                                                        "O1_ORDERKEY", "orderkey",
                                                        "O1_CUSTKEY", "custkey",
                                                        "O1_COMMENT", "comment"))))
                                        .right(exchange(filter(
                                                new Comparison(GREATER_THAN, new Reference(BIGINT, "O2_CUSTKEY"), new Constant(BIGINT, 10L)),
                                                tableScan("orders", ImmutableMap.of(
                                                        "O2_ORDERKEY", "orderkey",
                                                        "O2_CUSTKEY", "custkey")))))))))));
    }

    @Test
    public void testAsofLeftJoinInheritedPredicateNormalizesJoinToAsof()
    {
        assertPlan(
                """
                        SELECT 1
                        FROM orders o1
                        ASOF LEFT JOIN orders o2
                        ON o1.custkey = o2.custkey AND o2.orderkey <= o1.orderkey
                        WHERE o2.orderkey > 0
                        """,
                output(project(project(
                        filter(
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "O2_ORDERKEY"), new Constant(BIGINT, 0L)),
                                join(ASOF, builder -> builder
                                        .equiCriteria("O1_CUSTKEY", "O2_CUSTKEY")
                                        .filter(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "O2_ORDERKEY"), new Reference(BIGINT, "O1_ORDERKEY")))
                                        .dynamicFilter(ImmutableList.of(
                                                new DynamicFilterPattern(new Reference(BIGINT, "O1_CUSTKEY"), EQUAL, "O2_CUSTKEY"),
                                                new DynamicFilterPattern(new Reference(BIGINT, "O1_ORDERKEY"), GREATER_THAN_OR_EQUAL, "O2_ORDERKEY")))
                                        .left(filter(TRUE,
                                                tableScan("orders", ImmutableMap.of(
                                                        "O1_ORDERKEY", "orderkey",
                                                        "O1_CUSTKEY", "custkey"))))
                                        .right(exchange(
                                                tableScan("orders", ImmutableMap.of(
                                                        "O2_ORDERKEY", "orderkey",
                                                        "O2_CUSTKEY", "custkey"))))))))));
    }
}
