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
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.WindowNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;

public abstract class AbstractPredicatePushdownTest
        extends BasePlanTest
{
    private final boolean enableDynamicFiltering;

    protected AbstractPredicatePushdownTest(boolean enableDynamicFiltering)
    {
        super(ImmutableMap.of(ENABLE_DYNAMIC_FILTERING, Boolean.toString(enableDynamicFiltering)));
        this.enableDynamicFiltering = enableDynamicFiltering;
    }

    @Test
    public abstract void testCoercions();

    @Test
    public void testNonStraddlingJoinExpression()
    {
        assertPlan(
                "SELECT * FROM orders JOIN lineitem ON orders.orderkey = lineitem.orderkey AND cast(lineitem.linenumber AS varchar) = '2'",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                                .left(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                                .right(
                                        anyTree(
                                                filter("cast(LINEITEM_LINENUMBER as varchar) = VARCHAR '2'",
                                                        tableScan("lineitem", ImmutableMap.of(
                                                                "LINEITEM_OK", "orderkey",
                                                                "LINEITEM_LINENUMBER", "linenumber"))))))));
    }

    @Test
    public void testPushDownToLhsOfSemiJoin()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders)) " +
                        "WHERE linenumber = 2",
                noSemiJoinRewrite(),
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT", enableDynamicFiltering,
                                anyTree(
                                        filter("LINE_NUMBER = 2",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_NUMBER", "linenumber",
                                                        "LINE_QUANTITY", "quantity")))),
                                anyTree(tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey"))))));
    }

    @Test
    public void testNonDeterministicPredicatePropagatesOnlyToSourceSideOfSemiJoin()
    {
        assertPlan("SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders) AND orderkey = random(5)",
                noSemiJoinRewrite(),
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT", enableDynamicFiltering,
                                anyTree(
                                        filter("LINE_ORDER_KEY = CAST(random(5) AS bigint)",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey")))),
                                node(ExchangeNode.class, // NO filter here
                                        project(
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));

        assertPlan("SELECT * FROM lineitem WHERE orderkey NOT IN (SELECT orderkey FROM orders) AND orderkey = random(5)",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                anyTree(
                                        filter("LINE_ORDER_KEY = CAST(random(5) AS bigint)",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey")))),
                                anyTree(
                                        project(// NO filter here
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testNonDeterministicPredicateDoesNotPropagateFromFilteringSideToSourceSideOfSemiJoin()
    {
        assertPlan("SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey = random(5))",
                noSemiJoinRewrite(),
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT", enableDynamicFiltering,
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey"))),
                                node(ExchangeNode.class,
                                        project(
                                                filter("ORDERS_ORDER_KEY = CAST(random(5) AS bigint)",
                                                        tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey"))))))));
    }

    @Test
    public void testGreaterPredicateFromFilterSidePropagatesToSourceSideOfSemiJoin()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey > 2))",
                noSemiJoinRewrite(),
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT", enableDynamicFiltering,
                                anyTree(
                                        filter("LINE_ORDER_KEY > BIGINT '2'",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_QUANTITY", "quantity")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY > BIGINT '2'",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testEqualsPredicateFromFilterSidePropagatesToSourceSideOfSemiJoin()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey = 2))",
                noSemiJoinRewrite(),
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT", enableDynamicFiltering,
                                anyTree(
                                        filter("LINE_ORDER_KEY = BIGINT '2'",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_QUANTITY", "quantity")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY = BIGINT '2'",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testPredicateFromFilterSideNotPropagatesToSourceSideOfSemiJoinIfNotIn()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey NOT IN (SELECT orderkey FROM orders WHERE orderkey > 2))",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                // There should be no Filter above table scan, because we don't know whether SemiJoin's filtering source is empty.
                                // And filter would filter out NULLs from source side which is not what we need then.
                                project(
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey",
                                                "LINE_QUANTITY", "quantity"))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY > BIGINT '2'",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testGreaterPredicateFromSourceSidePropagatesToFilterSideOfSemiJoin()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders) AND orderkey > 2)",
                noSemiJoinRewrite(),
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT", enableDynamicFiltering,
                                anyTree(
                                        filter("LINE_ORDER_KEY > BIGINT '2'",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_QUANTITY", "quantity")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY > BIGINT '2'",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testEqualPredicateFromSourceSidePropagatesToFilterSideOfSemiJoin()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders) AND orderkey = 2)",
                noSemiJoinRewrite(),
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT", enableDynamicFiltering,
                                anyTree(
                                        filter("LINE_ORDER_KEY = BIGINT '2'",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_QUANTITY", "quantity")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY = BIGINT '2'",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testPredicateFromSourceSideNotPropagatesToFilterSideOfSemiJoinIfNotIn()
    {
        assertPlan("SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey NOT IN (SELECT orderkey FROM orders) AND orderkey > 2)",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                project(
                                        filter("LINE_ORDER_KEY > BIGINT '2'",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_QUANTITY", "quantity")))),
                                node(ExchangeNode.class, // NO filter here
                                        project(
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testPredicateFromFilterSideNotPropagatesToSourceSideOfSemiJoinUsedInProjection()
    {
        assertPlan("SELECT orderkey IN (SELECT orderkey FROM orders WHERE orderkey > 2) FROM lineitem",
                anyTree(
                        semiJoin("LINE_ORDER_KEY", "ORDERS_ORDER_KEY", "SEMI_JOIN_RESULT",
                                // NO filter here
                                project(
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey"))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY > BIGINT '2'",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testFilteredSelectFromPartitionedTable()
    {
        // use all optimizers, including AddExchanges
        List<PlanOptimizer> allOptimizers = getQueryRunner().getPlanOptimizers(false);

        assertPlan(
                "SELECT DISTINCT orderstatus FROM orders",
                // TODO this could be optimized to VALUES with values from partitions
                anyTree(
                        tableScan("orders")),
                allOptimizers);

        assertPlan(
                "SELECT orderstatus FROM orders WHERE orderstatus = 'O'",
                // predicate matches exactly single partition, no FilterNode needed
                output(
                        tableScan("orders")),
                allOptimizers);

        assertPlan(
                "SELECT orderstatus FROM orders WHERE orderstatus = 'no_such_partition_value'",
                output(
                        values("orderstatus")),
                allOptimizers);
    }

    @Test
    public void testPredicatePushDownThroughMarkDistinct()
    {
        assertPlan(
                "SELECT (SELECT a FROM (VALUES 1, 2, 3) t(a) WHERE a = b) FROM (VALUES 0, 1) p(b) WHERE b = 1",
                // TODO this could be optimized to VALUES with values from partitions
                anyTree(
                        join(LEFT, builder -> builder
                                .equiCriteria("A", "B")
                                .left(
                                        project(assignUniqueId("unique", filter("A = 1", values("A")))))
                                .right(
                                        project(filter("1 = B", values("B")))))));
    }

    @Test
    public void testPredicatePushDownOverProjection()
    {
        // Non-singletons should not be pushed down
        assertPlan(
                "WITH t AS (SELECT orderkey * 2 x FROM orders) " +
                        "SELECT * FROM t WHERE x + x > 1",
                anyTree(
                        filter("((expr + expr) > BIGINT '1')",
                                project(ImmutableMap.of("expr", expression("orderkey * BIGINT '2'")),
                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey"))))));

        // constant non-singleton should be pushed down
        assertPlan(
                "with t AS (SELECT orderkey * 2 x, 1 y FROM orders) " +
                        "SELECT * FROM t WHERE x + y + y >1",
                anyTree(
                        project(
                                filter("(((orderkey * BIGINT '2') + BIGINT '1') + BIGINT '1') > BIGINT '1'",
                                        tableScan("orders", ImmutableMap.of(
                                                "orderkey", "orderkey"))))));

        // singletons should be pushed down
        assertPlan(
                "WITH t AS (SELECT orderkey * 2 x FROM orders) " +
                        "SELECT * FROM t WHERE x > 1",
                anyTree(
                        project(
                                filter("(orderkey * BIGINT '2') > BIGINT '1'",
                                        tableScan("orders", ImmutableMap.of(
                                                "orderkey", "orderkey"))))));

        // composite singletons should be pushed down
        assertPlan(
                "with t AS (SELECT orderkey * 2 x, orderkey y FROM orders) " +
                        "SELECT * FROM t WHERE x + y > 1",
                anyTree(
                        project(
                                filter("((orderkey * BIGINT '2') + orderkey) > BIGINT '1'",
                                        tableScan("orders", ImmutableMap.of(
                                                "orderkey", "orderkey"))))));

        // Identities should be pushed down
        assertPlan(
                "WITH t AS (SELECT orderkey x FROM orders) " +
                        "SELECT * FROM t WHERE x >1",
                anyTree(
                        filter("orderkey > BIGINT '1'",
                                tableScan("orders", ImmutableMap.of(
                                        "orderkey", "orderkey")))));

        // Non-deterministic predicate should not be pushed down
        assertPlan(
                "WITH t AS (SELECT rand() * orderkey x FROM orders) " +
                        "SELECT * FROM t WHERE x > 5000",
                anyTree(
                        filter("expr > 5E3",
                                project(ImmutableMap.of("expr", expression("rand() * CAST(orderkey AS double)")),
                                        tableScan("orders", ImmutableMap.of(
                                                "orderkey", "orderkey"))))));
    }

    @Test
    public void testPredicatePushDownOverSymbolReferences()
    {
        // Identities should be pushed down
        assertPlan(
                "WITH t AS (SELECT orderkey x, (orderkey + 1) x2 FROM orders) " +
                        "SELECT * FROM t WHERE x > 1 OR x < 0",
                anyTree(
                        filter("orderkey < BIGINT '0' OR orderkey > BIGINT '1'",
                                tableScan("orders", ImmutableMap.of(
                                        "orderkey", "orderkey")))));
    }

    @Test
    public void testConjunctsOrder()
    {
        assertPlan(
                "select partkey " +
                        "from (" +
                        "  select" +
                        "    partkey," +
                        "    100/(size-1) x" +
                        "  from part" +
                        "  where size <> 1" +
                        ") " +
                        "where x = 2",
                anyTree(
                        // Order matters: size<>1 should be before 100/(size-1)=2.
                        // In this particular example, reversing the order leads to div-by-zero error.
                        filter("size <> 1 AND 100/(size - 1) = 2",
                                tableScan("part", ImmutableMap.of(
                                        "partkey", "partkey",
                                        "size", "size")))));
    }

    @Test
    public void testPredicateOnPartitionSymbolsPushedThroughWindow()
    {
        PlanMatchPattern tableScan = tableScan(
                "orders",
                ImmutableMap.of(
                        "CUST_KEY", "custkey",
                        "ORDER_KEY", "orderkey"));
        assertPlan(
                "SELECT * FROM (" +
                        "SELECT custkey, orderkey, rank() OVER (PARTITION BY custkey  ORDER BY orderdate ASC)" +
                        "FROM orders" +
                        ") WHERE custkey = 0 AND orderkey > 0",
                anyTree(
                        filter("ORDER_KEY > BIGINT '0'",
                                anyTree(
                                        node(WindowNode.class,
                                                anyTree(
                                                        filter("CUST_KEY = BIGINT '0'",
                                                                tableScan)))))));
    }

    @Test
    public void testPredicateOnNonDeterministicSymbolsPushedDown()
    {
        assertPlan(
                "SELECT * FROM (" +
                        "SELECT random_column, orderkey, rank() OVER (PARTITION BY random_column  ORDER BY orderdate ASC)" +
                        "FROM (select round(custkey*rand()) random_column, * from orders) " +
                        ") WHERE random_column > 100",
                anyTree(
                        node(WindowNode.class,
                                anyTree(
                                        filter("\"ROUND\" > 1E2",
                                                project(ImmutableMap.of("ROUND", expression("round(CAST(CUST_KEY AS double) * rand())")),
                                                        tableScan(
                                                                "orders",
                                                                ImmutableMap.of("CUST_KEY", "custkey"))))))));
    }

    @Test
    public void testNonDeterministicPredicateNotPushedDown()
    {
        assertPlan(
                "SELECT * FROM (" +
                        "SELECT custkey, orderkey, rank() OVER (PARTITION BY custkey  ORDER BY orderdate ASC)" +
                        "FROM orders" +
                        ") WHERE custkey > 100*rand()",
                anyTree(
                        filter("CAST(\"CUST_KEY\" AS double) > (\"rand\"() * 1E2)",
                                anyTree(
                                        node(WindowNode.class,
                                                anyTree(
                                                        tableScan(
                                                                "orders",
                                                                ImmutableMap.of("CUST_KEY", "custkey"))))))));
    }

    @Test
    public void testNormalizeOuterJoinToInner()
    {
        Session disableJoinReordering = Session.builder(getQueryRunner().getDefaultSession())
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
                                        anyTree(tableScan("orders", ImmutableMap.of("o_orderdate", "orderdate", "o_custkey", "custkey"))))
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
                                .left(anyTree(
                                        join(enableDynamicFiltering ? INNER : LEFT, // TODO (https://github.com/trinodb/trino/issues/2392) this should be INNER also when dynamic filtering is off
                                                leftJoinBuilder -> leftJoinBuilder
                                                        .equiCriteria("l_orderkey", "o_orderkey")
                                                        .left(
                                                                anyTree(tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey"))))
                                                        .right(
                                                                anyTree(tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey", "o_custkey", "custkey")))))))
                                .right(anyTree(
                                        filter(
                                                "NOT (c_name IS NULL)",
                                                tableScan("customer", ImmutableMap.of("c_custkey", "custkey", "c_name", "name"))))))));
    }

    @Test
    public void testRemovesRedundantTableScanPredicate()
    {
        assertPlan(
                "SELECT t1.orderstatus " +
                        "FROM (SELECT orderstatus FROM orders WHERE rand() = orderkey AND orderkey = 123) t1, (VALUES 'F', 'K') t2(col) " +
                        "WHERE t1.orderstatus = t2.col AND (t2.col = 'F' OR t2.col = 'K') AND length(t1.orderstatus) < 42",
                anyTree(
                        node(
                                JoinNode.class,
                                node(ProjectNode.class,
                                        filter("(ORDERKEY = BIGINT '123') AND rand() = CAST(ORDERKEY AS double) AND length(ORDERSTATUS) < BIGINT '42'",
                                                tableScan(
                                                        "orders",
                                                        ImmutableMap.of(
                                                                "ORDERSTATUS", "orderstatus",
                                                                "ORDERKEY", "orderkey")))),
                                anyTree(
                                        values("COL")))));
    }

    @Test
    public void testTablePredicateIsExtracted()
    {
        assertPlan(
                "SELECT * FROM orders, nation WHERE orderstatus = CAST(nation.name AS varchar(1)) AND orderstatus BETWEEN 'A' AND 'O'",
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        filter("CAST(NAME AS varchar(1)) IN ('F', 'O')",
                                                tableScan(
                                                        "nation",
                                                        ImmutableMap.of("NAME", "name")))),
                                anyTree(
                                        tableScan(
                                                "orders",
                                                ImmutableMap.of("ORDERSTATUS", "orderstatus"))))));

        assertPlan(
                "SELECT * FROM orders JOIN nation ON orderstatus = CAST(nation.name AS varchar(1))",
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        filter("CAST(NAME AS varchar(1)) IN ('F', 'O', 'P')",
                                                tableScan(
                                                        "nation",
                                                        ImmutableMap.of("NAME", "name")))),
                                anyTree(
                                        tableScan(
                                                "orders",
                                                ImmutableMap.of("ORDERSTATUS", "orderstatus"))))));
    }

    @Test
    public void testOnlyNullPredicateIsPushDownThroughJoinFilters()
    {
        assertPlan(
                """
                WITH t(a) AS (VALUES 'a', 'b')
                SELECT *
                FROM t t1 JOIN t t2 ON true
                WHERE t1.a = 'aa'
                """,
                output(values("field", "field_0")));
    }

    private Session noSemiJoinRewrite()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "false")
                .build();
    }
}
