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

import com.google.common.collect.ImmutableMap;
import io.trino.SystemSessionProperties;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;

public class TestEliminateCrossJoins
        extends BasePlanTest
{
    private static final PlanMatchPattern ORDERS_TABLESCAN = tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"));
    private static final PlanMatchPattern ORDERS_WITH_SHIPPRIORITY_TABLESCAN = tableScan(
            "orders",
            ImmutableMap.of("O_ORDERKEY", "orderkey", "O_SHIPPRIORITY", "shippriority"));
    private static final PlanMatchPattern PART_TABLESCAN = tableScan("part", ImmutableMap.of("P_PARTKEY", "partkey"));
    private static final PlanMatchPattern PART_WITH_NAME_TABLESCAN = tableScan("part", ImmutableMap.of("P_PARTKEY", "partkey", "P_NAME", "name"));
    private static final PlanMatchPattern LINEITEM_TABLESCAN = tableScan(
            "lineitem",
            ImmutableMap.of(
                    "L_PARTKEY", "partkey",
                    "L_ORDERKEY", "orderkey"));
    private static final PlanMatchPattern LINEITEM_WITH_RETURNFLAG_TABLESCAN = tableScan(
            "lineitem",
            ImmutableMap.of(
                    "L_PARTKEY", "partkey",
                    "L_ORDERKEY", "orderkey",
                    "L_RETURNFLAG", "returnflag"));
    private static final PlanMatchPattern LINEITEM_WITH_COMMENT_TABLESCAN = tableScan(
            "lineitem",
            ImmutableMap.of(
                    "L_PARTKEY", "partkey",
                    "L_ORDERKEY", "orderkey",
                    "L_COMMENT", "comment"));

    public TestEliminateCrossJoins()
    {
        super(ImmutableMap.of(SystemSessionProperties.JOIN_REORDERING_STRATEGY, "ELIMINATE_CROSS_JOINS"));
    }

    @Test
    public void testEliminateSimpleCrossJoin()
    {
        assertPlan("SELECT * FROM part p, orders o, lineitem l WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("L_ORDERKEY", "O_ORDERKEY")
                                .left(
                                        anyTree(
                                                join(INNER, leftJoinBuilder -> leftJoinBuilder
                                                        .equiCriteria("P_PARTKEY", "L_PARTKEY")
                                                        .left(anyTree(PART_TABLESCAN))
                                                        .right(anyTree(LINEITEM_TABLESCAN)))))
                                .right(anyTree(ORDERS_TABLESCAN)))));
    }

    @Test
    public void testDoesNotReorderJoinsWhenNoCrossJoinPresent()
    {
        assertPlan("SELECT o1.orderkey, o2.custkey, o3.orderstatus, o4.totalprice " +
                        "FROM (orders o1 JOIN orders o2 ON o1.orderkey = o2.orderkey) " +
                        "JOIN (orders o3 JOIN orders o4 ON o3.orderkey = o4.orderkey) ON o1.orderkey = o3.orderkey",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("O1_ORDERKEY", "O3_ORDERKEY")
                                .left(
                                        join(INNER, leftJoinBuilder -> leftJoinBuilder
                                                .equiCriteria("O1_ORDERKEY", "O2_ORDERKEY")
                                                .left(anyTree(strictTableScan("orders", ImmutableMap.of("O1_ORDERKEY", "orderkey"))))
                                                .right(anyTree(strictTableScan("orders", ImmutableMap.of("O2_ORDERKEY", "orderkey", "O2_CUSTKEY", "custkey"))))))
                                .right(
                                        anyTree(
                                                join(INNER, rightJoinBuilder -> rightJoinBuilder
                                                        .equiCriteria("O3_ORDERKEY", "O4_ORDERKEY")
                                                        .left(anyTree(strictTableScan("orders", ImmutableMap.of("O3_ORDERKEY", "orderkey", "O3_ORDERSTATUS", "orderstatus"))))
                                                        .right(anyTree(strictTableScan("orders", ImmutableMap.of("O4_ORDERKEY", "orderkey", "O4_totalprice", "totalprice"))))))))));
    }

    @Test
    public void testGiveUpOnCrossJoin()
    {
        assertPlan("SELECT o.orderkey FROM part p, orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("O_ORDERKEY", "L_ORDERKEY")
                                .left(
                                        anyTree(
                                                join(INNER, leftJoinBuilder -> leftJoinBuilder
                                                        .left(tableScan("part"))
                                                        .right(anyTree(tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey")))))))
                                .right(
                                        anyTree(tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey")))))));
    }

    @Test
    public void testEliminateCrossJoinWithNonEqualityCondition()
    {
        // "p.name < l.comment" expression is automatically transformed to "p.name < cast(L_COMMENT AS varchar(55))"
        // with PushInequalityFilterExpressionBelowJoinRuleSet the cast "cast(L_COMMENT AS varchar(55))" is pushed down
        assertPlan("SELECT o.orderkey FROM part p, orders o, lineitem l " +
                        "WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey AND p.partkey <> o.orderkey AND p.name < l.comment",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("L_ORDERKEY", "O_ORDERKEY")
                                .left(
                                        anyTree(
                                                join(INNER, leftJoinBuilder -> leftJoinBuilder
                                                        .equiCriteria("P_PARTKEY", "L_PARTKEY")
                                                        .filter("P_NAME < expr")
                                                        .left(anyTree(PART_WITH_NAME_TABLESCAN))
                                                        .right(
                                                                anyTree(
                                                                        project(
                                                                                ImmutableMap.of("expr", expression("cast(L_COMMENT AS varchar(55))")),
                                                                                filter("L_PARTKEY <> L_ORDERKEY",
                                                                                        LINEITEM_WITH_COMMENT_TABLESCAN)))))))
                                .right(anyTree(ORDERS_TABLESCAN)))));
    }

    @Test
    public void testEliminateCrossJoinPreserveFilters()
    {
        assertPlan("SELECT o.orderkey FROM part p, orders o, lineitem l " +
                        "WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey AND l.returnflag = 'R' AND shippriority >= 10",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("L_ORDERKEY", "O_ORDERKEY")
                                .left(
                                        anyTree(
                                                join(INNER, leftJoinBuilder -> leftJoinBuilder
                                                        .equiCriteria("P_PARTKEY", "L_PARTKEY")
                                                        .left(anyTree(PART_TABLESCAN))
                                                        .right(anyTree(filter("L_RETURNFLAG = 'R'", LINEITEM_WITH_RETURNFLAG_TABLESCAN))))))
                                .right(
                                        anyTree(filter("O_SHIPPRIORITY >= 10", ORDERS_WITH_SHIPPRIORITY_TABLESCAN))))));
    }
}
