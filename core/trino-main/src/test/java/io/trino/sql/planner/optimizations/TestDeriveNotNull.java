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
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.DERIVE_ISNOTNULL_PREDICATES;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;

public class TestDeriveNotNull
        extends BasePlanTest
{
    TestDeriveNotNull()
    {
        // Dynamic filtering creates a $internal$dynamic_filter_function which acts as a null-rejected condition by accident.
        // Need to disable it to rule out its side effect impacting outer to inner transformation in subquery, and only let the IS_NOT_NULL predicate do its job.
        // Also, set JOIN_REORDERING_STRATEGY to NONE to make sure the join orders in testSubqueryOuterToInner() follows the literal sequence.
        super(ImmutableMap.of(ENABLE_DYNAMIC_FILTERING, "false",
                JOIN_REORDERING_STRATEGY, "none",
                DERIVE_ISNOTNULL_PREDICATES, "true"));
    }

    @Test
    public void testInnerJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o INNER JOIN lineitem l ON o.orderkey = l.orderkey",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                Optional.empty(),
                                project(
                                        filter("NOT (ORDERS_OK IS NULL)",
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))),
                                exchange(
                                        project(
                                                filter("NOT (LINEITEM_OK IS NULL)",
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))))));

        // Implicit
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey = l.orderkey",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                Optional.empty(),
                                project(
                                        filter("NOT (ORDERS_OK IS NULL)",
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))),
                                exchange(
                                        project(
                                                filter("NOT (LINEITEM_OK IS NULL)",
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))))));
    }

    @Test
    public void testInnerJoinWithSimpleExpression()
    {
        // A simple addition expression on the join clause
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey + 1 = l.orderkey",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERKEY_PLUS_ONE", "LINEITEM_OK")),
                                Optional.empty(),
                                project(
                                        project(ImmutableMap.of("ORDERKEY_PLUS_ONE", expression("ORDERS_OK + BIGINT '1'")),
                                                filter("NOT (ORDERS_OK + BIGINT '1') IS NULL",
                                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))),
                                exchange(
                                        project(
                                                filter("NOT (LINEITEM_OK IS NULL)",
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))))));
    }

    @Test
    public void testInnerJoinWithCast()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE cast(l.orderkey as int) = cast(o.orderkey as int)",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_CAST", "LINEITEM_CAST")),
                                project(
                                        project(ImmutableMap.of("ORDERS_CAST", expression("CAST(ORDERS_OK AS INT)")),
                                                filter(PlanBuilder.expression("NOT (CAST(ORDERS_OK AS INT)) IS NULL"),
                                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))),
                                exchange(
                                        project(
                                                project(ImmutableMap.of("LINEITEM_CAST", expression("CAST(LINEITEM_OK AS INT)")),
                                                        filter(PlanBuilder.expression("NOT (CAST(LINEITEM_OK AS INT)) IS NULL"),
                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))))));
    }

    @Test
    public void testInnerJoinWithTrivialFilter()
    {
        // existing filter stays as is
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey = l.orderkey AND o.custkey = 1",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                Optional.empty(),
                                project(
                                        filter("(ORDERS_CK = BIGINT '1') AND (NOT (ORDERS_OK IS NULL))",
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey", "ORDERS_CK", "custkey")))),
                                exchange(
                                        project(
                                                filter("NOT (LINEITEM_OK IS NULL)",
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))))));

        // partitioning filter is not treated as a regular filter
        // Is orderstatus a partitioning column?
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey = l.orderkey AND o.orderstatus = 'O'",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                Optional.empty(),
                                project(
                                        filter("NOT (ORDERS_OK IS NULL)",
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))),
                                exchange(
                                        project(
                                                filter("NOT (LINEITEM_OK IS NULL)",
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))))));
    }

    @Test
    public void testInnerJoinWithInequalityFilter()
    {
        assertPlan("SELECT 1 FROM orders o JOIN lineitem l ON o.shippriority = l.linenumber AND o.orderkey < l.orderkey",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_SP", "LINEITEM_LN")),
                                Optional.of("ORDERS_OK < LINEITEM_OK"),
                                project(
                                        filter("NOT (ORDERS_SP IS NULL)",
                                                tableScan("orders", ImmutableMap.of("ORDERS_SP", "shippriority", "ORDERS_OK", "orderkey")))),
                                exchange(
                                        project(
                                                filter("NOT (LINEITEM_LN IS NULL)",
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_LN", "linenumber", "LINEITEM_OK", "orderkey"))))))));
    }

    @Test
    public void testInnerJoinWithDuplicateFilter()
    {
        // It's OK we derive a new IS NOT NULL predicate, as the de-dup is handled by PredicatePushDown -> combineConjuncts -> removeDuplicates
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey = l.orderkey AND o.orderkey IS NOT NULL",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                Optional.empty(),
                                project(
                                        filter("NOT (ORDERS_OK IS NULL)",
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))),
                                exchange(
                                        project(
                                                filter("NOT (LINEITEM_OK IS NULL)",
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))))));
    }

    @Test
    public void testInnerJoinWithConflictingFilter()
    {
        // the derived IS NOT NULL predicate and the existing IS NULL predicate conflict, and results in a FALSE join condition
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey = l.orderkey AND o.orderkey IS NULL",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("orderkey", "orderkey_0")),
                                Optional.empty(),
                                project(values("orderkey")),
                                project(values("orderkey_0")))));
    }

    @Test
    public void testInnerJoinWithConditionalJoinClause()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey = CASE WHEN l.orderkey = 1 THEN 2 END",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_CASE")),
                                Optional.empty(),
                                project(
                                        filter("NOT (ORDERS_OK IS NULL)",
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))),
                                exchange(
                                        project(
                                                project(ImmutableMap.of("LINEITEM_CASE", expression("CAST((CASE WHEN (LINEITEM_OK = BIGINT '1') THEN 2 END) AS bigint)")),
                                                        filter("(NOT (CAST((CASE WHEN (LINEITEM_OK = BIGINT '1') THEN 2 END) AS bigint) IS NULL))",
                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))))));

        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey = COALESCE(l.orderkey, 1)",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_COALESCE")),
                                Optional.empty(),
                                project(
                                        filter("NOT (ORDERS_OK IS NULL)",
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))),
                                exchange(
                                        project(
                                                project(ImmutableMap.of("LINEITEM_COALESCE", expression("COALESCE(LINEITEM_OK, BIGINT '1')")),
                                                        filter("NOT (COALESCE(LINEITEM_OK, BIGINT '1')) IS NULL",
                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))))));
    }

    @Test
    public void testInnerJoinWithNonDeterministic()
    {
        assertPlan("SELECT o.orderkey FROM orders o INNER JOIN lineitem l ON o.orderkey = l.orderkey + random(100)",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_RAND")),
                                Optional.empty(),
                                project(
                                        filter("NOT (ORDERS_OK IS NULL)",
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))),
                                exchange(
                                        project(
                                                filter("NOT LINEITEM_RAND IS NULL",
                                                        project(ImmutableMap.of("LINEITEM_RAND", expression("LINEITEM_OK + CAST(random(100) AS bigint)")),
                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))))));
    }

    @Test
    public void testConjunctiveEquiJoinClauses()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey AND l.partkey = o.custkey",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK"),
                                        equiJoinClause("ORDERS_CK", "LINEITEM_PK")),
                                Optional.empty(),
                                project(
                                        filter("(NOT (ORDERS_OK IS NULL)) AND (NOT (ORDERS_CK IS NULL))",
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey", "ORDERS_CK", "custkey")))),
                                exchange(
                                        project(
                                                filter("(NOT (LINEITEM_OK IS NULL)) AND (NOT (LINEITEM_PK IS NULL))",
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey", "LINEITEM_PK", "partkey"))))))));
    }

    @Test
    public void testMultiColumnEquiJoinClause()
    {
        // columns from the same table on one side
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey + o.custkey = l.orderkey + l.partkey",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDER_SUM", "LINEITEM_SUM")),
                                Optional.empty(),
                                project(
                                        project(ImmutableMap.of("ORDER_SUM", expression("ORDERS_OK + ORDERS_CK")),
                                                filter("NOT (ORDERS_OK + ORDERS_CK) IS NULL",
                                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey", "ORDERS_CK", "custkey"))))),
                                exchange(
                                        project(
                                                project(ImmutableMap.of("LINEITEM_SUM", expression("LINEITEM_OK + LINEITEM_PK")),
                                                        filter("NOT (LINEITEM_OK + LINEITEM_PK) IS NULL",
                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey", "LINEITEM_PK", "partkey")))))))));

        // columns from the different tables on one side
        // This results in CROSS JOIN, so no derived predicates
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey + l.orderkey = 10",
                anyTree(
                        filter("ORDERS_OK + LINEITEM_OK = BIGINT '10'",
                                join(INNER,
                                        ImmutableList.of(),
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testSubqueryOuterToInner()
    {
        // In the subquery the outer join is transformed to inner join thanks to the IS_NOT_NULL filter generated from outer query

        // LEFT OUTER => INNER
        assertPlan("SELECT totalprice " +
                        "FROM (SELECT orders.totalprice, orders.custkey " +
                        "      FROM lineitem LEFT JOIN orders " +
                        "      ON lineitem.orderkey = orders.orderkey) t, customer " +
                        "WHERE customer.custkey = t.custkey " +
                        "AND customer.name = 'Joe'",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_CK", "CUSTOMER_CK")),
                                Optional.empty(),
                                project(
                                        join(INNER,
                                                ImmutableList.of(equiJoinClause("LINEITEM_OK", "ORDERS_OK")),
                                                Optional.empty(),
                                                project(
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))),
                                                exchange(
                                                        project(
                                                                filter("NOT (ORDERS_CK IS NULL)",
                                                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey", "ORDERS_CK", "custkey", "ORDERS_TP", "totalprice"))))))),
                                exchange(
                                        project(
                                                filter("(CUSTOMER_NM = CAST('Joe' AS varchar(25))) AND (NOT (CUSTOMER_CK IS NULL))",
                                                        tableScan("customer", ImmutableMap.of("CUSTOMER_CK", "custkey", "CUSTOMER_NM", "name"))))))));

        // RIGHT OUTER => INNER
        // This is just a symmetry to the above query
        assertPlan("SELECT totalprice " +
                        "FROM (SELECT orders.totalprice, orders.custkey " +
                        "      FROM orders RIGHT JOIN lineitem" +
                        "      ON lineitem.orderkey = orders.orderkey) t, customer " +
                        "WHERE customer.custkey = t.custkey " +
                        "AND customer.name = 'Joe'",
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_CK", "CUSTOMER_CK")),
                                Optional.empty(),
                                project(
                                        join(INNER,
                                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                                Optional.empty(),
                                                project(
                                                        filter("NOT (ORDERS_CK IS NULL)",
                                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey", "ORDERS_CK", "custkey", "ORDERS_TP", "totalprice")))),
                                                exchange(
                                                        project(
                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))),
                                exchange(
                                        project(
                                                filter("(CUSTOMER_NM = CAST('Joe' AS varchar(25))) AND (NOT (CUSTOMER_CK IS NULL))",
                                                        tableScan("customer", ImmutableMap.of("CUSTOMER_CK", "custkey", "CUSTOMER_NM", "name"))))))));
    }

    // -- Below are negative tests, where no IS_NOT_NULL filters are derived --

    @Test
    public void testLeftOuterJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o LEFT JOIN lineitem l ON l.orderkey = o.orderkey",
                anyTree(
                        join(LEFT,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                project(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testRightOuterJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o RIGHT JOIN lineitem l ON l.orderkey = o.orderkey",
                anyTree(
                        join(RIGHT,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                project(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testFullOuterJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o FULL JOIN lineitem l ON l.orderkey = o.orderkey",
                anyTree(
                        join(FULL,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                project(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testCrossJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o CROSS JOIN lineitem l",
                anyTree(
                        join(INNER,
                                ImmutableList.of(),
                                tableScan("orders"),
                                exchange(
                                        tableScan("lineitem")))));

        // Implicit
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey = 1",
                anyTree(
                        join(INNER,
                                ImmutableList.of(),
                                filter("ORDERS_OK = BIGINT '1'",
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(
                                        tableScan("lineitem")))));

        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey <> l.orderkey",
                anyTree(
                        filter("ORDERS_OK <> LINEITEM_OK",
                                join(INNER,
                                        ImmutableList.of(),
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testNonEquiJoinClause()
    {   // this is converted to CROSS JOIN.
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey > l.orderkey",
                anyTree(
                        filter("ORDERS_OK > LINEITEM_OK",
                                join(INNER,
                                        ImmutableList.of(),
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testDisjunctiveEquiJoinClauses()
    {   // this is converted to CROSS JOIN.
        assertPlan("SELECT o.orderkey FROM orders o INNER JOIN lineitem l ON l.orderkey = o.orderkey OR l.partkey = o.custkey",
                anyTree(
                        filter("LINEITEM_OK = ORDERS_OK OR LINEITEM_PK = ORDERS_CK",
                                join(INNER,
                                        ImmutableList.of(),
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey", "ORDERS_CK", "custkey")),
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey", "LINEITEM_PK", "partkey")))))));

        // Implicit
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey OR l.partkey = o.custkey",
                anyTree(
                        filter("LINEITEM_OK = ORDERS_OK OR LINEITEM_PK = ORDERS_CK",
                                join(INNER,
                                        ImmutableList.of(),
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey", "ORDERS_CK", "custkey")),
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey", "LINEITEM_PK", "partkey")))))));
    }
}
