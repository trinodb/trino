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
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.MatchResult;
import io.trino.sql.planner.assertions.Matcher;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NumericParameter;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.assertions.PlanMatchPattern.DynamicFilterPattern;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyNot;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;

public class TestDynamicFilter
        extends BasePlanTest
{
    public TestDynamicFilter()
    {
        super(ImmutableMap.of(
                ENABLE_DYNAMIC_FILTERING, "true",
                JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name(),
                JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name()));
    }

    @Test
    public void testLeftEquiJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o LEFT JOIN lineitem l ON l.orderkey = o.orderkey",
                anyTree(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                ImmutableMap.of(),
                                project(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testFullEquiJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o FULL JOIN lineitem l ON l.orderkey = o.orderkey",
                anyTree(
                        join(
                                FULL,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                ImmutableMap.of(),
                                project(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testRightEquiJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o RIGHT JOIN lineitem l ON l.orderkey = o.orderkey",
                anyTree(
                        join(
                                RIGHT,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                ImmutableMap.of("ORDERS_OK", "LINEITEM_OK"),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testRightEquiJoinWithLeftExpression()
    {
        assertPlan("SELECT o.orderkey FROM orders o RIGHT JOIN lineitem l ON l.orderkey + 1 = o.orderkey",
                output(
                        join(
                                RIGHT,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "expr")),
                                ImmutableMap.of("ORDERS_OK", "expr"),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                anyTree(
                                        project(
                                                ImmutableMap.of("expr", expression("LINEITEM_OK + BIGINT '1'")),
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testRightNonEquiJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o RIGHT JOIN lineitem l ON l.orderkey < o.orderkey",
                anyTree(
                        join(
                                RIGHT,
                                ImmutableList.of(),
                                Optional.of("LINEITEM_OK < ORDERS_OK"),
                                Optional.of(ImmutableList.of(new DynamicFilterPattern("ORDERS_OK", GREATER_THAN, "LINEITEM_OK"))),
                                Optional.empty(),
                                Optional.empty(),
                                filter(
                                        TRUE_LITERAL,
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
    }

    @Test
    public void testEmptyJoinCriteria()
    {
        assertPlan("SELECT o.orderkey FROM orders o CROSS JOIN lineitem l",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(),
                                ImmutableMap.of(),
                                tableScan("orders"),
                                exchange(
                                        tableScan("lineitem")))));
    }

    @Test
    public void testCrossJoinInequalityDF()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey > l.orderkey",
                anyTree(filter("O_ORDERKEY > L_ORDERKEY",
                        join(
                                INNER,
                                ImmutableList.of(),
                                ImmutableList.of(new DynamicFilterPattern("O_ORDERKEY", GREATER_THAN, "L_ORDERKEY")),
                                filter(
                                        TRUE_LITERAL,
                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))),
                                exchange(
                                        tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey")))))));
    }

    @Test
    public void testCrossJoinInequalityDFWithConditionReversed()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey < o.orderkey",
                anyTree(filter("O_ORDERKEY > L_ORDERKEY",
                        join(
                                INNER,
                                ImmutableList.of(),
                                ImmutableList.of(new DynamicFilterPattern("O_ORDERKEY", GREATER_THAN, "L_ORDERKEY")),
                                filter(
                                        TRUE_LITERAL,
                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))),
                                exchange(
                                        tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey")))))));
    }

    @Test
    public void testCrossJoinBetweenDF()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey BETWEEN l.orderkey AND l.partkey",
                anyTree(filter("O_ORDERKEY BETWEEN L_ORDERKEY AND L_PARTKEY",
                        join(
                                INNER,
                                ImmutableList.of(),
                                ImmutableList.of(
                                        new DynamicFilterPattern("O_ORDERKEY", GREATER_THAN_OR_EQUAL, "L_ORDERKEY"),
                                        new DynamicFilterPattern("O_ORDERKEY", LESS_THAN_OR_EQUAL, "L_PARTKEY")),
                                filter(
                                        TRUE_LITERAL,
                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))),
                                exchange(
                                        tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey", "L_PARTKEY", "partkey")))))));

        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey BETWEEN l.orderkey AND l.partkey - 1",
                anyTree(filter("O_ORDERKEY BETWEEN L_ORDERKEY AND L_PARTKEY - BIGINT '1'",
                        join(
                                INNER,
                                ImmutableList.of(),
                                ImmutableList.of(
                                        new DynamicFilterPattern("O_ORDERKEY", GREATER_THAN_OR_EQUAL, "L_ORDERKEY")),
                                filter(
                                        TRUE_LITERAL,
                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))),
                                exchange(
                                        tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey", "L_PARTKEY", "partkey")))))));

        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey BETWEEN l.orderkey + 1 AND l.partkey",
                anyTree(filter("O_ORDERKEY BETWEEN L_ORDERKEY + BIGINT '1' AND L_PARTKEY",
                        join(
                                INNER,
                                ImmutableList.of(),
                                ImmutableList.of(
                                        new DynamicFilterPattern("O_ORDERKEY", LESS_THAN_OR_EQUAL, "L_PARTKEY")),
                                filter(
                                        TRUE_LITERAL,
                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))),
                                exchange(
                                        tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey", "L_PARTKEY", "partkey")))))));
    }

    @Test
    public void testCrossJoinInequalityWithCastOnTheLeft()
    {
        assertPlan("SELECT o.comment, l.comment FROM lineitem l, orders o WHERE o.comment < l.comment",
                anyTree(filter("CAST(L_COMMENT AS varchar(79)) > O_COMMENT",
                        join(
                                INNER,
                                ImmutableList.of(),
                                ImmutableList.of(
                                        new DynamicFilterPattern(
                                                typeOnlyCast("L_COMMENT", varchar(79)),
                                                GREATER_THAN,
                                                "O_COMMENT",
                                                false)),
                                filter(TRUE_LITERAL,
                                        tableScan("lineitem", ImmutableMap.of("L_COMMENT", "comment"))),
                                exchange(
                                        tableScan("orders", ImmutableMap.of("O_COMMENT", "comment")))))));
    }

    private DataType varchar(int size)
    {
        return new GenericDataType(
                Optional.empty(),
                new Identifier("varchar"),
                ImmutableList.of(new NumericParameter(Optional.empty(), String.valueOf(size))));
    }

    private Expression typeOnlyCast(String symbol, DataType asDataType)
    {
        return new Cast(new Symbol(symbol).toSymbolReference(), asDataType, false, true);
    }

    @Test
    public void testCrossJoinInequalityWithCastOnTheRight()
    {
        assertPlan("SELECT o.comment, l.comment FROM orders o, lineitem l WHERE o.comment < l.comment",
                anyTree(filter("O_COMMENT < CAST(L_COMMENT AS varchar(79))",
                        join(
                                INNER,
                                ImmutableList.of(),
                                tableScan("orders", ImmutableMap.of("O_COMMENT", "comment")),
                                exchange(
                                        tableScan("lineitem", ImmutableMap.of("L_COMMENT", "comment")))))));
    }

    @Test
    public void testJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                ImmutableMap.of("ORDERS_OK", "LINEITEM_OK"),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testInnerJoinWithConditionReversed()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey = l.orderkey",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                ImmutableMap.of("ORDERS_OK", "LINEITEM_OK"),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testIsNotDistinctFromJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey IS NOT DISTINCT FROM o.orderkey",
                anyTree(filter("O_ORDERKEY IS NOT DISTINCT FROM L_ORDERKEY",
                        join(
                                INNER,
                                ImmutableList.of(),
                                ImmutableList.of(new DynamicFilterPattern("O_ORDERKEY", EQUAL, "L_ORDERKEY", true)),
                                filter(
                                        TRUE_LITERAL,
                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))),
                                exchange(
                                        tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey")))))));

        // Dynamic filter is not supported for IS DISTINCT FROM
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey IS DISTINCT FROM o.orderkey",
                anyTree(filter("O_ORDERKEY IS DISTINCT FROM L_ORDERKEY",
                        join(
                                INNER,
                                ImmutableList.of(),
                                ImmutableList.of(),
                                tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey")),
                                exchange(
                                        tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey")))))));

        // extendedprice and totalprice are of DOUBLE type, dynamic filter is not supported with IS NOT DISTINCT FROM clause on DOUBLE or REAL types
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.extendedprice IS NOT DISTINCT FROM o.totalprice",
                anyTree(filter("O_TOTALPRICE IS NOT DISTINCT FROM L_EXTENDEDPRICE",
                        join(
                                INNER,
                                ImmutableList.of(),
                                ImmutableList.of(),
                                tableScan("orders", ImmutableMap.of("O_TOTALPRICE", "totalprice")),
                                exchange(
                                        tableScan("lineitem", ImmutableMap.of("L_EXTENDEDPRICE", "extendedprice")))))));
    }

    @Test
    public void testNotDistinctFromLeftJoin()
    {
        // IS NOT DISTINCT FROM condition does not promote LEFT to INNER
        assertPlan("SELECT 1 FROM (SELECT o.orderkey FROM nation n LEFT JOIN orders o ON n.nationkey = o.orderkey) o JOIN lineitem l ON o.orderkey IS NOT DISTINCT FROM l.orderkey",
                anyTree(join(
                        INNER,
                        ImmutableList.of(),
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("nationkey", "ORDERS_OK")),
                                anyTree(
                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))),
                        anyTree(
                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));

        // equi condition promotes LEFT to INNER
        assertPlan("SELECT 1 FROM (SELECT o.orderkey FROM nation n LEFT JOIN orders o ON n.nationkey = o.orderkey) o JOIN lineitem l ON o.orderkey = l.orderkey",
                anyTree(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("nationkey", "LINEITEM_OK")),
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("nationkey", "ORDERS_OK")),
                                anyTree(
                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))),
                        anyTree(
                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
    }

    @Test
    public void testJoinOnCast()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE cast(l.orderkey as int) = cast(o.orderkey as int)",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("expr_orders", "expr_lineitem")),
                                ImmutableMap.of(),
                                anyTree(
                                        project(
                                                ImmutableMap.of("expr_orders", expression("CAST(ORDERS_OK AS int)")),
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))),
                                anyTree(
                                        project(
                                                ImmutableMap.of("expr_lineitem", expression("CAST(LINEITEM_OK AS int)")),
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));

        // Dynamic filter is removed due to double cast on orders.orderkey
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = cast(o.orderkey as int)",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("expr_orders", "LINEITEM_OK")),
                                ImmutableMap.of(),
                                anyTree(
                                        project(
                                                ImmutableMap.of("expr_orders", expression("CAST(CAST(ORDERS_OK AS int) AS bigint)")),
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))),
                                anyTree(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testJoinImplicitCoercions()
    {
        // linenumber is integer and orderkey is bigint
        assertPlan("SELECT o.orderkey FROM lineitem l, orders o WHERE l.linenumber = o.orderkey",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("expr_linenumber", "ORDERS_OK")),
                                anyTree(
                                        project(
                                                ImmutableMap.of("expr_linenumber", expression("CAST(LINEITEM_LN AS bigint)")),
                                                node(FilterNode.class,
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_LN", "linenumber")))
                                                        .with(numberOfDynamicFilters(1)))),
                                exchange(
                                        project(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))))));
    }

    @Test
    public void testJoinMultipleEquiJoinClauses()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey AND l.partkey = o.custkey",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(
                                        equiJoinClause("ORDERS_OK", "LINEITEM_OK"),
                                        equiJoinClause("ORDERS_CK", "LINEITEM_PK")),
                                ImmutableMap.of("ORDERS_OK", "LINEITEM_OK", "ORDERS_CK", "LINEITEM_PK"),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey", "ORDERS_CK", "custkey"))),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey", "LINEITEM_PK", "partkey")))))));
    }

    @Test
    public void testJoinWithOrderBySameKey()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey ORDER BY l.orderkey ASC, o.orderkey ASC",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                ImmutableMap.of("ORDERS_OK", "LINEITEM_OK"),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testUncorrelatedSubqueries()
    {
        assertPlan("SELECT * FROM orders WHERE orderkey = (SELECT orderkey FROM lineitem ORDER BY orderkey LIMIT 1)",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("X", "Y")),
                                ImmutableMap.of("X", "Y"),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                project(
                                        node(
                                                EnforceSingleRowNode.class,
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));
    }

    @Test
    public void testInnerInequalityJoinWithEquiJoinConjuncts()
    {
        assertPlan("SELECT 1 FROM orders o JOIN lineitem l ON o.shippriority = l.linenumber AND o.orderkey < l.orderkey",
                anyTree(
                        anyNot(
                                FilterNode.class,
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("O_SHIPPRIORITY", "L_LINENUMBER")),
                                        Optional.of("O_ORDERKEY < L_ORDERKEY"),
                                        Optional.of(ImmutableList.of(
                                                new DynamicFilterPattern("O_SHIPPRIORITY", EQUAL, "L_LINENUMBER"),
                                                new DynamicFilterPattern("O_ORDERKEY", LESS_THAN, "L_ORDERKEY"))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        anyTree(tableScan("orders", ImmutableMap.of(
                                                "O_SHIPPRIORITY", "shippriority",
                                                "O_ORDERKEY", "orderkey"))),
                                        anyTree(tableScan("lineitem", ImmutableMap.of(
                                                "L_LINENUMBER", "linenumber",
                                                "L_ORDERKEY", "orderkey")))))));
    }

    @Test
    public void testSubTreeJoinDFOnProbeSide()
    {
        assertPlan(
                "SELECT part.partkey from part JOIN (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) ON part.partkey = lineitem.orderkey",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("PART_PK", "LINEITEM_OK")),
                                ImmutableMap.of("PART_PK", "LINEITEM_OK"),
                                anyTree(
                                        tableScan("part", ImmutableMap.of("PART_PK", "partkey"))),
                                anyTree(
                                        join(
                                                INNER,
                                                ImmutableList.of(equiJoinClause("LINEITEM_OK", "ORDERS_OK")),
                                                ImmutableMap.of("LINEITEM_OK", "ORDERS_OK"),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))),
                                                exchange(
                                                        project(tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))))))));
    }

    @Test
    public void testSubTreeJoinDFOnBuildSide()
    {
        assertPlan(
                "SELECT part.partkey from (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) JOIN part ON lineitem.orderkey = part.partkey",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("LINEITEM_OK", "PART_PK")),
                                ImmutableMap.of("LINEITEM_OK", "PART_PK", "ORDERS_OK", "PART_PK"),
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("LINEITEM_OK", "ORDERS_OK")),
                                        ImmutableMap.of("LINEITEM_OK", "ORDERS_OK"),
                                        anyTree(node(FilterNode.class,
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))
                                                .with(numberOfDynamicFilters(2))),
                                        anyTree(node(FilterNode.class,
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))
                                                .with(numberOfDynamicFilters(1)))),
                                exchange(
                                        project(tableScan("part", ImmutableMap.of("PART_PK", "partkey")))))));
    }

    @Test
    public void testNestedDynamicFiltersRemoval()
    {
        assertPlan(
                "WITH t AS (" +
                        "  SELECT o.clerk FROM (" +
                        "    (orders o LEFT JOIN orders o1 ON o1.clerk = o.clerk) " +
                        "      LEFT JOIN orders o2 ON o2.clerk = o1.clerk)" +
                        ") " +
                        "SELECT t.clerk " +
                        "FROM orders o3 JOIN t ON t.clerk = o3.clerk",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_CK", "ORDERS_CK6")),
                                ImmutableMap.of("ORDERS_CK", "ORDERS_CK6"),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_CK", "clerk"))),
                                anyTree(
                                        join(
                                                LEFT,
                                                ImmutableList.of(equiJoinClause("ORDERS_CK16", "ORDERS_CK27")),
                                                anyTree(
                                                        join(
                                                                LEFT,
                                                                ImmutableList.of(equiJoinClause("ORDERS_CK6", "ORDERS_CK16")),
                                                                project(
                                                                        tableScan("orders", ImmutableMap.of("ORDERS_CK6", "clerk"))),
                                                                exchange(
                                                                        project(
                                                                                tableScan("orders", ImmutableMap.of("ORDERS_CK16", "clerk")))))),
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("ORDERS_CK27", "clerk"))))))));
    }

    @Test
    public void testNonPushedDownJoinFilterRemoval()
    {
        assertPlan(
                "SELECT 1 FROM part t0, part t1, part t2 " +
                        "WHERE t0.partkey = t1.partkey AND t0.partkey = t2.partkey " +
                        "AND t0.size + t1.size = t2.size",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("K0", "K2"), equiJoinClause("S", "V2")),
                                Optional.empty(), // Should be ImmutableMap.of("K0", "K2", "K1", "K2") but K1 symbol is not available when matching current join node
                                project(
                                        project(
                                                ImmutableMap.of("S", expression("V0 + V1")),
                                                join(
                                                        INNER,
                                                        ImmutableList.of(equiJoinClause("K0", "K1")),
                                                        ImmutableMap.of("K0", "K1"),
                                                        project(
                                                                node(
                                                                        FilterNode.class,
                                                                        tableScan("part", ImmutableMap.of("K0", "partkey", "V0", "size")))
                                                                        .with(numberOfDynamicFilters(2))),
                                                        exchange(
                                                                project(
                                                                        node(
                                                                                FilterNode.class,
                                                                                tableScan("part", ImmutableMap.of("K1", "partkey", "V1", "size")))
                                                                                .with(numberOfDynamicFilters(1))))))),
                                exchange(
                                        project(
                                                tableScan("part", ImmutableMap.of("K2", "partkey", "V2", "size")))))));
    }

    @Test
    public void testSemiJoin()
    {
        assertPlan(
                "SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber % 4 = 0)",
                noSemiJoinRewrite(),
                anyTree(
                        filter("S",
                                project(
                                        semiJoin("X", "Y", "S", true,
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));
    }

    @Test
    public void testNonFilteringSemiJoin()
    {
        // Dynamic filtering is not applied to non-filtering semi-join queries
        assertPlan(
                "SELECT * FROM orders WHERE orderkey NOT IN (SELECT orderkey FROM lineitem WHERE linenumber < 0)",
                anyTree(
                        filter("NOT S",
                                project(
                                        semiJoin("X", "Y", "S", false,
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));

        assertPlan(
                "SELECT orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber < 0) FROM orders",
                anyTree(
                        semiJoin("X", "Y", "S", false,
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))));
    }

    @Test
    public void testSemiJoinWithStaticFiltering()
    {
        assertPlan(
                "SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber % 4 = 0) AND orderkey > 0",
                noSemiJoinRewrite(),
                anyTree(
                        filter("S",
                                project(
                                        semiJoin("X", "Y", "S", true,
                                                anyTree(
                                                        filter("X > BIGINT '0'",
                                                                tableScan("orders", ImmutableMap.of("X", "orderkey")))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));
    }

    @Test
    public void testMultiSemiJoin()
    {
        assertPlan(
                "SELECT part.partkey FROM part WHERE part.partkey IN " +
                        "(SELECT lineitem.partkey FROM lineitem WHERE lineitem.orderkey IN (SELECT orders.orderkey FROM orders))",
                noSemiJoinRewrite(),
                anyTree(
                        filter("S0",
                                project(
                                        semiJoin("PART_PK", "LINEITEM_PK", "S0", true,
                                                anyTree(
                                                        tableScan("part", ImmutableMap.of("PART_PK", "partkey"))),
                                                anyTree(
                                                        filter("S1",
                                                                project(
                                                                        semiJoin("LINEITEM_OK", "ORDERS_OK", "S1", true,
                                                                                anyTree(
                                                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_PK", "partkey", "LINEITEM_OK", "orderkey"))),
                                                                                anyTree(
                                                                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))))))))));
    }

    @Test
    public void testSemiJoinUnsupportedDynamicFilterRemoval()
    {
        // Dynamic filters are supported only after a table scan
        assertPlan(
                "WITH t AS (SELECT lineitem.partkey + 1000 partkey FROM lineitem) " +
                        "SELECT t.partkey FROM t WHERE t.partkey IN (SELECT part.partkey FROM part)",
                noSemiJoinRewrite(),
                anyTree(
                        filter("S0",
                                project(
                                        semiJoin("LINEITEM_PK_PLUS_1000", "PART_PK", "S0", false,
                                                anyTree(
                                                        project(ImmutableMap.of("LINEITEM_PK_PLUS_1000", expression("(LINEITEM_PK + BIGINT '1000')")),
                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_PK", "partkey")))),
                                                anyTree(
                                                        tableScan("part", ImmutableMap.of("PART_PK", "partkey"))))))));
    }

    private Matcher numberOfDynamicFilters(int numberOfDynamicFilters)
    {
        return new Matcher()
        {
            @Override
            public boolean shapeMatches(PlanNode node)
            {
                return node instanceof FilterNode;
            }

            @Override
            public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
            {
                FilterNode filterNode = (FilterNode) node;
                return new MatchResult(DynamicFilters.extractDynamicFilters(filterNode.getPredicate()).getDynamicConjuncts().size() == numberOfDynamicFilters);
            }
        };
    }

    private Session noSemiJoinRewrite()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "false")
                .build();
    }
}
