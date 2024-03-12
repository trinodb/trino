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
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.assertions.PlanMatchPattern.DynamicFilterPattern;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyNot;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
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
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.SUBTRACT;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;

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
                        join(LEFT, builder -> builder
                                .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                                .left(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testFullEquiJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o FULL JOIN lineitem l ON l.orderkey = o.orderkey",
                anyTree(
                        join(FULL, builder -> builder
                                .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                                .left(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testRightEquiJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o RIGHT JOIN lineitem l ON l.orderkey = o.orderkey",
                anyTree(
                        join(RIGHT, builder -> builder
                                .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                                .dynamicFilter("ORDERS_OK", "LINEITEM_OK")
                                .left(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testRightEquiJoinWithLeftExpression()
    {
        assertPlan("SELECT o.orderkey FROM orders o RIGHT JOIN lineitem l ON l.orderkey + 1 = o.orderkey",
                output(
                        join(RIGHT, builder -> builder
                                .equiCriteria("ORDERS_OK", "expr")
                                .dynamicFilter("ORDERS_OK", "expr")
                                .left(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                                .right(
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("expr", expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("LINEITEM_OK"), new GenericLiteral("BIGINT", "1")))),
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))))));
    }

    @Test
    public void testRightNonEquiJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o RIGHT JOIN lineitem l ON l.orderkey < o.orderkey",
                anyTree(
                        join(RIGHT, builder -> builder
                                .left(
                                        filter(
                                                TRUE_LITERAL,
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))
                                .filter(new ComparisonExpression(LESS_THAN, new SymbolReference("LINEITEM_OK"), new SymbolReference("ORDERS_OK")))
                                .dynamicFilter(ImmutableList.of(new DynamicFilterPattern(new SymbolReference("ORDERS_OK"), GREATER_THAN, "LINEITEM_OK"))))));
    }

    @Test
    public void testEmptyJoinCriteria()
    {
        assertPlan("SELECT o.orderkey FROM orders o CROSS JOIN lineitem l",
                anyTree(
                        join(INNER, builder -> builder
                                .left(
                                        tableScan("orders"))
                                .right(
                                        exchange(tableScan("lineitem"))))));
    }

    @Test
    public void testCrossJoinInequalityDF()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey > l.orderkey",
                anyTree(filter(
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("O_ORDERKEY"), new SymbolReference("L_ORDERKEY")),
                        join(INNER, builder -> builder
                                .dynamicFilter(ImmutableList.of(new DynamicFilterPattern(new SymbolReference("O_ORDERKEY"), GREATER_THAN, "L_ORDERKEY")))
                                .left(
                                        filter(
                                                TRUE_LITERAL,
                                                tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey"))))))));
    }

    @Test
    public void testCrossJoinInequalityDFWithConditionReversed()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey < o.orderkey",
                anyTree(filter(
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("O_ORDERKEY"), new SymbolReference("L_ORDERKEY")),
                        join(INNER, builder -> builder
                                .dynamicFilter(ImmutableList.of(new DynamicFilterPattern(new SymbolReference("O_ORDERKEY"), GREATER_THAN, "L_ORDERKEY")))
                                .left(
                                        filter(
                                                TRUE_LITERAL,
                                                tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey"))))))));
    }

    @Test
    public void testCrossJoinBetweenDF()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey BETWEEN l.orderkey AND l.partkey",
                anyTree(filter(
                        new BetweenPredicate(new SymbolReference("O_ORDERKEY"), new SymbolReference("L_ORDERKEY"), new SymbolReference("L_PARTKEY")),
                        join(INNER, builder -> builder
                                .dynamicFilter(
                                        ImmutableList.of(
                                                new DynamicFilterPattern(new SymbolReference("O_ORDERKEY"), GREATER_THAN_OR_EQUAL, "L_ORDERKEY"),
                                                new DynamicFilterPattern(new SymbolReference("O_ORDERKEY"), LESS_THAN_OR_EQUAL, "L_PARTKEY")))
                                .left(
                                        filter(
                                                TRUE_LITERAL,
                                                tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey", "L_PARTKEY", "partkey"))))))));

        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey BETWEEN l.orderkey AND l.partkey - 1",
                anyTree(filter(
                        new BetweenPredicate(new SymbolReference("O_ORDERKEY"), new SymbolReference("L_ORDERKEY"), new ArithmeticBinaryExpression(SUBTRACT, new SymbolReference("L_PARTKEY"), new GenericLiteral("BIGINT", "1"))),
                        join(INNER, builder -> builder
                                .dynamicFilter(ImmutableList.of(new DynamicFilterPattern(new SymbolReference("O_ORDERKEY"), GREATER_THAN_OR_EQUAL, "L_ORDERKEY")))
                                .left(
                                        filter(
                                                TRUE_LITERAL,
                                                tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey", "L_PARTKEY", "partkey"))))))));

        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey BETWEEN l.orderkey + 1 AND l.partkey",
                anyTree(filter(
                        new BetweenPredicate(new SymbolReference("O_ORDERKEY"), new ArithmeticBinaryExpression(ADD, new SymbolReference("L_ORDERKEY"), new GenericLiteral("BIGINT", "1")), new SymbolReference("L_PARTKEY")),
                        join(INNER, builder -> builder
                                .dynamicFilter(ImmutableList.of(
                                        new DynamicFilterPattern(new SymbolReference("O_ORDERKEY"), LESS_THAN_OR_EQUAL, "L_PARTKEY")))
                                .left(
                                        filter(
                                                TRUE_LITERAL,
                                                tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey", "L_PARTKEY", "partkey"))))))));
    }

    @Test
    public void testCrossJoinInequalityWithCastOnTheLeft()
    {
        assertPlan("SELECT o.comment, l.comment FROM lineitem l, orders o WHERE o.comment < l.comment",
                anyTree(filter(
                        new ComparisonExpression(GREATER_THAN, new Cast(new SymbolReference("L_COMMENT"), dataType("varchar(79)")), new SymbolReference("O_COMMENT")),
                        join(INNER, builder -> builder
                                .dynamicFilter(ImmutableList.of(
                                        new DynamicFilterPattern(typeOnlyCast("L_COMMENT", varchar(79)), GREATER_THAN, "O_COMMENT", false)))
                                .left(
                                        filter(TRUE_LITERAL,
                                                tableScan("lineitem", ImmutableMap.of("L_COMMENT", "comment"))))
                                .right(
                                        exchange(
                                                tableScan("orders", ImmutableMap.of("O_COMMENT", "comment"))))))));
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
        return new Cast(new Symbol(symbol).toSymbolReference(), asDataType, false);
    }

    @Test
    public void testCrossJoinInequalityWithCastOnTheRight()
    {
        // "o.comment < l.comment" expression is automatically transformed to "o.comment < CAST(l.comment AS varchar(79))"
        // with PushInequalityFilterExpressionBelowJoinRuleSet the cast "CAST(l.comment AS varchar(79))" is pushed down and dynamic filters are applied
        assertPlan("SELECT o.comment, l.comment FROM orders o, lineitem l WHERE o.comment < l.comment",
                anyTree(
                        project(
                                filter(
                                        new ComparisonExpression(LESS_THAN, new SymbolReference("O_COMMENT"), new SymbolReference("expr")),
                                        join(INNER, builder -> builder
                                                .dynamicFilter(ImmutableList.of(new DynamicFilterPattern(new SymbolReference("O_COMMENT"), LESS_THAN, "expr")))
                                                .left(
                                                        filter(
                                                                TRUE_LITERAL,
                                                                tableScan("orders", ImmutableMap.of("O_COMMENT", "comment"))))
                                                .right(
                                                        anyTree(
                                                                project(
                                                                        ImmutableMap.of("expr", expression(new Cast(new SymbolReference("L_COMMENT"), dataType("varchar(79)")))),
                                                                        tableScan("lineitem",
                                                                                ImmutableMap.of("L_COMMENT", "comment"))))))))));
    }

    @Test
    public void testJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                                .dynamicFilter("ORDERS_OK", "LINEITEM_OK")
                                .left(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testInnerJoinWithConditionReversed()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey = l.orderkey",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                                .dynamicFilter("ORDERS_OK", "LINEITEM_OK")
                                .left(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testIsNotDistinctFromJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey IS NOT DISTINCT FROM o.orderkey",
                anyTree(filter(
                        new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("O_ORDERKEY"), new SymbolReference("L_ORDERKEY"))),
                        join(INNER, builder -> builder
                                .dynamicFilter(ImmutableList.of(new DynamicFilterPattern(new SymbolReference("O_ORDERKEY"), EQUAL, "L_ORDERKEY", true)))
                                .left(
                                        filter(
                                                TRUE_LITERAL,
                                                tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey"))))))));

        // Dynamic filter is not supported for IS DISTINCT FROM
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey IS DISTINCT FROM o.orderkey",
                anyTree(filter(
                        new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("O_ORDERKEY"), new SymbolReference("L_ORDERKEY")),
                        join(INNER, builder -> builder
                                .left(
                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey")))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey"))))))));

        // extendedprice and totalprice are of DOUBLE type, dynamic filter is not supported with IS NOT DISTINCT FROM clause on DOUBLE or REAL types
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.extendedprice IS NOT DISTINCT FROM o.totalprice",
                anyTree(filter(
                        new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("O_TOTALPRICE"), new SymbolReference("L_EXTENDEDPRICE"))),
                        join(INNER, builder -> builder
                                .left(
                                        tableScan("orders", ImmutableMap.of("O_TOTALPRICE", "totalprice")))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("L_EXTENDEDPRICE", "extendedprice"))))))));
    }

    @Test
    public void testNotDistinctFromLeftJoin()
    {
        // IS NOT DISTINCT FROM condition does not promote LEFT to INNER
        assertPlan("SELECT 1 FROM (SELECT o.orderkey FROM nation n LEFT JOIN orders o ON n.nationkey = o.orderkey) o JOIN lineitem l ON o.orderkey IS NOT DISTINCT FROM l.orderkey",
                anyTree(
                        join(INNER, builder -> builder
                                .left(
                                        join(LEFT, leftJoinBuilder -> leftJoinBuilder
                                                .equiCriteria("nationkey", "ORDERS_OK")
                                                .left(
                                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))
                                                .right(
                                                        anyTree(
                                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))))
                                .right(
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));

        // equi condition promotes LEFT to INNER
        assertPlan("SELECT 1 FROM (SELECT o.orderkey FROM nation n LEFT JOIN orders o ON n.nationkey = o.orderkey) o JOIN lineitem l ON o.orderkey = l.orderkey",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("nationkey", "LINEITEM_OK")
                                .left(
                                        join(INNER, leftJoinBuilder -> leftJoinBuilder
                                                .equiCriteria("nationkey", "ORDERS_OK")
                                                .left(
                                                        anyTree(
                                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))
                                                .right(
                                                        anyTree(
                                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))))
                                .right(
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testJoinOnCast()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE cast(l.orderkey as int) = cast(o.orderkey as int)",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("expr_orders", "expr_lineitem")
                                .left(
                                        project(
                                                ImmutableMap.of("expr_orders", expression(new Cast(new SymbolReference("ORDERS_OK"), dataType("int")))),
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                                .right(
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("expr_lineitem", expression(new Cast(new SymbolReference("LINEITEM_OK"), dataType("int")))),
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))))));

        // Dynamic filter is removed due to double cast on orders.orderkey
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = cast(o.orderkey as int)",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("expr_orders", "LINEITEM_OK")
                                .left(
                                        project(
                                                ImmutableMap.of("expr_orders", expression(new Cast(new Cast(new SymbolReference("ORDERS_OK"), dataType("int")), dataType("bigint")))),
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                                .right(
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testJoinImplicitCoercions()
    {
        // linenumber is integer and orderkey is bigint
        assertPlan("SELECT o.orderkey FROM lineitem l, orders o WHERE l.linenumber = o.orderkey",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("expr_linenumber", "ORDERS_OK")
                                .left(
                                        project(
                                                ImmutableMap.of("expr_linenumber", expression(new Cast(new SymbolReference("LINEITEM_LN"), dataType("bigint")))),
                                                node(FilterNode.class,
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_LN", "linenumber")))
                                                        .with(numberOfDynamicFilters(1))))
                                .right(
                                        exchange(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))))));
    }

    @Test
    public void testJoinMultipleEquiJoinClauses()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey AND l.partkey = o.custkey",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria(ImmutableList.of(
                                        equiJoinClause("ORDERS_OK", "LINEITEM_OK"),
                                        equiJoinClause("ORDERS_CK", "LINEITEM_PK")))
                                .dynamicFilter(ImmutableMap.of(
                                        new SymbolReference("ORDERS_OK"), "LINEITEM_OK",
                                        new SymbolReference("ORDERS_CK"), "LINEITEM_PK"))
                                .left(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey", "ORDERS_CK", "custkey"))))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey", "LINEITEM_PK", "partkey")))))));
    }

    @Test
    public void testJoinWithOrderBySameKey()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey ORDER BY l.orderkey ASC, o.orderkey ASC",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("ORDERS_OK", "LINEITEM_OK")
                                .dynamicFilter("ORDERS_OK", "LINEITEM_OK")
                                .left(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                                .right(
                                        exchange(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testUncorrelatedSubqueries()
    {
        assertPlan("SELECT * FROM orders WHERE orderkey = (SELECT orderkey FROM lineitem ORDER BY orderkey LIMIT 1)",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("X", "Y")
                                .dynamicFilter("X", "Y")
                                .left(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("X", "orderkey"))))
                                .right(
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
                                join(INNER, builder -> builder
                                        .equiCriteria("O_SHIPPRIORITY", "L_LINENUMBER")
                                        .filter(new ComparisonExpression(LESS_THAN, new SymbolReference("O_ORDERKEY"), new SymbolReference("L_ORDERKEY")))
                                        .dynamicFilter(ImmutableList.of(
                                                new DynamicFilterPattern(new SymbolReference("O_SHIPPRIORITY"), EQUAL, "L_LINENUMBER"),
                                                new DynamicFilterPattern(new SymbolReference("O_ORDERKEY"), LESS_THAN, "L_ORDERKEY")))
                                        .left(
                                                anyTree(tableScan("orders", ImmutableMap.of(
                                                        "O_SHIPPRIORITY", "shippriority",
                                                        "O_ORDERKEY", "orderkey"))))
                                        .right(
                                                anyTree(tableScan("lineitem", ImmutableMap.of(
                                                        "L_LINENUMBER", "linenumber",
                                                        "L_ORDERKEY", "orderkey"))))))));
    }

    @Test
    public void testSubTreeJoinDFOnProbeSide()
    {
        assertPlan(
                "SELECT part.partkey from part JOIN (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) ON part.partkey = lineitem.orderkey",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("PART_PK", "LINEITEM_OK")
                                .dynamicFilter("PART_PK", "LINEITEM_OK")
                                .left(
                                        anyTree(tableScan("part", ImmutableMap.of("PART_PK", "partkey"))))
                                .right(
                                        anyTree(
                                                join(INNER, rightJoinBuilder -> rightJoinBuilder
                                                        .equiCriteria("LINEITEM_OK", "ORDERS_OK")
                                                        .dynamicFilter("LINEITEM_OK", "ORDERS_OK")
                                                        .left(
                                                                anyTree(
                                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))
                                                        .right(
                                                                exchange(
                                                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))))))));
    }

    @Test
    public void testSubTreeJoinDFOnBuildSide()
    {
        assertPlan(
                "SELECT part.partkey from (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) JOIN part ON lineitem.orderkey = part.partkey",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("LINEITEM_OK", "PART_PK")
                                .dynamicFilter(ImmutableMap.of(
                                        new SymbolReference("LINEITEM_OK"), "PART_PK",
                                        new SymbolReference("ORDERS_OK"), "PART_PK"))
                                .left(
                                        join(INNER, leftJoinBuilder -> leftJoinBuilder
                                                .equiCriteria("LINEITEM_OK", "ORDERS_OK")
                                                .dynamicFilter("LINEITEM_OK", "ORDERS_OK")
                                                .left(
                                                        node(FilterNode.class,
                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))
                                                                .with(numberOfDynamicFilters(2)))
                                                .right(
                                                        anyTree(node(FilterNode.class,
                                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))
                                                                .with(numberOfDynamicFilters(1))))))
                                .right(
                                        exchange(
                                                tableScan("part", ImmutableMap.of("PART_PK", "partkey")))))));
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
                        join(INNER, builder -> builder
                                .equiCriteria("ORDERS_CK", "ORDERS_CK6")
                                .dynamicFilter("ORDERS_CK", "ORDERS_CK6")
                                .left(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("ORDERS_CK", "clerk"))))
                                .right(
                                        anyTree(
                                                join(LEFT, rightJoinBuilder -> rightJoinBuilder
                                                        .equiCriteria("ORDERS_CK16", "ORDERS_CK27")
                                                        .left(
                                                                join(LEFT, leftJoinBuilder -> leftJoinBuilder
                                                                        .equiCriteria("ORDERS_CK6", "ORDERS_CK16")
                                                                        .left(
                                                                                tableScan("orders", ImmutableMap.of("ORDERS_CK6", "clerk")))
                                                                        .right(
                                                                                exchange(
                                                                                        tableScan("orders", ImmutableMap.of("ORDERS_CK16", "clerk"))))))
                                                        .right(
                                                                anyTree(
                                                                        tableScan("orders", ImmutableMap.of("ORDERS_CK27", "clerk"))))))))));
    }

    @Test
    public void testNonPushedDownJoinFilterRemoval()
    {
        assertPlan(
                "SELECT 1 FROM part t0, part t1, part t2 " +
                        "WHERE t0.partkey = t1.partkey AND t0.partkey = t2.partkey " +
                        "AND t0.size + t1.size = t2.size",
                anyTree(
                        // Expected filter should be ImmutableMap.of("K0", "K2", "K1", "K2") but K1 symbol is not available when matching current join node
                        join(INNER, builder -> builder
                                .equiCriteria(ImmutableList.of(equiJoinClause("K0", "K2"), equiJoinClause("S", "V2")))
                                .left(
                                        project(
                                                ImmutableMap.of("S", expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("V0"), new SymbolReference("V1")))),
                                                join(INNER, leftJoinBuilder -> leftJoinBuilder
                                                        .equiCriteria("K0", "K1")
                                                        .dynamicFilter("K0", "K1")
                                                        .left(
                                                                node(
                                                                        FilterNode.class,
                                                                        tableScan("part", ImmutableMap.of("K0", "partkey", "V0", "size")))
                                                                        .with(numberOfDynamicFilters(2)))
                                                        .right(
                                                                exchange(
                                                                        node(
                                                                                FilterNode.class,
                                                                                tableScan("part", ImmutableMap.of("K1", "partkey", "V1", "size")))
                                                                                .with(numberOfDynamicFilters(1)))))))
                                .right(
                                        exchange(
                                                tableScan("part", ImmutableMap.of("K2", "partkey", "V2", "size")))))));
    }

    @Test
    public void testSemiJoin()
    {
        assertPlan(
                "SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber % 4 = 0)",
                noSemiJoinRewrite(),
                anyTree(
                        filter(
                                new SymbolReference("S"),
                                semiJoin("X", "Y", "S", true,
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("Y", "orderkey")))))));
    }

    @Test
    public void testNonFilteringSemiJoin()
    {
        // Dynamic filtering is not applied to non-filtering semi-join queries
        assertPlan(
                "SELECT * FROM orders WHERE orderkey NOT IN (SELECT orderkey FROM lineitem WHERE linenumber < 0)",
                anyTree(
                        filter(
                                new NotExpression(new SymbolReference("S")),
                                semiJoin("X", "Y", "S", false,
                                        tableScan("orders", ImmutableMap.of("X", "orderkey")),
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("Y", "orderkey")))))));

        assertPlan(
                "SELECT orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber < 0) FROM orders",
                anyTree(
                        semiJoin("X", "Y", "S", false,
                                tableScan("orders", ImmutableMap.of("X", "orderkey")),
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
                        filter(
                                new SymbolReference("S"),
                                semiJoin("X", "Y", "S", true,
                                        filter(
                                                new ComparisonExpression(GREATER_THAN, new SymbolReference("X"), new GenericLiteral("BIGINT", "0")),
                                                tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("Y", "orderkey")))))));
    }

    @Test
    public void testMultiSemiJoin()
    {
        assertPlan(
                "SELECT part.partkey FROM part WHERE part.partkey IN " +
                        "(SELECT lineitem.partkey FROM lineitem WHERE lineitem.orderkey IN (SELECT orders.orderkey FROM orders))",
                noSemiJoinRewrite(),
                anyTree(
                        filter(
                                new SymbolReference("S0"),
                                semiJoin("PART_PK", "LINEITEM_PK", "S0", true,
                                        anyTree(
                                                tableScan("part", ImmutableMap.of("PART_PK", "partkey"))),
                                        anyTree(
                                                filter(
                                                        new SymbolReference("S1"),
                                                        project(
                                                                semiJoin("LINEITEM_OK", "ORDERS_OK", "S1", true,
                                                                        anyTree(
                                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_PK", "partkey", "LINEITEM_OK", "orderkey"))),
                                                                        anyTree(
                                                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))))))))));
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
                        filter(
                                new SymbolReference("S0"),
                                semiJoin("LINEITEM_PK_PLUS_1000", "PART_PK", "S0", false,
                                        project(ImmutableMap.of("LINEITEM_PK_PLUS_1000", expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("LINEITEM_PK"), new GenericLiteral("BIGINT", "1000")))),
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_PK", "partkey"))),
                                        anyTree(
                                                tableScan("part", ImmutableMap.of("PART_PK", "partkey")))))));
    }

    @Test
    public void testExpressionPushedDownToLeftJoinSourceWhenUsingOn()
    {
        assertPlan("SELECT o.orderkey FROM orders o JOIN lineitem l ON o.orderkey + 1 < l.orderkey",
                anyTree(
                        filter(
                                new ComparisonExpression(LESS_THAN, new SymbolReference("expr"), new SymbolReference("LINEITEM_OK")),
                                join(INNER, builder -> builder
                                        .left(
                                                project(
                                                        ImmutableMap.of("expr", expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("ORDERS_OK"), new GenericLiteral("BIGINT", "1")))),
                                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                                        .right(
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))))));
    }

    @Test
    public void testExpressionPushedDownToRightJoinSourceWhenUsingOn()
    {
        assertPlan("SELECT o.orderkey FROM orders o JOIN lineitem l ON o.orderkey < l.orderkey + 1",
                anyTree(
                        filter(
                                new ComparisonExpression(LESS_THAN, new SymbolReference("ORDERS_OK"), new SymbolReference("expr")),
                                join(INNER, builder -> builder
                                        .dynamicFilter(ImmutableList.of(new DynamicFilterPattern(new SymbolReference("ORDERS_OK"), LESS_THAN, "expr")))
                                        .left(
                                                filter(TRUE_LITERAL,
                                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                                        .right(
                                                anyTree(
                                                        project(
                                                                ImmutableMap.of("expr", expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("LINEITEM_OK"), new GenericLiteral("BIGINT", "1")))),
                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))))));
    }

    @Test
    public void testExpressionNotPushedDownToLeftJoinSource()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey + 1 < l.orderkey",
                anyTree(filter(
                        new ComparisonExpression(LESS_THAN, new ArithmeticBinaryExpression(ADD, new SymbolReference("ORDERS_OK"), new GenericLiteral("BIGINT", "1")), new SymbolReference("LINEITEM_OK")),
                        join(INNER, builder -> builder
                                .left(tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))
                                .right(exchange(
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))))));
    }

    @Test
    public void testExpressionPushedDownToRightJoinSource()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE o.orderkey < l.orderkey + 1",
                anyTree(
                        filter(
                                new ComparisonExpression(LESS_THAN, new SymbolReference("ORDERS_OK"), new SymbolReference("expr")),
                                join(INNER, builder -> builder
                                        .dynamicFilter(ImmutableList.of(new DynamicFilterPattern(new SymbolReference("ORDERS_OK"), LESS_THAN, "expr")))
                                        .left(
                                                filter(
                                                        TRUE_LITERAL,
                                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))
                                        .right(
                                                anyTree(
                                                        project(
                                                                ImmutableMap.of("expr", expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("LINEITEM_OK"), new GenericLiteral("BIGINT", "1")))),
                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))))));
    }

    @Test
    public void testDynamicFilterAliasDeDuplicated()
    {
        assertPlan(
                "SELECT f.name FROM supplier f, nation d " +
                        // 'mod' function is used in the join condition to trigger expression push-down by PushInequalityFilterExpressionBelowJoinRuleSet
                        "WHERE f.nationkey >= mod(d.nationkey, 2) AND f.suppkey >= mod(d.nationkey, 2)",
                anyTree(
                        filter(
                                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("nationkey"), new SymbolReference("mod")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("suppkey"), new SymbolReference("mod")))),
                                join(INNER, builder -> builder
                                        .dynamicFilter(
                                                ImmutableList.of(
                                                        new DynamicFilterPattern(new SymbolReference("nationkey"), GREATER_THAN_OR_EQUAL, "mod"),
                                                        new DynamicFilterPattern(new SymbolReference("suppkey"), GREATER_THAN_OR_EQUAL, "mod")))
                                        .left(
                                                anyTree(
                                                        tableScan("supplier", ImmutableMap.of("nationkey", "nationkey", "suppkey", "suppkey"))))
                                        .right(
                                                anyTree(
                                                        project(
                                                                ImmutableMap.of("mod", expression(new FunctionCall(QualifiedName.of("mod"), ImmutableList.of(new SymbolReference("n_nationkey"), new GenericLiteral("BIGINT", "2"))))),
                                                                tableScan("nation", ImmutableMap.of("n_nationkey", "nationkey")))))))));
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
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "false")
                .build();
    }
}
