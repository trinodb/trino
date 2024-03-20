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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.ir.ComparisonExpression.Operator;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.ir.LogicalExpression.Operator.AND;
import static io.trino.sql.ir.LogicalExpression.and;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.INNER;

public class TestPushInequalityFilterExpressionBelowJoinRuleSet
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));

    private PushInequalityFilterExpressionBelowJoinRuleSet ruleSet;

    @BeforeAll
    public void setUpBeforeClass()
    {
        ruleSet = new PushInequalityFilterExpressionBelowJoinRuleSet(tester().getTypeAnalyzer());
    }

    @Test
    public void testExpressionNotPushedDownToLeftJoinSource()
    {
        tester().assertThat(ruleSet.pushJoinInequalityFilterExpressionBelowJoinRule())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            p.values(a),
                            p.values(b),
                            comparison(LESS_THAN, add(a, 1), b.toSymbolReference()));
                })
                .doesNotFire();
    }

    @Test
    public void testJoinFilterExpressionPushedDownToRightJoinSource()
    {
        tester().assertThat(ruleSet.pushJoinInequalityFilterExpressionBelowJoinRule())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            p.values(a),
                            p.values(b),
                            comparison(LESS_THAN, add(b, 1), a.toSymbolReference()));
                })
                .matches(
                        join(INNER, builder -> builder
                                .filter(new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "expr"), new SymbolReference(BIGINT, "a")))
                                .left(values("a"))
                                .right(project(
                                        ImmutableMap.of("expr", expression(new ArithmeticBinaryExpression(ADD_BIGINT, ADD, new SymbolReference(BIGINT, "b"), new Constant(BIGINT, 1L)))),
                                        values("b")))));
    }

    @Test
    public void testManyJoinFilterExpressionsPushedDownToRightJoinSource()
    {
        tester().assertThat(ruleSet.pushJoinInequalityFilterExpressionBelowJoinRule())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            p.values(a),
                            p.values(b),
                            and(
                                    comparison(LESS_THAN, add(b, 1), a.toSymbolReference()),
                                    comparison(GREATER_THAN, add(b, 10), a.toSymbolReference())));
                })
                .matches(
                        join(INNER, builder -> builder
                                .filter(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "expr_less"), new SymbolReference(BIGINT, "a")), new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "expr_greater"), new SymbolReference(BIGINT, "a")))))
                                .left(values("a"))
                                .right(
                                        project(
                                                ImmutableMap.of(
                                                        "expr_less", expression(new ArithmeticBinaryExpression(ADD_BIGINT, ADD, new SymbolReference(BIGINT, "b"), new Constant(BIGINT, 1L))),
                                                        "expr_greater", expression(new ArithmeticBinaryExpression(ADD_BIGINT, ADD, new SymbolReference(BIGINT, "b"), new Constant(BIGINT, 10L)))),
                                                values("b")))));
    }

    @Test
    public void testOnlyRightJoinFilterExpressionPushedDownToRightJoinSource()
    {
        tester().assertThat(ruleSet.pushJoinInequalityFilterExpressionBelowJoinRule())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            p.values(a),
                            p.values(b),
                            comparison(LESS_THAN, add(b, 1), add(a, 2)));
                })
                .matches(
                        join(INNER, builder -> builder
                                .filter(new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "expr"), new ArithmeticBinaryExpression(ADD_BIGINT, ADD, new SymbolReference(BIGINT, "a"), new Constant(BIGINT, 2L))))
                                .left(values("a"))
                                .right(
                                        project(
                                                ImmutableMap.of("expr", expression(new ArithmeticBinaryExpression(ADD_BIGINT, ADD, new SymbolReference(BIGINT, "b"), new Constant(BIGINT, 1L)))),
                                                values("b")))));
    }

    @Test
    public void testParentFilterExpressionNotPushedDownToLeftJoinSource()
    {
        tester().assertThat(ruleSet.pushParentInequalityFilterExpressionBelowJoinRule())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.filter(
                            comparison(LESS_THAN, add(a, 1), b.toSymbolReference()),
                            p.join(
                                    INNER,
                                    p.values(a),
                                    p.values(b)));
                })
                .doesNotFire();
    }

    @Test
    public void testParentFilterExpressionPushedDownToRightJoinSource()
    {
        tester().assertThat(ruleSet.pushParentInequalityFilterExpressionBelowJoinRule())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.filter(
                            comparison(LESS_THAN, add(b, 1), a.toSymbolReference()),
                            p.join(
                                    INNER,
                                    p.values(a),
                                    p.values(b)));
                })
                .matches(
                        project(
                                filter(
                                        new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "expr"), new SymbolReference(BIGINT, "a")),
                                        join(INNER, builder -> builder
                                                .left(
                                                        values("a"))
                                                .right(
                                                        project(
                                                                ImmutableMap.of("expr", expression(new ArithmeticBinaryExpression(ADD_BIGINT, ADD, new SymbolReference(BIGINT, "b"), new Constant(BIGINT, 1L)))),
                                                                values("b")))))));
    }

    @Test
    public void testManyParentFilterExpressionsPushedDownToRightJoinSource()
    {
        tester().assertThat(ruleSet.pushParentInequalityFilterExpressionBelowJoinRule())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.filter(
                            and(
                                    comparison(LESS_THAN, add(b, 1), a.toSymbolReference()),
                                    comparison(GREATER_THAN, add(b, 10), a.toSymbolReference())),
                            p.join(
                                    INNER,
                                    p.values(a),
                                    p.values(b)));
                })
                .matches(
                        project(
                                filter(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "expr_less"), new SymbolReference(BIGINT, "a")), new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "expr_greater"), new SymbolReference(BIGINT, "a")))),
                                        join(INNER, builder -> builder
                                                .left(values("a"))
                                                .right(
                                                        project(
                                                                ImmutableMap.of(
                                                                        "expr_less", expression(new ArithmeticBinaryExpression(ADD_BIGINT, ADD, new SymbolReference(BIGINT, "b"), new Constant(BIGINT, 1L))),
                                                                        "expr_greater", expression(new ArithmeticBinaryExpression(ADD_BIGINT, ADD, new SymbolReference(BIGINT, "b"), new Constant(BIGINT, 10L)))),
                                                                values("b")))))));
    }

    @Test
    public void testOnlyParentFilterExpressionExposedInaJoin()
    {
        tester().assertThat(ruleSet.pushParentInequalityFilterExpressionBelowJoinRule())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.filter(
                            comparison(LESS_THAN, add(b, 1), a.toSymbolReference()),
                            p.join(
                                    INNER,
                                    p.values(a),
                                    p.values(b),
                                    comparison(LESS_THAN, add(b, 2), a.toSymbolReference())));
                })
                .matches(
                        project(
                                filter(
                                        new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "parent_expression"), new SymbolReference(BIGINT, "a")),
                                        join(INNER, builder -> builder
                                                .filter(new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "join_expression"), new SymbolReference(BIGINT, "a")))
                                                .left(values("a"))
                                                .right(
                                                        project(
                                                                ImmutableMap.of(
                                                                        "join_expression", expression(new ArithmeticBinaryExpression(ADD_BIGINT, ADD, new SymbolReference(BIGINT, "b"), new Constant(BIGINT, 2L))),
                                                                        "parent_expression", expression(new ArithmeticBinaryExpression(ADD_BIGINT, ADD, new SymbolReference(BIGINT, "b"), new Constant(BIGINT, 1L)))),
                                                                values("b"))))
                                                .withExactOutputs("a", "b", "parent_expression"))));
    }

    @Test
    public void testNoExpression()
    {
        tester().assertThat(ruleSet.pushJoinInequalityFilterExpressionBelowJoinRule())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            p.values(a),
                            p.values(b),
                            comparison(LESS_THAN, a.toSymbolReference(), b.toSymbolReference()));
                }).doesNotFire();
    }

    @Test
    public void testNotSupportedExpression()
    {
        tester().assertThat(ruleSet.pushJoinInequalityFilterExpressionBelowJoinRule())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            p.values(a),
                            p.values(b),
                            comparison(IS_DISTINCT_FROM, a.toSymbolReference(), b.toSymbolReference()));
                }).doesNotFire();
    }

    private static ComparisonExpression comparison(Operator operator, Expression left, Expression right)
    {
        return new ComparisonExpression(operator, left, right);
    }

    private ArithmeticBinaryExpression add(Symbol symbol, long value)
    {
        return new ArithmeticBinaryExpression(
                ADD_BIGINT,
                ADD,
                symbol.toSymbolReference(),
                new Constant(BIGINT, value));
    }
}
