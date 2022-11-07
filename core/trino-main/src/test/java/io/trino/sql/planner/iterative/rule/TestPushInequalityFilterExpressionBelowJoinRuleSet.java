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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ComparisonExpression.Operator;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.LogicalExpression.and;

public class TestPushInequalityFilterExpressionBelowJoinRuleSet
        extends BaseRuleTest
{
    private PushInequalityFilterExpressionBelowJoinRuleSet ruleSet;

    @BeforeClass
    public void setUpBeforeClass()
    {
        ruleSet = new PushInequalityFilterExpressionBelowJoinRuleSet(tester().getMetadata(), tester().getTypeAnalyzer());
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
                                .filter("expr < a")
                                .left(values("a"))
                                .right(project(
                                        ImmutableMap.of("expr", expression("b + BIGINT '1'")),
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
                                .filter("expr_less < a and expr_greater > a")
                                .left(values("a"))
                                .right(
                                        project(
                                                ImmutableMap.of(
                                                        "expr_less", expression("b + BIGINT '1'"),
                                                        "expr_greater", expression("b + BIGINT '10'")),
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
                                .filter("expr < a + BIGINT '2'")
                                .left(values("a"))
                                .right(
                                        project(
                                                ImmutableMap.of("expr", expression("b + BIGINT '1'")),
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
                                filter("expr < a",
                                        join(INNER, builder -> builder
                                                .left(
                                                        values("a"))
                                                .right(
                                                        project(
                                                                ImmutableMap.of("expr", expression("b + BIGINT '1'")),
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
                                filter("expr_less < a and expr_greater > a",
                                        join(INNER, builder -> builder
                                                .left(values("a"))
                                                .right(
                                                        project(
                                                                ImmutableMap.of(
                                                                        "expr_less", expression("b + BIGINT '1'"),
                                                                        "expr_greater", expression("b + BIGINT '10'")),
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
                                filter("parent_expression < a",
                                        join(INNER, builder -> builder
                                                .filter("join_expression < a")
                                                .left(values("a"))
                                                .right(
                                                        project(
                                                                ImmutableMap.of(
                                                                        "join_expression", expression("b + BIGINT '2'"),
                                                                        "parent_expression", expression("b + BIGINT '1'")),
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
                ADD,
                symbol.toSymbolReference(),
                new GenericLiteral("BIGINT", String.valueOf(value)));
    }
}
