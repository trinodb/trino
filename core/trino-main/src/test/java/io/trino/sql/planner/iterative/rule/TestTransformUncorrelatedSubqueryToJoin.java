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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.tree.ComparisonExpression;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.FULL;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.INNER;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.RIGHT;
import static io.trino.sql.planner.plan.JoinNode.Type;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static java.util.Collections.emptyList;

public class TestTransformUncorrelatedSubqueryToJoin
        extends BaseRuleTest
{
    @Test
    public void testRewriteInnerCorrelatedJoin()
    {
        tester().assertThat(new TransformUncorrelatedSubqueryToJoin())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            emptyList(),
                            p.values(a),
                            INNER,
                            new ComparisonExpression(
                                    GREATER_THAN,
                                    b.toSymbolReference(),
                                    a.toSymbolReference()),
                            p.values(b));
                })
                .matches(
                        join(Type.INNER, builder -> builder
                                .filter("b > a")
                                .left(values("a"))
                                .right(values("b"))));
    }

    @Test
    public void testRewriteLeftCorrelatedJoin()
    {
        tester().assertThat(new TransformUncorrelatedSubqueryToJoin())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            emptyList(),
                            p.values(a),
                            LEFT,
                            new ComparisonExpression(
                                    GREATER_THAN,
                                    b.toSymbolReference(),
                                    a.toSymbolReference()),
                            p.values(b));
                })
                .matches(
                        join(Type.LEFT, builder -> builder
                                .filter("b > a")
                                .left(values("a"))
                                .right(values("b"))));
    }

    @Test
    public void testRewriteRightCorrelatedJoin()
    {
        tester().assertThat(new TransformUncorrelatedSubqueryToJoin())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            emptyList(),
                            p.values(a),
                            RIGHT,
                            TRUE_LITERAL,
                            p.values(b));
                })
                .matches(
                        join(Type.INNER, builder -> builder
                                .left(values("a"))
                                .right(values("b"))));

        tester().assertThat(new TransformUncorrelatedSubqueryToJoin())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            emptyList(),
                            p.values(a),
                            RIGHT,
                            new ComparisonExpression(
                                    GREATER_THAN,
                                    b.toSymbolReference(),
                                    a.toSymbolReference()),
                            p.values(b));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression("if(b > a, a, null)"),
                                        "b", expression("b")),
                                join(Type.INNER, builder -> builder
                                        .left(values("a"))
                                        .right(values("b")))));
    }

    @Test
    public void testRewriteFullCorrelatedJoin()
    {
        tester().assertThat(new TransformUncorrelatedSubqueryToJoin())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            emptyList(),
                            p.values(a),
                            FULL,
                            TRUE_LITERAL,
                            p.values(b));
                })
                .matches(
                        join(Type.LEFT, builder -> builder
                                .left(values("a"))
                                .right(values("b"))));

        tester().assertThat(new TransformUncorrelatedSubqueryToJoin())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            emptyList(),
                            p.values(a),
                            FULL,
                            new ComparisonExpression(
                                    GREATER_THAN,
                                    b.toSymbolReference(),
                                    a.toSymbolReference()),
                            p.values(b));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFire()
    {
        Symbol symbol = new Symbol("x");
        tester()
                .assertThat(new TransformUncorrelatedSubqueryToJoin())
                .on(p -> p.correlatedJoin(ImmutableList.of(symbol), p.values(symbol), p.values()))
                .doesNotFire();
    }
}
