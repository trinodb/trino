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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.JoinNode.Type;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.LongLiteral;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.INNER;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.LEFT;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;

public class TestTransformCorrelatedJoinToJoin
        extends BaseRuleTest
{
    @Test
    public void testRewriteInnerCorrelatedJoin()
    {
        tester().assertThat(new TransformCorrelatedJoinToJoin(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            ImmutableList.of(a),
                            p.values(a),
                            p.filter(
                                    new ComparisonExpression(
                                            GREATER_THAN,
                                            b.toSymbolReference(),
                                            a.toSymbolReference()),
                                    p.values(b)));
                })
                .matches(
                        join(Type.INNER, builder -> builder
                                .filter("b > a")
                                .left(values("a"))
                                .right(
                                        filter(
                                                TRUE_LITERAL,
                                                values("b")))));

        tester().assertThat(new TransformCorrelatedJoinToJoin(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            ImmutableList.of(a),
                            p.values(a),
                            INNER,
                            new ComparisonExpression(
                                    LESS_THAN,
                                    b.toSymbolReference(),
                                    new LongLiteral("3")),
                            p.filter(
                                    new ComparisonExpression(
                                            GREATER_THAN,
                                            b.toSymbolReference(),
                                            a.toSymbolReference()),
                                    p.values(b)));
                })
                .matches(
                        join(Type.INNER, builder -> builder
                                .filter("b > a AND b < 3")
                                .left(values("a"))
                                .right(
                                        filter(
                                                TRUE_LITERAL,
                                                values("b")))));
    }

    @Test
    public void testRewriteLeftCorrelatedJoin()
    {
        tester().assertThat(new TransformCorrelatedJoinToJoin(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            ImmutableList.of(a),
                            p.values(a),
                            LEFT,
                            TRUE_LITERAL,
                            p.filter(
                                    new ComparisonExpression(
                                            GREATER_THAN,
                                            b.toSymbolReference(),
                                            a.toSymbolReference()),
                                    p.values(b)));
                })
                .matches(
                        join(Type.LEFT, builder -> builder
                                .filter("b > a")
                                .left(values("a"))
                                .right(
                                        filter(
                                                TRUE_LITERAL,
                                                values("b")))));

        tester().assertThat(new TransformCorrelatedJoinToJoin(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            ImmutableList.of(a),
                            p.values(a),
                            LEFT,
                            new ComparisonExpression(
                                    LESS_THAN,
                                    b.toSymbolReference(),
                                    new LongLiteral("3")),
                            p.filter(
                                    new ComparisonExpression(
                                            GREATER_THAN,
                                            b.toSymbolReference(),
                                            a.toSymbolReference()),
                                    p.values(b)));
                })
                .matches(
                        join(Type.LEFT, builder -> builder
                                .filter("b > a AND b < 3")
                                .left(values("a"))
                                .right(
                                        filter(
                                                TRUE_LITERAL,
                                                values("b")))));
    }

    @Test
    public void doesNotFireOnUncorrelated()
    {
        tester().assertThat(new TransformCorrelatedJoinToJoin(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(),
                        p.values(p.symbol("a")),
                        p.values(p.symbol("b"))))
                .doesNotFire();
    }
}
