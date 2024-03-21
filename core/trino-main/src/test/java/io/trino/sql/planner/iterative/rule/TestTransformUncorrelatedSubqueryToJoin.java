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
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.JoinType;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Collections.emptyList;

public class TestTransformUncorrelatedSubqueryToJoin
        extends BaseRuleTest
{
    @Test
    public void testRewriteLeftCorrelatedJoinWithScalarSubquery()
    {
        tester().assertThat(new TransformUncorrelatedSubqueryToJoin())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            emptyList(),
                            p.values(a),
                            LEFT,
                            TRUE,
                            p.values(1, b));
                })
                .matches(
                        join(JoinType.INNER, builder -> builder
                                .left(values("a"))
                                .right(values("b"))));
    }

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
                            LEFT,
                            new Comparison(
                                    GREATER_THAN,
                                    b.toSymbolReference(),
                                    a.toSymbolReference()),
                            p.values(b));
                })
                .matches(
                        join(JoinType.LEFT, builder -> builder
                                .filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))
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
                            new Comparison(
                                    GREATER_THAN,
                                    b.toSymbolReference(),
                                    a.toSymbolReference()),
                            p.values(b));
                })
                .matches(
                        join(JoinType.LEFT, builder -> builder
                                .filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))
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
                            TRUE,
                            p.values(b));
                })
                .matches(
                        join(JoinType.INNER, builder -> builder
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
                            new Comparison(
                                    GREATER_THAN,
                                    b.toSymbolReference(),
                                    a.toSymbolReference()),
                            p.values(b));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression(ifExpression(new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")), new Reference(BIGINT, "a"), new Constant(BIGINT, null))),
                                        "b", expression(new Reference(BIGINT, "b"))),
                                join(JoinType.INNER, builder -> builder
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
                            TRUE,
                            p.values(b));
                })
                .matches(
                        join(JoinType.LEFT, builder -> builder
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
                            new Comparison(
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
        Symbol symbol = new Symbol(UNKNOWN, "x");
        tester()
                .assertThat(new TransformUncorrelatedSubqueryToJoin())
                .on(p -> p.correlatedJoin(ImmutableList.of(symbol), p.values(symbol), p.values()))
                .doesNotFire();
    }
}
