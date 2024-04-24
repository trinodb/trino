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
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.JoinType;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;

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
                                    new Comparison(
                                            GREATER_THAN,
                                            b.toSymbolReference(),
                                            a.toSymbolReference()),
                                    p.values(b)));
                })
                .matches(
                        join(JoinType.INNER, builder -> builder
                                .filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))
                                .left(values("a"))
                                .right(
                                        filter(
                                                TRUE,
                                                values("b")))));

        tester().assertThat(new TransformCorrelatedJoinToJoin(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.correlatedJoin(
                            ImmutableList.of(a),
                            p.values(a),
                            INNER,
                            new Comparison(
                                    LESS_THAN,
                                    b.toSymbolReference(),
                                    new Constant(BIGINT, 3L)),
                            p.filter(
                                    new Comparison(
                                            GREATER_THAN,
                                            b.toSymbolReference(),
                                            a.toSymbolReference()),
                                    p.values(b)));
                })
                .matches(
                        join(JoinType.INNER, builder -> builder
                                .filter(new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")), new Comparison(LESS_THAN, new Reference(BIGINT, "b"), new Constant(BIGINT, 3L)))))
                                .left(values("a"))
                                .right(
                                        filter(
                                                TRUE,
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
                            TRUE,
                            p.filter(
                                    new Comparison(
                                            GREATER_THAN,
                                            b.toSymbolReference(),
                                            a.toSymbolReference()),
                                    p.values(b)));
                })
                .matches(
                        join(JoinType.LEFT, builder -> builder
                                .filter(new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))
                                .left(values("a"))
                                .right(
                                        filter(
                                                TRUE,
                                                values("b")))));

        tester().assertThat(new TransformCorrelatedJoinToJoin(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.correlatedJoin(
                            ImmutableList.of(a),
                            p.values(a),
                            LEFT,
                            new Comparison(
                                    LESS_THAN,
                                    b.toSymbolReference(),
                                    new Constant(BIGINT, 3L)),
                            p.filter(
                                    new Comparison(
                                            GREATER_THAN,
                                            b.toSymbolReference(),
                                            a.toSymbolReference()),
                                    p.values(b)));
                })
                .matches(
                        join(JoinType.LEFT, builder -> builder
                                .filter(new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")), new Comparison(LESS_THAN, new Reference(BIGINT, "b"), new Constant(BIGINT, 3L)))))
                                .left(values("a"))
                                .right(
                                        filter(
                                                TRUE,
                                                values("b")))));
    }

    @Test
    public void doesNotFireForEnforceSingleRow()
    {
        tester().assertThat(new TransformCorrelatedJoinToJoin(tester().getPlannerContext()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        INNER,
                        TRUE,
                        p.enforceSingleRow(
                                p.filter(
                                        new Comparison(EQUAL, new Reference(BIGINT, "corr"), new Reference(BIGINT, "a")),
                                        p.values(p.symbol("a"))))))
                .doesNotFire();
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
