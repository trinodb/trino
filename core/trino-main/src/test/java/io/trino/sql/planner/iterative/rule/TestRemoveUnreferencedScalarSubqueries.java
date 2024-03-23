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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static java.util.Collections.emptyList;

public class TestRemoveUnreferencedScalarSubqueries
        extends BaseRuleTest
{
    @Test
    public void testRemoveUnreferencedInput()
    {
        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> {
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            emptyList(),
                            p.values(emptyList(), ImmutableList.of(emptyList())),
                            p.values(2, b));
                })
                .matches(values("b"));

        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> {
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            emptyList(),
                            p.values(emptyList(), ImmutableList.of(emptyList())),
                            LEFT,
                            TRUE,
                            p.values(2, b));
                })
                .matches(values("b"));

        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> {
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            emptyList(),
                            p.values(emptyList(), ImmutableList.of(emptyList())),
                            RIGHT,
                            TRUE,
                            p.values(2, b));
                })
                .matches(values("b"));

        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> {
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            emptyList(),
                            p.values(emptyList(), ImmutableList.of(emptyList())),
                            FULL,
                            TRUE,
                            p.values(2, b));
                })
                .matches(values("b"));
    }

    @Test
    public void testDoNotRemoveInputOfLeftOrFullJoinWhenSubqueryPotentiallyEmpty()
    {
        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> {
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            emptyList(),
                            p.values(emptyList(), ImmutableList.of(emptyList())),
                            LEFT,
                            TRUE,
                            p.filter(
                                    new Comparison(
                                            LESS_THAN,
                                            b.toSymbolReference(),
                                            new Constant(BIGINT, 3L)),
                                    p.values(2, b)));
                })
                .doesNotFire();

        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> {
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            emptyList(),
                            p.values(emptyList(), ImmutableList.of(emptyList())),
                            FULL,
                            TRUE,
                            p.filter(
                                    new Comparison(
                                            LESS_THAN,
                                            b.toSymbolReference(),
                                            new Constant(BIGINT, 3L)),
                                    p.values(2, b)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotRemoveInputWhenCorrelationPresent()
    {
        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            ImmutableList.of(a),
                            p.values(1, a),
                            p.values(2, b));
                })
                .doesNotFire();
    }

    @Test
    public void testRemoveUnreferencedSubquery()
    {
        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> p.correlatedJoin(
                        emptyList(),
                        p.values(p.symbol("b", BIGINT)),
                        p.values(emptyList(), ImmutableList.of(emptyList()))))
                .matches(values("b"));

        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> p.correlatedJoin(
                        emptyList(),
                        p.values(p.symbol("b", BIGINT)),
                        LEFT,
                        TRUE,
                        p.values(emptyList(), ImmutableList.of(emptyList()))))
                .matches(values("b"));

        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> p.correlatedJoin(
                        emptyList(),
                        p.values(p.symbol("b", BIGINT)),
                        RIGHT,
                        TRUE,
                        p.values(emptyList(), ImmutableList.of(emptyList()))))
                .matches(values("b"));

        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> p.correlatedJoin(
                        emptyList(),
                        p.values(p.symbol("b", BIGINT)),
                        FULL,
                        TRUE,
                        p.values(emptyList(), ImmutableList.of(emptyList()))))
                .matches(values("b"));
    }

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> p.correlatedJoin(
                        emptyList(),
                        p.values(),
                        p.values()))
                .doesNotFire();
    }
}
