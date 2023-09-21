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
import io.trino.spi.type.BigintType;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.LongLiteral;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.FULL;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.RIGHT;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
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
                            TRUE_LITERAL,
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
                            TRUE_LITERAL,
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
                            TRUE_LITERAL,
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
                            TRUE_LITERAL,
                            p.filter(
                                    new ComparisonExpression(
                                            LESS_THAN,
                                            b.toSymbolReference(),
                                            new LongLiteral("3")),
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
                            TRUE_LITERAL,
                            p.filter(
                                    new ComparisonExpression(
                                            LESS_THAN,
                                            b.toSymbolReference(),
                                            new LongLiteral("3")),
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
                        p.values(p.symbol("b", BigintType.BIGINT)),
                        p.values(emptyList(), ImmutableList.of(emptyList()))))
                .matches(values("b"));

        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> p.correlatedJoin(
                        emptyList(),
                        p.values(p.symbol("b", BigintType.BIGINT)),
                        LEFT,
                        TRUE_LITERAL,
                        p.values(emptyList(), ImmutableList.of(emptyList()))))
                .matches(values("b"));

        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> p.correlatedJoin(
                        emptyList(),
                        p.values(p.symbol("b", BigintType.BIGINT)),
                        RIGHT,
                        TRUE_LITERAL,
                        p.values(emptyList(), ImmutableList.of(emptyList()))))
                .matches(values("b"));

        tester().assertThat(new RemoveUnreferencedScalarSubqueries())
                .on(p -> p.correlatedJoin(
                        emptyList(),
                        p.values(p.symbol("b", BigintType.BIGINT)),
                        FULL,
                        TRUE_LITERAL,
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
