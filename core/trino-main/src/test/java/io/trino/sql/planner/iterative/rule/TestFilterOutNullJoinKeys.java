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

import io.trino.Session;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.FILTER_OUT_NULL_JOIN_KEYS;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;

public class TestFilterOutNullJoinKeys
        extends BaseRuleTest
{
    private Session enabled()
    {
        return Session.builder(tester().getSession())
                .setSystemProperty(FILTER_OUT_NULL_JOIN_KEYS, "true")
                .build();
    }

    @Test
    public void testInnerJoinFiltersBothSides()
    {
        tester().assertThat(new FilterOutNullJoinKeys(tester().getPlannerContext()))
                .withSession(enabled())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.join(INNER, p.values(1, a), p.values(1, b), new EquiJoinClause(a, b));
                })
                .matches(join(INNER, builder -> builder
                        .equiCriteria("a", "b")
                        .left(filter(
                                not(tester().getMetadata(), getCharVarcharCoercion(tester().getSession()), new IsNull(new Reference(BIGINT, "a"))),
                                values("a")))
                        .right(filter(
                                not(tester().getMetadata(), getCharVarcharCoercion(tester().getSession()), new IsNull(new Reference(BIGINT, "b"))),
                                values("b")))));
    }

    @Test
    public void testOuterJoinFiltersOnlyNonPreservedSide()
    {
        tester().assertThat(new FilterOutNullJoinKeys(tester().getPlannerContext()))
                .withSession(enabled())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.join(LEFT, p.values(1, a), p.values(1, b), new EquiJoinClause(a, b));
                })
                .matches(join(LEFT, builder -> builder
                        .equiCriteria("a", "b")
                        .left(values("a"))
                        .right(filter(
                                not(tester().getMetadata(), getCharVarcharCoercion(tester().getSession()), new IsNull(new Reference(BIGINT, "b"))),
                                values("b")))));

        tester().assertThat(new FilterOutNullJoinKeys(tester().getPlannerContext()))
                .withSession(enabled())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.join(RIGHT, p.values(1, a), p.values(1, b), new EquiJoinClause(a, b));
                })
                .matches(join(RIGHT, builder -> builder
                        .equiCriteria("a", "b")
                        .left(filter(
                                not(tester().getMetadata(), getCharVarcharCoercion(tester().getSession()), new IsNull(new Reference(BIGINT, "a"))),
                                values("a")))
                        .right(values("b"))));
    }

    @Test
    public void testDoesNotFireOnFullJoin()
    {
        tester().assertThat(new FilterOutNullJoinKeys(tester().getPlannerContext()))
                .withSession(enabled())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.join(FULL, p.values(1, a), p.values(1, b), new EquiJoinClause(a, b));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithoutCriteria()
    {
        tester().assertThat(new FilterOutNullJoinKeys(tester().getPlannerContext()))
                .withSession(enabled())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.join(INNER, p.values(1, a), p.values(1, b));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnNonNullKeys()
    {
        // both keys are already non-null: the left one is filtered by a null-rejecting predicate
        // (the shape this rule itself produces, making it converge), the right one by a comparison
        tester().assertThat(new FilterOutNullJoinKeys(tester().getPlannerContext()))
                .withSession(enabled())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.join(
                            INNER,
                            p.filter(not(tester().getMetadata(), getCharVarcharCoercion(tester().getSession()), new IsNull(a.toSymbolReference())), p.values(1, a)),
                            p.filter(comparison(GREATER_THAN, b.toSymbolReference(), new Constant(BIGINT, 5L)), p.values(1, b)),
                            new EquiJoinClause(a, b));
                })
                .doesNotFire();
    }
}
