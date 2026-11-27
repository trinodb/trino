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
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.JoinType;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;

public class TestSimplifyCoalesceOverJoin
        extends BaseRuleTest
{
    // left side: key l, value lv; right side: key r, value rv; criteria l = r
    private static final Reference L = new Reference(BIGINT, "l");
    private static final Reference LV = new Reference(BIGINT, "lv");
    private static final Reference R = new Reference(BIGINT, "r");
    private static final Reference RV = new Reference(BIGINT, "rv");
    private static final Constant FIVE = new Constant(BIGINT, 5L);

    @Test
    public void testInnerJoin()
    {
        assertProjectSimplified(INNER, new Coalesce(L, R), L);
        assertProjectSimplified(INNER, new Coalesce(R, L), R);
        assertProjectSimplified(INNER, new Coalesce(L, RV, FIVE), L);
        assertProjectSimplified(INNER, new Coalesce(LV, R, RV), new Coalesce(LV, R));
        assertProjectUnchanged(INNER, new Coalesce(LV, RV));
    }

    @Test
    public void testLeftJoin()
    {
        assertProjectSimplified(LEFT, new Coalesce(L, R), L);
        assertProjectSimplified(LEFT, new Coalesce(L, RV), L);
        assertProjectSimplified(LEFT, new Coalesce(L, R, FIVE), new Coalesce(L, FIVE));
        assertProjectSimplified(LEFT, new Coalesce(L, LV, R), new Coalesce(L, LV));
        assertProjectSimplified(LEFT, new Coalesce(R, L), L);
        assertProjectUnchanged(LEFT, new Coalesce(R, LV));
        assertProjectUnchanged(LEFT, new Coalesce(R, FIVE, L));
        assertProjectUnchanged(LEFT, new Coalesce(LV, R));
        assertProjectUnchanged(LEFT, new Coalesce(L, FIVE));
    }

    @Test
    public void testRightJoin()
    {
        assertProjectSimplified(RIGHT, new Coalesce(R, L), R);
        assertProjectSimplified(RIGHT, new Coalesce(R, LV), R);
        assertProjectSimplified(RIGHT, new Coalesce(L, R), R);
        assertProjectSimplified(RIGHT, new Coalesce(L, R, FIVE), new Coalesce(R, FIVE));
        assertProjectUnchanged(RIGHT, new Coalesce(L, RV));
        assertProjectUnchanged(RIGHT, new Coalesce(L, FIVE, R));
        assertProjectUnchanged(RIGHT, new Coalesce(RV, L));
    }

    @Test
    public void testFullJoin()
    {
        assertProjectUnchanged(FULL, new Coalesce(L, R));
        assertProjectUnchanged(FULL, new Coalesce(R, L));
    }

    @Test
    public void testNestedCoalesce()
    {
        assertProjectSimplified(LEFT, comparison(EQUAL, new Coalesce(L, R), FIVE), comparison(EQUAL, L, FIVE));
    }

    @Test
    public void testFilter()
    {
        tester().assertThat(new SimplifyCoalesceOverJoin(tester().getPlannerContext()).filterOverJoinRule())
                .on(p -> p.filter(comparison(EQUAL, new Coalesce(L, R), FIVE), sourceJoin(p, LEFT)))
                .matches(filter(comparison(EQUAL, L, FIVE), joinPattern(LEFT)));

        tester().assertThat(new SimplifyCoalesceOverJoin(tester().getPlannerContext()).filterOverJoinRule())
                .on(p -> p.filter(comparison(EQUAL, new Coalesce(L, R), FIVE), sourceJoin(p, FULL)))
                .doesNotFire();
    }

    @Test
    public void testCrossJoin()
    {
        tester().assertThat(new SimplifyCoalesceOverJoin(tester().getPlannerContext()).projectOverJoinRule())
                .on(p -> p.project(
                        Assignments.of(p.symbol("x"), new Coalesce(L, R)),
                        p.join(INNER, p.values(p.symbol("l"), p.symbol("lv")), p.values(p.symbol("r"), p.symbol("rv")))))
                .doesNotFire();
    }

    private void assertProjectSimplified(JoinType joinType, Expression expression, Expression expected)
    {
        tester().assertThat(new SimplifyCoalesceOverJoin(tester().getPlannerContext()).projectOverJoinRule())
                .on(p -> p.project(Assignments.of(p.symbol("x", expression.type()), expression), sourceJoin(p, joinType)))
                .matches(strictProject(ImmutableMap.of("x", expression(expected)), joinPattern(joinType)));
    }

    private void assertProjectUnchanged(JoinType joinType, Expression expression)
    {
        tester().assertThat(new SimplifyCoalesceOverJoin(tester().getPlannerContext()).projectOverJoinRule())
                .on(p -> p.project(Assignments.of(p.symbol("x", expression.type()), expression), sourceJoin(p, joinType)))
                .doesNotFire();
    }

    private static JoinNode sourceJoin(PlanBuilder p, JoinType joinType)
    {
        return p.join(
                joinType,
                p.values(p.symbol("l"), p.symbol("lv")),
                p.values(p.symbol("r"), p.symbol("rv")),
                new EquiJoinClause(p.symbol("l"), p.symbol("r")));
    }

    private static PlanMatchPattern joinPattern(JoinType joinType)
    {
        return join(joinType, builder -> builder
                .equiCriteria("l", "r")
                .left(values("l", "lv"))
                .right(values("r", "rv")));
    }
}
