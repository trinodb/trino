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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.JoinNode.Type;
import io.prestosql.sql.planner.plan.PlanNode;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.SystemSessionProperties.OPTIMIZED_NULLS_IN_JOIN;
import static io.prestosql.SystemSessionProperties.OPTIMIZED_NULLS_IN_JOIN_THRESHOLD;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expressions;
import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;

public class TestInferNotNullThroughJoin
        extends BaseRuleTest
{
    private PlanNode buildPlan(PlanBuilder planBuilder, Type joinType)
    {
        return planBuilder.join(
                joinType,
                planBuilder.values(ImmutableList.of(planBuilder.symbol("COL1")), ImmutableList.of(expressions("10"), expressions("NULL"))),
                planBuilder.values(ImmutableList.of(planBuilder.symbol("COL2")), ImmutableList.of(expressions("10"), expressions("20"), expressions("NULL"))),
                ImmutableList.of(new JoinNode.EquiJoinClause(planBuilder.symbol("COL1"), planBuilder.symbol("COL2"))),
                ImmutableList.of(planBuilder.symbol("COL1")),
                ImmutableList.of(planBuilder.symbol("COL2")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Test
    public void testInnerJoinWithOptimize()
    {
        tester().assertThat(new InferNotNullThroughJoin())
                .setSystemProperty(OPTIMIZED_NULLS_IN_JOIN_THRESHOLD, "0")
                .on(p -> buildPlan(p, INNER))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("COL1", "COL2")),
                        Optional.of("COL1 IS NOT NULL AND COL2 IS NOT NULL"),
                        values(ImmutableList.of("COL1"), ImmutableList.of(expressions("10"), expressions("NULL"))),
                        values(ImmutableList.of("COL2"), ImmutableList.of(expressions("10"), expressions("20"), expressions("NULL")))));

        tester().assertThat(new InferNotNullThroughJoin())
                .setSystemProperty(OPTIMIZED_NULLS_IN_JOIN_THRESHOLD, "0.34")
                .on(p -> buildPlan(p, INNER))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("COL1", "COL2")),
                        Optional.of("COL1 IS NOT NULL"),
                        values(ImmutableList.of("COL1"), ImmutableList.of(expressions("10"), expressions("NULL"))),
                        values(ImmutableList.of("COL2"), ImmutableList.of(expressions("10"), expressions("20"), expressions("NULL")))));
    }

    @Test
    public void testDoesNotFireForInnerJoin()
    {
        tester().assertThat(new InferNotNullThroughJoin())
                .setSystemProperty(OPTIMIZED_NULLS_IN_JOIN, "false")
                .on(p -> buildPlan(p, INNER))
                .doesNotFire();

        tester().assertThat(new InferNotNullThroughJoin())
                .setSystemProperty(OPTIMIZED_NULLS_IN_JOIN_THRESHOLD, "0.51")
                .on(p -> buildPlan(p, INNER))
                .doesNotFire();
    }

    @Test
    public void testLeftJoinWithOptimize()
    {
        tester().assertThat(new InferNotNullThroughJoin())
                .setSystemProperty(OPTIMIZED_NULLS_IN_JOIN_THRESHOLD, "0")
                .on(p -> buildPlan(p, LEFT))
                .matches(join(
                        LEFT,
                        ImmutableList.of(equiJoinClause("COL1", "COL2")),
                        Optional.of("COL2 IS NOT NULL"),
                        values(ImmutableList.of("COL1"), ImmutableList.of(expressions("10"), expressions("NULL"))),
                        values(ImmutableList.of("COL2"), ImmutableList.of(expressions("10"), expressions("20"), expressions("NULL")))));
    }

    @Test
    public void testDoesNotFireForLeftJoin()
    {
        tester().assertThat(new InferNotNullThroughJoin())
                .setSystemProperty(OPTIMIZED_NULLS_IN_JOIN, "false")
                .on(p -> buildPlan(p, LEFT))
                .doesNotFire();
    }

    @Test
    public void testRightJoinWithOptimize()
    {
        tester().assertThat(new InferNotNullThroughJoin())
                .setSystemProperty(OPTIMIZED_NULLS_IN_JOIN_THRESHOLD, "0")
                .on(p -> buildPlan(p, RIGHT))
                .matches(join(
                        RIGHT,
                        ImmutableList.of(equiJoinClause("COL1", "COL2")),
                        Optional.of("COL1 IS NOT NULL"),
                        values(ImmutableList.of("COL1"), ImmutableList.of(expressions("10"), expressions("NULL"))),
                        values(ImmutableList.of("COL2"), ImmutableList.of(expressions("10"), expressions("20"), expressions("NULL")))));
    }

    @Test
    public void testDoesNotFireForRightJoin()
    {
        tester().assertThat(new InferNotNullThroughJoin())
                .setSystemProperty(OPTIMIZED_NULLS_IN_JOIN, "false")
                .on(p -> buildPlan(p, RIGHT))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForFullJoin()
    {
        tester().assertThat(new InferNotNullThroughJoin())
                .setSystemProperty(OPTIMIZED_NULLS_IN_JOIN, "false")
                .on(p -> buildPlan(p, FULL))
                .doesNotFire();

        tester().assertThat(new InferNotNullThroughJoin())
                .setSystemProperty(OPTIMIZED_NULLS_IN_JOIN_THRESHOLD, "0")
                .on(p -> buildPlan(p, FULL))
                .doesNotFire();
    }
}
