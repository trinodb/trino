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
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.JoinNode.EquiJoinClause;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;

public class TestSimplifyCoalesceExpressionOverJoin
        extends BaseRuleTest
{
    @Test
    public void testCoalesceSimplification()
    {
        tester().assertThat(new SimplifyCoalesceExpressionOverJoin())
                .on(p ->
                        p.project(
                                Assignments.builder().put(p.symbol("expr"), expression("COALESCE(leftKey,rightKey)"))
                                        .build(),
                                p.join(INNER,
                                        p.values(p.symbol("leftKey")),
                                        p.values(p.symbol("rightKey")),
                                        new EquiJoinClause(p.symbol("leftKey"), p.symbol("rightKey")))))
                .matches(
                        project(
                                ImmutableMap.of("expr", PlanMatchPattern.expression("leftKey")),
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                        values("leftKey"),
                                        values("rightKey"))));

        tester().assertThat(new SimplifyCoalesceExpressionOverJoin())
                .on(p ->
                        p.project(
                                Assignments.builder().put(p.symbol("expr"), expression("COALESCE(leftKey,rightKey)"))
                                        .build(),
                                p.join(LEFT,
                                        p.values(p.symbol("leftKey")),
                                        p.values(p.symbol("rightKey")),
                                        new EquiJoinClause(p.symbol("leftKey"), p.symbol("rightKey")))))
                .matches(
                        project(
                                ImmutableMap.of("expr", PlanMatchPattern.expression("leftKey")),
                                join(
                                        LEFT,
                                        ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                        values("leftKey"),
                                        values("rightKey"))));

        tester().assertThat(new SimplifyCoalesceExpressionOverJoin())
                .on(p ->
                        p.project(
                                Assignments.builder().put(p.symbol("expr"), expression("COALESCE(leftKey,rightKey)"))
                                        .build(),
                                p.join(RIGHT,
                                        p.values(p.symbol("leftKey")),
                                        p.values(p.symbol("rightKey")),
                                        new EquiJoinClause(p.symbol("leftKey"), p.symbol("rightKey")))))
                .matches(
                        project(
                                ImmutableMap.of("expr", PlanMatchPattern.expression("rightKey")),
                                join(
                                        RIGHT,
                                        ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                        values("leftKey"),
                                        values("rightKey"))));
    }

    @Test
    public void doesNotFire()
    {
        tester().assertThat(new SimplifyCoalesceExpressionOverJoin())
                .on(p ->
                        p.project(
                                Assignments.builder().put(p.symbol("expr"), expression("leftNonKey"))
                                        .build(),
                                p.join(INNER,
                                        p.values(p.symbol("leftKey"), p.symbol("leftNonKey")),
                                        p.values(p.symbol("rightKey")),
                                        new EquiJoinClause(p.symbol("leftKey"), p.symbol("rightKey")))))
                .doesNotFire();

        tester().assertThat(new SimplifyCoalesceExpressionOverJoin())
                .on(p ->
                        p.project(
                                Assignments.builder().put(p.symbol("expr"), expression("COALESCE(leftKey,rightKey)"))
                                        .build(),
                                p.join(FULL,
                                        p.values(p.symbol("leftKey")),
                                        p.values(p.symbol("rightKey")),
                                        new EquiJoinClause(p.symbol("leftKey"), p.symbol("rightKey")))))
                .doesNotFire();

        tester().assertThat(new SimplifyCoalesceExpressionOverJoin())
                .on(p ->
                        p.project(
                                Assignments.builder().put(p.symbol("expr"), expression("COALESCE(leftNonKey, leftKey,rightKey)"))
                                        .build(),
                                p.join(FULL,
                                        p.values(p.symbol("leftKey"), p.symbol("leftNonKey")),
                                        p.values(p.symbol("rightKey")),
                                        new EquiJoinClause(p.symbol("leftKey"), p.symbol("rightKey")))))
                .doesNotFire();
    }
}
