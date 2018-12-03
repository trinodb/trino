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
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.assertions.ExpressionMatcher;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.sort;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.topN;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.TopNNode.Step.PARTIAL;
import static io.prestosql.sql.tree.SortItem.NullOrdering.FIRST;
import static io.prestosql.sql.tree.SortItem.Ordering.ASCENDING;

public class TestPushTopNThroughOuterJoinRuleSet
        extends BaseRuleTest
{
    @Test
    public void testPushTopNThroughLeftJoinWithProject()
    {
        tester().assertThat(PushTopNThroughOuterJoinRuleSet.withProject())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol projectedLeftKey = p.symbol("projectedLeftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    Symbol projectedRightKey = p.symbol("projectedRightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(projectedLeftKey),
                            p.project(Assignments.of(projectedLeftKey, new SymbolReference("leftKey"), projectedRightKey, new SymbolReference("rightKey")),
                                    p.join(
                                            LEFT,
                                            p.values(5, leftKey),
                                            p.values(5, rightKey),
                                            new JoinNode.EquiJoinClause(leftKey, rightKey))));
                })
                .matches(
                        topN(
                                1,
                                ImmutableList.of(sort("leftKey", ASCENDING, FIRST)),
                                project(
                                        ImmutableMap.of(
                                                "projectedLeftKey", new ExpressionMatcher("leftKey"),
                                                "projectedRightKey", new ExpressionMatcher("rightKey")),
                                        join(
                                                LEFT,
                                                ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                                topN(1, ImmutableList.of(sort("leftKey", ASCENDING, FIRST)), PARTIAL, values("leftKey")),
                                                values("rightKey")))));
    }

    @Test
    public void testPushRightwardsTopNThroughFullOuterJoinWithProject()
    {
        tester().assertThat(PushTopNThroughOuterJoinRuleSet.withProject())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol projectedLeftKey = p.symbol("projectedLeftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    Symbol projectedRightKey = p.symbol("projectedRightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(projectedRightKey),
                            p.project(Assignments.of(projectedLeftKey, new SymbolReference("leftKey"), projectedRightKey, new SymbolReference("rightKey")),
                                    p.join(
                                            FULL,
                                            p.values(5, leftKey),
                                            p.values(5, rightKey),
                                            new JoinNode.EquiJoinClause(leftKey, rightKey))));
                })
                .matches(
                        topN(
                                1,
                                ImmutableList.of(sort("rightKey", ASCENDING, FIRST)),
                                project(
                                        ImmutableMap.of(
                                                "projectedLeftKey", new ExpressionMatcher("leftKey"),
                                                "projectedRightKey", new ExpressionMatcher("rightKey")),
                                        join(
                                                FULL,
                                                ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                                values("leftKey"),
                                                topN(1, ImmutableList.of(sort("rightKey", ASCENDING, FIRST)), PARTIAL, values("rightKey"))))));
    }

    @Test
    public void testPushLeftwardsTopNThroughFullOuterJoinWithProject()
    {
        tester().assertThat(PushTopNThroughOuterJoinRuleSet.withProject())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol projectedLeftKey = p.symbol("projectedLeftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    Symbol projectedRightKey = p.symbol("projectedRightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(projectedLeftKey),
                            p.project(Assignments.of(projectedLeftKey, new SymbolReference("leftKey"), projectedRightKey, new SymbolReference("rightKey")),
                                    p.join(
                                            FULL,
                                            p.values(5, leftKey),
                                            p.values(5, rightKey),
                                            new JoinNode.EquiJoinClause(leftKey, rightKey))));
                })
                .matches(
                        topN(
                                1,
                                ImmutableList.of(sort("leftKey", ASCENDING, FIRST)),
                                project(
                                        ImmutableMap.of(
                                                "projectedLeftKey", new ExpressionMatcher("leftKey"),
                                                "projectedRightKey", new ExpressionMatcher("rightKey")),
                                        join(
                                                FULL,
                                                ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                                topN(1, ImmutableList.of(sort("leftKey", ASCENDING, FIRST)), PARTIAL, values("leftKey")),
                                                values("rightKey")))));
    }

    @Test
    public void testDoNotPushWhenAlreadyLimitedWithProject()
    {
        tester().assertThat(PushTopNThroughOuterJoinRuleSet.withProject())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol projectedLeftKey = p.symbol("projectedLeftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    Symbol projectedRightKey = p.symbol("projectedRightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(projectedLeftKey),
                            p.project(Assignments.of(projectedLeftKey, new SymbolReference("leftKey"), projectedRightKey, new SymbolReference("rightKey")),
                                    p.join(
                                            LEFT,
                                            p.limit(1, p.values(5, leftKey)),
                                            p.values(5, rightKey),
                                            new JoinNode.EquiJoinClause(leftKey, rightKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testPushTopNThroughLeftJoinWithoutProject()
    {
        tester().assertThat(PushTopNThroughOuterJoinRuleSet.withoutProject())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey),
                            p.join(
                                    LEFT,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        topN(
                                1,
                                ImmutableList.of(sort("leftKey", ASCENDING, FIRST)),
                                join(
                                        LEFT,
                                        ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                        topN(1, ImmutableList.of(sort("leftKey", ASCENDING, FIRST)), PARTIAL, values("leftKey")),
                                        values("rightKey"))));
    }

    @Test
    public void testPushRightwardsTopNThroughFullOuterJoinWithoutProject()
    {
        tester().assertThat(PushTopNThroughOuterJoinRuleSet.withoutProject())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(rightKey),
                            p.join(
                                    FULL,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        topN(
                                1,
                                ImmutableList.of(sort("rightKey", ASCENDING, FIRST)),
                                join(
                                        FULL,
                                        ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                        values("leftKey"),
                                        topN(1, ImmutableList.of(sort("rightKey", ASCENDING, FIRST)), PARTIAL, values("rightKey")))));
    }

    @Test
    public void testPushLeftwardsTopNThroughFullOuterJoinWithoutProject()
    {
        tester().assertThat(PushTopNThroughOuterJoinRuleSet.withoutProject())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey),
                            p.join(
                                    FULL,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        topN(
                                1,
                                ImmutableList.of(sort("leftKey", ASCENDING, FIRST)),
                                join(
                                        FULL,
                                        ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                        topN(1, ImmutableList.of(sort("leftKey", ASCENDING, FIRST)), PARTIAL, values("leftKey")),
                                        values("rightKey"))));
    }

    @Test
    public void testDoNotPushWhenAlreadyLimitedWithoutProject()
    {
        tester().assertThat(PushTopNThroughOuterJoinRuleSet.withoutProject())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey),
                            p.join(
                                    LEFT,
                                    p.limit(1, p.values(5, leftKey)),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }
}
