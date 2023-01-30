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
import io.trino.sql.planner.plan.JoinNode;
import org.testng.annotations.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.trino.sql.planner.plan.TopNNode.Step.FINAL;
import static io.trino.sql.planner.plan.TopNNode.Step.PARTIAL;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestPushTopNThroughOuterJoin
        extends BaseRuleTest
{
    @Test
    public void testPushTopNThroughLeftJoin()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey),
                            PARTIAL,
                            p.join(
                                    LEFT,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        join(LEFT, builder -> builder
                                .equiCriteria("leftKey", "rightKey")
                                .left(topN(1, ImmutableList.of(sort("leftKey", ASCENDING, FIRST)), PARTIAL, values("leftKey")))
                                .right(values("rightKey"))));
    }

    @Test
    public void testPushTopNThroughRightJoin()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(rightKey),
                            PARTIAL,
                            p.join(
                                    RIGHT,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        join(RIGHT, builder -> builder
                                .equiCriteria("leftKey", "rightKey")
                                .left(values("leftKey"))
                                .right(topN(1, ImmutableList.of(sort("rightKey", ASCENDING, FIRST)), PARTIAL, values("rightKey")))));
    }

    @Test
    public void testFullJoin()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(rightKey),
                            PARTIAL,
                            p.join(
                                    FULL,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();

        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey),
                            PARTIAL,
                            p.join(
                                    FULL,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPushTopNWhenSymbolsFromBothSources()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey, rightKey),
                            PARTIAL,
                            p.join(
                                    FULL,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPushWhenAlreadyLimited()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey),
                            PARTIAL,
                            p.join(
                                    LEFT,
                                    p.limit(1, p.values(5, leftKey)),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPushWhenStepNotPartial()
    {
        tester().assertThat(new PushTopNThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.topN(
                            1,
                            ImmutableList.of(leftKey),
                            FINAL,
                            p.join(
                                    FULL,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new JoinNode.EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }
}
