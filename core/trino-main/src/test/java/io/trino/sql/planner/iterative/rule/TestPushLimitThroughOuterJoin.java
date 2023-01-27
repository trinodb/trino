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
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import org.testng.annotations.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;

public class TestPushLimitThroughOuterJoin
        extends BaseRuleTest
{
    @Test
    public void testPushLimitThroughLeftJoin()
    {
        tester().assertThat(new PushLimitThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.limit(1,
                            p.join(
                                    LEFT,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        limit(1,
                               join(LEFT, builder -> builder
                                        .equiCriteria("leftKey", "rightKey")
                                        .left(limit(1, ImmutableList.of(), true, values("leftKey")))
                                        .right(values("rightKey")))));
    }

    @Test
    public void testPushLimitThroughRightJoin()
    {
        tester().assertThat(new PushLimitThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.limit(1,
                            p.join(
                                    RIGHT,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        limit(1,
                                join(RIGHT, builder -> builder
                                        .equiCriteria("leftKey", "rightKey")
                                        .left(values("leftKey"))
                                        .right(limit(1, ImmutableList.of(), true, values("rightKey"))))));
    }

    @Test
    public void testPushLimitThroughFullOuterJoin()
    {
        tester().assertThat(new PushLimitThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.limit(1,
                            p.join(
                                    FULL,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPushWhenAlreadyLimited()
    {
        tester().assertThat(new PushLimitThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.limit(1,
                            p.join(
                                    LEFT,
                                    p.limit(1, p.values(5, leftKey)),
                                    p.values(5, rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPushLimitWithTies()
    {
        tester().assertThat(new PushLimitThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.limit(
                            1,
                            ImmutableList.of(leftKey),
                            p.join(
                                    LEFT,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testLimitWithPreSortedInputsLeftJoin()
    {
        tester().assertThat(new PushLimitThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.limit(
                            1,
                            false,
                            ImmutableList.of(rightKey),
                            p.join(
                                    LEFT,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();

        tester().assertThat(new PushLimitThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.limit(
                            1,
                            false,
                            ImmutableList.of(leftKey),
                            p.join(
                                    LEFT,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        limit(1, ImmutableList.of(), false, ImmutableList.of("leftKey"),
                               join(LEFT, builder -> builder
                                        .equiCriteria("leftKey", "rightKey")
                                        .left(limit(1, ImmutableList.of(), true, ImmutableList.of("leftKey"), values("leftKey")))
                                        .right(values("rightKey")))));
    }

    @Test
    public void testLimitWithPreSortedInputsRightJoin()
    {
        tester().assertThat(new PushLimitThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.limit(
                            1,
                            false,
                            ImmutableList.of(leftKey),
                            p.join(
                                    RIGHT,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();

        tester().assertThat(new PushLimitThroughOuterJoin())
                .on(p -> {
                    Symbol leftKey = p.symbol("leftKey");
                    Symbol rightKey = p.symbol("rightKey");
                    return p.limit(
                            1,
                            false,
                            ImmutableList.of(rightKey),
                            p.join(
                                    RIGHT,
                                    p.values(5, leftKey),
                                    p.values(5, rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        limit(1, ImmutableList.of(), false, ImmutableList.of("rightKey"),
                                join(RIGHT, builder -> builder
                                        .equiCriteria("leftKey", "rightKey")
                                        .left(values("leftKey"))
                                        .right(limit(1, ImmutableList.of(), true, ImmutableList.of("rightKey"), values("rightKey"))))));
    }
}
