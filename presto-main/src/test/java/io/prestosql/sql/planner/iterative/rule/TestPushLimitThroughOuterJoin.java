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
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.JoinNode.EquiJoinClause;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.limit;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;

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
                                join(
                                        LEFT,
                                        ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                        limit(1, true, values("leftKey")),
                                        values("rightKey"))));
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
                                join(
                                        RIGHT,
                                        ImmutableList.of(equiJoinClause("leftKey", "rightKey")),
                                        values("leftKey"),
                                        limit(1, true, values("rightKey")))));
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
}
