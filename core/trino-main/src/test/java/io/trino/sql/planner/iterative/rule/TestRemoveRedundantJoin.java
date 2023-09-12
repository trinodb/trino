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

import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;

public class TestRemoveRedundantJoin
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveRedundantJoin())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(10, p.symbol("a")),
                                p.values(10, p.symbol("b"))))
                .doesNotFire();

        tester().assertThat(new RemoveRedundantJoin())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(10, p.symbol("a")),
                                p.values(10, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testInnerJoinRemoval()
    {
        tester().assertThat(new RemoveRedundantJoin())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(10, p.symbol("a")),
                                p.values(0)))
                .matches(values("a"));

        tester().assertThat(new RemoveRedundantJoin())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(0),
                                p.values(10, p.symbol("b"))))
                .matches(values("b"));
    }

    @Test
    public void testLeftJoinRemoval()
    {
        tester().assertThat(new RemoveRedundantJoin())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(0),
                                p.values(10, p.symbol("b"))))
                .matches(values("a"));
    }

    @Test
    public void testRightJoinRemoval()
    {
        tester().assertThat(new RemoveRedundantJoin())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(10, p.symbol("a")),
                                p.values(0)))
                .matches(values("a"));
    }

    @Test
    public void testFullJoinRemoval()
    {
        tester().assertThat(new RemoveRedundantJoin())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(0, p.symbol("a")),
                                p.values(0, p.symbol("b"))))
                .matches(values("a", "b"));
    }
}
