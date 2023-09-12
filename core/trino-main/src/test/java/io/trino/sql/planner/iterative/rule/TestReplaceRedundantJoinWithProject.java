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
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.tree.NullLiteral;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static java.util.Collections.nCopies;

public class TestReplaceRedundantJoinWithProject
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireOnInnerJoin()
    {
        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(0, p.symbol("a")),
                                p.values(0, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenOuterSourceEmpty()
    {
        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(0, p.symbol("a")),
                                p.values(0, p.symbol("b"))))
                .doesNotFire();

        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(0, p.symbol("a")),
                                p.values(0, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnFullJoinWithBothSourcesEmpty()
    {
        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(0, p.symbol("a")),
                                p.values(0, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testReplaceLeftJoin()
    {
        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(10, p.symbol("a")),
                                p.values(0, p.symbol("b"))))
                .matches(
                        project(
                                ImmutableMap.of("a", expression("a"), "b", expression("CAST(null AS bigint)")),
                                values(ImmutableList.of("a"), nCopies(10, ImmutableList.of(new NullLiteral())))));
    }

    @Test
    public void testReplaceRightJoin()
    {
        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(0, p.symbol("a")),
                                p.values(10, p.symbol("b"))))
                .matches(
                        project(
                                ImmutableMap.of("a", expression("CAST(null AS bigint)"), "b", expression("b")),
                                values(ImmutableList.of("b"), nCopies(10, ImmutableList.of(new NullLiteral())))));
    }

    @Test
    public void testReplaceFULLJoin()
    {
        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(10, p.symbol("a")),
                                p.values(0, p.symbol("b"))))
                .matches(
                        project(
                                ImmutableMap.of("a", expression("a"), "b", expression("CAST(null AS bigint)")),
                                values(ImmutableList.of("a"), nCopies(10, ImmutableList.of(new NullLiteral())))));

        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(0, p.symbol("a")),
                                p.values(10, p.symbol("b"))))
                .matches(
                        project(
                                ImmutableMap.of("a", expression("CAST(null AS bigint)"), "b", expression("b")),
                                values(ImmutableList.of("b"), nCopies(10, ImmutableList.of(new NullLiteral())))));
    }
}
