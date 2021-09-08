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
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.DERIVE_ISNOTNULL_PREDICATES;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;

public class TestDeriveNotNull
        extends BaseRuleTest
{
    @Test
    public void testDeriveNotNull()
    {
        tester().assertThat(new DeriveNotNull())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(p.symbol("a")),
                                p.values(p.symbol("b")),
                                ImmutableList.of(new EquiJoinClause(p.symbol("a"), p.symbol("b"))),
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(p.symbol("b")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .setSystemProperty(DERIVE_ISNOTNULL_PREDICATES, "true")
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("a", "b")),
                        Optional.of("((true AND (\"a\" IS NOT NULL)) AND (\"b\" IS NOT NULL))"),
                        values(ImmutableMap.of("a", 0)),
                        values(ImmutableMap.of("b", 0))));
    }

    @Test
    public void testCrossJoinDoesNotFire()
    {
        tester().assertThat(new DeriveNotNull())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(p.symbol("a")),
                                p.values(p.symbol("b")),
                                ImmutableList.of(),
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(p.symbol("b")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .doesNotFire();
    }

    @Test
    public void testOuterJoinDoesNotFire()
    {
        tester().assertThat(new DeriveNotNull())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(p.symbol("a")),
                                p.values(p.symbol("b")),
                                ImmutableList.of(new EquiJoinClause(p.symbol("a"), p.symbol("b"))),
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(p.symbol("b")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .doesNotFire();

        tester().assertThat(new DeriveNotNull())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(p.symbol("a")),
                                p.values(p.symbol("b")),
                                ImmutableList.of(new EquiJoinClause(p.symbol("a"), p.symbol("b"))),
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(p.symbol("b")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .doesNotFire();

        tester().assertThat(new DeriveNotNull())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(p.symbol("a")),
                                p.values(p.symbol("b")),
                                ImmutableList.of(new EquiJoinClause(p.symbol("a"), p.symbol("b"))),
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(p.symbol("b")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .doesNotFire();
    }
}
