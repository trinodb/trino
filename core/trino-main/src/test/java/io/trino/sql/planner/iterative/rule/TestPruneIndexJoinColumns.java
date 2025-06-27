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
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.IndexJoinNode.EquiJoinClause;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.indexJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.indexJoinEquiClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.IndexJoinNode.Type.INNER;

public class TestPruneIndexJoinColumns
        extends BaseRuleTest
{
    @Test
    public void testPruneInputColumn()
    {
        tester().assertThat(new PruneIndexJoinColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    return p.project(
                            Assignments.identity(a, b),
                            p.indexJoin(
                                    INNER,
                                    p.values(a),
                                    p.values(b, c),
                                    ImmutableList.of(new EquiJoinClause(a, b))));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("a", expression(new Reference(BIGINT, "a")), "b", expression(new Reference(BIGINT, "b"))),
                                indexJoin(
                                        INNER,
                                        ImmutableList.of(indexJoinEquiClause("a", "b")),
                                        values("a"),
                                        strictProject(
                                                ImmutableMap.of("b", expression(new Reference(BIGINT, "b"))),
                                                values("b", "c")))));

        tester().assertThat(new PruneIndexJoinColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    return p.project(
                            Assignments.identity(a, c),
                            p.indexJoin(
                                    INNER,
                                    p.values(a, b),
                                    p.values(c, d),
                                    ImmutableList.of(new EquiJoinClause(a, c))));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("a", expression(new Reference(BIGINT, "a")), "c", expression(new Reference(BIGINT, "c"))),
                                indexJoin(
                                        INNER,
                                        ImmutableList.of(indexJoinEquiClause("a", "c")),
                                        strictProject(
                                                ImmutableMap.of("a", expression(new Reference(BIGINT, "a"))),
                                                values("a", "b")),
                                        strictProject(
                                                ImmutableMap.of("c", expression(new Reference(BIGINT, "c"))),
                                                values("c", "d")))));
    }

    @Test
    public void testDoNotPruneEquiClauseSymbol()
    {
        tester().assertThat(new PruneIndexJoinColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.identity(a),
                            p.indexJoin(
                                    INNER,
                                    p.values(a),
                                    p.values(b),
                                    ImmutableList.of(new EquiJoinClause(a, b))));
                })
                .doesNotFire();
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneIndexJoinColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    return p.project(
                            Assignments.identity(a, b, c),
                            p.indexJoin(
                                    INNER,
                                    p.values(a),
                                    p.values(b, c),
                                    ImmutableList.of(new EquiJoinClause(a, b))));
                })
                .doesNotFire();
    }
}
