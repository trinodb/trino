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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestPruneLimitColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllInputsReferenced()
    {
        tester().assertThat(new PruneLimitColumns())
                .on(p -> buildProjectedLimit(p, symbol -> symbol.getName().equals("b")))
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression("b")),
                                limit(
                                        1,
                                        strictProject(
                                                ImmutableMap.of("b", expression("b")),
                                                values("a", "b")))));
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneLimitColumns())
                .on(p -> buildProjectedLimit(p, alwaysTrue()))
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneTiesResolvingSymbols()
    {
        tester().assertThat(new PruneLimitColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.of(),
                            p.limit(1, ImmutableList.of(a), p.values(a, b)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of(),
                                limit(
                                        1,
                                        ImmutableList.of(sort("a", ASCENDING, FIRST)),
                                        strictProject(
                                                ImmutableMap.of("a", expression("a")),
                                                values("a", "b")))));
    }

    @Test
    public void testDoNotPrunePreSortedInputSymbols()
    {
        tester().assertThat(new PruneLimitColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    List<Symbol> orderBy = ImmutableList.of(a);
                    return p.project(
                            Assignments.of(),
                            p.limit(1, false, orderBy, p.values(a, b)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of(),
                                limit(
                                        1,
                                        ImmutableList.of(),
                                        false,
                                        ImmutableList.of("a"),
                                        strictProject(
                                                ImmutableMap.of("a", expression("a")),
                                                values("a", "b")))));
    }

    private ProjectNode buildProjectedLimit(PlanBuilder planBuilder, Predicate<Symbol> projectionFilter)
    {
        Symbol a = planBuilder.symbol("a");
        Symbol b = planBuilder.symbol("b");
        return planBuilder.project(
                Assignments.identity(Stream.of(a, b).filter(projectionFilter).collect(toImmutableSet())),
                planBuilder.limit(1, planBuilder.values(a, b)));
    }
}
