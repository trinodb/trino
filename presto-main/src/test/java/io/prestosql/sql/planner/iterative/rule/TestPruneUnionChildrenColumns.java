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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.union;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneUnionChildrenColumns
        extends BaseRuleTest

{
    @Test
    public void testNotAllInputsReferencedForUnion()
    {
        tester().assertThat(new PruneUnionChildrenColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    return p.union(ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(a, a)
                                    .put(a, c)
                                    .build(),
                            ImmutableList.of(
                                    p.values(a, b),
                                    p.values(c, d)));
                })
                .matches(
                        union(
                                strictProject(
                                        ImmutableMap.of("a", PlanMatchPattern.expression("a")),
                                        values(ImmutableList.of("a", "b"))),
                                strictProject(
                                        ImmutableMap.of("c", PlanMatchPattern.expression("c")),
                                        values(ImmutableList.of("c", "d")))));
    }

    @Test
    public void testAllInputsReferencedForUnion()
    {
        tester().assertThat(new PruneUnionChildrenColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    return p.union(ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(a, a)
                                    .put(a, c)
                                    .put(b, b)
                                    .putAll(b, d)
                                    .build(),
                            ImmutableList.of(
                                    p.values(a, b),
                                    p.values(c, d)));
                }).doesNotFire();
    }
}
