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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.PlanNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.union;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneUnionColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllOutputsReferenced()
    {
        tester().assertThat(new PruneUnionColumns())
                .on(p -> buildProjectedUnion(p, symbol -> symbol.getName().equals("b")))
                .matches(
                        strictProject(
                                ImmutableMap.of("b", PlanMatchPattern.expression("b")),
                                union(
                                        values(ImmutableList.of("a", "b")),
                                        values(ImmutableList.of("c", "d")))
                                        .withExactOutputs("b")));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneUnionColumns())
                .on(p -> buildProjectedUnion(p, Predicates.alwaysTrue()))
                .doesNotFire();
    }

    private static PlanNode buildProjectedUnion(PlanBuilder p, Predicate<Symbol> projectionFilter)
    {
        Symbol a = p.symbol("a");
        Symbol b = p.symbol("b");
        Symbol c = p.symbol("c");
        Symbol d = p.symbol("d");

        List<Symbol> outputs = ImmutableList.of(a, b);
        return p.project(
                Assignments.identity(
                        outputs.stream()
                                .filter(projectionFilter)
                                .collect(toImmutableList())),
                p.union(ImmutableListMultimap.<Symbol, Symbol>builder()
                                .put(a, a)
                                .put(a, c)
                                .put(b, b)
                                .put(b, d)
                                .build(),
                        ImmutableList.of(
                                p.values(a, b),
                                p.values(c, d))));
    }
}
