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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import org.testng.annotations.Test;

import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.correlatedJoin;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneCorrelatedJoinColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllInputsReferenced()
    {
        tester().assertThat(new PruneCorrelatedJoinColumns())
                .on(p -> buildProjectedCorrelated(p, symbol -> symbol.getName().equals("b")))
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression("b"), "c", expression("c")),
                                correlatedJoin(
                                        ImmutableList.of(),
                                        strictProject(
                                                ImmutableMap.of("b", expression("b")),
                                                values("a", "b")),
                                        values("c"))));
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneCorrelatedJoinColumns())
                .on(p -> buildProjectedCorrelated(p, Predicates.alwaysTrue()))
                .doesNotFire();
    }

    private ProjectNode buildProjectedCorrelated(PlanBuilder planBuilder, Predicate<Symbol> projectionFilter)
    {
        Symbol a = planBuilder.symbol("a");
        Symbol b = planBuilder.symbol("b");
        Symbol c = planBuilder.symbol("c");
        return planBuilder.project(
                Assignments.identity(Streams.concat(Stream.of(a, b).filter(projectionFilter), Stream.of(c)).collect(toImmutableSet())),
                planBuilder.correlatedJoin(
                        ImmutableList.of(),
                        planBuilder.values(a, b),
                        planBuilder.values(c)));
    }
}
