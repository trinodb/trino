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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;

public class TestPruneJoinColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllOutputsReferenced()
    {
        tester().assertThat(new PruneJoinColumns())
                .on(p -> buildProjectedJoin(p, symbol -> symbol.getName().equals("rightValue")))
                .matches(
                        strictProject(
                                ImmutableMap.of("rightValue", PlanMatchPattern.expression("rightValue")),
                                join(INNER, builder -> builder
                                        .equiCriteria("leftKey", "rightKey")
                                        .left(values(ImmutableList.of("leftKey", "leftValue")))
                                        .right(values(ImmutableList.of("rightKey", "rightValue"))))
                                        .withExactOutputs("rightValue")));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneJoinColumns())
                .on(p -> buildProjectedJoin(p, Predicates.alwaysTrue()))
                .doesNotFire();
    }

    @Test
    public void testCrossJoin()
    {
        tester().assertThat(new PruneJoinColumns())
                .on(p -> {
                    Symbol leftValue = p.symbol("leftValue");
                    Symbol rightValue = p.symbol("rightValue");
                    return p.project(
                            Assignments.of(),
                            p.join(
                                    INNER,
                                    p.values(leftValue),
                                    p.values(rightValue),
                                    ImmutableList.of(),
                                    ImmutableList.of(leftValue),
                                    ImmutableList.of(rightValue),
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of(),
                                join(INNER, builder -> builder
                                        .left(values(ImmutableList.of("leftValue")))
                                        .right(values(ImmutableList.of("rightValue"))))
                                        .withExactOutputs()));
    }

    private static PlanNode buildProjectedJoin(PlanBuilder p, Predicate<Symbol> projectionFilter)
    {
        Symbol leftKey = p.symbol("leftKey");
        Symbol leftValue = p.symbol("leftValue");
        Symbol rightKey = p.symbol("rightKey");
        Symbol rightValue = p.symbol("rightValue");
        List<Symbol> leftOutputs = ImmutableList.of(leftKey, leftValue);
        List<Symbol> rightOutputs = ImmutableList.of(rightKey, rightValue);
        return p.project(
                Assignments.identity(
                        ImmutableList.of(leftKey, leftValue, rightKey, rightValue).stream()
                                .filter(projectionFilter)
                                .collect(toImmutableList())),
                p.join(
                        INNER,
                        p.values(leftKey, leftValue),
                        p.values(rightKey, rightValue),
                        ImmutableList.of(new JoinNode.EquiJoinClause(leftKey, rightKey)),
                        leftOutputs,
                        rightOutputs,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }
}
