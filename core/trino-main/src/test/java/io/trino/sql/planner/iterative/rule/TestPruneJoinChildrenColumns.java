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
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;

public class TestPruneJoinChildrenColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllInputsReferenced()
    {
        tester().assertThat(new PruneJoinChildrenColumns())
                .on(p -> buildJoin(p, symbol -> symbol.getName().equals("leftValue")))
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("leftKey", "rightKey")
                                .filter("leftValue > 5")
                                .left(values("leftKey", "leftKeyHash", "leftValue"))
                                .right(
                                        strictProject(
                                                ImmutableMap.of(
                                                        "rightKey", PlanMatchPattern.expression("rightKey"),
                                                        "rightKeyHash", PlanMatchPattern.expression("rightKeyHash")),
                                                values("rightKey", "rightKeyHash", "rightValue")))));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneJoinChildrenColumns())
                .on(p -> buildJoin(p, Predicates.alwaysTrue()))
                .doesNotFire();
    }

    @Test
    public void testCrossJoin()
    {
        tester().assertThat(new PruneJoinChildrenColumns())
                .on(p -> {
                    Symbol leftValue = p.symbol("leftValue");
                    Symbol rightValue = p.symbol("rightValue");
                    return p.join(
                            INNER,
                            p.values(leftValue),
                            p.values(rightValue),
                            ImmutableList.of(),
                            ImmutableList.of(leftValue),
                            ImmutableList.of(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty());
                })
                .matches(
                        join(INNER, builder -> builder
                                .left(values("leftValue"))
                                .right(
                                        strictProject(
                                                ImmutableMap.of(),
                                                values("rightValue")))));
    }

    private static PlanNode buildJoin(PlanBuilder p, Predicate<Symbol> joinOutputFilter)
    {
        Symbol leftKey = p.symbol("leftKey");
        Symbol leftKeyHash = p.symbol("leftKeyHash");
        Symbol leftValue = p.symbol("leftValue");
        Symbol rightKey = p.symbol("rightKey");
        Symbol rightKeyHash = p.symbol("rightKeyHash");
        Symbol rightValue = p.symbol("rightValue");
        List<Symbol> leftOutputs = ImmutableList.of(leftValue);
        List<Symbol> rightOutputs = ImmutableList.of(rightValue);
        return p.join(
                INNER,
                p.values(leftKey, leftKeyHash, leftValue),
                p.values(rightKey, rightKeyHash, rightValue),
                ImmutableList.of(new JoinNode.EquiJoinClause(leftKey, rightKey)),
                leftOutputs.stream()
                        .filter(joinOutputFilter)
                        .collect(toImmutableList()),
                rightOutputs.stream()
                        .filter(joinOutputFilter)
                        .collect(toImmutableList()),
                Optional.of(expression("leftValue > 5")),
                Optional.of(leftKeyHash),
                Optional.of(rightKeyHash));
    }
}
