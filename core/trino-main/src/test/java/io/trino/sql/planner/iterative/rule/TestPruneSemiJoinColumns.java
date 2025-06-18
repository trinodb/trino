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
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneSemiJoinColumns
        extends BaseRuleTest
{
    @Test
    public void testSemiJoinNotNeeded()
    {
        tester().assertThat(new PruneSemiJoinColumns())
                .on(p -> buildProjectedSemiJoin(p, symbol -> symbol.name().equals("leftValue")))
                .matches(
                        strictProject(
                                ImmutableMap.of("leftValue", expression(new Reference(BIGINT, "leftValue"))),
                                values("leftKey", "leftValue")));
    }

    @Test
    public void testAllColumnsNeeded()
    {
        tester().assertThat(new PruneSemiJoinColumns())
                .on(p -> buildProjectedSemiJoin(p, symbol -> true))
                .doesNotFire();
    }

    @Test
    public void testKeysNotNeeded()
    {
        tester().assertThat(new PruneSemiJoinColumns())
                .on(p -> buildProjectedSemiJoin(p, symbol -> (symbol.name().equals("leftValue") || symbol.name().equals("match"))))
                .doesNotFire();
    }

    @Test
    public void testValueNotNeeded()
    {
        tester().assertThat(new PruneSemiJoinColumns())
                .on(p -> buildProjectedSemiJoin(p, symbol -> symbol.name().equals("match")))
                .matches(
                        strictProject(
                                ImmutableMap.of("match", expression(new Reference(BOOLEAN, "match"))),
                                semiJoin("leftKey", "rightKey", "match",
                                        strictProject(
                                                ImmutableMap.of(
                                                        "leftKey", expression(new Reference(BIGINT, "leftKey"))),
                                                values("leftKey", "leftValue")),
                                        values("rightKey"))));
    }

    private static PlanNode buildProjectedSemiJoin(PlanBuilder p, Predicate<Symbol> projectionFilter)
    {
        Symbol match = p.symbol("match");
        Symbol leftKey = p.symbol("leftKey");
        Symbol leftValue = p.symbol("leftValue");
        Symbol rightKey = p.symbol("rightKey");
        List<Symbol> outputs = ImmutableList.of(match, leftKey, leftValue);
        return p.project(
                Assignments.identity(
                        outputs.stream()
                                .filter(projectionFilter)
                                .collect(toImmutableList())),
                p.semiJoin(
                        leftKey,
                        rightKey,
                        match,
                        p.values(leftKey, leftValue),
                        p.values(rightKey)));
    }
}
