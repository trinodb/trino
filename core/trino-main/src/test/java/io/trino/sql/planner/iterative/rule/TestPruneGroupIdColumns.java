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
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.groupId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneGroupIdColumns
        extends BaseRuleTest
{
    @Test
    public void testPruneAggregationArgument()
    {
        tester().assertThat(new PruneGroupIdColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol k = p.symbol("k");
                    Symbol groupId = p.symbol("group_id");
                    return p.project(
                            Assignments.identity(a, k, groupId),
                            p.groupId(
                                    ImmutableList.of(ImmutableList.of(k), ImmutableList.of(k)),
                                    ImmutableList.of(a, b),
                                    groupId,
                                    p.values(a, b, k)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("a", expression("a"), "k", expression("k"), "group_id", expression("group_id")),
                                groupId(
                                        ImmutableList.of(ImmutableList.of("k"), ImmutableList.of("k")),
                                        ImmutableList.of("a"),
                                        "group_id",
                                        values("a", "b", "k"))));
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneGroupIdColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol k = p.symbol("k");
                    Symbol groupId = p.symbol("group_id");
                    return p.project(
                            Assignments.identity(a, k, groupId),
                            p.groupId(
                                    ImmutableList.of(ImmutableList.of(k), ImmutableList.of(k)),
                                    ImmutableList.of(a),
                                    groupId,
                                    p.values(a, k)));
                }).doesNotFire();
    }

    @Test
    public void doNotPruneGroupingSymbols()
    {
        tester().assertThat(new PruneGroupIdColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol k = p.symbol("k");
                    Symbol groupId = p.symbol("group_id");
                    return p.project(
                            Assignments.identity(a, groupId),
                            p.groupId(
                                    ImmutableList.of(ImmutableList.of(k), ImmutableList.of(k)),
                                    ImmutableList.of(a),
                                    groupId,
                                    p.values(a, k)));
                }).doesNotFire();
    }

    @Test
    public void testGroupIdSymbolUnreferenced()
    {
        tester().assertThat(new PruneGroupIdColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol k = p.symbol("k");
                    Symbol groupId = p.symbol("group_id");
                    return p.project(
                            Assignments.identity(a, k),
                            p.groupId(
                                    ImmutableList.of(ImmutableList.of(k), ImmutableList.of(k)),
                                    ImmutableList.of(a),
                                    groupId,
                                    p.values(a, k)));
                }).doesNotFire();
    }
}
