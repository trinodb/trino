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
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.groupId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneGroupIdSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testPruneInputColumn()
    {
        tester().assertThat(new PruneGroupIdSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol k = p.symbol("k");
                    Symbol groupId = p.symbol("group_id");
                    return p.groupId(
                            ImmutableList.of(ImmutableList.of(k), ImmutableList.of(k)),
                            ImmutableList.of(a),
                            groupId,
                            p.values(a, b, k));
                })
                .matches(
                        groupId(
                                ImmutableList.of(ImmutableList.of("k"), ImmutableList.of("k")),
                                ImmutableList.of("a"),
                                "group_id",
                                strictProject(
                                        ImmutableMap.of("a", expression("a"), "k", expression("k")),
                                        values("a", "b", "k"))));
    }

    @Test
    public void testPruneInputColumnWithMapping()
    {
        tester().assertThat(new PruneGroupIdSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol k = p.symbol("k");
                    Symbol newK = p.symbol("newK");
                    Symbol groupId = p.symbol("group_id");
                    return p.groupId(
                            ImmutableList.of(ImmutableList.of(newK)),
                            ImmutableMap.of(newK, k),
                            ImmutableList.of(a),
                            groupId,
                            p.values(a, b, k));
                })
                .matches(
                        groupId(
                                ImmutableList.of(ImmutableList.of("newK")),
                                ImmutableMap.of("newK", "k"),
                                ImmutableList.of("a"),
                                "group_id",
                                strictProject(
                                        ImmutableMap.of("a", expression("a"), "k", expression("k")),
                                        values("a", "b", "k"))));
    }

    @Test
    public void allInputsReferenced()
    {
        tester().assertThat(new PruneGroupIdSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol k = p.symbol("k");
                    Symbol groupId = p.symbol("group_id");
                    return p.groupId(
                            ImmutableList.of(ImmutableList.of(k), ImmutableList.of(k)),
                            ImmutableList.of(a),
                            groupId,
                            p.values(a, k));
                })
                .doesNotFire();
    }
}
