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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.SetOperationOutputMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.union;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;

public class TestImplementIntersectAll
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new ImplementIntersectAll(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol a1 = p.symbol("a_1");
                    Symbol a2 = p.symbol("a_2");
                    Symbol b = p.symbol("b");
                    Symbol b1 = p.symbol("b_1");
                    Symbol b2 = p.symbol("b_2");
                    return p.intersect(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(a, a1)
                                    .put(a, a2)
                                    .put(b, b1)
                                    .put(b, b2)
                                    .build(),
                            ImmutableList.of(
                                    p.values(a1, b1),
                                    p.values(a2, b2)),
                            false);
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("a", expression("a"), "b", expression("b")),
                                filter(
                                        "row_number <= least(count_1, count_2)",
                                        strictProject(
                                                ImmutableMap.of("a", expression("a"), "b", expression("b"), "count_1", expression("count_1"), "count_2", expression("count_2"), "row_number", expression("row_number")),
                                                window(builder -> builder
                                                                .specification(specification(
                                                                        ImmutableList.of("a", "b"),
                                                                        ImmutableList.of(),
                                                                        ImmutableMap.of()))
                                                                .addFunction(
                                                                        "count_1",
                                                                        functionCall(
                                                                                "count",
                                                                                Optional.empty(),
                                                                                ImmutableList.of("marker_1")))
                                                                .addFunction(
                                                                        "count_2",
                                                                        functionCall(
                                                                                "count",
                                                                                Optional.empty(),
                                                                                ImmutableList.of("marker_2")))
                                                                .addFunction(
                                                                        "row_number",
                                                                        functionCall(
                                                                                "row_number",
                                                                                Optional.empty(),
                                                                                ImmutableList.of())),
                                                        union(
                                                                project(
                                                                        ImmutableMap.of(
                                                                                "a1", expression("a_1"),
                                                                                "b1", expression("b_1"),
                                                                                "marker_left_1", expression("true"),
                                                                                "marker_left_2", expression("CAST(null AS boolean)")),
                                                                        values("a_1", "b_1")),
                                                                project(
                                                                        ImmutableMap.of(
                                                                                "a2", expression("a_2"),
                                                                                "b2", expression("b_2"),
                                                                                "marker_right_1", expression("CAST(null AS boolean)"),
                                                                                "marker_right_2", expression("true")),
                                                                        values("a_2", "b_2")))
                                                                .withAlias("a", new SetOperationOutputMatcher(0))
                                                                .withAlias("b", new SetOperationOutputMatcher(1))
                                                                .withAlias("marker_1", new SetOperationOutputMatcher(2))
                                                                .withAlias("marker_2", new SetOperationOutputMatcher(3)))))));
    }
}
