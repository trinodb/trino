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
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.FilterNode;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.union;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestImplementExceptAsUnion
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new ImplementExceptAsUnion())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    return p.except(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(c, a)
                                    .put(c, b)
                                    .build(),
                            ImmutableList.of(
                                    p.values(a),
                                    p.values(b)));
                })
                .matches(
                        project(
                                node(FilterNode.class,
                                        node(AggregationNode.class,
                                                union(
                                                        project(
                                                                ImmutableMap.of(
                                                                        "leftValue", expression("a"),
                                                                        "left_marker_1", expression("true"),
                                                                        "left_marker_2", expression("CAST(null as boolean)")),
                                                                values("a")),
                                                        project(
                                                                ImmutableMap.of(
                                                                        "rightValue", expression("b"),
                                                                        "right_marker_1", expression("CAST(null as boolean)"),
                                                                        "right_marker_2", expression("true")),
                                                                values("b")))))));
    }
}
