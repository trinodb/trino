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
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.union;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestImplementIntersectDistinctAsUnion
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new ImplementIntersectDistinctAsUnion(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    return p.intersect(
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
                                                                        "leftValue", expression(new Reference(BIGINT, "a")),
                                                                        "left_marker_1", expression(TRUE),
                                                                        "left_marker_2", expression(new Constant(BOOLEAN, null))),
                                                                values("a")),
                                                        project(
                                                                ImmutableMap.of(
                                                                        "rightValue", expression(new Reference(BIGINT, "b")),
                                                                        "right_marker_1", expression(new Constant(BOOLEAN, null)),
                                                                        "right_marker_2", expression(TRUE)),
                                                                values("b")))))));
    }
}
