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
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.SpatialJoinNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.spatialJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPruneSpatialJoinChildrenColumns
        extends BaseRuleTest
{
    @Test
    public void testPruneOneChild()
    {
        tester().assertThat(new PruneSpatialJoinChildrenColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol r = p.symbol("r");
                    Symbol unused = p.symbol("unused");
                    return p.spatialJoin(
                            SpatialJoinNode.Type.INNER,
                            p.values(a, unused),
                            p.values(b, r),
                            ImmutableList.of(a, b, r),
                            expression("ST_Distance(a, b) <= r"));
                })
                .matches(
                        spatialJoin(
                                "ST_Distance(a, b) <= r",
                                Optional.empty(),
                                Optional.of(ImmutableList.of("a", "b", "r")),
                                strictProject(
                                        ImmutableMap.of("a", PlanMatchPattern.expression("a")),
                                        values("a", "unused")),
                                values("b", "r")));
    }

    @Test
    public void testPruneBothChildren()
    {
        tester().assertThat(new PruneSpatialJoinChildrenColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol r = p.symbol("r");
                    Symbol unusedLeft = p.symbol("unused_left");
                    Symbol unusedRight = p.symbol("unused_right");
                    return p.spatialJoin(
                            SpatialJoinNode.Type.INNER,
                            p.values(a, unusedLeft),
                            p.values(b, r, unusedRight),
                            ImmutableList.of(a, b, r),
                            expression("ST_Distance(a, b) <= r"));
                })
                .matches(
                        spatialJoin(
                                "ST_Distance(a, b) <= r",
                                Optional.empty(),
                                Optional.of(ImmutableList.of("a", "b", "r")),
                                strictProject(
                                        ImmutableMap.of("a", PlanMatchPattern.expression("a")),
                                        values("a", "unused_left")),
                                strictProject(
                                        ImmutableMap.of("b", PlanMatchPattern.expression("b"), "r", PlanMatchPattern.expression("r")),
                                        values("b", "r", "unused_right"))));
    }

    @Test
    public void testDoNotPruneOneOutputOrFilterSymbols()
    {
        tester().assertThat(new PruneSpatialJoinChildrenColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol r = p.symbol("r");
                    Symbol output = p.symbol("output");
                    return p.spatialJoin(
                            SpatialJoinNode.Type.INNER,
                            p.values(a),
                            p.values(b, r, output),
                            ImmutableList.of(output),
                            expression("ST_Distance(a, b) <= r"));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPrunePartitionSymbols()
    {
        tester().assertThat(new PruneSpatialJoinChildrenColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol r = p.symbol("r");
                    Symbol leftPartitionSymbol = p.symbol("left_partition_symbol");
                    Symbol rightPartitionSymbol = p.symbol("right_partition_symbol");
                    return p.spatialJoin(
                            SpatialJoinNode.Type.INNER,
                            p.values(a, leftPartitionSymbol),
                            p.values(b, r, rightPartitionSymbol),
                            ImmutableList.of(a, b, r),
                            expression("ST_Distance(a, b) <= r"),
                            Optional.of(leftPartitionSymbol),
                            Optional.of(rightPartitionSymbol),
                            Optional.of("some nice kdb tree"));
                })
                .doesNotFire();
    }
}
