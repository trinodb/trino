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
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.SpatialJoinNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.spatialJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPruneSpatialJoinColumns
        extends BaseRuleTest
{
    @Test
    public void notAllOutputsReferenced()
    {
        tester().assertThat(new PruneSpatialJoinColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol r = p.symbol("r");
                    return p.project(
                            Assignments.identity(a),
                            p.spatialJoin(
                                    SpatialJoinNode.Type.INNER,
                                    p.values(a),
                                    p.values(b, r),
                                    ImmutableList.of(a, b, r),
                                    expression("ST_Distance(a, b) <= r")));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("a", PlanMatchPattern.expression("a")),
                                spatialJoin(
                                        "ST_Distance(a, b) <= r",
                                        Optional.empty(),
                                        Optional.of(ImmutableList.of("a")),
                                        values("a"),
                                        values("b", "r"))));
    }

    @Test
    public void allOutputsReferenced()
    {
        tester().assertThat(new PruneSpatialJoinColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol r = p.symbol("r");
                    return p.project(
                            Assignments.identity(a, b, r),
                            p.spatialJoin(
                                    SpatialJoinNode.Type.INNER,
                                    p.values(a),
                                    p.values(b, r),
                                    ImmutableList.of(a, b, r),
                                    expression("ST_Distance(a, b) <= r")));
                })
                .doesNotFire();
    }
}
