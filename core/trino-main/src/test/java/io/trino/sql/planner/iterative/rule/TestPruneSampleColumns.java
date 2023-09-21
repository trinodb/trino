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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.SampleNode;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.SampleNode.Type.SYSTEM;

public class TestPruneSampleColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllInputsReferenced()
    {
        tester().assertThat(new PruneSampleColumns())
                .on(p -> p.project(
                        Assignments.identity(p.symbol("b")),
                        p.sample(
                                0.5,
                                SYSTEM,
                                p.values(p.symbol("a"), p.symbol("b")))))
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression("b")),
                                node(SampleNode.class,
                                        strictProject(
                                                ImmutableMap.of("b", expression("b")),
                                                values("a", "b")))));
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneSampleColumns())
                .on(p -> p.project(
                        Assignments.identity(p.symbol("a"), p.symbol("b")),
                        p.sample(
                                0.5,
                                SYSTEM,
                                p.values(p.symbol("a"), p.symbol("b"))))).doesNotFire();
    }
}
