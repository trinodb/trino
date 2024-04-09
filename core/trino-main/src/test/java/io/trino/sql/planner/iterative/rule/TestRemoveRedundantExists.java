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
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.testing.TestingMetadata;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestRemoveRedundantExists
        extends BaseRuleTest
{
    @Test
    public void testExistsFalse()
    {
        tester().assertThat(new RemoveRedundantExists())
                .on(p -> p.apply(ImmutableMap.of(p.symbol("exists", BOOLEAN), new ApplyNode.Exists()),
                        ImmutableList.of(),
                        p.values(1),
                        p.values(0)))
                .matches(
                        project(
                                ImmutableMap.of("exists", expression(FALSE)),
                                values()));
    }

    @Test
    public void testExistsTrue()
    {
        tester().assertThat(new RemoveRedundantExists())
                .on(p -> p.apply(ImmutableMap.of(p.symbol("exists", BOOLEAN), new ApplyNode.Exists()),
                        ImmutableList.of(),
                        p.values(1),
                        p.values(1)))
                .matches(
                        project(
                                ImmutableMap.of("exists", expression(TRUE)),
                                values()));
    }

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveRedundantExists())
                .on(p -> p.apply(ImmutableMap.of(p.symbol("exists", BOOLEAN), new ApplyNode.Exists()),
                        ImmutableList.of(),
                        p.values(1),
                        p.tableScan(ImmutableList.of(), ImmutableMap.of())))
                .doesNotFire();

        tester().assertThat(new RemoveRedundantExists())
                .on(p -> p.apply(
                        ImmutableMap.<Symbol, ApplyNode.SetExpression>builder()
                                .put(p.symbol("exists", BOOLEAN), new ApplyNode.Exists())
                                .put(p.symbol("other"), new ApplyNode.In(p.symbol("value"), p.symbol("list")))
                                .buildOrThrow(),
                        ImmutableList.of(),
                        p.values(1, p.symbol("value")),
                        p.tableScan(ImmutableList.of(p.symbol("list")), ImmutableMap.of(p.symbol("list"), new TestingMetadata.TestingColumnHandle("list")))))
                .doesNotFire();
    }
}
