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
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.distinctLimit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneDistinctLimitSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testPruneInputColumn()
    {
        tester().assertThat(new PruneDistinctLimitSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.distinctLimit(
                            5,
                            ImmutableList.of(a),
                            p.values(a, b));
                })
                .matches(
                        distinctLimit(
                                5,
                                ImmutableList.of("a"),
                                strictProject(
                                        ImmutableMap.of("a", expression(new Reference(BIGINT, "a"))),
                                        values("a", "b"))));

        tester().assertThat(new PruneDistinctLimitSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.distinctLimit(
                            5,
                            ImmutableList.of(a),
                            p.values(a, b));
                })
                .matches(
                        distinctLimit(
                                5,
                                ImmutableList.of("a"),
                                strictProject(
                                        ImmutableMap.of("a", expression(new Reference(BIGINT, "a"))),
                                        values("a", "b"))));
    }

    @Test
    public void allInputsNeeded()
    {
        tester().assertThat(new PruneDistinctLimitSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.distinctLimit(
                            5,
                            ImmutableList.of(a, b),
                            p.values(a, b));
                })
                .doesNotFire();
    }
}
