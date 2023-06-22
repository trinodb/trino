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
import io.trino.sql.planner.plan.UnnestNode.Mapping;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.UnnestMapping.unnestMapping;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneUnnestSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllInputsReferenced()
    {
        tester().assertThat(new PruneUnnestSourceColumns())
                .on(p -> {
                    Symbol replicateSymbol = p.symbol("replicate_symbol");
                    Symbol unnestSymbol = p.symbol("unnest_symbol");
                    Symbol unusedSymbol = p.symbol("unused_symbol");
                    Symbol unnestedSymbol = p.symbol("unnested_symbol");
                    return p.unnest(
                            ImmutableList.of(replicateSymbol),
                            ImmutableList.of(new Mapping(unnestSymbol, ImmutableList.of(unnestedSymbol))),
                            p.values(replicateSymbol, unnestSymbol, unusedSymbol));
                })
                .matches(
                        unnest(
                                ImmutableList.of("replicate_symbol"),
                                ImmutableList.of(unnestMapping("unnest_symbol", ImmutableList.of("unnested_symbol"))),
                                strictProject(
                                        ImmutableMap.of("replicate_symbol", expression("replicate_symbol"), "unnest_symbol", expression("unnest_symbol")),
                                        values("replicate_symbol", "unnest_symbol", "unused_symbol"))));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneUnnestSourceColumns())
                .on(p -> {
                    Symbol replicateSymbol = p.symbol("replicate_symbol");
                    Symbol unnestSymbol = p.symbol("unnest_symbol");
                    Symbol unnestedSymbol = p.symbol("unnested_symbol");
                    return p.unnest(
                            ImmutableList.of(replicateSymbol),
                            ImmutableList.of(new Mapping(unnestSymbol, ImmutableList.of(unnestedSymbol))),
                            p.values(replicateSymbol, unnestSymbol));
                })
                .doesNotFire();
    }
}
