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
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.UnnestNode.Mapping;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.UnnestMapping.unnestMapping;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;

public class TestPruneUnnestColumns
        extends BaseRuleTest
{
    @Test
    public void testPruneOrdinalitySymbol()
    {
        tester().assertThat(new PruneUnnestColumns())
                .on(p -> {
                    Symbol replicateSymbol = p.symbol("replicate_symbol");
                    Symbol unnestSymbol = p.symbol("unnest_symbol");
                    Symbol unnestedSymbol = p.symbol("unnested_symbol");
                    Symbol ordinalitySymbol = p.symbol("ordinality_symbol");
                    return p.project(
                            Assignments.identity(replicateSymbol, unnestedSymbol),
                            p.unnest(
                                    ImmutableList.of(replicateSymbol),
                                    ImmutableList.of(new Mapping(unnestSymbol, ImmutableList.of(unnestedSymbol))),
                                    Optional.of(ordinalitySymbol),
                                    INNER,
                                    Optional.empty(),
                                    p.values(replicateSymbol, unnestSymbol)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("replicate_symbol", expression("replicate_symbol"), "unnested_symbol", expression("unnested_symbol")),
                                unnest(
                                        ImmutableList.of("replicate_symbol"),
                                        ImmutableList.of(unnestMapping("unnest_symbol", ImmutableList.of("unnested_symbol"))),
                                        Optional.empty(),
                                        INNER,
                                        Optional.empty(),
                                        values("replicate_symbol", "unnest_symbol"))));
    }

    @Test
    public void testPruneReplicateSymbol()
    {
        tester().assertThat(new PruneUnnestColumns())
                .on(p -> {
                    Symbol replicateSymbol = p.symbol("replicate_symbol");
                    Symbol unnestSymbol = p.symbol("unnest_symbol");
                    Symbol unnestedSymbol = p.symbol("unnested_symbol");
                    Symbol ordinalitySymbol = p.symbol("ordinality_symbol");
                    return p.project(
                            Assignments.identity(unnestedSymbol, ordinalitySymbol),
                            p.unnest(
                                    ImmutableList.of(replicateSymbol),
                                    ImmutableList.of(new Mapping(unnestSymbol, ImmutableList.of(unnestedSymbol))),
                                    Optional.of(ordinalitySymbol),
                                    INNER,
                                    Optional.empty(),
                                    p.values(replicateSymbol, unnestSymbol)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("unnested_symbol", expression("unnested_symbol"), "ordinality_symbol", expression("ordinality_symbol")),
                                unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(unnestMapping("unnest_symbol", ImmutableList.of("unnested_symbol"))),
                                        Optional.of("ordinality_symbol"),
                                        INNER,
                                        Optional.empty(),
                                        values("replicate_symbol", "unnest_symbol"))));
    }

    @Test
    public void testDoNotPruneOrdinalitySymbolUsedInFilter()
    {
        tester().assertThat(new PruneUnnestColumns())
                .on(p -> {
                    Symbol replicateSymbol = p.symbol("replicate_symbol");
                    Symbol unnestSymbol = p.symbol("unnest_symbol");
                    Symbol unnestedSymbol = p.symbol("unnested_symbol");
                    Symbol ordinalitySymbol = p.symbol("ordinality_symbol");
                    return p.project(
                            Assignments.identity(replicateSymbol, unnestedSymbol),
                            p.unnest(
                                    ImmutableList.of(replicateSymbol),
                                    ImmutableList.of(new Mapping(unnestSymbol, ImmutableList.of(unnestedSymbol))),
                                    Optional.of(ordinalitySymbol),
                                    INNER,
                                    Optional.of(PlanBuilder.expression("ordinality_symbol < BIGINT '5'")),
                                    p.values(replicateSymbol, unnestSymbol)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneReplicateSymbolUsedInFilter()
    {
        tester().assertThat(new PruneUnnestColumns())
                .on(p -> {
                    Symbol replicateSymbol = p.symbol("replicate_symbol");
                    Symbol unnestSymbol = p.symbol("unnest_symbol");
                    Symbol unnestedSymbol = p.symbol("unnested_symbol");
                    Symbol ordinalitySymbol = p.symbol("ordinality_symbol");
                    return p.project(
                            Assignments.identity(unnestedSymbol, ordinalitySymbol),
                            p.unnest(
                                    ImmutableList.of(replicateSymbol),
                                    ImmutableList.of(new Mapping(unnestSymbol, ImmutableList.of(unnestedSymbol))),
                                    Optional.of(ordinalitySymbol),
                                    INNER,
                                    Optional.of(PlanBuilder.expression("replicate_symbol < BIGINT '5'")),
                                    p.values(replicateSymbol, unnestSymbol)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneUnnestedSymbol()
    {
        tester().assertThat(new PruneUnnestColumns())
                .on(p -> {
                    Symbol replicateSymbol = p.symbol("replicate_symbol");
                    Symbol unnestSymbol = p.symbol("unnest_symbol");
                    Symbol unnestedSymbol = p.symbol("unnested_symbol");
                    Symbol ordinalitySymbol = p.symbol("ordinality_symbol");
                    return p.project(
                            Assignments.identity(replicateSymbol, ordinalitySymbol),
                            p.unnest(
                                    ImmutableList.of(replicateSymbol),
                                    ImmutableList.of(new Mapping(unnestSymbol, ImmutableList.of(unnestedSymbol))),
                                    Optional.of(ordinalitySymbol),
                                    INNER,
                                    Optional.empty(),
                                    p.values(replicateSymbol, unnestSymbol)));
                })
                .doesNotFire();
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneUnnestColumns())
                .on(p -> {
                    Symbol replicateSymbol = p.symbol("replicate_symbol");
                    Symbol unnestSymbol = p.symbol("unnest_symbol");
                    Symbol unnestedSymbol = p.symbol("unnested_symbol");
                    Symbol ordinalitySymbol = p.symbol("ordinality_symbol");
                    return p.project(
                            Assignments.identity(replicateSymbol, unnestedSymbol, ordinalitySymbol),
                            p.unnest(
                                    ImmutableList.of(replicateSymbol),
                                    ImmutableList.of(new Mapping(unnestSymbol, ImmutableList.of(unnestedSymbol))),
                                    Optional.of(ordinalitySymbol),
                                    INNER,
                                    Optional.empty(),
                                    p.values(replicateSymbol, unnestSymbol)));
                })
                .doesNotFire();
    }
}
