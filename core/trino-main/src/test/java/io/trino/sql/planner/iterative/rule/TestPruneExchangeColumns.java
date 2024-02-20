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
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;

public class TestPruneExchangeColumns
        extends BaseRuleTest
{
    @Test
    public void testDoNotPruneReferencedOutputSymbol()
    {
        tester().assertThat(new PruneExchangeColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.identity(a),
                            p.exchange(e -> e
                                    .addSource(p.values(b))
                                    .addInputsSet(b)
                                    .singleDistributionPartitioningScheme(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPrunePartitioningSymbol()
    {
        tester().assertThat(new PruneExchangeColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.of(),
                            p.exchange(e -> e
                                    .addSource(p.values(b))
                                    .addInputsSet(b)
                                    .fixedHashDistributionPartitioningScheme(
                                            ImmutableList.of(a),
                                            ImmutableList.of(a))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneHashSymbol()
    {
        tester().assertThat(new PruneExchangeColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol h = p.symbol("h");
                    Symbol b = p.symbol("b");
                    Symbol h1 = p.symbol("h_1");
                    return p.project(
                            Assignments.identity(a),
                            p.exchange(e -> e
                                    .addSource(p.values(b, h1))
                                    .addInputsSet(b, h1)
                                    .fixedHashDistributionPartitioningScheme(
                                            ImmutableList.of(a, h),
                                            ImmutableList.of(a),
                                            h)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneOrderingSymbol()
    {
        tester().assertThat(new PruneExchangeColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.of(),
                            p.exchange(e -> e
                                    .addSource(p.values(b))
                                    .addInputsSet(b)
                                    .singleDistributionPartitioningScheme(a)
                                    .orderingScheme(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST)))));
                })
                .doesNotFire();
    }

    @Test
    public void testPruneUnreferencedSymbol()
    {
        tester().assertThat(new PruneExchangeColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.identity(a),
                            p.exchange(e -> e
                                    .addSource(p.values(a, b))
                                    .addInputsSet(a, b)
                                    .singleDistributionPartitioningScheme(a, b)));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", expression("a")),
                                exchange(
                                        REMOTE,
                                        GATHER,
                                        ImmutableList.of(),
                                        ImmutableSet.of(),
                                        Optional.of(ImmutableList.of(ImmutableList.of("a"))),
                                        values(ImmutableList.of("a", "b")))
                                        .withExactOutputs("a")));
    }

    @Test
    public void testPruneUnreferencedSymbolMultipleSources()
    {
        tester().assertThat(new PruneExchangeColumns())
                .on(p -> {
                    Symbol a1 = p.symbol("a_1");
                    Symbol a2 = p.symbol("a_2");
                    Symbol b1 = p.symbol("b_1");
                    Symbol b2 = p.symbol("b_2");
                    Symbol c1 = p.symbol("c_1");
                    Symbol c2 = p.symbol("c_2");
                    return p.project(
                            Assignments.identity(a1),
                            p.exchange(e -> e
                                    .addSource(p.values(b1, b2))
                                    .addInputsSet(b1, b2)
                                    .addSource(p.values(c1, c2))
                                    .addInputsSet(c1, c2)
                                    .singleDistributionPartitioningScheme(a1, a2)));
                })
                .matches(
                        project(
                                exchange(
                                        REMOTE,
                                        GATHER,
                                        ImmutableList.of(),
                                        ImmutableSet.of(),
                                        Optional.of(ImmutableList.of(ImmutableList.of("b_1"), ImmutableList.of("c_1"))),
                                        values(ImmutableList.of("b_1", "b_2")),
                                        values(ImmutableList.of("c_1", "c_2")))
                                        .withNumberOfOutputColumns(1)));
    }
}
