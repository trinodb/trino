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
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.WindowNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.metadata.TestMetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;
import static io.trino.sql.tree.SortItem.NullOrdering.LAST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestPruneOrderByInWindowAggregation
        extends BaseRuleTest
{
    private static final Metadata METADATA = createTestMetadataManager();

    @Test
    public void testBasics()
    {
        tester().assertThat(new PruneOrderByInWindowAggregation(METADATA))
                .on(planBuilder -> {
                    Symbol avg = planBuilder.symbol("avg");
                    Symbol arrayAgg = planBuilder.symbol("araray_agg");
                    Symbol input = planBuilder.symbol("input");
                    Symbol key = planBuilder.symbol("key");
                    Symbol keyHash = planBuilder.symbol("keyHash");
                    Symbol mask = planBuilder.symbol("mask");
                    List<Symbol> sourceSymbols = ImmutableList.of(input, key, keyHash, mask);

                    ResolvedFunction avgFunction = METADATA.resolveBuiltinFunction("avg", fromTypes(BIGINT));
                    ResolvedFunction arrayAggFunction = METADATA.resolveBuiltinFunction("array_agg", fromTypes(BIGINT));

                    return planBuilder.window(
                            new DataOrganizationSpecification(ImmutableList.of(planBuilder.symbol("key", BIGINT)), Optional.empty()),
                            ImmutableMap.of(
                                    avg, new WindowNode.Function(avgFunction,
                                            ImmutableList.of(new Reference(BIGINT, "input")),
                                            Optional.of(new OrderingScheme(
                                                    ImmutableList.of(new Symbol(BIGINT, "input")),
                                                    ImmutableMap.of(new Symbol(BIGINT, "input"), SortOrder.ASC_NULLS_LAST))),
                                            DEFAULT_FRAME,
                                            false,
                                            false),
                                    arrayAgg, new WindowNode.Function(arrayAggFunction,
                                            ImmutableList.of(new Reference(BIGINT, "input")),
                                            Optional.of(new OrderingScheme(
                                                    ImmutableList.of(new Symbol(BIGINT, "input")),
                                                    ImmutableMap.of(new Symbol(BIGINT, "input"), SortOrder.ASC_NULLS_LAST))),
                                            DEFAULT_FRAME,
                                            false,
                                            false)),
                            planBuilder.values(sourceSymbols, ImmutableList.of()));
                })
                .matches(
                        window(
                                windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(
                                                ImmutableList.of("key"),
                                                ImmutableList.of(),
                                                ImmutableMap.of()))
                                        .addFunction(
                                                "avg",
                                                windowFunction("avg", ImmutableList.of("input"), DEFAULT_FRAME))
                                        .addFunction(
                                                "array_agg",
                                                windowFunction("array_agg", ImmutableList.of("input"), DEFAULT_FRAME, ImmutableList.of(sort("input", ASCENDING, LAST)))),
                                values("input", "key", "keyHash", "mask")));
    }
}
