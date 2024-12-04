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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.ExpectedValueProvider;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.WindowNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;

public class TestSwapAdjacentWindowsBySpecifications
        extends BaseRuleTest
{
    private final ResolvedFunction resolvedFunction;

    public TestSwapAdjacentWindowsBySpecifications()
    {
        resolvedFunction = new TestingFunctionResolution().resolveFunction("avg", fromTypes(BIGINT));
    }

    @Test
    public void doesNotFireOnPlanWithoutWindowFunctions()
    {
        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p -> p.values(p.symbol("a")))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnPlanWithSingleWindowNode()
    {
        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p -> p.window(new DataOrganizationSpecification(
                                ImmutableList.of(p.symbol("a")),
                                Optional.empty()),
                        ImmutableMap.of(p.symbol("avg_1"),
                                new WindowNode.Function(resolvedFunction, ImmutableList.of(), Optional.empty(), DEFAULT_FRAME, false, false)),
                        p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void subsetComesFirst()
    {
        String columnAAlias = "ALIAS_A";
        String columnBAlias = "ALIAS_B";

        ExpectedValueProvider<DataOrganizationSpecification> specificationA = specification(ImmutableList.of(columnAAlias), ImmutableList.of(), ImmutableMap.of());
        ExpectedValueProvider<DataOrganizationSpecification> specificationAB = specification(ImmutableList.of(columnAAlias, columnBAlias), ImmutableList.of(), ImmutableMap.of());

        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p ->
                        p.window(new DataOrganizationSpecification(
                                        ImmutableList.of(p.symbol("a")),
                                        Optional.empty()),
                                ImmutableMap.of(p.symbol("avg_1", DOUBLE),
                                        new WindowNode.Function(resolvedFunction, ImmutableList.of(new Reference(BIGINT, "a")), Optional.empty(), DEFAULT_FRAME, false, false)),
                                p.window(new DataOrganizationSpecification(
                                                ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                                Optional.empty()),
                                        ImmutableMap.of(p.symbol("avg_2", DOUBLE),
                                                new WindowNode.Function(resolvedFunction, ImmutableList.of(new Reference(BIGINT, "b")), Optional.empty(), DEFAULT_FRAME, false, false)),
                                        p.values(p.symbol("a"), p.symbol("b")))))
                .matches(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationAB)
                                        .addFunction(windowFunction("avg", ImmutableList.of(columnBAlias), DEFAULT_FRAME)),
                                window(windowMatcherBuilder -> windowMatcherBuilder
                                                .specification(specificationA)
                                                .addFunction(windowFunction("avg", ImmutableList.of(columnAAlias), DEFAULT_FRAME)),
                                        values(ImmutableMap.of(columnAAlias, 0, columnBAlias, 1)))));
    }

    @Test
    public void dependentWindowsAreNotReordered()
    {
        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p ->
                        p.window(new DataOrganizationSpecification(
                                        ImmutableList.of(p.symbol("a", BIGINT)),
                                        Optional.empty()),
                                ImmutableMap.of(p.symbol("avg_1", DOUBLE),
                                        new WindowNode.Function(resolvedFunction, ImmutableList.of(new Reference(DOUBLE, "avg_2")), Optional.empty(), DEFAULT_FRAME, false, false)),
                                p.window(new DataOrganizationSpecification(
                                                ImmutableList.of(p.symbol("a", BIGINT), p.symbol("b", BIGINT)),
                                                Optional.empty()),
                                        ImmutableMap.of(p.symbol("avg_2", DOUBLE),
                                                new WindowNode.Function(resolvedFunction, ImmutableList.of(new Reference(BIGINT, "a")), Optional.empty(), DEFAULT_FRAME, false, false)),
                                        p.values(p.symbol("a", BIGINT), p.symbol("b", BIGINT)))))
                .doesNotFire();
    }
}
