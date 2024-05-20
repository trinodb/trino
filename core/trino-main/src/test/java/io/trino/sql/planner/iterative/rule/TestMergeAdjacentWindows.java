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
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.ExpectedValueProvider;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.WindowNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;

public class TestMergeAdjacentWindows
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final ResolvedFunction AVG = FUNCTION_RESOLUTION.resolveFunction("avg", fromTypes(DOUBLE));
    private static final ResolvedFunction SUM = FUNCTION_RESOLUTION.resolveFunction("sum", fromTypes(DOUBLE));
    private static final ResolvedFunction LAG = FUNCTION_RESOLUTION.resolveFunction("lag", fromTypes(DOUBLE));

    private static final String columnAAlias = "ALIAS_A";
    private static final ExpectedValueProvider<DataOrganizationSpecification> specificationA =
            specification(ImmutableList.of(columnAAlias), ImmutableList.of(), ImmutableMap.of());

    @Test
    public void testPlanWithoutWindowNode()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p -> p.values(p.symbol("a")))
                .doesNotFire();
    }

    @Test
    public void testPlanWithSingleWindowNode()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_1"), newWindowNodeFunction(AVG, "a")),
                                p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void testDistinctAdjacentWindowSpecifications()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_1"), newWindowNodeFunction(AVG, "a")),
                                p.window(
                                        newWindowNodeSpecification(p, "b"),
                                        ImmutableMap.of(p.symbol("sum_1"), newWindowNodeFunction(SUM, "b")),
                                        p.values(p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testIntermediateNonProjectNode()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(1))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_2"), newWindowNodeFunction(AVG, "a")),
                                p.filter(
                                        new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 5L)),
                                        p.window(
                                                newWindowNodeSpecification(p, "a"),
                                                ImmutableMap.of(p.symbol("avg_1"), newWindowNodeFunction(AVG, "a")),
                                                p.values(p.symbol("a"))))))
                .doesNotFire();
    }

    @Test
    public void testDependentAdjacentWindowsIdenticalSpecifications()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_1", DOUBLE), newWindowNodeFunction(AVG, "avg_2")),
                                p.window(
                                        newWindowNodeSpecification(p, "a"),
                                        ImmutableMap.of(p.symbol("avg_2", DOUBLE), newWindowNodeFunction(AVG, "a")),
                                        p.values(p.symbol("a", BIGINT)))))
                .doesNotFire();
    }

    @Test
    public void testDependentAdjacentWindowsDistinctSpecifications()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_1"), newWindowNodeFunction(AVG, "avg_2")),
                                p.window(
                                        newWindowNodeSpecification(p, "b"),
                                        ImmutableMap.of(p.symbol("avg_2"), newWindowNodeFunction(AVG, "a")),
                                        p.values(p.symbol("a"), p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testIdenticalAdjacentWindowSpecifications()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(new Symbol(DOUBLE, "a")),
                                ImmutableMap.of(p.symbol("avg_1", DOUBLE), newWindowNodeFunction(AVG, new Symbol(DOUBLE, "a"))),
                                p.window(
                                        newWindowNodeSpecification(new Symbol(DOUBLE, "a")),
                                        ImmutableMap.of(p.symbol("sum_1", DOUBLE), newWindowNodeFunction(SUM, new Symbol(DOUBLE, "a"))),
                                        p.values(p.symbol("a", DOUBLE)))))
                .matches(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationA)
                                        .addFunction(windowFunction(AVG.signature().getName().getFunctionName(), ImmutableList.of(columnAAlias), DEFAULT_FRAME))
                                        .addFunction(windowFunction(SUM.signature().getName().getFunctionName(), ImmutableList.of(columnAAlias), DEFAULT_FRAME)),
                                values(ImmutableMap.of(columnAAlias, 0))));
    }

    @Test
    public void testIntermediateProjectNodes()
    {
        String oneAlias = "ALIAS_one";
        String unusedAlias = "ALIAS_unused";
        String lagOutputAlias = "ALIAS_lagOutput";
        String avgOutputAlias = "ALIAS_avgOutput";

        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(2))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(new Symbol(DOUBLE, "a")),
                                ImmutableMap.of(p.symbol("lagOutput"), newWindowNodeFunction(LAG, new Symbol(DOUBLE, "a"), new Symbol(INTEGER, "one"))),
                                p.project(
                                        Assignments.builder()
                                                .put(p.symbol("one", INTEGER), new Constant(INTEGER, 1L))
                                                .putIdentities(ImmutableList.of(new Symbol(DOUBLE, "a"), p.symbol("avgOutput", DOUBLE)))
                                                .build(),
                                        p.project(
                                                Assignments.identity(new Symbol(DOUBLE, "a"), p.symbol("avgOutput", DOUBLE), p.symbol("unused")),
                                                p.window(
                                                        newWindowNodeSpecification(new Symbol(DOUBLE, "a")),
                                                        ImmutableMap.of(p.symbol("avgOutput", DOUBLE), newWindowNodeFunction(AVG, "a")),
                                                        p.values(p.symbol("a", DOUBLE), p.symbol("unused")))))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        columnAAlias, PlanMatchPattern.expression(new Reference(BIGINT, columnAAlias)),
                                        oneAlias, PlanMatchPattern.expression(new Reference(BIGINT, oneAlias)),
                                        lagOutputAlias, PlanMatchPattern.expression(new Reference(DOUBLE, lagOutputAlias)),
                                        avgOutputAlias, PlanMatchPattern.expression(new Reference(DOUBLE, avgOutputAlias))),
                                window(windowMatcherBuilder -> windowMatcherBuilder
                                                .specification(specificationA)
                                                .addFunction(lagOutputAlias, windowFunction(LAG.signature().getName().getFunctionName(), ImmutableList.of(columnAAlias, oneAlias), DEFAULT_FRAME))
                                                .addFunction(avgOutputAlias, windowFunction(AVG.signature().getName().getFunctionName(), ImmutableList.of(columnAAlias), DEFAULT_FRAME)),
                                        strictProject(
                                                ImmutableMap.of(
                                                        oneAlias, PlanMatchPattern.expression(new Constant(INTEGER, 1L)),
                                                        columnAAlias, PlanMatchPattern.expression(new Reference(BIGINT, columnAAlias)),
                                                        unusedAlias, PlanMatchPattern.expression(new Reference(BIGINT, unusedAlias))),
                                                strictProject(
                                                        ImmutableMap.of(
                                                                columnAAlias, PlanMatchPattern.expression(new Reference(BIGINT, columnAAlias)),
                                                                unusedAlias, PlanMatchPattern.expression(new Reference(BIGINT, unusedAlias))),
                                                        values(columnAAlias, unusedAlias))))));
    }

    private static DataOrganizationSpecification newWindowNodeSpecification(Symbol symbol)
    {
        return new DataOrganizationSpecification(ImmutableList.of(symbol), Optional.empty());
    }

    private static DataOrganizationSpecification newWindowNodeSpecification(PlanBuilder planBuilder, String symbolName)
    {
        return new DataOrganizationSpecification(ImmutableList.of(planBuilder.symbol(symbolName, BIGINT)), Optional.empty());
    }

    private static WindowNode.Function newWindowNodeFunction(ResolvedFunction resolvedFunction, Symbol... symbols)
    {
        return new WindowNode.Function(
                resolvedFunction,
                Arrays.stream(symbols)
                        .map(Symbol::toSymbolReference)
                        .collect(Collectors.toList()),
                DEFAULT_FRAME,
                false);
    }

    private static WindowNode.Function newWindowNodeFunction(ResolvedFunction resolvedFunction, String... symbols)
    {
        return new WindowNode.Function(
                resolvedFunction,
                Arrays.stream(symbols)
                        .map(name -> new Reference(DOUBLE, name))
                        .collect(Collectors.toList()),
                DEFAULT_FRAME,
                false);
    }
}
