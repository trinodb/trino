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
import com.google.common.reflect.ClassPath;
import io.trino.FeaturesConfig;
import io.trino.metadata.SystemFunctionBundle;
import io.trino.operator.ParametricImplementationsGroup;
import io.trino.operator.aggregation.AggregationFromAnnotationsParser;
import io.trino.operator.aggregation.ParametricAggregation;
import io.trino.operator.aggregation.ParametricAggregationImplementation;
import io.trino.spi.NodeVersion;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeTemplate;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.type.BlockTypeOperators;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.INTERMEDIATE;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeduplicatePartialAggregations
        extends BaseRuleTest
{
    @Test
    public void testSharedStateFunctionsMatchAggregationClasses()
            throws IOException
    {
        TypeOperators typeOperators = new TypeOperators();
        Set<FunctionId> systemFunctions = SystemFunctionBundle.create(new FeaturesConfig(), typeOperators, new BlockTypeOperators(typeOperators), NodeVersion.UNKNOWN)
                .getFunctions().stream()
                .map(FunctionMetadata::getFunctionId)
                .collect(toImmutableSet());

        Map<SharedInput, Set<String>> signaturesBySharedInput = new HashMap<>();
        String mainClasses = SystemFunctionBundle.class.getProtectionDomain().getCodeSource().getLocation().toString();
        for (ClassPath.ClassInfo classInfo : ClassPath.from(getClass().getClassLoader()).getTopLevelClassesRecursive("io.trino")) {
            if (!classInfo.url().toString().startsWith(mainClasses)) {
                continue;
            }
            Class<?> clazz = classInfo.load();
            if (!clazz.isAnnotationPresent(AggregationFunction.class)) {
                continue;
            }
            for (ParametricAggregation function : AggregationFromAnnotationsParser.parseFunctionDefinitions(clazz)) {
                if (!systemFunctions.contains(function.getFunctionMetadata().getFunctionId()) || !function.getAggregationMetadata().isDecomposable()) {
                    continue;
                }
                ParametricImplementationsGroup<ParametricAggregationImplementation> implementations = function.getImplementations();
                Stream.of(implementations.getExactImplementations().values(), implementations.getGenericImplementations(), implementations.getSpecializedImplementations())
                        .flatMap(Collection::stream)
                        .forEach(implementation -> signaturesBySharedInput
                                .computeIfAbsent(new SharedInput(implementation.getDefinitionClass(), implementation.getSignature().getArgumentTypes()), _ -> new HashSet<>())
                                .add(function.getFunctionMetadata().getCanonicalName() + implementation.getSignature().getArgumentTypes().stream()
                                        .map(TypeTemplate::render)
                                        .collect(joining(", ", "(", ")"))));
            }
        }

        Set<Set<String>> sharedStateFunctions = signaturesBySharedInput.values().stream()
                .filter(signatures -> signatures.size() > 1)
                .collect(toImmutableSet());
        assertThat(DeduplicatePartialAggregations.SHARED_STATE_FUNCTIONS.stream()
                .map(group -> group.stream()
                        .map(function -> function.name().functionName() + function.argumentTypes().stream()
                                .map(type -> type.getTypeDescriptor().toString())
                                .collect(joining(", ", "(", ")")))
                        .collect(toImmutableSet()))
                .collect(toImmutableSet()))
                .isEqualTo(sharedStateFunctions);
    }

    @Test
    public void testDeduplicatesPartialAggregation()
    {
        assertDeduplicates(ImmutableList.of("variance", "var_pop", "stddev", "stddev_pop"), DOUBLE);
        assertDeduplicates(ImmutableList.of("variance", "var_pop", "stddev", "stddev_pop"), BIGINT);
        assertDeduplicates(ImmutableList.of("skewness", "kurtosis"), DOUBLE);
        assertDeduplicates(ImmutableList.of("skewness", "kurtosis"), BIGINT);
        assertDeduplicates(ImmutableList.of("covar_samp", "covar_pop"), DOUBLE, DOUBLE);
        assertDeduplicates(ImmutableList.of("covar_samp", "covar_pop"), REAL, REAL);
        assertDeduplicates(ImmutableList.of("regr_slope", "regr_intercept"), DOUBLE, DOUBLE);
        assertDeduplicates(ImmutableList.of("regr_slope", "regr_intercept"), REAL, REAL);
    }

    @Test
    public void testDeduplicatesIntermediateAggregation()
    {
        tester().assertThat(new DeduplicatePartialAggregations())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .step(INTERMEDIATE)
                        .addAggregation(p.symbol("variance", DOUBLE), PlanBuilder.aggregation("variance", ImmutableList.of(new Reference(DOUBLE, "state"))), ImmutableList.of(DOUBLE))
                        .addAggregation(p.symbol("stddev", DOUBLE), PlanBuilder.aggregation("stddev", ImmutableList.of(new Reference(DOUBLE, "state"))), ImmutableList.of(DOUBLE))
                        .source(p.values(p.symbol("state", DOUBLE)))))
                .matches(
                        renamingProject(
                                ImmutableMap.of("variance", "variance", "stddev", "variance"),
                                aggregations(
                                        INTERMEDIATE,
                                        ImmutableMap.of("variance", "variance(state)"),
                                        values("state"))));
    }

    @Test
    public void testDeduplicatesWithGroupingKeysAndSameFilter()
    {
        tester().assertThat(new DeduplicatePartialAggregations())
                .on(p -> {
                    Symbol g = p.symbol("g", BIGINT);
                    Symbol x = p.symbol("x", DOUBLE);
                    Symbol f = p.symbol("f", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(g)
                            .step(PARTIAL)
                            .addAggregation(p.symbol("variance", DOUBLE), PlanBuilder.aggregation("variance", ImmutableList.of(new Reference(DOUBLE, "x")), f), ImmutableList.of(DOUBLE))
                            .addAggregation(p.symbol("stddev", DOUBLE), PlanBuilder.aggregation("stddev", ImmutableList.of(new Reference(DOUBLE, "x")), f), ImmutableList.of(DOUBLE))
                            .addAggregation(p.symbol("sum", DOUBLE), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(DOUBLE, "x"))), ImmutableList.of(DOUBLE))
                            .source(p.values(g, x, f)));
                })
                .matches(
                        renamingProject(
                                ImmutableMap.of("g", "g", "variance", "variance", "stddev", "variance", "sum", "sum"),
                                aggregations(
                                        PARTIAL,
                                        ImmutableMap.of("variance", "variance(x) FILTER f", "sum", "sum(x)"),
                                        values("g", "x", "f"))));
    }

    @Test
    public void testDoesNotDeduplicateDifferentArguments()
    {
        assertDoesNotFire((p, builder) -> builder
                .addAggregation(p.symbol("variance", DOUBLE), PlanBuilder.aggregation("variance", ImmutableList.of(new Reference(DOUBLE, "x"))), ImmutableList.of(DOUBLE))
                .addAggregation(p.symbol("stddev", DOUBLE), PlanBuilder.aggregation("stddev", ImmutableList.of(new Reference(DOUBLE, "y"))), ImmutableList.of(DOUBLE)));
        assertDoesNotFire((p, builder) -> builder
                .addAggregation(p.symbol("covar_samp", DOUBLE), PlanBuilder.aggregation("covar_samp", ImmutableList.of(new Reference(DOUBLE, "x"), new Reference(DOUBLE, "y"))), ImmutableList.of(DOUBLE, DOUBLE))
                .addAggregation(p.symbol("covar_pop", DOUBLE), PlanBuilder.aggregation("covar_pop", ImmutableList.of(new Reference(DOUBLE, "y"), new Reference(DOUBLE, "x"))), ImmutableList.of(DOUBLE, DOUBLE)));
    }

    @Test
    public void testDoesNotDeduplicateDifferentFilterOrMask()
    {
        assertDoesNotFire((p, builder) -> builder
                .addAggregation(p.symbol("variance", DOUBLE), PlanBuilder.aggregation("variance", ImmutableList.of(new Reference(DOUBLE, "x"))), ImmutableList.of(DOUBLE))
                .addAggregation(p.symbol("stddev", DOUBLE), PlanBuilder.aggregation("stddev", ImmutableList.of(new Reference(DOUBLE, "x")), p.symbol("f", BOOLEAN)), ImmutableList.of(DOUBLE)));
        assertDoesNotFire((p, builder) -> builder
                .addAggregation(p.symbol("variance", DOUBLE), PlanBuilder.aggregation("variance", ImmutableList.of(new Reference(DOUBLE, "x"))), ImmutableList.of(DOUBLE))
                .addAggregation(p.symbol("stddev", DOUBLE), PlanBuilder.aggregation("stddev", ImmutableList.of(new Reference(DOUBLE, "x"))), ImmutableList.of(DOUBLE), p.symbol("f", BOOLEAN)));
    }

    @Test
    public void testDoesNotDeduplicateDifferentGroups()
    {
        assertDoesNotFire((p, builder) -> builder
                .addAggregation(p.symbol("variance", DOUBLE), PlanBuilder.aggregation("variance", ImmutableList.of(new Reference(DOUBLE, "x"))), ImmutableList.of(DOUBLE))
                .addAggregation(p.symbol("skewness", DOUBLE), PlanBuilder.aggregation("skewness", ImmutableList.of(new Reference(DOUBLE, "x"))), ImmutableList.of(DOUBLE)));
        assertDoesNotFire((p, builder) -> builder
                .addAggregation(p.symbol("sum", DOUBLE), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(DOUBLE, "x"))), ImmutableList.of(DOUBLE))
                .addAggregation(p.symbol("avg", DOUBLE), PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(DOUBLE, "x"))), ImmutableList.of(DOUBLE)));
    }

    @Test
    public void testDoesNotFireOnSingleOrFinal()
    {
        for (AggregationNode.Step step : ImmutableList.of(SINGLE, FINAL)) {
            tester().assertThat(new DeduplicatePartialAggregations())
                    .on(p -> p.aggregation(builder -> builder
                            .globalGrouping()
                            .step(step)
                            .addAggregation(p.symbol("variance", DOUBLE), PlanBuilder.aggregation("variance", ImmutableList.of(new Reference(DOUBLE, "x"))), ImmutableList.of(DOUBLE))
                            .addAggregation(p.symbol("stddev", DOUBLE), PlanBuilder.aggregation("stddev", ImmutableList.of(new Reference(DOUBLE, "x"))), ImmutableList.of(DOUBLE))
                            .source(p.values(p.symbol("x", DOUBLE)))))
                    .doesNotFire();
        }
    }

    private void assertDeduplicates(List<String> functions, Type... argumentTypes)
    {
        List<String> arguments = IntStream.range(0, argumentTypes.length)
                .mapToObj(index -> "arg" + index)
                .collect(toImmutableList());
        String shared = functions.getFirst();
        tester().assertThat(new DeduplicatePartialAggregations())
                .on(p -> {
                    List<Symbol> argumentSymbols = IntStream.range(0, argumentTypes.length)
                            .mapToObj(index -> p.symbol(arguments.get(index), argumentTypes[index]))
                            .collect(toImmutableList());
                    return p.aggregation(builder -> {
                        builder.globalGrouping().step(PARTIAL);
                        for (String function : functions) {
                            builder.addAggregation(
                                    p.symbol(function, DOUBLE),
                                    PlanBuilder.aggregation(function, argumentSymbols.stream().map(Symbol::toSymbolReference).collect(toImmutableList())),
                                    ImmutableList.copyOf(argumentTypes));
                        }
                        builder.source(p.values(argumentSymbols.toArray(Symbol[]::new)));
                    });
                })
                .matches(
                        renamingProject(
                                functions.stream().collect(toImmutableMap(function -> function, _ -> shared)),
                                aggregations(
                                        PARTIAL,
                                        ImmutableMap.of(shared, shared + arguments.stream().collect(joining(", ", "(", ")"))),
                                        values(arguments))));
    }

    private void assertDoesNotFire(BiConsumer<PlanBuilder, PlanBuilder.AggregationBuilder> aggregations)
    {
        tester().assertThat(new DeduplicatePartialAggregations())
                .on(p -> p.aggregation(builder -> {
                    builder.singleGroupingSet(p.symbol("g", BIGINT)).step(PARTIAL);
                    aggregations.accept(p, builder);
                    builder.source(p.values(p.symbol("g", BIGINT), p.symbol("x", DOUBLE), p.symbol("y", DOUBLE), p.symbol("f", BOOLEAN)));
                }))
                .doesNotFire();
    }

    private static PlanMatchPattern aggregations(AggregationNode.Step step, Map<String, String> aggregations, PlanMatchPattern source)
    {
        return aggregation(
                ImmutableMap.of(),
                node -> node.getStep() == step && node.getAggregations().entrySet().stream()
                        .collect(toImmutableMap(entry -> entry.getKey().name(), entry -> describe(entry.getValue())))
                        .equals(aggregations),
                source);
    }

    private static String describe(Aggregation aggregation)
    {
        return aggregation.getResolvedFunction().signature().getName().functionName() +
                aggregation.getArguments().stream().map(argument -> ((Reference) argument).name()).collect(joining(", ", "(", ")")) +
                aggregation.getFilter().map(filter -> " FILTER " + filter.name()).orElse("");
    }

    private static PlanMatchPattern renamingProject(Map<String, String> renames, PlanMatchPattern source)
    {
        return node(ProjectNode.class, source).with(ProjectNode.class, project -> project.getAssignments().entrySet().stream()
                .collect(toImmutableMap(assignment -> assignment.getKey().name(), assignment -> ((Reference) assignment.getValue()).name()))
                .equals(renames));
    }

    private record SharedInput(Class<?> definitionClass, List<TypeTemplate> argumentTypes) {}
}
