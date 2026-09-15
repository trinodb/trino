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
package io.trino.typesolver;

import io.trino.spi.TrinoException;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeParameter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;

import static io.trino.spi.StandardErrorCode.TYPE_RESOLUTION_LIMIT_EXCEEDED;
import static io.trino.spi.StandardErrorCode.USER_CANCELED;
import static io.trino.typesolver.Expression.BinaryOperator.ADD;
import static io.trino.typesolver.Expression.BinaryOperator.EQUAL;
import static io.trino.typesolver.Expression.BinaryOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.typesolver.Expression.BinaryOperator.LESS_THAN_OR_EQUAL;
import static io.trino.typesolver.Expression.apply;
import static io.trino.typesolver.Expression.field;
import static io.trino.typesolver.Expression.function;
import static io.trino.typesolver.Expression.literal;
import static io.trino.typesolver.Expression.operation;
import static io.trino.typesolver.Expression.row;
import static io.trino.typesolver.Expression.symbol;
import static io.trino.typesolver.Expression.variable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSolverComplexity
{
    private static final CompiledSignature SHARED_TYPE = new CompiledSignature(
            List.of(variable("T")),
            List.of(),
            function(List.of(variable("T"), variable("T")), variable("T")));

    @Test
    void testWideRowCoercionWork()
    {
        long smaller = rowWork(100);
        long larger = rowWork(1000);
        assertThat(larger).isLessThan(smaller * 15);
        assertThat(larger).isLessThan(1_000_000);
    }

    private static long rowWork(int width)
    {
        Expression left = new Expression.Row(IntStream.range(0, width).mapToObj(index -> field("f" + index, symbol("integer"))).toList());
        Expression right = new Expression.Row(IntStream.range(0, width).mapToObj(index -> field("f" + index, symbol("bigint"))).toList());
        return matchWork(left, right);
    }

    @Test
    void testWideRowWithDistinctNumericParameters()
    {
        Expression left = new Expression.Row(IntStream.range(0, 1000).mapToObj(index -> field("f" + index, apply("varchar", literal(index + 1)))).toList());
        Expression right = new Expression.Row(IntStream.range(0, 1000).mapToObj(index -> field("f" + index, apply("varchar", literal(index + 2)))).toList());
        assertThat(matchWork(left, right)).isLessThan(1_000_000);
        assertThat(SHARED_TYPE.matchFunctionCallOutcome(List.of(right, left), TrinoPreset.typeSystem(), true))
                .isInstanceOfSatisfying(CompiledSignature.Satisfied.class, match -> assertThat(match.result().returnType()).isEqualTo(right));
    }

    @Test
    void testNestedArrayCoercionWork()
    {
        long smaller = arrayWork(20);
        long larger = arrayWork(40);
        assertThat(larger).isLessThan(smaller * 6);
        assertThat(larger).isLessThan(1_000_000);
    }

    private static long arrayWork(int depth)
    {
        Expression left = symbol("integer");
        Expression right = symbol("bigint");
        for (int index = 0; index < depth; index++) {
            left = apply("array", left);
            right = apply("array", right);
        }
        return matchWork(left, right);
    }

    private static long matchWork(Expression left, Expression right)
    {
        ResolutionBudget budget = new ResolutionBudget(1_000_000);
        CompiledSignature.MatchOutcome outcome = budget.run(() -> SHARED_TYPE.matchFunctionCallOutcome(List.of(left, right), TrinoPreset.typeSystem(), true));
        assertThat(outcome).isInstanceOfSatisfying(CompiledSignature.Satisfied.class, match -> {
            assertThat(match.result().returnType()).isEqualTo(right);
            assertThat(match.result().parameterTypes()).containsExactly(right, right);
        });
        return budget.work();
    }

    @Test
    void testStructuralBindingPreservesRowNameIntersection()
    {
        Expression left = row(field("left", symbol("integer")));
        Expression right = row(field("right", symbol("bigint")));
        Expression expected = row(Expression.anonymousField(symbol("bigint")));
        for (List<Expression> arguments : List.of(List.of(left, right), List.of(right, left))) {
            assertThat(SHARED_TYPE.matchFunctionCallOutcome(arguments, TrinoPreset.typeSystem(), true))
                    .isInstanceOfSatisfying(CompiledSignature.Satisfied.class, match -> assertThat(match.result().returnType()).isEqualTo(expected));
        }
    }

    @Test
    void testGroundNumericComparisonValidatesConstructorsAndAdditionalRules()
    {
        SubtypeOracle oracle = new SubtypeOracle(TrinoPreset.typeSystem());
        assertThat(oracle.isSubtype(apply("varchar", literal(-1)), apply("varchar", literal(10)))).isFalse();
        assertThat(oracle.isSubtype(apply("decimal", literal(5), literal(10)), apply("decimal", literal(6), literal(10)))).isFalse();
        assertThat(oracle.isSubtype(apply("varchar", literal(10)), apply("varchar", literal(11)))).isTrue();
        assertThat(oracle.isSubtype(apply("varchar", literal(11)), apply("varchar", literal(10)))).isFalse();

        Expression source = apply("varchar", literal(20));
        Expression target = apply("varchar", literal(10));
        List<CoercionRule> rules = new ArrayList<>(TrinoPreset.coercionRules());
        rules.add(new PatternCoercion(source, target, List.of()));
        assertThat(new SubtypeOracle(new TypeSystem(TrinoPreset.typeConstructors(), rules)).isSubtype(source, target)).isTrue();
    }

    @Test
    void testRepeatedCoercionsKeepFieldConstraintsIndependent()
    {
        Expression left = row(
                field("a", symbol("integer")),
                field("b", symbol("integer")),
                field("c", symbol("integer")));
        Expression right = row(
                field("a", apply("decimal", literal(5), literal(3))),
                field("b", apply("decimal", literal(12), literal(2))),
                field("c", apply("decimal", literal(38), literal(38))));
        Expression expected = row(
                field("a", apply("decimal", literal(13), literal(3))),
                field("b", apply("decimal", literal(12), literal(2))),
                field("c", apply("decimal", literal(38), literal(38))));
        for (List<Expression> arguments : List.of(List.of(left, right), List.of(right, left))) {
            assertThat(SHARED_TYPE.matchFunctionCallOutcome(arguments, TrinoPreset.typeSystem(), true))
                    .isInstanceOfSatisfying(CompiledSignature.Satisfied.class, match -> assertThat(match.result().returnType()).isEqualTo(expected));
        }
    }

    @Test
    void testCachedCoercionsUseFreshVariables()
    {
        TypeSystem types = TrinoPreset.typeSystem();
        for (boolean from : List.of(false, true)) {
            CoercionCache cache = new CoercionCache(types);
            Expression bound = apply("decimal", literal(10), literal(2));
            List<TypeSystem.CoercionResult> first = from ? cache.from(bound, new VariableAllocator()) : cache.to(bound, new VariableAllocator());
            List<TypeSystem.CoercionResult> second = from ? cache.from(bound, new VariableAllocator()) : cache.to(bound, new VariableAllocator());
            Set<String> firstVariables = new HashSet<>();
            first.forEach(result -> {
                firstVariables.addAll(Solver.variables(result.type()));
                result.guards().forEach(guard -> firstVariables.addAll(Solver.variables(guard)));
            });
            Set<String> secondVariables = new HashSet<>();
            second.forEach(result -> {
                secondVariables.addAll(Solver.variables(result.type()));
                result.guards().forEach(guard -> secondVariables.addAll(Solver.variables(guard)));
            });
            assertThat(firstVariables).isNotEmpty();
            assertThat(secondVariables).isNotEmpty().doesNotContainAnyElementsOf(firstVariables);
        }
    }

    @Test
    void testMaterializationIsReusedAndStateIsIsolated()
    {
        TypeVariableState state = new TypeVariableState();
        state.domain().constrain(List.of(new Alternative(symbol("integer"), Set.of())));
        Solver.Result result = new Solver.Result(TrinoPreset.typeSystem(), Map.of("T", state), List.of());
        state.domain().replace(List.of(new Alternative(symbol("boolean"), Set.of())));
        ResolutionBudget budget = new ResolutionBudget(100_000);
        budget.run(() -> {
            assertThat(result.materializedTypeVariables()).containsEntry("T", symbol("integer"));
            long work = budget.work();
            ((TypeVariableState) result.variableBounds().get("T")).domain().replace(List.of(new Alternative(symbol("boolean"), Set.of())));
            result.materializedNumericValues();
            result.selectedAlternatives();
            assertThat(result.materializedTypeVariables()).containsEntry("T", symbol("integer"));
            assertThat(budget.work()).isEqualTo(work);
            return null;
        });
    }

    @Test
    void testTraitWakesThroughTypeAlias()
    {
        List<Constraint> constraints = List.of(
                new ExactType("T", variable("U")),
                new RequireComparable("T"),
                new ExactType("U", function(List.of(symbol("integer")), symbol("integer"))));
        assertThat(new Solver(TrinoPreset.typeSystem()).solveOutcome(constraints)).isInstanceOf(Solver.Unsatisfied.class);
        assertThat(new Solver(TrinoPreset.typeSystem()).solveOutcome(constraints.reversed())).isInstanceOf(Solver.Unsatisfied.class);
    }

    @Test
    void testNumericDependencyChain()
    {
        int count = 500;
        List<Constraint> constraints = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            constraints.add(new RequireKind("n" + index, Kind.NUMBER));
        }
        for (int index = 0; index < count - 1; index++) {
            constraints.add(new NumericRelation(operation(EQUAL, variable("n" + index), variable("n" + (index + 1)))));
        }
        constraints.add(new NumericRelation(operation(EQUAL, variable("n" + (count - 1)), literal(7))));
        ResolutionBudget budget = new ResolutionBudget(100_000);
        Solver.Result result = budget.run(() -> new Solver(TrinoPreset.typeSystem()).solve(constraints));
        assertThat(result.materializedNumericValues()).hasSize(count).allSatisfy((_, value) -> assertThat(value).isEqualTo(7));
    }

    @Test
    void testIndependentMaterializationChoices()
    {
        Map<String, VariableState> states = new HashMap<>();
        for (int index = 0; index < 1000; index++) {
            TypeVariableState state = new TypeVariableState();
            state.domain().constrain(List.of(new Alternative(symbol("integer"), Set.of()), new Alternative(symbol("bigint"), Set.of())));
            states.put("T" + index, state);
        }
        ResolutionBudget budget = new ResolutionBudget(100_000);
        Map<String, Expression> result = budget.run(() -> new Solver.Result(TrinoPreset.typeSystem(), states, List.of()).materializedTypeVariables());
        assertThat(result).hasSize(1000).allSatisfy((_, type) -> assertThat(type).isEqualTo(symbol("bigint")));
    }

    @Test
    void testCoupledChoicesPreserveNumericGuards()
    {
        Map<String, VariableState> states = new HashMap<>();
        TypeVariableState left = new TypeVariableState();
        left.domain().constrain(List.of(
                new Alternative(symbol("integer"), Set.of(new NumericRelation(operation(EQUAL, variable("n"), literal(1))))),
                new Alternative(symbol("bigint"), Set.of(new NumericRelation(operation(EQUAL, variable("n"), literal(2)))))));
        TypeVariableState right = new TypeVariableState();
        right.domain().constrain(List.of(
                new Alternative(symbol("bigint"), Set.of(new NumericRelation(operation(EQUAL, variable("m"), literal(0))))),
                new Alternative(symbol("integer"), Set.of(new NumericRelation(operation(EQUAL, variable("m"), literal(1)))))));
        states.put("T", left);
        states.put("U", right);
        states.put("n", new NumericVariableState());
        states.put("m", new NumericVariableState());
        for (int index = 0; index < 100; index++) {
            TypeVariableState independent = new TypeVariableState();
            independent.domain().constrain(List.of(new Alternative(symbol("integer"), Set.of()), new Alternative(symbol("bigint"), Set.of())));
            states.put("independent" + index, independent);
        }
        List<Constraint> constraints = List.of(new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("n"), variable("m"))));
        new ResolutionBudget(100_000).run(() -> {
            Solver.Result result = new Solver.Result(TrinoPreset.typeSystem(), states, constraints);
            assertThat(result.materializedTypeVariables()).containsEntry("T", symbol("integer")).containsEntry("U", symbol("integer"));
            assertThat(result.materializedNumericValues()).containsEntry("n", 1).containsEntry("m", 1);
            return null;
        });

        right.domain().replace(List.of(new Alternative(symbol("bigint"), Set.of(new NumericRelation(operation(EQUAL, variable("m"), literal(0)))))));
        assertThatThrownBy(() -> new ResolutionBudget(100_000).run(() ->
                new Solver.Result(TrinoPreset.typeSystem(), states, constraints).materializedTypeVariables()))
                .isInstanceOf(UnsatisfiableException.class);
    }

    @Test
    void testCyclicNumericGrowthExhaustsBudget()
    {
        Map<String, VariableState> states = Map.of("n", new NumericVariableState(), "m", new NumericVariableState());
        List<Constraint> constraints = List.of(
                new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("n"), operation(ADD, variable("m"), literal(1)))),
                new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("m"), operation(ADD, variable("n"), literal(1)))));
        assertThatThrownBy(() -> new ResolutionBudget(1000).run(() ->
                new Solver.Result(TrinoPreset.typeSystem(), states, constraints).materializedNumericValues()))
                .isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(TYPE_RESOLUTION_LIMIT_EXCEEDED.toErrorCode()));
    }

    @Test
    void testNumericMaterializationChecksInitialBounds()
    {
        Map<String, VariableState> states = Map.of("n", new NumericVariableState(OptionalInt.of(2), OptionalInt.of(1)));
        assertThatThrownBy(() -> new Solver.Result(TrinoPreset.typeSystem(), states, List.of()).materializedNumericValues())
                .isInstanceOf(UnsatisfiableException.class);
    }

    @Test
    void testWorkLimitIsNotNoMatch()
    {
        assertThatThrownBy(() -> new ResolutionBudget(1).run(() ->
                SHARED_TYPE.matchFunctionCallOutcome(List.of(symbol("integer"), symbol("bigint")), TrinoPreset.typeSystem(), true)))
                .isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(TYPE_RESOLUTION_LIMIT_EXCEEDED.toErrorCode()));
    }

    @Test
    void testExcessiveNestingFailsBeforeRecursiveCompilation()
    {
        TypeDescriptor descriptor = new TypeDescriptor("integer");
        Expression expression = symbol("integer");
        for (int index = 0; index < 1000; index++) {
            descriptor = new TypeDescriptor("array", TypeParameter.typeParameter(descriptor));
            expression = apply("array", expression);
        }
        TypeDescriptor input = descriptor;
        List<Expression> arguments = List.of(expression, expression);
        assertThatThrownBy(() -> TypeCompiler.compile(input))
                .isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(TYPE_RESOLUTION_LIMIT_EXCEEDED.toErrorCode()));
        assertThatThrownBy(() -> SHARED_TYPE.matchFunctionCallOutcome(arguments, TrinoPreset.typeSystem(), false))
                .isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(TYPE_RESOLUTION_LIMIT_EXCEEDED.toErrorCode()));
    }

    @Test
    void testStructuralComparisonHonorsAdditionalRules()
    {
        Expression left = apply("array", symbol("boolean"));
        Expression right = apply("array", symbol("integer"));
        List<CoercionRule> rules = new ArrayList<>(TrinoPreset.coercionRules());
        rules.add(new PatternCoercion(left, right, List.of()));
        TypeSystem typeSystem = new TypeSystem(TrinoPreset.typeConstructors(), rules);
        assertThat(new SubtypeOracle(typeSystem).isSubtype(left, right)).isTrue();
    }

    @Test
    void testStructuralComparisonHonorsSelfCoercionSubclass()
    {
        Expression left = apply("array", symbol("boolean"));
        Expression right = apply("array", symbol("integer"));
        List<CoercionRule> rules = new ArrayList<>(TrinoPreset.coercionRules());
        rules.add(new SelfCoercion()
        {
            @Override
            public Optional<Match> matches(VariableAllocator allocator, Expression from, Expression to)
            {
                if (from.equals(left) && to.equals(right)) {
                    return Optional.of(new Match(Set.of(), CoercionPlan.direct(from, to, List.of("custom-array"))));
                }
                return super.matches(allocator, from, to);
            }
        });
        assertThat(new SubtypeOracle(new TypeSystem(TrinoPreset.typeConstructors(), rules)).isSubtype(left, right)).isTrue();
    }

    @Test
    void testDeferredMaterializationRetainsBudget()
    {
        Solver.Result result = new ResolutionBudget(10).run(() -> {
            ResolutionBudget.consume(10);
            return new Solver.Result(TrinoPreset.typeSystem(), Map.of("n", new NumericVariableState()), List.of());
        });
        assertThatThrownBy(result::materializedNumericValues)
                .isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(TYPE_RESOLUTION_LIMIT_EXCEEDED.toErrorCode()));
    }

    @Test
    void testSubstitutionChainCannotOverflowStack()
    {
        Map<String, Expression> substitutions = new HashMap<>();
        for (int index = 0; index < 10_000; index++) {
            substitutions.put("T" + index, variable("T" + (index + 1)));
        }
        assertThatThrownBy(() -> Expression.substitute(variable("T0"), substitutions))
                .isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(TYPE_RESOLUTION_LIMIT_EXCEEDED.toErrorCode()));
    }

    @Test
    void testNestedBudgetsCannotResetAllowance()
    {
        ResolutionBudget budget = new ResolutionBudget(10);
        assertThatThrownBy(() -> budget.run(() -> {
            ResolutionBudget.consume(6);
            return new ResolutionBudget(100).run(() -> {
                ResolutionBudget.consume(5);
                return null;
            });
        })).isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(TYPE_RESOLUTION_LIMIT_EXCEEDED.toErrorCode()));
        assertThat(budget.work()).isEqualTo(10);
    }

    @Test
    void testCancellationPreservesInterrupt()
    {
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> new Solver(TrinoPreset.typeSystem()).solve(List.of()))
                    .isInstanceOfSatisfying(TrinoException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(USER_CANCELED.toErrorCode()));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        }
        finally {
            Thread.interrupted();
        }
    }
}
