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
package org.weakref.solver;

import io.trino.lib.TrinoPreset;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.solver.Expression.anyRow;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.field;
import static org.weakref.solver.Expression.function;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.row;
import static org.weakref.solver.Expression.symbol;
import static org.weakref.solver.Expression.variable;
import static org.weakref.solver.Expression.variadicFunction;

public class FunctionResolverTest
{
    @Test
    void testResolverPrefersExactMatchOverCoercion()
    {
        TypeScheme integerFunction = new TypeScheme(List.of(), List.of(), function(List.of(symbol("integer")), symbol("integer")));
        TypeScheme bigintFunction = new TypeScheme(List.of(), List.of(), function(List.of(symbol("bigint")), symbol("bigint")));

        FunctionResolver.Resolution resolution = resolved(List.of(bigintFunction, integerFunction), List.of(symbol("integer")));

        assertThat(resolution.scheme())
                .isEqualTo(integerFunction);
        assertThat(resolution.returnType())
                .isEqualTo(symbol("integer"));
        assertThat(resolution.argumentCoercions())
                .extracting(FunctionResolver.ArgumentCoercion::coercionNeeded)
                .containsExactly(false);
    }

    @Test
    void testResolverAmbiguousWhenTwoCoercionsTieOnCount()
    {
        // smallint can widen to either integer or bigint — both candidates need one coercion.
        // The framework default (BY_COERCION_COUNT) has no preference and reports Ambiguous.
        // A dialect-specific Specificity would break this tie; see the preset-level tests.
        TypeScheme integerFunction = new TypeScheme(List.of(), List.of(), function(List.of(symbol("integer")), symbol("integer")));
        TypeScheme bigintFunction = new TypeScheme(List.of(), List.of(), function(List.of(symbol("bigint")), symbol("bigint")));

        assertThat(new FunctionResolver(TrinoPreset.typeSystem())
                .resolveOutcome(List.of(bigintFunction, integerFunction), List.of(symbol("smallint"))))
                .isInstanceOfSatisfying(FunctionResolver.Ambiguous.class, outcome ->
                        assertThat(outcome.candidates())
                                .extracting(FunctionResolver.Resolution::scheme)
                                .containsExactlyInAnyOrder(integerFunction, bigintFunction));
    }

    @Test
    void testResolverReportsPrimitiveWideningPlanDetails()
    {
        // Drive a single smallint→integer widening and check the plan is reported correctly.
        TypeScheme integerFunction = new TypeScheme(List.of(), List.of(), function(List.of(symbol("integer")), symbol("integer")));

        FunctionResolver.Resolution resolution = resolved(List.of(integerFunction), List.of(symbol("smallint")));

        assertThat(resolution.argumentCoercions())
                .singleElement()
                .satisfies(coercion -> {
                    assertThat(coercion.coercionNeeded()).isTrue();
                    assertThat(coercion.formalType()).isEqualTo(symbol("integer"));
                    assertThat(coercion.coercionPlan().sourceType()).isEqualTo(symbol("smallint"));
                    assertThat(coercion.coercionPlan().targetType()).isEqualTo(symbol("integer"));
                    assertThat(coercion.coercionPlan().kind()).isEqualTo(CoercionPlan.Kind.DIRECT);
                    assertThat(coercion.coercionPlan().steps())
                            .singleElement()
                            .isInstanceOfSatisfying(CoercionPlan.DirectRule.class, step -> {
                                assertThat(step.ruleId()).isEqualTo("primitive:smallint->integer");
                                assertThat(step.conditions()).isEmpty();
                            });
                });
    }

    @Test
    void testResolverAmbiguousBetweenBoundedRowVariableAndWildcardRow()
    {
        // Both candidates match with zero coercions (the row's fields match @R exactly and the
        // wildcard accepts any row). Framework default has no basis to pick; reports Ambiguous.
        // A Trino-style Specificity would prefer the non-wildcard (more specific) candidate.
        TypeScheme wildcard = new TypeScheme(
                List.of(),
                List.of(),
                function(List.of(anyRow(), anyRow()), symbol("wildcard")));
        TypeScheme sameRow = new TypeScheme(
                List.of(variable("@R")),
                List.of(new Subtype(variable("@R"), anyRow())),
                function(List.of(variable("@R"), variable("@R")), symbol("same-row")));

        Expression.Row actual = row(
                field("left", symbol("integer")),
                field("right", symbol("varchar")));

        assertThat(new FunctionResolver(TrinoPreset.typeSystem())
                .resolveOutcome(List.of(wildcard, sameRow), List.of(actual, actual)))
                .isInstanceOfSatisfying(FunctionResolver.Ambiguous.class, outcome ->
                        assertThat(outcome.candidates())
                                .extracting(FunctionResolver.Resolution::scheme)
                                .containsExactlyInAnyOrder(wildcard, sameRow));
    }

    @Test
    void testResolverBindsBoundedRowVariableWhenItsTheOnlyCandidate()
    {
        TypeScheme sameRow = new TypeScheme(
                List.of(variable("@R")),
                List.of(new Subtype(variable("@R"), anyRow())),
                function(List.of(variable("@R"), variable("@R")), symbol("same-row")));

        Expression.Row actual = row(
                field("left", symbol("integer")),
                field("right", symbol("varchar")));

        FunctionResolver.Resolution resolution = resolved(List.of(sameRow), List.of(actual, actual));

        assertThat(resolution.scheme()).isEqualTo(sameRow);
        assertThat(resolution.returnType()).isEqualTo(symbol("same-row"));
        assertThat(resolution.argumentCoercions())
                .extracting(FunctionResolver.ArgumentCoercion::coercionNeeded)
                .containsExactly(false, false);
        assertThat(resolution.typeBindings())
                .containsValue(actual);
    }

    @Test
    void testResolverReportsResolvedTypeBindings()
    {
        TypeScheme elementFunction = new TypeScheme(
                List.of(variable("@E")),
                List.of(),
                function(List.of(apply("array", variable("@E"))), variable("@E")));

        FunctionResolver.Resolution resolution = resolved(List.of(elementFunction), List.of(apply("array", symbol("integer"))));

        assertThat(resolution.returnType())
                .isEqualTo(symbol("integer"));
        assertThat(resolution.argumentCoercions())
                .singleElement()
                .satisfies(coercion -> {
                    assertThat(coercion.coercionNeeded()).isFalse();
                    assertThat(coercion.formalType()).isEqualTo(apply("array", symbol("integer")));
                });
        assertThat(resolution.typeBindings())
                .containsValue(symbol("integer"));
    }

    @Test
    void testResolverInfersAcrossMultipleArgumentsAndReturnType()
    {
        TypeScheme signature = new TypeScheme(
                List.of(variable("@K"), variable("@V")),
                List.of(),
                function(
                        List.of(
                                apply("array", variable("@K")),
                                apply("map", variable("@K"), variable("@V"))),
                        row(
                                field("key", variable("@K")),
                                field("value", variable("@V")))));

        FunctionResolver.Resolution resolution = resolved(
                List.of(signature),
                List.of(
                        apply("array", symbol("smallint")),
                        apply("map", symbol("integer"), symbol("bigint"))));

        assertThat(resolution.returnType())
                .isInstanceOfSatisfying(Expression.Row.class, rowType -> assertThat(rowType.fields())
                        .extracting(Expression.RowField::type)
                        .containsExactly(symbol("integer"), symbol("bigint")));
        assertThat(resolution.argumentCoercions())
                .extracting(FunctionResolver.ArgumentCoercion::coercionNeeded)
                .containsExactly(true, false);
        assertThat(resolution.argumentCoercions().getFirst().coercionPlan().kind())
                .isEqualTo(CoercionPlan.Kind.DERIVED);
        assertThat(resolution.typeBindings())
                .containsEntry("@K", symbol("integer"))
                .containsEntry("@V", symbol("bigint"));
    }

    @Test
    void testResolverReportsDerivedStructuralCoercions()
    {
        TypeScheme arrayFunction = new TypeScheme(
                List.of(),
                List.of(),
                function(List.of(apply("array", symbol("integer"))), symbol("boolean")));

        FunctionResolver.Resolution resolution = resolved(List.of(arrayFunction), List.of(apply("array", symbol("smallint"))));

        assertThat(resolution.argumentCoercions())
                .singleElement()
                .satisfies(coercion -> {
                    assertThat(coercion.coercionPlan().kind()).isEqualTo(CoercionPlan.Kind.DERIVED);
                    assertThat(coercion.coercionPlan().sourceType()).isEqualTo(apply("array", symbol("smallint")));
                    assertThat(coercion.coercionPlan().targetType()).isEqualTo(apply("array", symbol("integer")));
                    assertThat(coercion.coercionPlan().steps())
                            .singleElement()
                            .isInstanceOfSatisfying(CoercionPlan.Structural.class, step -> {
                                assertThat(step.constructor()).isEqualTo("array");
                                assertThat(step.children())
                                        .singleElement()
                                        .satisfies(child -> {
                                            assertThat(child.sourceType()).isEqualTo(symbol("smallint"));
                                            assertThat(child.targetType()).isEqualTo(symbol("integer"));
                                            assertThat(child.steps())
                                                    .singleElement()
                                                    .isInstanceOfSatisfying(CoercionPlan.DirectRule.class, direct -> {
                                                        assertThat(direct.ruleId()).isEqualTo("primitive:smallint->integer");
                                                        assertThat(direct.conditions()).isEmpty();
                                                    });
                                        });
                            });
                });
    }

    @Test
    void testResolverTreatsRowNameDifferencesAsExact()
    {
        TypeScheme rowFunction = new TypeScheme(
                List.of(),
                List.of(),
                function(List.of(row(field("x", symbol("integer")))), symbol("boolean")));

        FunctionResolver.Resolution resolution = resolved(List.of(rowFunction), List.of(row(field("y", symbol("integer")))));

        assertThat(resolution.argumentCoercions())
                .singleElement()
                .satisfies(coercion -> {
                    assertThat(coercion.coercionPlan().isExact()).isTrue();
                    assertThat(coercion.coercionPlan().steps()).isEmpty();
                });
    }

    @Test
    void testResolverReportsAmbiguousExactMatches()
    {
        TypeScheme first = new TypeScheme(List.of(), List.of(), function(List.of(symbol("integer")), symbol("boolean")));
        TypeScheme second = new TypeScheme(List.of(), List.of(), function(List.of(symbol("integer")), symbol("boolean")));

        assertThat(new FunctionResolver(TrinoPreset.typeSystem())
                .resolveOutcome(List.of(first, second), List.of(symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Ambiguous.class, outcome ->
                        assertThat(outcome.candidates())
                                .extracting(FunctionResolver.Resolution::scheme)
                                .containsExactlyInAnyOrder(first, second));
    }

    @Test
    void testResolverRejectsGroundUnreachableCandidate()
    {
        TypeScheme signature = new TypeScheme(List.of(), List.of(), function(List.of(symbol("binary")), symbol("boolean")));

        // integer has no coercion to binary and both are ground — the candidate is definitively
        // unreachable, so the resolver returns NoMatch (no candidates satisfied OR deferred).
        assertThat(new FunctionResolver(TrinoPreset.typeSystem())
                .resolveOutcome(List.of(signature), List.of(symbol("integer"))))
                .isInstanceOf(FunctionResolver.NoMatch.class);
    }

    @Test
    void testResolverPrefersSatisfiedCandidateOverIncompleteCandidate()
    {
        TypeScheme exact = new TypeScheme(List.of(), List.of(), function(List.of(symbol("integer")), symbol("boolean")));
        TypeScheme incomplete = new TypeScheme(List.of(), List.of(), function(List.of(symbol("binary")), symbol("boolean")));

        assertThat(new FunctionResolver(TrinoPreset.typeSystem())
                .resolveOutcome(List.of(exact, incomplete), List.of(symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().scheme()).isEqualTo(exact));
    }

    @Test
    void testResolverReportsInstantiatedPatternConditions()
    {
        TypeScheme decimalFunction = new TypeScheme(
                List.of(),
                List.of(),
                function(List.of(apply("decimal", literal(10), literal(0))), symbol("boolean")));

        FunctionResolver.Resolution resolution = resolved(List.of(decimalFunction), List.of(symbol("integer")));

        assertThat(resolution.argumentCoercions())
                .singleElement()
                .satisfies(coercion -> {
                    assertThat(coercion.coercionPlan().kind()).isEqualTo(CoercionPlan.Kind.DIRECT);
                    assertThat(coercion.coercionPlan().steps())
                            .singleElement()
                            .isInstanceOfSatisfying(CoercionPlan.DirectRule.class, direct -> {
                                assertThat(direct.ruleId()).isEqualTo("pattern:integer->decimal(@p, @s)");
                                assertThat(direct.conditions()).containsExactly(
                                        new NumericRelation(Expression.operation(
                                                Expression.BinaryOperator.GREATER_THAN_OR_EQUAL,
                                                Expression.operation(Expression.BinaryOperator.SUBTRACT, literal(10), literal(0)),
                                                literal(10))));
                            });
                });
    }

    @Test
    void testResolverSupportsVariadicArguments()
    {
        TypeScheme signature = new TypeScheme(
                List.of(),
                List.of(),
                variadicFunction(List.of(symbol("varchar")), symbol("integer"), symbol("boolean")));

        FunctionResolver.Resolution resolution = resolved(
                List.of(signature),
                List.of(symbol("varchar"), symbol("smallint"), symbol("integer")));

        assertThat(resolution.argumentCoercions())
                .extracting(FunctionResolver.ArgumentCoercion::coercionNeeded)
                .containsExactly(false, true, false);
    }

    @Test
    void testResolverAmbiguousBetweenNumericWideningAndPatternDecimalCoercion()
    {
        // integer can widen to bigint (primitive) OR to decimal(10, 0) (pattern). Both paths
        // are 1 coercion; the framework default can't distinguish them and reports Ambiguous.
        // Dialects that want a preference (e.g. Trino prefers primitive widening) layer their
        // own Specificity.
        TypeScheme bigintFunction = new TypeScheme(List.of(), List.of(), function(List.of(symbol("bigint")), symbol("bigint")));
        TypeScheme decimalFunction = new TypeScheme(List.of(), List.of(), function(List.of(apply("decimal", literal(10), literal(0))), symbol("decimal")));

        assertThat(new FunctionResolver(TrinoPreset.typeSystem())
                .resolveOutcome(List.of(decimalFunction, bigintFunction), List.of(symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Ambiguous.class, outcome ->
                        assertThat(outcome.candidates())
                                .extracting(FunctionResolver.Resolution::scheme)
                                .containsExactlyInAnyOrder(bigintFunction, decimalFunction));
    }

    @Test
    void testResolverReportsWildcardRowAcceptance()
    {
        TypeScheme wildcard = new TypeScheme(
                List.of(),
                List.of(),
                function(List.of(anyRow()), symbol("boolean")));

        FunctionResolver.Resolution resolution = resolved(List.of(wildcard), List.of(row(field("x", symbol("integer")))));

        // A concrete row flowing into AnyRow is a solver-level acceptance, not a coercion —
        // the plan is EXACT and no runtime conversion is needed.
        assertThat(resolution.argumentCoercions())
                .singleElement()
                .satisfies(coercion -> {
                    assertThat(coercion.coercionNeeded()).isFalse();
                    assertThat(coercion.actualType()).isInstanceOf(Expression.Row.class);
                    assertThat(coercion.formalType()).isInstanceOf(Expression.AnyRow.class);
                    assertThat(coercion.coercionPlan().isExact()).isTrue();
                });
    }

    @Test
    void testCustomSpecificityCanOverrideDefault()
    {
        TypeScheme integerFunction = new TypeScheme(List.of(), List.of(), function(List.of(symbol("integer")), symbol("integer")));
        TypeScheme bigintFunction = new TypeScheme(List.of(), List.of(), function(List.of(symbol("bigint")), symbol("bigint")));

        // Specificity that ranks by scheme identity alone: integer-fn always wins over anything.
        Specificity prefersInteger = (left, right) -> {
            if (left.scheme() == integerFunction && right.scheme() != integerFunction) {
                return Specificity.Order.MORE_SPECIFIC;
            }
            if (right.scheme() == integerFunction && left.scheme() != integerFunction) {
                return Specificity.Order.LESS_SPECIFIC;
            }
            return Specificity.Order.EQUIVALENT;
        };

        FunctionResolver resolver = new FunctionResolver(TrinoPreset.typeSystem(), prefersInteger);

        assertThat(resolver.resolveOutcome(List.of(bigintFunction, integerFunction), List.of(symbol("smallint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().scheme()).isEqualTo(integerFunction));
    }

    @Test
    void testResolverReportsNoMatchWhenNoCandidatesApply()
    {
        TypeScheme binaryFunction = new TypeScheme(List.of(), List.of(), function(List.of(symbol("binary"), symbol("binary")), symbol("boolean")));

        assertThat(new FunctionResolver(TrinoPreset.typeSystem())
                .resolveOutcome(List.of(binaryFunction), List.of(symbol("integer"))))
                .isInstanceOf(FunctionResolver.NoMatch.class);
    }

    @Test
    void testIncompleteCandidatesDoNotBlockResolvedWinner()
    {
        TypeScheme exact = new TypeScheme(List.of(), List.of(), function(List.of(symbol("integer")), symbol("boolean")));
        TypeScheme incomplete = new TypeScheme(List.of(), List.of(), function(List.of(symbol("binary")), symbol("boolean")));

        assertThat(new FunctionResolver(TrinoPreset.typeSystem())
                .resolveOutcome(List.of(exact, incomplete), List.of(symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().scheme()).isEqualTo(exact));
    }

    @Test
    void testNamedArgumentsMapToDeclaredPositions()
    {
        // f(a integer, b bigint) -> bigint; a named call written out of order resolves to the
        // declared positions, so the coercion that integer->bigint needs lands on the b position.
        TypeScheme scheme = new TypeScheme(
                List.of(),
                List.of(),
                function(List.of(symbol("integer"), symbol("bigint")), symbol("bigint")),
                List.of(Optional.of("a"), Optional.of("b")));

        FunctionResolver.ResolutionOutcome outcome = new FunctionResolver(TrinoPreset.typeSystem()).resolveNamed(
                List.of(scheme),
                List.of(
                        FunctionResolver.Argument.named("b", symbol("integer")),
                        FunctionResolver.Argument.named("a", symbol("integer"))));

        assertThat(outcome).isInstanceOfSatisfying(FunctionResolver.Resolved.class, resolved -> {
            assertThat(resolved.resolution().returnType()).isEqualTo(symbol("bigint"));
            // Coercions are reported against the written positions: the 'b' actual (written first,
            // index 0) widens integer->bigint; the 'a' actual (written second, index 1) is exact.
            assertThat(resolved.resolution().argumentCoercions())
                    .filteredOn(FunctionResolver.ArgumentCoercion::coercionNeeded)
                    .extracting(FunctionResolver.ArgumentCoercion::index)
                    .containsExactly(0);
        });
    }

    @Test
    void testNamedArgumentResolutionIsPerCandidate()
    {
        // Two overloads declare DIFFERENT parameter names at the same arity — exactly what the
        // analyzer's name-to-position pass cannot do (it assumes one binding across overloads).
        // The solver permutes per candidate, so a call naming 'flag' picks the boolean overload
        // and a call naming 'count' picks the integer one.
        TypeScheme booleanOverload = new TypeScheme(
                List.of(),
                List.of(),
                function(List.of(symbol("varchar"), symbol("boolean")), symbol("varchar")),
                List.of(Optional.of("value"), Optional.of("flag")));
        TypeScheme integerOverload = new TypeScheme(
                List.of(),
                List.of(),
                function(List.of(symbol("varchar"), symbol("integer")), symbol("integer")),
                List.of(Optional.of("value"), Optional.of("count")));
        List<TypeScheme> candidates = List.of(booleanOverload, integerOverload);
        FunctionResolver resolver = new FunctionResolver(TrinoPreset.typeSystem());

        assertThat(resolver.resolveNamed(candidates, List.of(
                FunctionResolver.Argument.named("value", symbol("varchar")),
                FunctionResolver.Argument.named("flag", symbol("boolean")))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, resolved ->
                        assertThat(resolved.resolution().scheme()).isEqualTo(booleanOverload));

        assertThat(resolver.resolveNamed(candidates, List.of(
                FunctionResolver.Argument.named("value", symbol("varchar")),
                FunctionResolver.Argument.named("count", symbol("integer")))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, resolved ->
                        assertThat(resolved.resolution().scheme()).isEqualTo(integerOverload));
    }

    @Test
    void testCallNamingAnUndeclaredArgumentDoesNotMatch()
    {
        TypeScheme scheme = new TypeScheme(
                List.of(),
                List.of(),
                function(List.of(symbol("integer")), symbol("integer")),
                List.of(Optional.of("value")));

        assertThat(new FunctionResolver(TrinoPreset.typeSystem()).resolveNamed(
                List.of(scheme),
                List.of(FunctionResolver.Argument.named("nope", symbol("integer")))))
                .isInstanceOf(FunctionResolver.NoMatch.class);
    }

    @Test
    void testNamedArgumentReferringToAPositionalSlotDoesNotMatch()
    {
        // f(a, b): the call (1, a => 2) names 'a', which is already supplied positionally — no match.
        TypeScheme scheme = new TypeScheme(
                List.of(),
                List.of(),
                function(List.of(symbol("integer"), symbol("integer")), symbol("integer")),
                List.of(Optional.of("a"), Optional.of("b")));

        assertThat(new FunctionResolver(TrinoPreset.typeSystem()).resolveNamed(
                List.of(scheme),
                List.of(
                        FunctionResolver.Argument.positional(symbol("integer")),
                        FunctionResolver.Argument.named("a", symbol("integer")))))
                .isInstanceOf(FunctionResolver.NoMatch.class);
    }

    private static FunctionResolver.Resolution resolved(List<TypeScheme> candidates, List<Expression> arguments)
    {
        return switch (new FunctionResolver(TrinoPreset.typeSystem()).resolveOutcome(candidates, arguments)) {
            case FunctionResolver.Resolved resolved -> resolved.resolution();
            case FunctionResolver.NoMatch _ -> throw new AssertionError("Expected resolution but got no match");
            case FunctionResolver.Incomplete _ -> throw new AssertionError("Expected resolution but got incomplete");
            case FunctionResolver.Ambiguous _ -> throw new AssertionError("Expected resolution but got ambiguous");
        };
    }
}
