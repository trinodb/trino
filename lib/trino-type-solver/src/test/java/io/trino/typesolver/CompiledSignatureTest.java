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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.stream.IntStream;

import static io.trino.typesolver.Expression.anyRow;
import static io.trino.typesolver.Expression.apply;
import static io.trino.typesolver.Expression.field;
import static io.trino.typesolver.Expression.function;
import static io.trino.typesolver.Expression.literal;
import static io.trino.typesolver.Expression.row;
import static io.trino.typesolver.Expression.symbol;
import static io.trino.typesolver.Expression.variable;
import static org.assertj.core.api.Assertions.assertThat;

public class CompiledSignatureTest
{
    @Test
    @Timeout(30)
    void testExactWideRowBinding()
    {
        CompiledSignature signature = new CompiledSignature(
                List.of(variable("t")),
                List.of(),
                function(List.of(variable("t")), variable("t")));
        Expression.Row row = new Expression.Row(IntStream.range(0, 10_000)
                .mapToObj(index -> field("field_" + index, symbol("bigint")))
                .toList());
        assertThat(signature.matchFunctionCallOutcome(List.of(row), TrinoPreset.typeSystem(), false))
                .isInstanceOfSatisfying(CompiledSignature.Satisfied.class, outcome -> {
                    assertThat(outcome.result().returnType()).isEqualTo(row);
                    assertThat(outcome.result().parameterTypes()).containsExactly(row);
                });
    }

    @Test
    void testExactMatchingRejectsCoercion()
    {
        CompiledSignature signature = new CompiledSignature(
                List.of(variable("t")),
                List.of(),
                function(List.of(variable("t"), variable("t")), variable("t")));
        assertThat(signature.matchFunctionCallOutcome(List.of(symbol("integer"), symbol("bigint")), TrinoPreset.typeSystem(), false))
                .isInstanceOf(CompiledSignature.Unsatisfied.class);

        CompiledSignature concrete = new CompiledSignature(List.of(), List.of(), function(List.of(symbol("bigint")), symbol("bigint")));
        assertThat(concrete.matchFunctionCallOutcome(List.of(symbol("integer")), TrinoPreset.typeSystem(), false))
                .isInstanceOf(CompiledSignature.Unsatisfied.class);

        CompiledSignature parametric = new CompiledSignature(
                List.of(variable("p")),
                List.of(),
                function(List.of(apply("timestamp", variable("p")), apply("timestamp", variable("p"))), symbol("bigint")));
        assertThat(parametric.matchFunctionCallOutcome(List.of(apply("timestamp", literal(3)), apply("timestamp", literal(6))), TrinoPreset.typeSystem(), false))
                .isInstanceOf(CompiledSignature.Unsatisfied.class);
    }

    @Test
    void testReturnOnlyTypeVariable()
    {
        CompiledSignature signature = new CompiledSignature(
                List.of(variable("e")),
                List.of(),
                function(List.of(symbol("unknown"), variable("e")), variable("e")));
        assertThat(signature.matchFunctionCallOutcome(List.of(symbol("unknown"), symbol("varchar")), TrinoPreset.typeSystem()))
                .isInstanceOfSatisfying(CompiledSignature.Satisfied.class, outcome -> {
                    assertThat(outcome.result().returnType()).isEqualTo(symbol("varchar"));
                    assertThat(outcome.result().typeBindings()).containsEntry("e", symbol("varchar"));
                });
    }

    @Test
    void testBoundedRowVariableMatchesSameConcreteRowArguments()
    {
        CompiledSignature signature = new CompiledSignature(
                List.of(variable("R")),
                List.of(new Subtype(variable("R"), anyRow())),
                function(List.of(variable("R"), variable("R")), symbol("boolean")));

        Expression.Row actual = row(
                field("left", symbol("integer")),
                field("right", symbol("varchar")));

        CompiledSignature.MatchResult result = satisfiedMatch(signature, List.of(actual, actual));

        assertThat(result.returnType())
                .isEqualTo(symbol("boolean"));
        assertThat(result.typeBindings())
                .containsValue(actual);
    }

    @Test
    void testBoundedRowVariableRejectsDifferentConcreteRows()
    {
        CompiledSignature signature = new CompiledSignature(
                List.of(variable("R")),
                List.of(new Subtype(variable("R"), anyRow())),
                function(List.of(variable("R"), variable("R")), symbol("boolean")));

        assertThat(signature.matchFunctionCallOutcome(
                List.of(
                        row(field("a", symbol("integer"))),
                        row(field("b", symbol("varchar")))),
                TrinoPreset.typeSystem()))
                .isInstanceOf(CompiledSignature.Unsatisfied.class);
    }

    @Test
    void testBoundedRowVariableRejectsNonRowArguments()
    {
        CompiledSignature signature = new CompiledSignature(
                List.of(variable("R")),
                List.of(new Subtype(variable("R"), anyRow())),
                function(List.of(variable("R"), variable("R")), symbol("boolean")));

        assertThat(signature.matchFunctionCallOutcome(
                List.of(symbol("integer"), symbol("integer")),
                TrinoPreset.typeSystem()))
                .isInstanceOf(CompiledSignature.Unsatisfied.class);
    }

    @Test
    void testWildcardRowParameterAcceptsAnyConcreteRow()
    {
        CompiledSignature signature = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(anyRow()), symbol("boolean")));

        assertThat(signature.matchFunctionCallOutcome(
                List.of(row(
                        field("a", symbol("bigint")),
                        field("b", symbol("varchar")))),
                TrinoPreset.typeSystem()))
                .isInstanceOf(CompiledSignature.Satisfied.class);
    }

    @Test
    void testMatchFunctionCallOutcomeRejectsGroundUnreachableCall()
    {
        CompiledSignature signature = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(symbol("binary")), symbol("boolean")));

        assertThat(signature.matchFunctionCallOutcome(List.of(symbol("integer")), TrinoPreset.typeSystem()))
                .isInstanceOf(CompiledSignature.Unsatisfied.class);
    }

    @Test
    void testStructuredOutcomeApiDistinguishesUnsatisfied()
    {
        CompiledSignature signature = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(symbol("binary")), symbol("boolean")));

        assertThat(signature.matchFunctionCallOutcome(List.of(symbol("integer")), TrinoPreset.typeSystem()))
                .isInstanceOf(CompiledSignature.Unsatisfied.class);
    }

    /// A ground parametric formal accepts a smaller instance by whole-type widening — varchar(0)
    /// flows into a literal varchar(1) parameter through the varchar coercion rule, not through a
    /// per-parameter 0 <: 1 relation (which no rule covers). The reverse direction must not narrow.
    @Test
    void testGroundParametricFormalCoercesAsWholeType()
    {
        CompiledSignature codepoint = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(apply("varchar", literal(1))), symbol("integer")));

        CompiledSignature.MatchResult widened = satisfiedMatch(codepoint, List.of(apply("varchar", literal(0))));
        assertThat(widened.returnType()).isEqualTo(symbol("integer"));
        assertThat(widened.parameterTypes()).isEqualTo(List.of(apply("varchar", literal(1))));

        CompiledSignature.MatchResult exact = satisfiedMatch(codepoint, List.of(apply("varchar", literal(1))));
        assertThat(exact.returnType()).isEqualTo(symbol("integer"));

        assertThat(codepoint.matchFunctionCallOutcome(List.of(apply("varchar", literal(2))), TrinoPreset.typeSystem()))
                .isInstanceOf(CompiledSignature.Unsatisfied.class);
    }

    /// A null argument (the ground unknown type) flowing into a parametric formal binds the
    /// minimum instance — varchar(0), decimal(1, 0) — mirroring Trino's unknown defaults; the
    /// other arguments still bind their own positions independently.
    @Test
    void testUnknownArgumentBindsMinimumParametricInstance()
    {
        CompiledSignature strpos = new CompiledSignature(
                List.of(variable("x"), variable("y")),
                List.of(),
                function(
                        List.of(apply("varchar", variable("x")), apply("varchar", variable("y"))),
                        symbol("bigint")));
        assertThat(satisfiedMatch(strpos, List.of(symbol("unknown"), apply("varchar", literal(3)))).parameterTypes())
                .isEqualTo(List.of(apply("varchar", literal(0)), apply("varchar", literal(3))));

        CompiledSignature truncate = new CompiledSignature(
                List.of(variable("p"), variable("s")),
                List.of(),
                function(
                        List.of(apply("decimal", variable("p"), variable("s"))),
                        apply("decimal", variable("p"), literal(0))));
        CompiledSignature.MatchResult result = satisfiedMatch(truncate, List.of(symbol("unknown")));
        assertThat(result.parameterTypes()).isEqualTo(List.of(apply("decimal", literal(1), literal(0))));
        assertThat(result.returnType()).isEqualTo(apply("decimal", literal(1), literal(0)));
    }

    /// A type variable constrained only by null arguments resolves to unknown — the bottom of the
    /// binding lattice; it survives only because nothing rebinds it. (A concrete sibling argument
    /// rebinding the variable is covered by the common-supertype tests.)
    @Test
    void testUnknownOnlyArgumentsBindVariableToUnknown()
    {
        CompiledSignature arrayFirst = new CompiledSignature(
                List.of(variable("e")),
                List.of(),
                function(List.of(apply("array", variable("e"))), variable("e")));

        CompiledSignature.MatchResult result = satisfiedMatch(arrayFirst, List.of(apply("array", symbol("unknown"))));
        assertThat(result.returnType()).isEqualTo(symbol("unknown"));
        assertThat(result.parameterTypes()).isEqualTo(List.of(apply("array", symbol("unknown"))));
    }

    /// A numeric variable shared across argument positions widens to the largest actual, the way
    /// the engine resolves date_diff over two timestamps of different precision — the narrower
    /// argument coerces up, and a single-position variable still binds its actual exactly.
    @Test
    void testSharedNumericVariableWidensAcrossPositions()
    {
        CompiledSignature dateDiff = new CompiledSignature(
                List.of(variable("p")),
                List.of(),
                function(
                        List.of(apply("timestamp", variable("p")), apply("timestamp", variable("p"))),
                        symbol("bigint")));

        CompiledSignature.MatchResult result = satisfiedMatch(dateDiff, List.of(
                apply("timestamp", literal(11)),
                apply("timestamp", literal(12))));
        assertThat(result.parameterTypes())
                .isEqualTo(List.of(apply("timestamp", literal(12)), apply("timestamp", literal(12))));

        CompiledSignature exact = new CompiledSignature(
                List.of(variable("x")),
                List.of(),
                function(List.of(apply("varchar", variable("x"))), apply("varchar", variable("x"))));
        assertThat(satisfiedMatch(exact, List.of(apply("varchar", literal(3)))).returnType())
                .isEqualTo(apply("varchar", literal(3)));
    }

    @Test
    void testPartialDecimalParameters()
    {
        CompiledSignature fixedPrecision = new CompiledSignature(
                List.of(variable("s")),
                List.of(),
                function(List.of(apply("decimal", literal(4), variable("s"))), symbol("boolean")));
        assertThat(satisfiedMatch(fixedPrecision, List.of(apply("decimal", literal(2), literal(1)))).parameterTypes())
                .containsExactly(apply("decimal", literal(4), literal(1)));

        CompiledSignature fixedScale = new CompiledSignature(
                List.of(variable("p")),
                List.of(),
                function(List.of(apply("decimal", variable("p"), literal(1))), symbol("boolean")));
        assertThat(satisfiedMatch(fixedScale, List.of(apply("decimal", literal(2), literal(0)))).parameterTypes())
                .containsExactly(apply("decimal", literal(3), literal(1)));
        assertThat(fixedScale.matchFunctionCallOutcome(List.of(apply("decimal", literal(2), literal(0))), TrinoPreset.typeSystem(), false))
                .isInstanceOf(CompiledSignature.Unsatisfied.class);
        assertThat(fixedScale.matchFunctionCallOutcome(List.of(apply("decimal", literal(2), literal(1))), TrinoPreset.typeSystem(), false))
                .isInstanceOfSatisfying(CompiledSignature.Satisfied.class, outcome ->
                        assertThat(outcome.result().parameterTypes()).containsExactly(apply("decimal", literal(2), literal(1))));
    }

    @Test
    void testSharedDecimalParametersPreserveIntegralDigits()
    {
        Expression decimal = apply("decimal", variable("p"), variable("s"));
        CompiledSignature signature = new CompiledSignature(
                List.of(variable("p"), variable("s")),
                List.of(),
                function(List.of(decimal, decimal), decimal));
        for (List<Expression> arguments : List.<List<Expression>>of(
                List.of(apply("decimal", literal(10), literal(2)), apply("decimal", literal(10), literal(8))),
                List.of(apply("decimal", literal(10), literal(8)), apply("decimal", literal(10), literal(2))))) {
            CompiledSignature.MatchResult result = satisfiedMatch(signature, arguments);
            assertThat(result.returnType()).isEqualTo(apply("decimal", literal(16), literal(8)));
            assertThat(result.parameterTypes()).containsExactly(result.returnType(), result.returnType());
            assertThat(signature.matchFunctionCallOutcome(arguments, TrinoPreset.typeSystem(), false))
                    .isInstanceOf(CompiledSignature.Unsatisfied.class);
        }
    }

    @Test
    void testInternalVariablesDoNotCollideWithDeclaredVNames()
    {
        CompiledSignature transformValues = new CompiledSignature(
                List.of(variable("k"), variable("v1"), variable("v2")),
                List.of(),
                function(
                        List.of(
                                apply("map", variable("k"), variable("v1")),
                                function(List.of(variable("k"), variable("v1")), variable("v2"))),
                        apply("map", variable("k"), variable("v2"))));

        CompiledSignature.MatchResult result = satisfiedMatch(transformValues, List.of(
                apply("map", apply("varchar", literal(1)), symbol("integer")),
                function(List.of(apply("varchar", literal(1)), symbol("integer")), symbol("bigint"))));
        assertThat(result.returnType()).isEqualTo(apply("map", apply("varchar", literal(1)), symbol("bigint")));
    }

    private static CompiledSignature.MatchResult satisfiedMatch(CompiledSignature scheme, List<Expression> arguments)
    {
        return switch (scheme.matchFunctionCallOutcome(arguments, TrinoPreset.typeSystem())) {
            case CompiledSignature.Satisfied satisfied -> satisfied.result();
            case CompiledSignature.Unsatisfied unsatisfied -> throw new AssertionError("Expected match but got unsatisfied: " + unsatisfied.message());
            case CompiledSignature.Incomplete _ -> throw new AssertionError("Expected match but got incomplete");
        };
    }
}
