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

import java.util.List;

import static io.trino.typesolver.Expression.anyRow;
import static io.trino.typesolver.Expression.apply;
import static io.trino.typesolver.Expression.field;
import static io.trino.typesolver.Expression.function;
import static io.trino.typesolver.Expression.literal;
import static io.trino.typesolver.Expression.row;
import static io.trino.typesolver.Expression.symbol;
import static io.trino.typesolver.Expression.variable;
import static io.trino.typesolver.Expression.variadicFunction;
import static org.assertj.core.api.Assertions.assertThat;

/// Verifies that [TrinoSpecificity] (installed via [TestingTrinoLibrary#install]) breaks
/// ties the framework default leaves as [FunctionResolver.Ambiguous].
///
/// Cases mirror the test suite of Trino's [`FunctionSpecificityComparator`](https://github.com/trinodb/trino/pull/28791).
public class TrinoSpecificityTest
{
    @Test
    void testPrefersConcreteOverGeneric()
    {
        // f(boolean, double) vs f<T>(T, double). Called with (boolean, double), both match with
        // zero coercions. Trino prefers the concrete overload.
        CompiledSignature concrete = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(symbol("boolean"), symbol("double")), symbol("bigint")));
        CompiledSignature generic = new CompiledSignature(
                List.of(variable("T")),
                List.of(),
                function(List.of(variable("T"), symbol("double")), symbol("bigint")));

        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerFunction("f", concrete)
                .registerFunction("f", generic)
                .build();

        assertThat(library.resolveFunction("f", List.of(symbol("boolean"), symbol("double"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().compiledSignature()).isEqualTo(concrete));
    }

    @Test
    void testPrefersFixedArityOverVariadic()
    {
        // f(boolean, double, double) vs f(boolean, double...). Called with 3 args, both apply.
        // Trino prefers the fixed overload.
        CompiledSignature fixed = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(symbol("boolean"), symbol("double"), symbol("double")), symbol("boolean")));
        CompiledSignature variadic = new CompiledSignature(
                List.of(),
                List.of(),
                variadicFunction(List.of(symbol("boolean")), symbol("double"), symbol("boolean")));

        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerFunction("f", fixed)
                .registerFunction("f", variadic)
                .build();

        assertThat(library.resolveFunction("f", List.of(symbol("boolean"), symbol("double"), symbol("double"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().compiledSignature()).isEqualTo(fixed));
    }

    @Test
    void testPrefersConcreteDecimalOverCalculated()
    {
        // f(decimal(3, 1), double) vs f<p, s>(decimal(p, s), double). Called with an exact
        // decimal(3, 1), the concrete signature is narrower.
        CompiledSignature concreteDecimal = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(apply("decimal", literal(3), literal(1)), symbol("double")), symbol("boolean")));
        CompiledSignature calculatedDecimal = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(apply("decimal", variable("p"), variable("s")), symbol("double")), symbol("boolean")));

        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerFunction("f", concreteDecimal)
                .registerFunction("f", calculatedDecimal)
                .build();

        assertThat(library.resolveFunction("f", List.of(apply("decimal", literal(3), literal(1)), symbol("double"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().compiledSignature()).isEqualTo(concreteDecimal));
    }

    @Test
    void testPrefersRepeatedVariableOverIndependent()
    {
        // f<T>(T, T) vs f<T, U>(T, U). Called with (bigint, bigint), both apply, but
        // f<T>(T, T) encodes an equality obligation between the two positions — richer and
        // therefore more specific under Trino.
        CompiledSignature repeated = new CompiledSignature(
                List.of(variable("T")),
                List.of(),
                function(List.of(variable("T"), variable("T")), symbol("boolean")));
        CompiledSignature independent = new CompiledSignature(
                List.of(variable("T"), variable("U")),
                List.of(),
                function(List.of(variable("T"), variable("U")), symbol("boolean")));

        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerFunction("f", repeated)
                .registerFunction("f", independent)
                .build();

        assertThat(library.resolveFunction("f", List.of(symbol("bigint"), symbol("bigint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().compiledSignature()).isEqualTo(repeated));
    }

    @Test
    void testPrefersNarrowerConcreteCoercion()
    {
        // Two widening targets for smallint: integer and bigint. Both apply with one coercion
        // each. Under framework default this is Ambiguous; Trino's specificity picks integer
        // (the narrower widening target).
        CompiledSignature toInteger = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(symbol("integer")), symbol("integer")));
        CompiledSignature toBigint = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(symbol("bigint")), symbol("bigint")));

        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerFunction("f", toInteger)
                .registerFunction("f", toBigint)
                .build();

        assertThat(library.resolveFunction("f", List.of(symbol("smallint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().compiledSignature()).isEqualTo(toInteger));
    }

    @Test
    void testUnrelatedBoundTargetsRemainAmbiguous()
    {
        // integer can widen to either bigint (primitive) OR decimal(10, 0) (pattern rule), but
        // the two target types are unrelated — bigint does NOT coerce to decimal(10, 0)
        // (requires p - s ≥ 19) and decimal(10, 0) does NOT coerce to bigint. Neither direction
        // of Trino's forwardability check succeeds, so the resolver reports Ambiguous. This is
        // the same answer the PR produces.
        CompiledSignature bigintFn = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(symbol("bigint")), symbol("bigint")));
        CompiledSignature decimalFn = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(apply("decimal", literal(10), literal(0))), symbol("decimal")));

        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerFunction("f", bigintFn)
                .registerFunction("f", decimalFn)
                .build();

        assertThat(library.resolveFunction("f", List.of(symbol("integer"))))
                .isInstanceOf(FunctionResolver.Ambiguous.class);
    }

    @Test
    void testPrefersBoundedRowVariableOverWildcardRow()
    {
        // Same setup as the framework-level test, but under Trino's specificity the sameRow
        // scheme (more constrained — all positions must share a row type) wins.
        CompiledSignature wildcard = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(anyRow(), anyRow()), symbol("wildcard")));
        CompiledSignature sameRow = new CompiledSignature(
                List.of(variable("R")),
                List.of(new Subtype(variable("R"), anyRow())),
                function(List.of(variable("R"), variable("R")), symbol("same-row")));

        Expression.Row actualRow = row(
                field("left", symbol("integer")),
                field("right", symbol("varchar")));

        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerFunction("f", wildcard)
                .registerFunction("f", sameRow)
                .build();

        assertThat(library.resolveFunction("f", List.of(actualRow, actualRow)))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().compiledSignature()).isEqualTo(sameRow));
    }

    @Test
    void testReturnsIncomparableWhenCandidatesAreUnrelated()
    {
        // Candidates that don't forward either direction remain distinct. Called with a boolean
        // argument, only one of these two is applicable — the other is filtered out before
        // specificity even runs. Just verify the resolver behaves sensibly.
        CompiledSignature booleanFn = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(symbol("boolean")), symbol("bigint")));
        CompiledSignature varcharFn = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(symbol("varchar")), symbol("bigint")));

        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerFunction("f", booleanFn)
                .registerFunction("f", varcharFn)
                .build();

        assertThat(library.resolveFunction("f", List.of(symbol("boolean"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().compiledSignature()).isEqualTo(booleanFn));
    }

    @Test
    void testIdenticalExactMatchesReportAmbiguous()
    {
        // Two byte-identical signatures — neither dominates the other even under Trino's
        // specificity, so the outcome remains Ambiguous.
        CompiledSignature first = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(symbol("integer")), symbol("boolean")));
        CompiledSignature second = new CompiledSignature(
                List.of(),
                List.of(),
                function(List.of(symbol("integer")), symbol("boolean")));

        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerFunction("f", first)
                .registerFunction("f", second)
                .build();

        assertThat(library.resolveFunction("f", List.of(symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Ambiguous.class, outcome ->
                        assertThat(outcome.candidates()).hasSize(2));
    }
}
