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

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.solver.Expression.anyRow;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.field;
import static org.weakref.solver.Expression.function;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.row;
import static org.weakref.solver.Expression.symbol;
import static org.weakref.solver.Expression.variable;

public class TypeSchemeTest
{
    @Test
    void testBoundedRowVariableMatchesSameConcreteRowArguments()
    {
        TypeScheme signature = new TypeScheme(
                List.of(variable("@R")),
                List.of(new Subtype(variable("@R"), anyRow())),
                function(List.of(variable("@R"), variable("@R")), symbol("boolean")));

        Expression.Row actual = row(
                field("left", symbol("integer")),
                field("right", symbol("varchar")));

        TypeScheme.MatchResult result = satisfiedMatch(signature, List.of(actual, actual));

        assertThat(result.returnType())
                .isEqualTo(symbol("boolean"));
        assertThat(result.typeBindings())
                .containsValue(actual);
    }

    @Test
    void testBoundedRowVariableRejectsDifferentConcreteRows()
    {
        TypeScheme signature = new TypeScheme(
                List.of(variable("@R")),
                List.of(new Subtype(variable("@R"), anyRow())),
                function(List.of(variable("@R"), variable("@R")), symbol("boolean")));

        assertThat(signature.matchFunctionCallOutcome(
                List.of(
                        row(field("a", symbol("integer"))),
                        row(field("b", symbol("varchar")))),
                TrinoPreset.typeSystem()))
                .isInstanceOf(TypeScheme.Unsatisfied.class);
    }

    @Test
    void testBoundedRowVariableRejectsNonRowArguments()
    {
        TypeScheme signature = new TypeScheme(
                List.of(variable("@R")),
                List.of(new Subtype(variable("@R"), anyRow())),
                function(List.of(variable("@R"), variable("@R")), symbol("boolean")));

        assertThat(signature.matchFunctionCallOutcome(
                List.of(symbol("integer"), symbol("integer")),
                TrinoPreset.typeSystem()))
                .isInstanceOf(TypeScheme.Unsatisfied.class);
    }

    @Test
    void testWildcardRowParameterAcceptsAnyConcreteRow()
    {
        TypeScheme signature = new TypeScheme(
                List.of(),
                List.of(),
                function(List.of(anyRow()), symbol("boolean")));

        assertThat(signature.matchFunctionCallOutcome(
                List.of(row(
                        field("a", symbol("bigint")),
                        field("b", symbol("varchar")))),
                TrinoPreset.typeSystem()))
                .isInstanceOf(TypeScheme.Satisfied.class);
    }

    @Test
    void testMatchFunctionCallOutcomeRejectsGroundUnreachableCall()
    {
        TypeScheme signature = new TypeScheme(
                List.of(),
                List.of(),
                function(List.of(symbol("binary")), symbol("boolean")));

        assertThat(signature.matchFunctionCallOutcome(List.of(symbol("integer")), TrinoPreset.typeSystem()))
                .isInstanceOf(TypeScheme.Unsatisfied.class);
    }

    @Test
    void testStructuredOutcomeApiDistinguishesUnsatisfied()
    {
        TypeScheme signature = new TypeScheme(
                List.of(),
                List.of(),
                function(List.of(symbol("binary")), symbol("boolean")));

        assertThat(signature.matchFunctionCallOutcome(List.of(symbol("integer")), TrinoPreset.typeSystem()))
                .isInstanceOf(TypeScheme.Unsatisfied.class);
    }

    /**
     * A ground parametric formal accepts a smaller instance by whole-type widening — varchar(0)
     * flows into a literal varchar(1) parameter through the varchar coercion rule, not through a
     * per-parameter 0 <: 1 relation (which no rule covers). The reverse direction must not narrow.
     */
    @Test
    void testGroundParametricFormalCoercesAsWholeType()
    {
        TypeScheme codepoint = new TypeScheme(
                List.of(),
                List.of(),
                function(List.of(apply("varchar", literal(1))), symbol("integer")));

        TypeScheme.MatchResult widened = satisfiedMatch(codepoint, List.of(apply("varchar", literal(0))));
        assertThat(widened.returnType()).isEqualTo(symbol("integer"));
        assertThat(widened.parameterTypes()).isEqualTo(List.of(apply("varchar", literal(1))));

        TypeScheme.MatchResult exact = satisfiedMatch(codepoint, List.of(apply("varchar", literal(1))));
        assertThat(exact.returnType()).isEqualTo(symbol("integer"));

        assertThat(codepoint.matchFunctionCallOutcome(List.of(apply("varchar", literal(2))), TrinoPreset.typeSystem()))
                .isInstanceOf(TypeScheme.Unsatisfied.class);
    }

    private static TypeScheme.MatchResult satisfiedMatch(TypeScheme scheme, List<Expression> arguments)
    {
        return switch (scheme.matchFunctionCallOutcome(arguments, TrinoPreset.typeSystem())) {
            case TypeScheme.Satisfied satisfied -> satisfied.result();
            case TypeScheme.Unsatisfied unsatisfied -> throw new AssertionError("Expected match but got unsatisfied: " + unsatisfied.message());
            case TypeScheme.Incomplete _ -> throw new AssertionError("Expected match but got incomplete");
        };
    }
}
