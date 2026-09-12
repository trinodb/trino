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
import static org.weakref.solver.Expression.BinaryOperator.ADD;
import static org.weakref.solver.Expression.BinaryOperator.GREATER_THAN;
import static org.weakref.solver.Expression.BinaryOperator.GREATER_THAN_OR_EQUAL;
import static org.weakref.solver.Expression.BinaryOperator.LESS_THAN_OR_EQUAL;
import static org.weakref.solver.Expression.BinaryOperator.MIN;
import static org.weakref.solver.Expression.BinaryOperator.MULTIPLY;
import static org.weakref.solver.Expression.BinaryOperator.SUBTRACT;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.conditional;
import static org.weakref.solver.Expression.function;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.operation;
import static org.weakref.solver.Expression.variable;

public class CalculatedReturnTypeTest
{
    /**
     * decimal addition common-supertype:
     *   target scale s = max(s1, s2)
     *   target precision p = max(p1-s1, p2-s2) + s, clamped to 38
     *
     * Expressed via lower-bound constraints on @p, @s. Max emerges from the solver taking the
     * tightest lower bound; no MIN/MAX operator needed.
     */
    @Test
    void testDecimalAdditionReturnTypeViaLowerBoundConstraints()
    {
        TypeScheme addDecimal = new TypeScheme(
                List.of(
                        variable("@p1"),
                        variable("@s1"),
                        variable("@p2"),
                        variable("@s2"),
                        variable("@p"),
                        variable("@s")),
                List.of(
                        new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("@s1"), variable("@s"))),
                        new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("@s2"), variable("@s"))),
                        new NumericRelation(operation(
                                LESS_THAN_OR_EQUAL,
                                operation(SUBTRACT, variable("@p1"), variable("@s1")),
                                operation(SUBTRACT, variable("@p"), variable("@s")))),
                        new NumericRelation(operation(
                                LESS_THAN_OR_EQUAL,
                                operation(SUBTRACT, variable("@p2"), variable("@s2")),
                                operation(SUBTRACT, variable("@p"), variable("@s")))),
                        new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("@p"), literal(38))),
                        new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("@s"), literal(0)))),
                function(
                        List.of(
                                apply("decimal", variable("@p1"), variable("@s1")),
                                apply("decimal", variable("@p2"), variable("@s2"))),
                        apply("decimal", variable("@p"), variable("@s"))));

        TypeScheme.MatchOutcome outcome = addDecimal.matchFunctionCallOutcome(
                List.of(
                        apply("decimal", literal(10), literal(2)),
                        apply("decimal", literal(5), literal(3))),
                TrinoPreset.typeSystem());

        assertThat(outcome).isInstanceOfSatisfying(TypeScheme.Satisfied.class, satisfied -> {
            assertThat(satisfied.result().returnType()).isEqualTo(apply("decimal", literal(11), literal(3)));
            assertThat(satisfied.result().numericBindings()).containsEntry("@p", 11).containsEntry("@s", 3);
        });
    }

    /**
     * decimal multiplication exact-arithmetic return type:
     *   p = p1 + p2
     *   s = s1 + s2
     *
     * The return type uses BinaryOperation(ADD, ...). After bindings are materialized, the
     * literal-arithmetic evaluator reduces (10 + 5) to 15, etc.
     */
    @Test
    void testDecimalMultiplicationReturnTypeReducesArithmetic()
    {
        TypeScheme multiplyDecimal = new TypeScheme(
                List.of(
                        variable("@p1"),
                        variable("@s1"),
                        variable("@p2"),
                        variable("@s2")),
                List.of(),
                function(
                        List.of(
                                apply("decimal", variable("@p1"), variable("@s1")),
                                apply("decimal", variable("@p2"), variable("@s2"))),
                        apply("decimal",
                                operation(ADD, variable("@p1"), variable("@p2")),
                                operation(ADD, variable("@s1"), variable("@s2")))));

        TypeScheme.MatchOutcome outcome = multiplyDecimal.matchFunctionCallOutcome(
                List.of(
                        apply("decimal", literal(10), literal(2)),
                        apply("decimal", literal(5), literal(3))),
                TrinoPreset.typeSystem());

        assertThat(outcome).isInstanceOfSatisfying(TypeScheme.Satisfied.class, satisfied ->
                assertThat(satisfied.result().returnType()).isEqualTo(apply("decimal", literal(15), literal(5))));
    }

    /**
     * varchar concat exact-arithmetic return type:
     *   n = n1 + n2
     *
     * Tests the literal-arithmetic evaluator on a single numeric parameter.
     */
    @Test
    void testVarcharConcatReturnTypeReducesArithmetic()
    {
        TypeScheme concatVarchar = new TypeScheme(
                List.of(variable("@n1"), variable("@n2")),
                List.of(),
                function(
                        List.of(
                                apply("varchar", variable("@n1")),
                                apply("varchar", variable("@n2"))),
                        apply("varchar", operation(ADD, variable("@n1"), variable("@n2")))));

        TypeScheme.MatchOutcome outcome = concatVarchar.matchFunctionCallOutcome(
                List.of(
                        apply("varchar", literal(5)),
                        apply("varchar", literal(10))),
                TrinoPreset.typeSystem());

        assertThat(outcome).isInstanceOfSatisfying(TypeScheme.Satisfied.class, satisfied ->
                assertThat(satisfied.result().returnType()).isEqualTo(apply("varchar", literal(15))));
    }

    /**
     * A calculated varchar length that overflows 32-bit arithmetic saturates to
     * {@link Integer#MAX_VALUE} rather than wrapping to a negative length, mirroring Trino's
     * 64-bit {@code TypeCalculation}. Trino's growth formulas clamp with {@code min(2147483647, ...)},
     * so the saturated fold reaches the same (unbounded) length.
     */
    @Test
    void testCalculatedLengthSaturatesOnOverflow()
    {
        TypeScheme grow = new TypeScheme(
                List.of(variable("@n1"), variable("@n2")),
                List.of(),
                function(
                        List.of(apply("varchar", variable("@n1")), apply("varchar", variable("@n2"))),
                        apply("varchar", operation(MIN, literal(Integer.MAX_VALUE), operation(MULTIPLY, variable("@n1"), variable("@n2"))))));

        TypeScheme.MatchOutcome outcome = grow.matchFunctionCallOutcome(
                List.of(apply("varchar", literal(100000)), apply("varchar", literal(100000))),
                TrinoPreset.typeSystem());

        assertThat(outcome).isInstanceOfSatisfying(TypeScheme.Satisfied.class, satisfied ->
                assertThat(satisfied.result().returnType()).isEqualTo(apply("varchar", literal(Integer.MAX_VALUE))));
    }

    /**
     * Trino's decimal multiplication and division clamp their result scale through a conditional:
     *   p = min(p1 + p2, 38)
     *   s = min(s1 + s2, if((p1 + p2) - (s1 + s2) > 32, 6, 38 - ((p1 + p2) - (s1 + s2))))
     * Exercises {@link Expression.Conditional} through return-type materialization on both branches.
     */
    @Test
    void testConditionalReturnTypeEvaluatesChosenBranch()
    {
        Expression integerDigits = operation(
                SUBTRACT,
                operation(ADD, variable("@p1"), variable("@p2")),
                operation(ADD, variable("@s1"), variable("@s2")));
        TypeScheme multiplyDecimal = new TypeScheme(
                List.of(variable("@p1"), variable("@s1"), variable("@p2"), variable("@s2")),
                List.of(),
                function(
                        List.of(
                                apply("decimal", variable("@p1"), variable("@s1")),
                                apply("decimal", variable("@p2"), variable("@s2"))),
                        apply("decimal",
                                operation(MIN, operation(ADD, variable("@p1"), variable("@p2")), literal(38)),
                                operation(MIN,
                                        operation(ADD, variable("@s1"), variable("@s2")),
                                        conditional(
                                                operation(GREATER_THAN, integerDigits, literal(32)),
                                                literal(6),
                                                operation(SUBTRACT, literal(38), integerDigits))))));

        // Few integer digits: the condition is false and the scale keeps the un-clamped sum
        assertThat(multiplyDecimal.matchFunctionCallOutcome(
                List.of(apply("decimal", literal(2), literal(1)), apply("decimal", literal(3), literal(2))),
                TrinoPreset.typeSystem()))
                .isInstanceOfSatisfying(TypeScheme.Satisfied.class, satisfied ->
                        assertThat(satisfied.result().returnType()).isEqualTo(apply("decimal", literal(5), literal(3))));

        // Integer digits beyond 32: the condition is true and the scale clamps to 6
        assertThat(multiplyDecimal.matchFunctionCallOutcome(
                List.of(apply("decimal", literal(25), literal(4)), apply("decimal", literal(20), literal(4))),
                TrinoPreset.typeSystem()))
                .isInstanceOfSatisfying(TypeScheme.Satisfied.class, satisfied ->
                        assertThat(satisfied.result().returnType()).isEqualTo(apply("decimal", literal(38), literal(6))));
    }
}
