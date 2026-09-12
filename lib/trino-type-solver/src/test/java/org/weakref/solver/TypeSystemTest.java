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

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.solver.Expression.BinaryOperator.GREATER_THAN_OR_EQUAL;
import static org.weakref.solver.Expression.BinaryOperator.SUBTRACT;
import static org.weakref.solver.Expression.anonymousField;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.operation;
import static org.weakref.solver.Expression.row;
import static org.weakref.solver.Expression.symbol;
import static org.weakref.solver.Expression.variable;
import static org.weakref.solver.Kind.TYPE;

public class TypeSystemTest
{
    @Test
    void testCoercionPlanCarriesInstantiatedPatternConditions()
    {
        assertThat(TrinoPreset.typeSystem().coercionPlan(symbol("integer"), apply("decimal", literal(10), literal(0))))
                .hasValueSatisfying(plan -> {
                    assertThat(plan.kind()).isEqualTo(CoercionPlan.Kind.DIRECT);
                    assertThat(plan.steps())
                            .singleElement()
                            .isInstanceOfSatisfying(CoercionPlan.DirectRule.class, direct -> {
                                assertThat(direct.ruleId()).isEqualTo("pattern:integer->decimal(@p, @s)");
                                assertThat(direct.conditions()).containsExactly(
                                        new NumericRelation(operation(
                                                GREATER_THAN_OR_EQUAL,
                                                operation(SUBTRACT, literal(10), literal(0)),
                                                literal(10))));
                            });
                });
    }

    @Test
    void testVariadicConstructorExpandsTemplatePerArgument()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();

        assertThat(typeSystem.instantiateValidationConstraints(apply("row", variable("@x"), variable("@y"))))
                .containsExactly(
                        new RequireKind("@x", TYPE),
                        new RequireKind("@y", TYPE));

        assertThat(typeSystem.instantiateValidationConstraints(apply("row", symbol("integer"))))
                .isEmpty();

        assertThat(typeSystem.instantiateValidationConstraints(apply("row")))
                .isEmpty();
    }

    @Test
    void testComparabilityAndOrderabilityOfTrinoTypes()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();

        // Most scalar types are both comparable and orderable.
        assertThat(typeSystem.isComparable(symbol("integer"))).isTrue();
        assertThat(typeSystem.isOrderable(symbol("integer"))).isTrue();
        assertThat(typeSystem.isComparable(apply("decimal", literal(10), literal(2)))).isTrue();
        assertThat(typeSystem.isOrderable(apply("decimal", literal(10), literal(2)))).isTrue();

        // JSON supports equality (like the engine's JsonType) but not ordering.
        assertThat(typeSystem.isComparable(symbol("json"))).isTrue();
        assertThat(typeSystem.isOrderable(symbol("json"))).isFalse();

        // HyperLogLog supports neither.
        assertThat(typeSystem.isComparable(symbol("hyperloglog"))).isFalse();
        assertThat(typeSystem.isOrderable(symbol("hyperloglog"))).isFalse();

        // Function types support neither.
        assertThat(typeSystem.isComparable(Expression.function(List.of(symbol("integer")), symbol("integer")))).isFalse();
        assertThat(typeSystem.isOrderable(Expression.function(List.of(symbol("integer")), symbol("integer")))).isFalse();

        // Maps are comparable iff key and value are, but never orderable.
        assertThat(typeSystem.isComparable(apply("map", symbol("integer"), symbol("integer")))).isTrue();
        assertThat(typeSystem.isComparable(apply("map", symbol("integer"), symbol("hyperloglog")))).isFalse();
        assertThat(typeSystem.isOrderable(apply("map", symbol("integer"), symbol("integer")))).isFalse();
    }

    @Test
    void testStructuralTraitsRecurseThroughContainers()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();

        // Arrays and rows inherit the trait from their element/field types.
        assertThat(typeSystem.isComparable(apply("array", symbol("integer")))).isTrue();
        assertThat(typeSystem.isOrderable(apply("array", symbol("integer")))).isTrue();
        assertThat(typeSystem.isComparable(apply("array", symbol("hyperloglog")))).isFalse();
        assertThat(typeSystem.isComparable(apply("array", apply("array", symbol("hyperloglog"))))).isFalse();
        assertThat(typeSystem.isComparable(apply("array", apply("map", symbol("integer"), symbol("integer"))))).isTrue();
        // An array of maps is comparable (maps are) but not orderable (maps aren't).
        assertThat(typeSystem.isOrderable(apply("array", apply("map", symbol("integer"), symbol("integer"))))).isFalse();
    }

    @Test
    void testUnresolvedVariablesAreNotRefuted()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();

        // A free variable hasn't been forced to a concrete type, so neither trait can be refuted yet.
        assertThat(typeSystem.isComparable(variable("@T"))).isTrue();
        assertThat(typeSystem.isOrderable(variable("@T"))).isTrue();
        assertThat(typeSystem.isComparable(apply("array", variable("@T")))).isTrue();
    }

    @Test
    void testRowSupertypeKeepsOnlyAgreedFieldNames()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();

        // A named row against an anonymous constructor merges to the anonymous shape — field
        // names survive only where both sides agree, matching the engine's row supertype
        Expression named = row(Expression.field("a", symbol("bigint")), Expression.field("b", symbol("bigint")));
        Expression anonymous = row(anonymousField(symbol("bigint")), anonymousField(symbol("bigint")));
        assertThat(typeSystem.getCommonSupertype(named, anonymous)).contains(anonymous);
        assertThat(typeSystem.getCommonSupertype(anonymous, named)).contains(anonymous);

        // Identical names on both sides survive
        assertThat(typeSystem.getCommonSupertype(named, named)).contains(named);

        // Names drop at every nesting level, even when an inner coercion forces the field
        // variable to bind to a named symbolic witness before the anonymous side intersects it
        Expression nestedNamed = row(
                Expression.field("c1", symbol("bigint")),
                Expression.field("c2", row(Expression.field("c21", symbol("bigint")), Expression.field("c22", symbol("bigint")))));
        Expression nestedAnonymous = row(
                anonymousField(symbol("integer")),
                anonymousField(row(anonymousField(symbol("integer")), anonymousField(symbol("integer")))));
        Expression nestedExpected = row(
                anonymousField(symbol("bigint")),
                anonymousField(row(anonymousField(symbol("bigint")), anonymousField(symbol("bigint")))));
        assertThat(typeSystem.getCommonSupertype(nestedNamed, nestedAnonymous)).contains(nestedExpected);
        assertThat(typeSystem.getCommonSupertype(nestedAnonymous, nestedNamed)).contains(nestedExpected);
    }

    @Test
    void testRowCoercionDomainStaysSymbolic()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();

        // The coercion domain of a covariant type is one symbolic witness per direction, not the
        // cartesian product of its components' candidates — the product grows exponentially with
        // width (a wide row would materialize millions of witnesses and exhaust the heap)
        Expression.RowField[] fields = new Expression.RowField[12];
        Arrays.fill(fields, anonymousField(symbol("integer")));
        assertThat(typeSystem.coercionsFrom(row(fields), new VariableAllocator()))
                .hasSizeLessThan(8)
                .anySatisfy(result -> assertThat(result.type()).isInstanceOf(Expression.Row.class));
    }

    @Test
    void testWideRowCommonSupertypeResolvesFieldwise()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();

        // The symbolic domain must still produce the fieldwise least upper bound a narrow row
        // would: every field pair resolves independently through the scalar machinery
        Expression.RowField[] left = new Expression.RowField[12];
        Expression.RowField[] right = new Expression.RowField[12];
        Expression.RowField[] expected = new Expression.RowField[12];
        Arrays.fill(left, anonymousField(symbol("integer")));
        Arrays.fill(right, anonymousField(symbol("bigint")));
        Arrays.fill(expected, anonymousField(symbol("bigint")));
        left[5] = anonymousField(apply("varchar", literal(5)));
        right[5] = anonymousField(apply("varchar", literal(10)));
        expected[5] = anonymousField(apply("varchar", literal(10)));

        assertThat(typeSystem.getCommonSupertype(row(left), row(right)))
                .contains(row(expected));
    }

    @Test
    void testFrameworkSolvesConstraintsWithNoRegisteredTypes()
    {
        TypeSystem typeSystem = new TypeSystem(List.of(), List.of());
        assertThat(typeSystem.types()).isEmpty();
        assertThat(typeSystem.coercions()).isEmpty();

        Solver solver = new Solver(typeSystem);
        Solver.SolveOutcome outcome = solver.solveOutcome(List.of(
                new Subtype(variable("@X"), variable("@Y"))));
        assertThat(outcome).isNotNull();
    }
}
