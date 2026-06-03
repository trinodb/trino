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
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.weakref.solver.Expression.BinaryOperator.ADD;
import static org.weakref.solver.Expression.BinaryOperator.EQUAL;
import static org.weakref.solver.Expression.BinaryOperator.GREATER_THAN_OR_EQUAL;
import static org.weakref.solver.Expression.BinaryOperator.LESS_THAN_OR_EQUAL;
import static org.weakref.solver.Expression.BinaryOperator.SUBTRACT;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.field;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.operation;
import static org.weakref.solver.Expression.row;
import static org.weakref.solver.Expression.symbol;
import static org.weakref.solver.Expression.variable;

public class SolverTest
{
    @Test
    void testUpperBoundPrimitiveDomainNarrowsToInteger()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        variable("@T"),
                        symbol("integer")),
                new Subtype(
                        variable("@T"),
                        symbol("bigint"))));

        assertThat(((TypeVariableState) result.variableBounds().get("@T")).binding())
                .contains(symbol("integer"));
        assertThat(result.materializedTypeVariables())
                .containsEntry("@T", symbol("integer"));
    }

    @Test
    void testExactArrayBoundBindsTypeVariable()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        variable("@T"),
                        symbol("array(integer)"))));

        assertThat(((TypeVariableState) result.variableBounds().get("@T")).binding())
                .contains(symbol("array(integer)"));
        assertThat(result.materializedTypeVariables())
                .containsEntry("@T", symbol("array(integer)"));
    }

    @Test
    void testNoCommonSupertype()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        assertThatThrownBy(() -> solver.solve(List.of(
                new Subtype(
                        symbol("integer"),
                        variable("@T")),
                new Subtype(
                        symbol("binary"),
                        variable("@T")))))
                .hasMessageContaining("Unsatisfiable domain for variable @T");
    }

    @Test
    void testSolveOutcomeReportsUnsatisfiedConstraints()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        assertThat(solver.solveOutcome(List.of(
                new Subtype(
                        symbol("integer"),
                        variable("@T")),
                new Subtype(
                        symbol("binary"),
                        variable("@T")))))
                .isInstanceOfSatisfying(Solver.Unsatisfied.class, outcome -> assertThat(outcome.message())
                        .contains("Unsatisfiable domain for variable @T"));
    }

    @Test
    void testSolveOutcomeRejectsUnreachableGroundSubtype()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        // Both sides are ground and no coercion rule applies — solver knows there's no way
        // forward. Returns Unsatisfied rather than deferring as Incomplete.
        assertThat(solver.solveOutcome(List.of(
                new Subtype(
                        symbol("integer"),
                        symbol("binary")))))
                .isInstanceOf(Solver.Unsatisfied.class);
    }

    @Test
    void testSolveOutcomeTreatsMaterializableNumericConstraintsAsSatisfied()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        assertThat(solver.solveOutcome(List.of(
                new NumericRelation(operation(
                        GREATER_THAN_OR_EQUAL,
                        operation(SUBTRACT, variable("@p"), variable("@s")),
                        literal(10))),
                new NumericRelation(operation(EQUAL, variable("@s"), literal(0))),
                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("@p"), literal(10))))))
                .isInstanceOf(Solver.Satisfied.class);
    }

    @Test
    void testSolveOutcomeTreatsOffsetAdditionConstraintsAsSatisfied()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        assertThat(solver.solveOutcome(List.of(
                new NumericRelation(operation(
                        GREATER_THAN_OR_EQUAL,
                        operation(ADD, variable("@p"), literal(2)),
                        literal(12))),
                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("@p"), literal(10))))))
                .isInstanceOf(Solver.Satisfied.class);
    }

    @Test
    void testSolveOutcomeTreatsVariablePlusOffsetConstraintsAsSatisfied()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        assertThat(solver.solveOutcome(List.of(
                new NumericRelation(operation(
                        LESS_THAN_OR_EQUAL,
                        operation(ADD, variable("@p"), literal(2)),
                        variable("@q"))),
                new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("@p"), literal(10))),
                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("@q"), literal(12))))))
                .isInstanceOf(Solver.Satisfied.class);
    }

    @Test
    void testPrimitiveUpperBoundAgainstSymbolicDecimalMaterializesParameters()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        variable("@T"),
                        symbol("integer")),
                new Subtype(
                        variable("@T"),
                        apply("decimal", variable("@pp"), variable("@ps")))));

        assertThat(result.materializedTypeVariables())
                .containsEntry("@T", symbol("integer"));
        assertThat(result.materializedNumericValues())
                .containsEntry("@pp", 10)
                .containsEntry("@ps", 0);
    }

    @Test
    void testExactPrimitiveBoundBindsTypeVariable()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(new Subtype(
                symbol("binary"),
                variable("@T"))));

        assertThat(((TypeVariableState) result.variableBounds().get("@T")).binding())
                .contains(symbol("binary"));
        assertThat(result.materializedTypeVariables())
                .containsEntry("@T", symbol("binary"));
    }

    @Test
    void testPrimitiveUpperBoundAgainstSymbolicDecimalChoosesIntegerWitness()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        variable("@T"),
                        symbol("integer")),
                new Subtype(
                        variable("@T"),
                        apply("decimal", variable("@p"), variable("@s")))));

        assertThat(result.materializedTypeVariables())
                .containsEntry("@T", symbol("integer"));
        assertThat(result.materializedNumericValues())
                .containsEntry("@p", 10)
                .containsEntry("@s", 0);
    }

    @Test
    void testVarcharLengthBoundsMaterializeMinimumSatisfyingValue()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        apply("varchar", variable("@T")),
                        apply("varchar", literal(30))),
                new Subtype(
                        apply("varchar", literal(10)),
                        apply("varchar", variable("@T"))),
                new Subtype(
                        apply("varchar", literal(20)),
                        apply("varchar", variable("@T")))));

        assertThat(result.variableBounds().get("@T"))
                .isEqualTo(new NumericVariableState(java.util.OptionalInt.of(20), java.util.OptionalInt.of(30)));
        assertThat(result.materializedNumericValues())
                .containsEntry("@T", 20);
    }

    @Test
    void testConcreteArrayCovarianceSubtypeDischarges()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        apply("array", symbol("smallint")),
                        apply("array", symbol("integer")))));

        assertThat(result.variableBounds())
                .isEmpty();
        assertThat(result.nextBatch())
                .isEmpty();
    }

    @Test
    void testConcreteMapCovarianceSubtypeDischarges()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        apply("map", symbol("smallint"), symbol("tinyint")),
                        apply("map", symbol("integer"), symbol("smallint")))));

        assertThat(result.variableBounds())
                .isEmpty();
        assertThat(result.nextBatch())
                .isEmpty();
    }

    @Test
    void testArrayCommonSupertypeUsesElementSupertype()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        apply("array", symbol("smallint")),
                        variable("@T")),
                new Subtype(
                        apply("array", symbol("integer")),
                        variable("@T"))));

        assertThat(result.materializedTypeVariables())
                .containsEntry("@T", apply("array", symbol("integer")));
    }

    @Test
    void testMapCommonSupertypeUsesComponentSupertypes()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        apply("map", symbol("smallint"), symbol("tinyint")),
                        variable("@T")),
                new Subtype(
                        apply("map", symbol("integer"), symbol("smallint")),
                        variable("@T"))));

        assertThat(result.materializedTypeVariables())
                .containsEntry("@T", apply("map", symbol("integer"), symbol("smallint")));
    }

    @Test
    void testConcreteDecimalSubtypeDischarges()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        apply("decimal", literal(3), literal(0)),
                        apply("decimal", literal(10), literal(0)))));

        assertThat(result.variableBounds())
                .isEmpty();
        assertThat(result.nextBatch())
                .isEmpty();
    }

    @Test
    void testConcreteDecimalSubtypeFailsWhenScaleWouldShrink()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        assertThatThrownBy(() -> solver.solve(List.of(
                new Subtype(
                        apply("decimal", literal(10), literal(2)),
                        apply("decimal", literal(10), literal(1))))))
                .hasMessageContaining("Unsatisfiable");
    }

    @Test
    void testConcreteRowSubtypeIgnoresFieldNames()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        row(
                                field("a", symbol("smallint")),
                                field("b", symbol("tinyint"))),
                        row(
                                field("x", symbol("integer")),
                                field("y", symbol("smallint"))))));

        assertThat(result.variableBounds())
                .isEmpty();
    }

    @Test
    void testConcreteRowSubtypeFailsOnArityMismatch()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        assertThatThrownBy(() -> solver.solve(List.of(
                new Subtype(
                        row(field("a", symbol("integer"))),
                        row(
                                field("x", symbol("integer")),
                                field("y", symbol("bigint")))))))
                .hasMessageContaining("row arity mismatch");
    }

    @Test
    void testExactRowBoundBindsTypeVariable()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Expression.Row rowType = row(
                field("left", symbol("integer")),
                field("right", symbol("varchar")));

        Solver.Result result = solver.solve(List.of(new Subtype(
                rowType,
                variable("@T"))));

        assertThat(result.materializedTypeVariables())
                .containsEntry("@T", rowType);
    }

    @Test
    void testRowCommonSupertypeUsesFieldSupertypes()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        row(
                                field("a", symbol("smallint")),
                                field("b", symbol("tinyint"))),
                        variable("@T")),
                new Subtype(
                        row(
                                field("x", symbol("integer")),
                                field("y", symbol("smallint"))),
                        variable("@T"))));

        assertThat(result.materializedTypeVariables().get("@T"))
                .isInstanceOfSatisfying(Expression.Row.class, rowType -> assertThat(rowType.fields())
                        .extracting(Expression.RowField::type)
                        .containsExactly(symbol("integer"), symbol("smallint")));
    }

    @Test
    void testRowCommonSubtypeUsesFieldSubtypes()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        variable("@T"),
                        row(
                                field("left", symbol("bigint")),
                                field("right", symbol("bigint")))),
                new Subtype(
                        variable("@T"),
                        row(
                                field("first", symbol("integer")),
                                field("second", symbol("smallint"))))));

        assertThat(result.materializedTypeVariables().get("@T"))
                .isInstanceOfSatisfying(Expression.Row.class, rowType -> assertThat(rowType.fields())
                        .extracting(Expression.RowField::type)
                        .containsExactly(symbol("integer"), symbol("smallint")));
    }

    @Test
    void testNumericUnsatisfiable()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        assertThatThrownBy(() -> solver.solve(List.of(
                new Subtype(
                        apply("varchar", variable("@n")),
                        apply("varchar", literal(10))),
                new Subtype(
                        apply("varchar", literal(20)),
                        apply("varchar", variable("@n"))))))
                .hasMessageContaining("Unsatisfiable");
    }

    @Test
    void testMixNumericAndType()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        assertThatThrownBy(() -> solver.solve(List.of(
                new Subtype(
                        apply("varchar", variable("@n")),
                        apply("varchar", literal(10))),
                new Subtype(
                        apply("array", variable("@T")),
                        apply("array", variable("@n"))))))
                .hasMessageContaining("Expected @n to be of TYPE kind");
    }

    @Test
    void testPrimitiveCommonSuperType()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        symbol("integer"),
                        variable("@T")),
                new Subtype(
                        symbol("smallint"),
                        variable("@T"))));

        assertThat(result.materializedTypeVariables())
                .containsEntry("@T", symbol("integer"));
    }

    @Test
    void testPrimitiveAndDecimalCommonSuperType()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        symbol("integer"),
                        variable("@T")),
                new Subtype(
                        apply("decimal", literal(3), literal(0)),
                        variable("@T"))));

        assertThat(result.materializedTypeVariables())
                .containsEntry("@T", apply("decimal", literal(10), literal(0)));
    }

    @Test
    void testDomainNarrowsToSingleWitness()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(symbol("integer"), variable("@T")),
                new Subtype(variable("@T"), symbol("integer"))));

        assertThat(result.variableBounds())
                .containsKey("@T");
        assertThat(((TypeVariableState) result.variableBounds().get("@T")).binding())
                .contains(symbol("integer"));
    }

    @Test
    void testDomainBecomesUnsatisfiable()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        assertThatThrownBy(() -> solver.solve(List.of(
                new Subtype(symbol("bigint"), variable("@T")),
                new Subtype(variable("@T"), symbol("integer")))))
                .hasMessageContaining("Unsatisfiable domain for variable @T");
    }

    @Test
    void testDifferenceConstraintPropagation()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new NumericRelation(operation(GREATER_THAN_OR_EQUAL, operation(Expression.BinaryOperator.SUBTRACT, variable("@p"), variable("@s")), literal(10))),
                new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("@s"), literal(2))),
                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("@p"), literal(38)))));

        assertThat(result.variableBounds().get("@p"))
                .isEqualTo(new NumericVariableState(java.util.OptionalInt.of(12), java.util.OptionalInt.of(38)));
        assertThat(result.variableBounds().get("@s"))
                .isEqualTo(new NumericVariableState(java.util.OptionalInt.of(2), java.util.OptionalInt.of(28)));
        assertThat(result.materializedNumericValues())
                .containsEntry("@p", 12)
                .containsEntry("@s", 2);
    }

    @Test
    void testMaterializationChoosesFeasibleGuardedAlternative()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(
                        variable("@T"),
                        symbol("integer")),
                new Subtype(
                        variable("@T"),
                        apply("decimal", variable("@p"), variable("@s"))),
                new NumericRelation(operation(EQUAL, variable("@p"), literal(4))),
                new NumericRelation(operation(EQUAL, variable("@s"), literal(0)))));

        assertThat(result.materializedTypeVariables())
                .containsEntry("@T", symbol("tinyint"));
        assertThat(result.materializedNumericValues())
                .containsEntry("@p", 4)
                .containsEntry("@s", 0);
    }

    @Test
    void testChoicePrunesRedundantSameWitnessAlternative()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Choice(List.of(
                        new Alternative(
                                symbol("integer"),
                                Set.of(new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("@p"), literal(3))))),
                        new Alternative(
                                symbol("integer"),
                                Set.of(
                                        new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("@p"), literal(3))),
                                        new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("@p"), literal(5))))))),
                new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("@p"), literal(4)))));

        assertThat(result.nextBatch().stream().noneMatch(Choice.class::isInstance))
                .isTrue();
        assertThat(result.materializedNumericValues())
                .containsEntry("@p", 4);
    }

    @Test
    void testChoicePrunesEquivalentComparableWitnessAlternatives()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Choice(List.of(
                        new Alternative(
                                symbol("integer"),
                                Set.of(new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("@p"), literal(3))))),
                        new Alternative(
                                symbol("bigint"),
                                Set.of(new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("@p"), literal(3))))))),
                new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("@p"), literal(4)))));

        assertThat(result.nextBatch().stream().noneMatch(Choice.class::isInstance))
                .isTrue();
        assertThat(result.materializedNumericValues())
                .containsEntry("@p", 4);
    }

    @Test
    void testForcedChoiceWitnessInstantiatesValidationConstraints()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        assertThatThrownBy(() -> solver.solve(List.of(
                new Choice(List.of(
                        new Alternative(
                                apply("decimal", variable("@p"), variable("@s")),
                                Set.of()))),
                new NumericRelation(operation(EQUAL, variable("@p"), literal(4))),
                new NumericRelation(operation(EQUAL, variable("@s"), literal(5))))))
                .hasMessageContaining("Unsatisfiable");
    }

    @Test
    void testChoiceCollapsesAfterNumericSubstitution()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Choice(List.of(
                        new Alternative(
                                apply("decimal", variable("@p"), variable("@s")),
                                Set.of()),
                        new Alternative(
                                apply("decimal", literal(10), literal(0)),
                                Set.of()))),
                new NumericRelation(operation(EQUAL, variable("@p"), literal(10))),
                new NumericRelation(operation(EQUAL, variable("@s"), literal(0)))));

        assertThat(result.nextBatch().stream().noneMatch(Choice.class::isInstance))
                .isTrue();
        assertThat(result.materializedNumericValues())
                .containsEntry("@p", 10)
                .containsEntry("@s", 0);
    }

    @Test
    void testFreshCoercionVariablesDoNotCollideWithInputVariables()
    {
        // The input names the target's two decimal parameters @v3 and @v4 — variables a
        // different allocator would have minted. While solving, the engine instantiates the
        // guarded tinyint -> decimal(@p, @s) coercion (guard @p - @s >= 3) with its own fresh
        // variables. Those must stay disjoint from @v3/@v4: if the fresh names overlap, the
        // two parameters alias to a single variable and the guard degenerates to the
        // unsatisfiable @v3 - @v3 >= 3, sinking the coercion. The guard must survive as a
        // relation between the two distinct parameters.
        TypeSystem typeSystem = TrinoPreset.typeSystem();
        Solver solver = new Solver(typeSystem);

        Solver.Result result = solver.solve(List.of(
                new Subtype(symbol("tinyint"), apply("decimal", variable("@v3"), variable("@v4")))));

        assertThat(result.nextBatch())
                .contains(new NumericRelation(operation(
                        GREATER_THAN_OR_EQUAL,
                        operation(SUBTRACT, variable("@v3"), variable("@v4")),
                        literal(3))));
    }

    @Test
    void testGuardedDecimalCoercionResolvesMixedDecimalIntegerArithmetic()
    {
        // End-to-end symptom of the variable collision: resolving `decimal(10,2) + tinyint`
        // instantiates the additive overload with scheme variables @v1..@v4 and coerces
        // tinyint into its second parameter decimal(@v3, @v4). When the coercion's fresh
        // guard variables collided with @v3/@v4 the decimal overload was dropped, leaving the
        // call spuriously ambiguous between the real/double/number overloads. Trino resolves
        // it to decimal(11, 2).
        TypeLibrary library = TrinoPreset.library();

        for (String integerType : List.of("tinyint", "smallint")) {
            assertThat(library.resolveFunction("+", List.of(apply("decimal", literal(10), literal(2)), symbol(integerType))))
                    .isInstanceOfSatisfying(FunctionResolver.Resolved.class, resolved ->
                            assertThat(resolved.resolution().returnType()).isEqualTo(apply("decimal", literal(11), literal(2))));
        }
    }
}
