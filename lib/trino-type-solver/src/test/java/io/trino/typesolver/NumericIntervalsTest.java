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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.typesolver.Expression.BinaryOperator.ADD;
import static io.trino.typesolver.Expression.BinaryOperator.EQUAL;
import static io.trino.typesolver.Expression.BinaryOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.typesolver.Expression.BinaryOperator.LESS_THAN;
import static io.trino.typesolver.Expression.BinaryOperator.LESS_THAN_OR_EQUAL;
import static io.trino.typesolver.Expression.apply;
import static io.trino.typesolver.Expression.literal;
import static io.trino.typesolver.Expression.operation;
import static io.trino.typesolver.Expression.symbol;
import static io.trino.typesolver.Expression.variable;
import static org.assertj.core.api.Assertions.assertThat;

class NumericIntervalsTest
{
    @Test
    void testIndependentIntervalsAndInclusiveEndpoints()
    {
        assertThat(NumericIntervals.satisfiable(List.of(
                new NumericRelation(operation(LESS_THAN_OR_EQUAL, literal(10), variable("n"))),
                new NumericRelation(operation(GREATER_THAN_OR_EQUAL, literal(12), variable("n"))),
                new NumericRelation(operation(EQUAL, variable("m"), literal(Integer.MIN_VALUE))),
                new NumericRelation(operation(EQUAL, variable("k"), literal(Integer.MAX_VALUE))))))
                .contains(true);
        assertThat(NumericIntervals.satisfiable(List.of(
                new NumericRelation(operation(EQUAL, variable("n"), literal(Integer.MAX_VALUE))),
                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("n"), literal(Integer.MAX_VALUE - 1))))))
                .contains(false);
    }

    @Test
    void testCoupledAndUnsupportedConstraintsDefer()
    {
        for (Constraint constraint : List.of(
                new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("n"), variable("m"))),
                new NumericRelation(operation(LESS_THAN, variable("n"), literal(Integer.MIN_VALUE))),
                new NumericRelation(operation(EQUAL, operation(ADD, variable("n"), literal(1)), literal(2))),
                new ExactType("n", variable("m")),
                new RequireKind("n", Kind.TYPE))) {
            assertThat(NumericIntervals.satisfiable(List.of(constraint))).isEmpty();
        }
    }

    @Test
    void testNumericSubtypeQueriesMatchFullSolver()
    {
        TypeSystem types = TrinoPreset.typeSystem();
        for (String name : List.of("varchar", "char", "time", "timestamp")) {
            for (int value : List.of(-1, 0, 1, 10, Integer.MAX_VALUE)) {
                Expression ground = apply(name, literal(value));
                // These names overlap the rule allocator's initial fresh names.
                for (String variable : List.of("n", "$v1", "$v2")) {
                    Expression open = apply(name, variable(variable));
                    assertMatchesFullSolver(types, ground, open);
                    assertMatchesFullSolver(types, open, ground);
                }
            }
        }
        assertMatchesFullSolver(types, apply("varchar", variable("n")), apply("varchar", variable("m")));
        assertMatchesFullSolver(types, apply("decimal", literal(10), literal(2)), apply("decimal", variable("p"), variable("s")));
        assertMatchesFullSolver(types, apply("decimal", literal(10), literal(8)), apply("decimal", variable("p"), literal(2)));
    }

    @Test
    void testCachedQueriesPreserveVariableCorrelations()
    {
        TypeSystem types = TrinoPreset.typeSystem();
        SubtypeOracle shared = new SubtypeOracle(types);
        List<Expression> expressions = List.of(
                symbol("integer"),
                symbol("varchar"),
                apply("varchar", literal(-1)),
                apply("varchar", literal(10)),
                apply("varchar", variable("n")),
                apply("varchar", variable("$canonical0")),
                apply("decimal", literal(10), literal(5)),
                apply("decimal", variable("n"), variable("n")),
                apply("decimal", variable("n"), variable("m")),
                apply("decimal", variable("$canonical1"), variable("$canonical0")),
                apply("decimal", variable("n"), literal(0)),
                apply("decimal", literal(10), variable("n")),
                apply("array", variable("T")),
                apply("array", symbol("integer")));
        for (Expression left : expressions) {
            for (Expression right : expressions) {
                assertThat(shared.classify(left, right))
                        .as("%s <: %s", left, right)
                        .isEqualTo(new SubtypeOracle(types).classify(left, right));
            }
        }
    }

    @Test
    void testConstructorValidationStillRefutesOpenQueries()
    {
        SubtypeOracle oracle = new SubtypeOracle(TrinoPreset.typeSystem());
        assertThat(oracle.classify(apply("varchar", literal(-1)), apply("varchar", variable("n"))))
                .isEqualTo(SubtypeOracle.Relation.UNSATISFIED);
        assertThat(oracle.classify(apply("varchar", variable("n")), apply("varchar", literal(-1))))
                .isEqualTo(SubtypeOracle.Relation.UNSATISFIED);
    }

    @Test
    void testAdditionalRulesAndConditionalRepresentationBridges()
    {
        Expression source = apply("varchar", literal(10));
        Expression target = apply("varchar", variable("n"));
        List<CoercionRule> rules = new ArrayList<>(TrinoPreset.coercionRules());
        rules.add(new PatternCoercion(source, apply("varchar", variable("length")), List.of(new NumericRelation(operation(EQUAL, variable("length"), literal(2))))));
        assertMatchesFullSolver(new TypeSystem(TrinoPreset.typeConstructors(), rules), source, target);

        CoercionRule bridge = new CoercionRule()
        {
            @Override
            public Optional<Match> matches(VariableAllocator allocator, Expression from, Expression to)
            {
                if (from.equals(symbol("varchar")) && to.equals(target)) {
                    return Optional.of(new Match(Set.of(new NumericRelation(operation(EQUAL, variable("n"), literal(Integer.MAX_VALUE))))));
                }
                return Optional.empty();
            }

            @Override
            public boolean isRepresentationBridge()
            {
                return true;
            }
        };
        TypeSystem bridged = new TypeSystem(TrinoPreset.typeConstructors(), List.of(bridge));
        assertThat(new SubtypeOracle(bridged).classify(symbol("varchar"), target)).isEqualTo(SubtypeOracle.Relation.INCOMPLETE);
    }

    @Test
    void testTypeParametersAndUnknownConstructorsUseFullSolver()
    {
        TypeSystem types = TrinoPreset.typeSystem();
        assertMatchesFullSolver(types, apply("array", variable("T")), apply("array", symbol("integer")));
        assertMatchesFullSolver(types, apply("unregistered", variable("n")), apply("unregistered", literal(3)));
    }

    private static void assertMatchesFullSolver(TypeSystem types, Expression left, Expression right)
    {
        SubtypeOracle.Relation expected = switch (new Solver(types).solveOutcome(List.of(new Subtype(left, right)))) {
            case Solver.Satisfied _ -> SubtypeOracle.Relation.SATISFIED;
            case Solver.Unsatisfied _ -> SubtypeOracle.Relation.UNSATISFIED;
            case Solver.Incomplete _ -> SubtypeOracle.Relation.INCOMPLETE;
        };
        assertThat(new SubtypeOracle(types).classify(left, right)).as("%s <: %s", left, right).isEqualTo(expected);
    }
}
