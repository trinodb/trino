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
import java.util.stream.IntStream;

import static io.trino.typesolver.Expression.apply;
import static io.trino.typesolver.Expression.field;
import static io.trino.typesolver.Expression.row;
import static io.trino.typesolver.Expression.symbol;
import static io.trino.typesolver.Expression.variable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class GroundBoundCheckTest
{
    @Test
    void testImpossibleWideRowStopsBeforeDomainExpansion()
    {
        Expression left = new Expression.Row(IntStream.range(0, 1000).mapToObj(index -> field("f" + index, symbol("integer"))).toList());
        Expression right = new Expression.Row(IntStream.range(0, 1000).mapToObj(index -> field("f" + index, symbol(index == 999 ? "boolean" : "bigint"))).toList());
        ResolutionBudget budget = new ResolutionBudget(30_000);
        assertThat(budget.run(() -> new Solver(TrinoPreset.typeSystem()).solveOutcome(bounds(left, right))))
                .isInstanceOf(Solver.Unsatisfied.class);
    }

    @Test
    void testImpossibleNestedFields()
    {
        for (Expression left : List.of(symbol("integer"), apply("array", symbol("integer")), row(field("nested", symbol("integer"))))) {
            Expression right = switch (left) {
                case Expression.Application _ -> apply("array", symbol("boolean"));
                case Expression.Row _ -> row(field("nested", symbol("boolean")));
                default -> symbol("boolean");
            };
            assertThat(new Solver(TrinoPreset.typeSystem()).solveOutcome(bounds(row(field("a", left)), row(field("a", right)))))
                    .isInstanceOf(Solver.Unsatisfied.class);
        }
    }

    @Test
    void testAdditionalRowConversionRemainsAvailable()
    {
        Expression left = row(field("a", symbol("integer")));
        Expression right = row(field("a", symbol("boolean")));
        List<CoercionRule> rules = new ArrayList<>(TrinoPreset.coercionRules());
        rules.add(new PatternCoercion(left, symbol("common"), List.of()));
        rules.add(new PatternCoercion(right, symbol("common"), List.of()));
        TypeSystem types = new TypeSystem(TrinoPreset.typeConstructors(), rules);
        assertThat(types.getCommonSupertype(left, right)).contains(symbol("common"));
        assertThat(types.getCommonSupertype(right, left)).contains(symbol("common"));
    }

    @Test
    void testWildcardSourceRemainsAvailable()
    {
        List<CoercionRule> rules = new ArrayList<>(TrinoPreset.coercionRules());
        rules.add(new PatternCoercion(variable("source"), symbol("common"), List.of()));
        TypeSystem types = new TypeSystem(TrinoPreset.typeConstructors(), rules);
        assertThat(types.getCommonSupertype(row(field("a", symbol("integer"))), row(field("a", symbol("boolean")))))
                .isPresent();
    }

    @Test
    void testUnknownTargetsAndCustomRulesDisableRejection()
    {
        CoercionRule custom = new SelfCoercion()
        {
            @Override
            public Optional<Match> matches(VariableAllocator allocator, Expression from, Expression to)
            {
                return Optional.of(new Match(Set.of(), CoercionPlan.exact(from, to)));
            }
        };
        for (CoercionRule extra : List.of(
                custom,
                new PatternCoercion(symbol("integer"), variable("anyTarget"), List.of()),
                new PatternCoercion(variable("anySource"), variable("anyTarget"), List.of()))) {
            List<CoercionRule> rules = new ArrayList<>(TrinoPreset.coercionRules());
            rules.add(extra);
            TypeSystem types = new TypeSystem(TrinoPreset.typeConstructors(), rules);
            assertThatCode(() -> GroundBoundCheck.checkRows(types, bounds(row(field("a", symbol("integer"))), row(field("a", symbol("boolean"))))))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void testOpenBoundsUseFullSolver()
    {
        assertThat(new Solver(TrinoPreset.typeSystem()).solve(bounds(row(field("a", variable("U"))), row(field("a", symbol("integer"))))).materializedTypeVariables())
                .containsEntry("T", row(field("a", symbol("integer"))));
    }

    private static List<Constraint> bounds(Expression left, Expression right)
    {
        return List.of(new Subtype(left, variable("T")), new Subtype(right, variable("T")));
    }
}
