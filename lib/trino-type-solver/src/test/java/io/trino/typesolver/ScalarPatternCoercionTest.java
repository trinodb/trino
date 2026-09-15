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
import java.util.stream.IntStream;

import static io.trino.typesolver.Expression.BinaryOperator.EQUAL;
import static io.trino.typesolver.Expression.BinaryOperator.LESS_THAN_OR_EQUAL;
import static io.trino.typesolver.Expression.apply;
import static io.trino.typesolver.Expression.literal;
import static io.trino.typesolver.Expression.operation;
import static io.trino.typesolver.Expression.symbol;
import static io.trino.typesolver.Expression.variable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScalarPatternCoercionTest
{
    @Test
    void testPreparedMatchesPreservePlansAndFreshNames()
    {
        List<Expression> inputs = List.of(
                symbol("integer"),
                symbol("varchar"),
                apply("varchar", literal(2)),
                apply("varchar", literal(10)),
                apply("varchar", variable("input")),
                apply("varchar", variable("n1")),
                apply("varchar", variable("$v1")),
                apply("decimal", literal(10), literal(2)),
                apply("decimal", variable("p"), variable("p")),
                apply("decimal", variable("p"), variable("s")),
                apply("array", variable("element")),
                variable("$v1"),
                variable("$v100"));
        for (TrinoPreset.CharVarcharCoercion policy : TrinoPreset.CharVarcharCoercion.values()) {
            List<CoercionRule> rules = new ArrayList<>(TrinoPreset.coercionRules(policy));
            rules.add(new PatternCoercion(apply("varchar", variable("n")), apply("varchar", variable("n")), List.of()));
            rules.add(new PatternCoercion(apply("varchar", literal(2)), apply("varchar", variable("n")), List.of()));
            rules.add(new PatternCoercion(apply("decimal", variable("p"), variable("p")), apply("varchar", variable("p")), List.of()));
            for (CoercionRule rule : rules) {
                CoercionRule prepared = ScalarPatternCoercion.prepare(rule);
                for (Expression from : inputs) {
                    for (Expression to : inputs) {
                        assertMatchesOriginal(rule, prepared, from, to);
                    }
                }
            }
        }
    }

    @Test
    void testUnsupportedGuardsAndAllocatorNamesRetainOriginalMatching()
    {
        for (PatternCoercion rule : List.of(
                new PatternCoercion(
                        apply("varchar", variable("n")),
                        apply("varchar", variable("m")),
                        List.of(new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("n"), variable("extra"))))),
                new PatternCoercion(apply("array", variable("T")), apply("array", variable("U")), List.of(new Subtype(variable("T"), variable("U")))),
                new PatternCoercion(
                        apply("varchar", variable("$v1")),
                        apply("varchar", variable("$v2")),
                        List.of(new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("$v1"), variable("$v2"))))))) {
            assertMatchesOriginal(rule, ScalarPatternCoercion.prepare(rule), apply("varchar", literal(2)), variable("$v1"));
            assertMatchesOriginal(rule, ScalarPatternCoercion.prepare(rule), apply("array", symbol("integer")), apply("array", variable("T")));
        }
    }

    @Test
    void testSharedPreparedRuleKeepsBindingsIndependent()
    {
        PatternCoercion rule = new PatternCoercion(
                apply("varchar", variable("n")),
                apply("varchar", variable("m")),
                List.of(new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("n"), variable("m")))));
        TypeSystem types = new TypeSystem(TrinoPreset.typeConstructors(), List.of(rule));
        IntStream.range(0, 1000).parallel().forEach(value -> {
            Expression from = apply("varchar", literal(value));
            Expression to = variable("$v100");
            assertMatchesOriginal(rule, types.candidateCoercions(from, to).getFirst(), from, to);
        });
    }

    @Test
    void testChangedConstraintListUsesCurrentRule()
    {
        List<Constraint> constraints = new ArrayList<>();
        PatternCoercion rule = new PatternCoercion(apply("varchar", variable("n")), apply("varchar", variable("m")), constraints);
        CoercionRule prepared = ScalarPatternCoercion.prepare(rule);
        constraints.add(new NumericRelation(operation(EQUAL, variable("n"), literal(2))));
        assertMatchesOriginal(rule, prepared, apply("varchar", literal(2)), variable("$v1"));
        assertMatchesOriginal(rule, prepared, apply("varchar", literal(3)), apply("varchar", variable("input")));
    }

    private static void assertMatchesOriginal(CoercionRule original, CoercionRule prepared, Expression from, Expression to)
    {
        VariableAllocator originalAllocator = new VariableAllocator();
        VariableAllocator preparedAllocator = new VariableAllocator();
        originalAllocator.reserveThrough(5);
        preparedAllocator.reserveThrough(5);
        Optional<CoercionRule.Match> expected;
        try {
            expected = original.matches(originalAllocator, from, to);
        }
        catch (IllegalArgumentException exception) {
            // Some matrix pairs are self-referential. Preparation must retain the
            // original matcher's failure as well as its successful results.
            assertThatThrownBy(() -> prepared.matches(preparedAllocator, from, to))
                    .isExactlyInstanceOf(exception.getClass())
                    .hasMessage(exception.getMessage());
            assertThat(preparedAllocator.newVariable()).isEqualTo(originalAllocator.newVariable());
            return;
        }
        assertThat(prepared.matches(preparedAllocator, from, to))
                .as("%s: %s <: %s", original, from, to)
                .isEqualTo(expected);
        String nextVariable = originalAllocator.newVariable();
        assertThat(preparedAllocator.newVariable()).isEqualTo(nextVariable);
        if (prepared instanceof ScalarPatternCoercion scalar) {
            VariableAllocator constraintsAllocator = new VariableAllocator();
            constraintsAllocator.reserveThrough(5);
            assertThat(scalar.matchConstraints(constraintsAllocator, from, to)).isEqualTo(expected.map(CoercionRule.Match::constraints));
            assertThat(constraintsAllocator.newVariable()).isEqualTo(nextVariable);
        }
    }
}
