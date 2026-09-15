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
import java.util.Set;

import static io.trino.typesolver.Expression.BinaryOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.typesolver.Expression.BinaryOperator.LESS_THAN_OR_EQUAL;
import static io.trino.typesolver.Expression.BinaryOperator.SUBTRACT;
import static io.trino.typesolver.Expression.apply;
import static io.trino.typesolver.Expression.literal;
import static io.trino.typesolver.Expression.operation;
import static io.trino.typesolver.Expression.symbol;
import static io.trino.typesolver.Expression.variable;
import static org.assertj.core.api.Assertions.assertThat;

public class PatternCoercionTest
{
    @Test
    void testConstraintMatchingPreservesObligationsAndFreshNames()
    {
        List<Expression> inputs = List.of(
                symbol("integer"),
                symbol("varchar"),
                apply("varchar", literal(10)),
                apply("varchar", variable("n1")),
                apply("decimal", literal(10), literal(2)),
                apply("decimal", variable("p"), variable("p")),
                apply("array", variable("element")),
                variable("$v1"));
        for (CoercionRule candidate : TrinoPreset.coercionRules()) {
            if (!(candidate instanceof PatternCoercion rule)) {
                continue;
            }
            for (Expression from : inputs) {
                for (Expression to : inputs) {
                    VariableAllocator fullAllocator = new VariableAllocator();
                    VariableAllocator constraintsAllocator = new VariableAllocator();
                    fullAllocator.reserveThrough(100);
                    constraintsAllocator.reserveThrough(100);
                    assertThat(rule.matchConstraints(constraintsAllocator, from, to))
                            .as("%s: %s -> %s", rule, from, to)
                            .isEqualTo(rule.matches(fullAllocator, from, to).map(CoercionRule.Match::constraints));
                    assertThat(constraintsAllocator.newVariable()).isEqualTo(fullAllocator.newVariable());
                }
            }
        }
    }

    @Test
    void testOpenScalarPatternPreservesGuardAndExactPlan()
    {
        PatternCoercion coercion = new PatternCoercion(
                apply("varchar", variable("x")),
                apply("varchar", variable("y")),
                List.of(new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("x"), variable("y")))));
        Expression source = apply("varchar", literal(10));
        Expression target = apply("varchar", variable("$v1"));
        NumericRelation guard = new NumericRelation(operation(LESS_THAN_OR_EQUAL, literal(10), variable("$v1")));
        assertThat(coercion.matches(new VariableAllocator(), source, target)).hasValueSatisfying(match -> {
            assertThat(match.constraints()).containsExactly(guard);
            assertThat(match.plan()).contains(CoercionPlan.directSteps(source, target, List.of(new CoercionPlan.DirectRule(coercion.ruleId(), List.of(guard)))));
        });
        assertThat(coercion.matches(new VariableAllocator(), target, target)).hasValueSatisfying(match ->
                assertThat(match.plan()).contains(CoercionPlan.exact(target, target)));
    }

    @Test
    void testOpenScalarPatternRetainsAliasesAndAdditionalGuardVariables()
    {
        PatternCoercion shared = new PatternCoercion(apply("varchar", variable("n")), apply("varchar", variable("n")), List.of());
        assertThat(shared.matches(new VariableAllocator(), apply("varchar", variable("a")), apply("varchar", variable("b"))))
                .hasValueSatisfying(match -> assertThat(match.constraints()).anyMatch(ExactType.class::isInstance));

        PatternCoercion guarded = new PatternCoercion(
                apply("varchar", variable("x")),
                apply("varchar", variable("y")),
                List.of(new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("x"), variable("extra")))));
        assertThat(guarded.matches(new VariableAllocator(), apply("varchar", variable("input")), apply("varchar", literal(10))))
                .hasValueSatisfying(match -> assertThat(match.constraints()).contains(new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("input"), variable("extra")))));
    }

    @Test
    void testPatternCoercionPreservesRepeatedVariableRelationsInPlan()
    {
        VariableAllocator allocator = new VariableAllocator();

        PatternCoercion coercion = new PatternCoercion(
                apply("decimal", variable("p"), variable("s1")),
                apply("decimal", variable("p"), variable("s2")),
                List.of(
                        new NumericRelation(
                                operation(
                                        GREATER_THAN_OR_EQUAL,
                                        operation(SUBTRACT, variable("p"), variable("s2")),
                                        operation(SUBTRACT, variable("p"), variable("s1"))))));

        assertThat(coercion.matches(
                allocator,
                apply("decimal", variable("ep"), variable("es1")),
                apply("decimal", variable("ep"), variable("es2"))))
                .isPresent()
                .hasValueSatisfying(match -> {
                    NumericRelation relation = new NumericRelation(
                            operation(
                                    GREATER_THAN_OR_EQUAL,
                                    operation(SUBTRACT, variable("ep"), variable("es2")),
                                    operation(SUBTRACT, variable("ep"), variable("es1"))));
                    assertThat(match.constraints()).isEqualTo(Set.of(relation));
                    assertThat(match.plan())
                            .contains(CoercionPlan.directSteps(
                                    apply("decimal", variable("ep"), variable("es1")),
                                    apply("decimal", variable("ep"), variable("es2")),
                                    List.of(new CoercionPlan.DirectRule(
                                            "pattern:decimal(p, s1)->decimal(p, s2)",
                                            List.of(relation)))));
                });
    }

    @Test
    void testVarcharPatternInstantiatesNumericGuard()
    {
        VariableAllocator allocator = new VariableAllocator();

        PatternCoercion coercion = new PatternCoercion(
                apply("varchar", variable("x")),
                apply("varchar", variable("y")),
                List.of(new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("x"), variable("y")))));

        assertThat(coercion.matches(
                allocator,
                apply("varchar", literal(10)),
                apply("varchar", literal(20))))
                .isPresent()
                .hasValueSatisfying(match -> {
                    assertThat(match.constraints()).isEqualTo(
                            Set.of(new NumericRelation(operation(LESS_THAN_OR_EQUAL, literal(10), literal(20)))));
                    assertThat(match.plan())
                            .contains(CoercionPlan.directSteps(
                                    apply("varchar", literal(10)),
                                    apply("varchar", literal(20)),
                                    List.of(new CoercionPlan.DirectRule(
                                            "pattern:varchar(x)->varchar(y)",
                                            List.of(new NumericRelation(operation(LESS_THAN_OR_EQUAL, literal(10), literal(20))))))));
                });
    }

    @Test
    void testPatternCoercionTranslatesSharedBindingsAcrossBothSides()
    {
        VariableAllocator allocator = new VariableAllocator();

        // map(pk, pv) -> map(pk, pu)  <=> pv <: pu
        PatternCoercion coercion = new PatternCoercion(
                apply("map", variable("pk"), variable("pv")),
                apply("map", variable("pk"), variable("pu")),
                List.of(new Subtype(variable("pv"), variable("pu"))));

        // map(integer,smallint) -> map(ek,bigint)
        assertThat(coercion.matches(
                allocator,
                apply("map", symbol("integer"), symbol("smallint")),
                apply("map", variable("ek"), symbol("bigint"))))
                .isPresent()
                .hasValueSatisfying(match -> {
                    assertThat(match.constraints()).isEqualTo(Set.of(
                            new Subtype(symbol("smallint"), symbol("bigint")),
                            new ExactType("ek", symbol("integer"))));
                    assertThat(match.plan())
                            .contains(CoercionPlan.directSteps(
                                    apply("map", symbol("integer"), symbol("smallint")),
                                    apply("map", variable("ek"), symbol("bigint")),
                                    List.of(new CoercionPlan.DirectRule(
                                            "pattern:map(pk, pv)->map(pk, pu)",
                                            List.of(new Subtype(symbol("smallint"), symbol("bigint")))))));
                });

        // map(integer,varchar(10)) -> map(bigint,varchar(20))
        assertThat(coercion.matches(
                allocator,
                apply("map", symbol("integer"), apply("varchar", List.of(literal(10)))),
                apply("map", symbol("bigint"), apply("varchar", List.of(literal(20))))))
                .isEmpty();

        // map(integer,varchar(10)) -> map(integer,varchar(20))
        assertThat(coercion.matches(
                allocator,
                apply("map", symbol("integer"), apply("varchar", List.of(literal(10)))),
                apply("map", symbol("integer"), apply("varchar", List.of(literal(20))))))
                .isPresent()
                .hasValueSatisfying(match -> {
                    assertThat(match.constraints()).isEqualTo(Set.of(
                            new Subtype(
                                    apply("varchar", literal(10)),
                                    apply("varchar", literal(20)))));
                    assertThat(match.plan())
                            .contains(CoercionPlan.directSteps(
                                    apply("map", symbol("integer"), apply("varchar", literal(10))),
                                    apply("map", symbol("integer"), apply("varchar", literal(20))),
                                    List.of(new CoercionPlan.DirectRule(
                                            "pattern:map(pk, pv)->map(pk, pu)",
                                            List.of(new Subtype(
                                                    apply("varchar", literal(10)),
                                                    apply("varchar", literal(20))))))));
                });

        // map(integer,varchar(10)) -> map(integer,varchar(n))
        assertThat(coercion.matches(
                allocator,
                apply("map", symbol("integer"), apply("varchar", List.of(literal(10)))),
                apply("map", symbol("integer"), apply("varchar", List.of(variable("n"))))))
                .isPresent()
                .hasValueSatisfying(match -> {
                    assertThat(match.constraints()).isEqualTo(Set.of(
                            new Subtype(
                                    apply("varchar", literal(10)),
                                    apply("varchar", variable("n")))));
                    assertThat(match.plan())
                            .contains(CoercionPlan.directSteps(
                                    apply("map", symbol("integer"), apply("varchar", literal(10))),
                                    apply("map", symbol("integer"), apply("varchar", variable("n"))),
                                    List.of(new CoercionPlan.DirectRule(
                                            "pattern:map(pk, pv)->map(pk, pu)",
                                            List.of(new Subtype(
                                                    apply("varchar", literal(10)),
                                                    apply("varchar", variable("n"))))))));
                });
    }

    @Test
    void testIntegerToDecimalPatternCarriesInstantiatedGuard()
    {
        VariableAllocator allocator = new VariableAllocator();

        //  integer -> decimal(p, s)  <=> p - s >= 10
        PatternCoercion coercion = new PatternCoercion(
                symbol("integer"),
                apply("decimal", variable("p"), variable("s")),
                List.of(new NumericRelation(operation(
                        GREATER_THAN_OR_EQUAL,
                        operation(SUBTRACT, variable("p"), variable("s")),
                        literal(10)))));

        assertThat(coercion.matches(
                allocator,
                symbol("integer"),
                apply("decimal", variable("p"), variable("s"))))
                .isPresent()
                .hasValueSatisfying(match -> {
                    assertThat(match.constraints()).isEqualTo(Set.of(
                            new NumericRelation(operation(
                                    GREATER_THAN_OR_EQUAL,
                                    operation(SUBTRACT, variable("p"), variable("s")),
                                    literal(10)))));
                    assertThat(match.plan())
                            .contains(CoercionPlan.directSteps(
                                    symbol("integer"),
                                    apply("decimal", variable("p"), variable("s")),
                                    List.of(new CoercionPlan.DirectRule(
                                            "pattern:integer->decimal(p, s)",
                                            List.of(new NumericRelation(operation(
                                                    GREATER_THAN_OR_EQUAL,
                                                    operation(SUBTRACT, variable("p"), variable("s")),
                                                    literal(10))))))));
                });
    }
}
