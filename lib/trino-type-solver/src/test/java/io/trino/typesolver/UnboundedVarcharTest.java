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

import static io.trino.typesolver.Expression.apply;
import static io.trino.typesolver.Expression.function;
import static io.trino.typesolver.Expression.literal;
import static io.trino.typesolver.Expression.symbol;
import static io.trino.typesolver.Expression.variable;
import static org.assertj.core.api.Assertions.assertThat;

public class UnboundedVarcharTest
{
    private final TypeSystem typeSystem = TrinoPreset.typeSystem();

    @Test
    void testCalculatedLengthWithUnboundedInput()
    {
        Expression length = Expression.operation(
                Expression.BinaryOperator.MIN,
                literal(Integer.MAX_VALUE),
                Expression.operation(Expression.BinaryOperator.ADD, variable("x"), variable("y")));
        CompiledSignature signature = new CompiledSignature(
                List.of(variable("x"), variable("y")),
                List.of(),
                function(List.of(apply("varchar", variable("x")), apply("varchar", variable("y"))), apply("varchar", length)));

        assertThat(signature.matchFunctionCallOutcome(List.of(symbol("varchar"), apply("varchar", literal(2))), typeSystem))
                .isInstanceOfSatisfying(CompiledSignature.Satisfied.class, outcome ->
                        assertThat(TypeCompiler.toTypeDescriptor(outcome.result().returnType()).toString()).isEqualTo("varchar"));
    }

    @Test
    void testUnboundedVarcharBindsLengthVariable()
    {
        CompiledSignature signature = new CompiledSignature(
                List.of(variable("n")),
                List.of(),
                function(List.of(apply("varchar", variable("n"))), apply("varchar", variable("n"))));

        assertThat(signature.matchFunctionCallOutcome(List.of(symbol("varchar")), typeSystem))
                .isInstanceOfSatisfying(CompiledSignature.Satisfied.class, outcome -> {
                    assertThat(outcome.result().numericBindings()).containsEntry("n", Integer.MAX_VALUE);
                    assertThat(TypeCompiler.toTypeDescriptor(outcome.result().returnType()).toString()).isEqualTo("varchar");
                });
    }

    @Test
    void testBoundedVarcharCoercesToUnbounded()
    {
        assertThat(typeSystem.coercionPlan(apply("varchar", literal(5)), symbol("varchar"))).isPresent();
    }

    @Test
    void testUnboundedVarcharDoesNotCoerceToBounded()
    {
        assertThat(typeSystem.coercionPlan(symbol("varchar"), apply("varchar", literal(10)))).isEmpty();
    }

    @Test
    void testUnboundedVarcharSelfCoerces()
    {
        assertThat(typeSystem.coercionPlan(symbol("varchar"), symbol("varchar"))).isPresent();
    }

    @Test
    void testFunctionExpectingUnboundedVarcharAcceptsBounded()
    {
        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerFunction("accepts_unbounded", function(List.of(symbol("varchar")), symbol("boolean")))
                .build();

        assertThat(library.resolveFunction("accepts_unbounded", List.of(apply("varchar", literal(5)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("boolean")));
    }

    @Test
    void testFunctionExpectingBoundedVarcharRejectsUnbounded()
    {
        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerFunction("accepts_bounded", function(List.of(apply("varchar", literal(10))), symbol("boolean")))
                .build();

        assertThat(library.resolveFunction("accepts_bounded", List.of(symbol("varchar"))))
                .isInstanceOf(FunctionResolver.NoMatch.class);
    }
}
