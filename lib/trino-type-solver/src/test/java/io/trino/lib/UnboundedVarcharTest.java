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
package io.trino.lib;

import org.junit.jupiter.api.Test;
import org.weakref.solver.FunctionResolver;
import org.weakref.solver.TypeLibrary;
import org.weakref.solver.TypeSystem;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.function;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.symbol;

public class UnboundedVarcharTest
{
    private final TypeSystem typeSystem = TrinoPreset.typeSystem();

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
        TypeLibrary library = TrinoPreset.install(TypeLibrary.builder())
                .registerFunction("accepts_unbounded", function(List.of(symbol("varchar")), symbol("boolean")))
                .build();

        assertThat(library.resolveFunction("accepts_unbounded", List.of(apply("varchar", literal(5)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("boolean")));
    }

    @Test
    void testFunctionExpectingBoundedVarcharRejectsUnbounded()
    {
        TypeLibrary library = TrinoPreset.install(TypeLibrary.builder())
                .registerFunction("accepts_bounded", function(List.of(apply("varchar", literal(10))), symbol("boolean")))
                .build();

        assertThat(library.resolveFunction("accepts_bounded", List.of(symbol("varchar"))))
                .isInstanceOf(FunctionResolver.NoMatch.class);
    }
}
