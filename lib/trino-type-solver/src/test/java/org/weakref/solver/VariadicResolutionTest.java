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
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.symbol;
import static org.weakref.solver.Expression.variable;
import static org.weakref.solver.Expression.variadicFunction;

public class VariadicResolutionTest
{
    private static final TypeSystem TYPE_SYSTEM = TrinoPreset.typeSystem();

    @Test
    void testVariadicAcceptsZeroVariadicArguments()
    {
        TypeScheme signature = new TypeScheme(
                List.of(),
                List.of(),
                variadicFunction(List.of(symbol("integer")), symbol("varchar"), symbol("boolean")));

        assertThat(new FunctionResolver(TYPE_SYSTEM).resolveOutcome(List.of(signature), List.of(symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome -> {
                    FunctionResolver.Resolution resolution = outcome.resolution();
                    assertThat(resolution.returnType()).isEqualTo(symbol("boolean"));
                    assertThat(resolution.argumentCoercions()).hasSize(1);
                });
    }

    @Test
    void testVariadicAcceptsManyVariadicArguments()
    {
        TypeScheme signature = new TypeScheme(
                List.of(),
                List.of(),
                variadicFunction(List.of(), symbol("integer"), symbol("integer")));

        assertThat(new FunctionResolver(TYPE_SYSTEM).resolveOutcome(
                List.of(signature),
                List.of(symbol("tinyint"), symbol("smallint"), symbol("integer"), symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome -> {
                    FunctionResolver.Resolution resolution = outcome.resolution();
                    assertThat(resolution.returnType()).isEqualTo(symbol("integer"));
                    assertThat(resolution.argumentCoercions()).hasSize(4);
                });
    }

    @Test
    void testPurelyVariadicAcceptsZeroArguments()
    {
        TypeScheme signature = new TypeScheme(
                List.of(),
                List.of(),
                variadicFunction(List.of(), symbol("integer"), symbol("bigint")));

        assertThat(new FunctionResolver(TYPE_SYSTEM).resolveOutcome(List.of(signature), List.of()))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome -> {
                    assertThat(outcome.resolution().returnType()).isEqualTo(symbol("bigint"));
                    assertThat(outcome.resolution().argumentCoercions()).isEmpty();
                });
    }

    @Test
    void testPolymorphicVariadicBindsCommonSupertype()
    {
        TypeScheme signature = new TypeScheme(
                List.of(variable("@T")),
                List.of(),
                variadicFunction(List.of(), variable("@T"), apply("array", variable("@T"))));

        assertThat(new FunctionResolver(TYPE_SYSTEM).resolveOutcome(
                List.of(signature),
                List.of(symbol("integer"), symbol("bigint"), symbol("tinyint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome -> {
                    FunctionResolver.Resolution resolution = outcome.resolution();
                    assertThat(resolution.returnType()).isEqualTo(apply("array", symbol("bigint")));
                    assertThat(resolution.typeBindings()).containsEntry("@T", symbol("bigint"));
                });
    }

    @Test
    void testVariadicArityMismatchIsRejected()
    {
        TypeScheme signature = new TypeScheme(
                List.of(),
                List.of(),
                variadicFunction(List.of(symbol("integer"), symbol("integer")), symbol("varchar"), symbol("boolean")));

        assertThat(new FunctionResolver(TYPE_SYSTEM).resolveOutcome(List.of(signature), List.of(symbol("integer"))))
                .isInstanceOf(FunctionResolver.NoMatch.class);
    }
}
