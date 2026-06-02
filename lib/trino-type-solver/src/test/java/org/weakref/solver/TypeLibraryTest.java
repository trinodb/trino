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
import org.weakref.solver.type.Type;
import org.weakref.solver.type.TypeConstructor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.solver.Expression.BinaryOperator.LESS_THAN_OR_EQUAL;
import static org.weakref.solver.Expression.function;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.operation;
import static org.weakref.solver.Expression.symbol;
import static org.weakref.solver.Expression.variable;

public class TypeLibraryTest
{
    @Test
    void testFrameworkDefaultReturnsAmbiguousWhenTwoWideningsTie()
    {
        // Two coercions of equal count; BY_COERCION_COUNT has no basis to pick. The test builds
        // on TrinoPreset for the types + coercions but explicitly resets the specificity to
        // the framework default to show the baseline behavior.
        TypeLibrary library = TrinoPreset.install(TypeLibrary.builder())
                .registerFunction("f", function(List.of(symbol("integer")), symbol("integer")))
                .registerFunction("f", function(List.of(symbol("bigint")), symbol("bigint")))
                .withSpecificity(org.weakref.solver.Specificity.BY_COERCION_COUNT)
                .build();

        assertThat(library.resolveFunction("f", List.of(symbol("smallint"))))
                .isInstanceOfSatisfying(FunctionResolver.Ambiguous.class, outcome ->
                        assertThat(outcome.candidates()).hasSize(2));
    }

    @Test
    void testLibraryResolvesFunctionWithUnknownArgument()
    {
        TypeLibrary library = TrinoPreset.install(TypeLibrary.builder())
                .registerFunction("f", function(List.of(symbol("integer")), symbol("integer")))
                .build();

        assertThat(library.resolveFunction("f", List.of(symbol("unknown"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome -> {
                    FunctionResolver.Resolution resolution = outcome.resolution();
                    assertThat(resolution.returnType()).isEqualTo(symbol("integer"));
                    assertThat(resolution.argumentCoercions()).hasSize(1);
                });
    }

    @Test
    void testLibrarySupportsCustomCoercionRegistration()
    {
        TypeLibrary library = TypeLibrary.builder()
                .registerCoercion(new SelfCoercion())
                .registerCoercion(new PrimitiveTypeCoercion("smallint", "integer"))
                .registerFunction("custom", function(List.of(symbol("integer")), symbol("boolean")))
                .build();

        assertThat(library.resolveFunction("custom", List.of(symbol("smallint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome -> assertThat(outcome.resolution().returnType())
                        .isEqualTo(symbol("boolean")));
    }

    @Test
    void testLibrarySupportsCustomTypeRegistration()
    {
        TypeLibrary library = TypeLibrary.builder()
                .registerType(new TestTypeConstructor())
                .build();

        assertThat(library.typeSystem().types())
                .extracting(TypeConstructor::name)
                .containsExactly("widget");
        assertThat(library.typeSystem().instantiateValidationConstraints(symbol("widget")))
                .isEmpty();
        assertThat(library.typeSystem().instantiateValidationConstraints(org.weakref.solver.Expression.apply("widget", literal(11))))
                .containsExactly(new NumericRelation(operation(LESS_THAN_OR_EQUAL, literal(11), literal(10))));
    }

    private static final class TestTypeConstructor
            implements TypeConstructor
    {
        @Override
        public String name()
        {
            return "widget";
        }

        @Override
        public List<String> parameters()
        {
            return List.of("@n");
        }

        @Override
        public List<Constraint> constraints()
        {
            return List.of(new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("@n"), literal(10))));
        }

        @Override
        public Type newInstance(List<Argument> arguments)
        {
            return new TestType();
        }
    }

    private record TestType()
            implements Type {}
}
