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

import static io.trino.spi.type.TypeTemplates.argument;
import static io.trino.spi.type.TypeTemplates.numericVariable;
import static io.trino.typesolver.Expression.BinaryOperator.LESS_THAN_OR_EQUAL;
import static io.trino.typesolver.Expression.apply;
import static io.trino.typesolver.Expression.function;
import static io.trino.typesolver.Expression.literal;
import static io.trino.typesolver.Expression.operation;
import static io.trino.typesolver.Expression.symbol;
import static io.trino.typesolver.Expression.variable;
import static io.trino.typesolver.TypeConstructor.parametric;
import static org.assertj.core.api.Assertions.assertThat;

public class TypeLibraryTest
{
    @Test
    void testFrameworkDefaultReturnsAmbiguousWhenTwoWideningsTie()
    {
        // Two coercions of equal count; BY_COERCION_COUNT has no basis to pick. The test builds
        // on TrinoPreset for the types + coercions but explicitly resets the specificity to
        // the framework default to show the baseline behavior.
        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerFunction("f", function(List.of(symbol("integer")), symbol("integer")))
                .registerFunction("f", function(List.of(symbol("bigint")), symbol("bigint")))
                .withSpecificity(Specificity.BY_COERCION_COUNT)
                .build();

        assertThat(library.resolveFunction("f", List.of(symbol("smallint"))))
                .isInstanceOfSatisfying(FunctionResolver.Ambiguous.class, outcome ->
                        assertThat(outcome.candidates()).hasSize(2));
    }

    @Test
    void testLibraryResolvesFunctionWithUnknownArgument()
    {
        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
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
    void testResolvesInstanceMethod()
    {
        // A fabricated instance method on varchar (a name the preset does not register): the
        // receiver is the candidate's leading formal
        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerInstanceMethod("codeunits", "varchar", function(List.of(symbol("varchar")), symbol("bigint")))
                .build();

        assertThat(library.resolveInstanceMethod("codeunits", "varchar", List.of(symbol("varchar"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("bigint")));
        // No instance method of that name on a different receiver base does not resolve
        assertThat(library.resolveInstanceMethod("codeunits", "varbinary", List.of(symbol("varbinary"))))
                .isInstanceOf(FunctionResolver.NoMatch.class);
    }

    @Test
    void testResolvesStaticMethod()
    {
        // A fabricated static method on varchar: the receiver type is a selector only, not an argument
        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerStaticMethod("decode", "varchar", function(List.of(symbol("varbinary")), symbol("varchar")))
                .build();

        assertThat(library.resolveStaticMethod("decode", "varchar", List.of(symbol("varbinary"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("varchar")));
    }

    @Test
    void testInstanceAndStaticMethodsOfTheSameNameAreScopedSeparately()
    {
        // The same name registered both ways resolves to different candidates by call kind, and
        // neither leaks into free-function resolution
        TypeLibrary library = TestingTrinoLibrary.install(TypeLibrary.builder())
                .registerInstanceMethod("m", "varchar", function(List.of(symbol("varchar")), symbol("bigint")))
                .registerStaticMethod("m", "varchar", function(List.of(symbol("integer")), symbol("boolean")))
                .build();

        assertThat(library.resolveInstanceMethod("m", "varchar", List.of(symbol("varchar"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("bigint")));
        assertThat(library.resolveStaticMethod("m", "varchar", List.of(symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("boolean")));
        assertThat(library.functions("m")).isEmpty();
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
                .registerType(parametric(
                        "widget",
                        List.of(argument(numericVariable("n"))),
                        List.of(new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("n"), literal(10))))))
                .build();

        assertThat(library.typeSystem().types())
                .extracting(TypeConstructor::name)
                .containsExactly("widget");
        assertThat(library.typeSystem().instantiateValidationConstraints(symbol("widget")))
                .isEmpty();
        assertThat(library.typeSystem().instantiateValidationConstraints(apply("widget", literal(11))))
                .containsExactly(new NumericRelation(operation(LESS_THAN_OR_EQUAL, literal(11), literal(10))));
    }
}
