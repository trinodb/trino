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

import io.trino.spi.function.Signature;
import io.trino.spi.type.NumericExpression;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeParameter;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.TypeParameter.namedField;
import static io.trino.spi.type.TypeParameter.numericParameter;
import static io.trino.spi.type.TypeParameter.typeParameter;
import static io.trino.spi.type.TypeTemplates.argument;
import static io.trino.spi.type.TypeTemplates.arrayType;
import static io.trino.spi.type.TypeTemplates.fromTypeDescriptor;
import static io.trino.spi.type.TypeTemplates.numericVariable;
import static io.trino.spi.type.TypeTemplates.type;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.typesolver.Expression.apply;
import static io.trino.typesolver.Expression.literal;
import static io.trino.typesolver.Expression.symbol;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TypeCompilerTest
{
    @Test
    void testIntegerAlias()
    {
        assertThat(TypeCompiler.compile(type("int"))).isEqualTo(symbol("integer"));
        assertThat(TypeCompiler.compile(type("array", argument(type("INT"))))).isEqualTo(apply("array", symbol("integer")));
    }

    @Test
    void testVariablesInsideLambdaAreQuantified()
    {
        CompiledSignature signature = SignatureCompiler.compile(Signature.builder()
                .typeVariable("T")
                .typeVariable("R")
                .argumentType(type("function", argument(typeVariable("T")), argument(typeVariable("R"))))
                .returnType(type("boolean"))
                .build());

        assertThat(signature.parameters()).containsExactlyInAnyOrder(Expression.variable("t"), Expression.variable("r"));
        assertThat(signature.matchFunctionCallOutcome(
                List.of(Expression.function(List.of(symbol("bigint")), symbol("varchar"))),
                TrinoPreset.typeSystem()))
                .isInstanceOfSatisfying(CompiledSignature.Satisfied.class, outcome ->
                        assertThat(outcome.result().typeBindings()).containsEntry("t", symbol("bigint")).containsEntry("r", symbol("varchar")));
    }

    @Test
    void testNumericParameterBoundaries()
    {
        for (int value : List.of(Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE)) {
            TypeDescriptor descriptor = new TypeDescriptor("custom", numericParameter(value));
            Expression compiled = TypeCompiler.compile(descriptor);
            assertThat(compiled).isEqualTo(apply("custom", literal(value)));
            assertThat(TypeCompiler.toTypeDescriptor(compiled)).isEqualTo(descriptor);
        }
    }

    @Test
    void testRejectUnrepresentableNumericParameters()
    {
        for (long value : List.of(Long.MIN_VALUE, (long) Integer.MIN_VALUE - 1, (long) Integer.MAX_VALUE + 1, (long) Integer.MAX_VALUE + 2, Long.MAX_VALUE)) {
            TypeDescriptor descriptor = new TypeDescriptor("custom", numericParameter(value));
            assertThatThrownBy(() -> TypeCompiler.compile(descriptor))
                    .isInstanceOf(ArithmeticException.class);
            assertThatThrownBy(() -> TypeCompiler.compile(fromTypeDescriptor(descriptor)))
                    .isInstanceOf(ArithmeticException.class);
            assertThatThrownBy(() -> TypeCompiler.compile(new TypeDescriptor("array", typeParameter(descriptor))))
                    .isInstanceOf(ArithmeticException.class);
        }
    }

    @Test
    void testRejectUnrepresentableCalculatedLiteral()
    {
        assertThatThrownBy(() -> TypeCompiler.compile(
                type("custom", argument(numericVariable("n"))),
                Map.of("n", new NumericExpression.Literal((long) Integer.MAX_VALUE + 1))))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void testUnknownParametricDescriptorKeepsParameters()
    {
        // A plugin-provided parametric type the compiler has no structural mapping for
        // (modelled on trino-ml's classifier(T)) must carry its parameters through
        // the descriptor instead of collapsing to a bare base symbol
        assertThat(TypeCompiler.compile(new TypeDescriptor("Classifier", typeParameter(VARCHAR.getTypeDescriptor()))))
                .isEqualTo(apply("classifier", symbol("varchar")));

        // Numeric parameters of nested descriptors survive too
        assertThat(TypeCompiler.compile(new TypeDescriptor("Classifier", typeParameter(createVarcharType(10).getTypeDescriptor()))))
                .isEqualTo(apply("classifier", apply("varchar", literal(10))));
    }

    @Test
    void testExpressionConvertsBackToDescriptor()
    {
        // Multi-word bases reverse the underscore encoding the expression language uses
        TypeDescriptor timestamp = TypeCompiler.toTypeDescriptor(apply("timestamp_with_time_zone", literal(3)));
        assertThat(timestamp.getBase()).isEqualTo("timestamp with time zone");
        assertThat(timestamp.getParameters()).containsExactly(TypeParameter.numericParameter(3));

        TypeDescriptor row = TypeCompiler.toTypeDescriptor(Expression.row(
                Expression.field("name", symbol("varchar")),
                Expression.anonymousField(apply("decimal", literal(10), literal(2)))));
        assertThat(row.getBase()).isEqualTo("row");
        assertThat(row.getParameters()).hasSize(2);
    }

    @Test
    void testUnknownSimpleDescriptorMapsToBareSymbol()
    {
        assertThat(TypeCompiler.compile(new TypeDescriptor("Model")))
                .isEqualTo(symbol("model"));
    }

    @Test
    void testCanonicalTemplateConversion()
    {
        assertThat(TypeCompiler.compile(arrayType(typeVariable("T"))))
                .isEqualTo(apply("array", Expression.variable("t")));

        assertThat(TypeCompiler.compile(TypeDescriptor.rowType(List.of(namedField("value", VARCHAR.getTypeDescriptor())))))
                .isEqualTo(Expression.row(Expression.field("value", symbol("varchar"))));
    }
}
