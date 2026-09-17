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
package io.trino.metadata;

import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeTemplates;
import io.trino.sql.analyzer.TypeDescriptorProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.stream.IntStream;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.type.TypeResolutionPolicy.SQL_STANDARD;
import static io.trino.type.UnknownType.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;

class TestSolverSignatureBinder
{
    @Test
    void testCastUnknownToVariantImplementation()
    {
        ResolvedFunction cast = PLANNER_CONTEXT.getMetadata().getCoercion(SQL_STANDARD, UNKNOWN, VARIANT);
        assertThat(PLANNER_CONTEXT.getFunctionManager()
                .getScalarFunctionImplementation(cast, simpleConvention(FAIL_ON_NULL, NEVER_NULL))
                .getMethodHandle().type().parameterList())
                .containsExactly(UNKNOWN.getJavaType());
    }

    @Test
    void testUnknownDoesNotBindRowVariable()
    {
        Signature signature = Signature.builder()
                .typeVariableConstraint(TypeVariableConstraint.builder("T").rowType().build())
                .argumentType(typeVariable("T"))
                .returnType(VARIANT)
                .build();
        for (boolean allowCoercion : new boolean[] {false, true}) {
            assertThat(binder(signature, allowCoercion).canBind(fromTypes(List.of(UNKNOWN)), VARIANT.getTypeDescriptor())).isFalse();
            assertThat(binder(signature, allowCoercion).canBind(fromTypes(List.of(RowType.anonymous(List.of(BIGINT)))), VARIANT.getTypeDescriptor())).isTrue();
        }
    }

    @Test
    @Timeout(30)
    void testCastUnknownToWideRow()
    {
        RowType row = RowType.from(IntStream.range(0, 10_000)
                .mapToObj(index -> RowType.field("field_" + index, BIGINT))
                .toList());
        Signature signature = Signature.builder()
                .typeVariable("T")
                .argumentType(UNKNOWN)
                .returnType(typeVariable("T"))
                .build();
        assertThat(binder(signature, false).canBind(fromTypes(List.of(UNKNOWN)), row.getTypeDescriptor())).isTrue();
    }

    @Test
    void testSharedVariableAndExactMatching()
    {
        Signature signature = Signature.builder()
                .typeVariable("T")
                .argumentType(typeVariable("T"))
                .argumentType(typeVariable("T"))
                .returnType(typeVariable("T"))
                .build();
        assertThat(binder(signature, false).bind(fromTypes(List.of(BIGINT, DOUBLE)))).isEmpty();
        assertThat(binder(signature, true).bind(fromTypes(List.of(BIGINT, DOUBLE))))
                .contains(new SignatureBinder.GroundSignature(DOUBLE.getTypeDescriptor(), List.of(DOUBLE.getTypeDescriptor(), DOUBLE.getTypeDescriptor())));
    }

    @Test
    void testReturnOnlyVariableAndCanonicalVarchar()
    {
        Signature signature = Signature.builder()
                .typeVariable("T")
                .argumentType(UNKNOWN)
                .returnType(typeVariable("T"))
                .build();
        assertThat(binder(signature, false).canBind(fromTypes(List.of(UNKNOWN)), VARCHAR.getTypeDescriptor())).isTrue();
        assertThat(binder(signature, false).canBind(fromTypes(List.of(BIGINT)), VARCHAR.getTypeDescriptor())).isFalse();
    }

    @Test
    void testLambdaInputsBecomeAvailableInLaterIterations()
    {
        Signature signature = Signature.builder()
                .typeVariable("T")
                .typeVariable("U")
                .typeVariable("V")
                .argumentType(typeVariable("T"))
                .argumentType(TypeTemplates.functionType(typeVariable("U"), typeVariable("V")))
                .argumentType(TypeTemplates.functionType(typeVariable("T"), typeVariable("U")))
                .returnType(typeVariable("V"))
                .build();
        List<TypeDescriptorProvider> arguments = List.of(
                new TypeDescriptorProvider(INTEGER.getTypeDescriptor()),
                new TypeDescriptorProvider(inputs -> {
                    assertThat(inputs).containsExactly(VARCHAR);
                    return new FunctionType(inputs, DOUBLE).getTypeDescriptor();
                }),
                new TypeDescriptorProvider(inputs -> {
                    assertThat(inputs).containsExactly(INTEGER);
                    return new FunctionType(inputs, VARCHAR).getTypeDescriptor();
                }));
        for (boolean allowCoercion : new boolean[] {false, true}) {
            assertThat(binder(signature, allowCoercion).bind(arguments))
                    .hasValueSatisfying(bound -> {
                        assertThat(bound.returnType()).isEqualTo(DOUBLE.getTypeDescriptor());
                        assertThat(bound.argumentTypes().stream().map(PLANNER_CONTEXT.getTypeManager()::getType))
                                .containsExactly(INTEGER, new FunctionType(List.of(VARCHAR), DOUBLE), new FunctionType(List.of(INTEGER), VARCHAR));
                    });
        }
    }

    @Test
    @Timeout(10)
    void testUnresolvedLambdaInputsWithoutProgress()
    {
        Signature signature = Signature.builder()
                .typeVariable("T")
                .typeVariable("U")
                .argumentType(TypeTemplates.functionType(typeVariable("T"), typeVariable("U")))
                .argumentType(TypeTemplates.functionType(typeVariable("U"), typeVariable("T")))
                .returnType(typeVariable("T"))
                .build();
        TypeDescriptorProvider lambda = new TypeDescriptorProvider(_ -> {
            throw new AssertionError("Lambda inputs are unresolved");
        });
        for (boolean allowCoercion : new boolean[] {false, true}) {
            assertThat(binder(signature, allowCoercion).bind(List.of(lambda, lambda))).isEmpty();
        }
    }

    @Test
    void testVariadicBinding()
    {
        Signature signature = Signature.builder()
                .typeVariable("T")
                .argumentType(typeVariable("T"))
                .variableArity()
                .returnType(typeVariable("T"))
                .build();
        assertThat(binder(signature, true).bind(fromTypes(List.of(BIGINT, BIGINT, DOUBLE))))
                .contains(new SignatureBinder.GroundSignature(DOUBLE.getTypeDescriptor(), List.of(DOUBLE.getTypeDescriptor(), DOUBLE.getTypeDescriptor(), DOUBLE.getTypeDescriptor())));
    }

    private static SolverSignatureBinder binder(Signature signature, boolean allowCoercion)
    {
        return new SolverSignatureBinder(PLANNER_CONTEXT.getMetadata(), PLANNER_CONTEXT.getTypeManager(), signature, allowCoercion, SQL_STANDARD);
    }
}
