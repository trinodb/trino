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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;

import static com.google.common.primitives.Primitives.wrap;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static java.lang.invoke.MethodHandles.catchException;
import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodType.methodType;

public class TryCastFunction
        extends SqlScalarFunction
{
    public static final TryCastFunction TRY_CAST = new TryCastFunction();

    public TryCastFunction()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name("TRY_CAST")
                        .castableToTypeParameter("F", new TypeSignature("T"))
                        .typeVariable("T")
                        .returnType(new TypeSignature("T"))
                        .argumentType(new TypeSignature("F"))
                        .build())
                .nullable()
                .hidden()
                .noDescription()
                .build());
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return FunctionDependencyDeclaration.builder()
                .addCastSignature(new TypeSignature("F"), new TypeSignature("T"))
                .build();
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        Type fromType = boundSignature.getArgumentType(0);
        Type toType = boundSignature.getReturnType();

        Class<?> returnType = wrap(toType.getJavaType());

        // the resulting method needs to return a boxed type
        InvocationConvention invocationConvention = new InvocationConvention(ImmutableList.of(NEVER_NULL), NULLABLE_RETURN, true, false);
        MethodHandle coercion = functionDependencies.getCastImplementation(fromType, toType, invocationConvention).getMethodHandle();
        coercion = coercion.asType(methodType(returnType, coercion.type()));

        MethodHandle exceptionHandler = dropArguments(constant(returnType, null), 0, RuntimeException.class);
        MethodHandle tryCastHandle = catchException(coercion, RuntimeException.class, exceptionHandler);

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL),
                tryCastHandle);
    }
}
