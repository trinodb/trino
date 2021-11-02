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
import com.google.common.primitives.Primitives;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.lambda.UnaryFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.TypeSignature.functionType;
import static io.trino.util.Reflection.methodHandle;

/**
 * This scalar function exists primarily to test lambda expression support.
 */
public final class ApplyFunction
        extends SqlScalarFunction
{
    public static final ApplyFunction APPLY_FUNCTION = new ApplyFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(ApplyFunction.class, "apply", Object.class, UnaryFunctionInterface.class);

    private ApplyFunction()
    {
        super(new FunctionMetadata(
                new Signature(
                        "apply",
                        ImmutableList.of(typeVariable("T"), typeVariable("U")),
                        ImmutableList.of(),
                        new TypeSignature("U"),
                        ImmutableList.of(
                                new TypeSignature("T"),
                                functionType(new TypeSignature("T"), new TypeSignature("U"))),
                        false),
                new FunctionNullability(true, ImmutableList.of(true, false)),
                true,
                true,
                "lambda apply function",
                SCALAR));
    }

    @Override
    protected ScalarFunctionImplementation specialize(BoundSignature boundSignature)
    {
        Type argumentType = boundSignature.getArgumentTypes().get(0);
        Type returnType = boundSignature.getReturnType();
        return new ChoicesScalarFunctionImplementation(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(BOXED_NULLABLE, FUNCTION),
                ImmutableList.of(UnaryFunctionInterface.class),
                METHOD_HANDLE.asType(
                        METHOD_HANDLE.type()
                                .changeReturnType(Primitives.wrap(returnType.getJavaType()))
                                .changeParameterType(0, Primitives.wrap(argumentType.getJavaType()))),
                Optional.empty());
    }

    @UsedByGeneratedCode
    public static Object apply(Object input, UnaryFunctionInterface function)
    {
        return function.apply(input);
    }
}
