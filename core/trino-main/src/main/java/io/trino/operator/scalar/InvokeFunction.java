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
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.lambda.LambdaFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.TypeSignature.functionType;
import static io.trino.util.Reflection.methodHandle;

/**
 * This scalar function exists primarily to test lambda expression support.
 */
public final class InvokeFunction
        extends SqlScalarFunction
{
    public static final InvokeFunction INVOKE_FUNCTION = new InvokeFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(InvokeFunction.class, "invoke", InvokeLambda.class);

    private InvokeFunction()
    {
        super(new FunctionMetadata(
                new Signature(
                        "invoke",
                        ImmutableList.of(typeVariable("T")),
                        ImmutableList.of(),
                        new TypeSignature("T"),
                        ImmutableList.of(functionType(new TypeSignature("T"))),
                        false),
                new FunctionNullability(true, ImmutableList.of(false)),
                true,
                true,
                "lambda invoke function",
                SCALAR));
    }

    @Override
    protected ScalarFunctionImplementation specialize(BoundSignature boundSignature)
    {
        Type returnType = boundSignature.getReturnType();
        return new ChoicesScalarFunctionImplementation(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(FUNCTION),
                ImmutableList.of(InvokeLambda.class),
                METHOD_HANDLE.asType(
                        METHOD_HANDLE.type()
                                .changeReturnType(Primitives.wrap(returnType.getJavaType()))),
                Optional.empty());
    }

    public static Object invoke(InvokeLambda function)
    {
        return function.apply();
    }

    @FunctionalInterface
    public interface InvokeLambda
            extends LambdaFunctionInterface
    {
        Object apply();
    }
}
