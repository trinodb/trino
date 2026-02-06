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
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.Signature;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.lambda.UnaryFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.functionType;
import static io.trino.util.Reflection.methodHandle;

public class SwitchFunction
        extends SqlScalarFunction
{
    public static final SwitchFunction SWITCH_FUNCTION = new SwitchFunction();
    private static final MethodHandle SWITCH_METHOD_HANDLE = methodHandle(SwitchFunction.class, "switchCase", Object.class, UnaryFunctionInterface[].class);

    private SwitchFunction()
    {
        super(FunctionMetadata.scalarBuilder("switch")
                .description("Switch function using lambdas, for example switch(getValue(), v -> if(v = 'value1', 'return value 1'), v -> if(v = 'value2', 'return value 2', 'default value'))")
                .signature(Signature.builder()
                        .typeVariable("V")
                        .typeVariable("R")
                        .returnType(new TypeSignature("R"))
                        .argumentType(new TypeSignature("V"))
                        .argumentType(functionType(new TypeSignature("V"), new TypeSignature("R")))
                        .variableArity()
                        .build())
                .build());
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        int switchCases = boundSignature.getArity() - 1;
        if (switchCases < 1) {
            throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "There must be at least one switch case, usually v -> if(v = ... , 'return value')");
        }

        List<InvocationConvention.InvocationArgumentConvention> argumentConventions = new ArrayList<>();
        argumentConventions.add(BOXED_NULLABLE);
        List<Class<?>> interfaces = new ArrayList<>();
        for (int i = 0; i < switchCases; i++) {
            argumentConventions.add(FUNCTION);
            interfaces.add(UnaryFunctionInterface.class);
        }

        Class<?> valueType = boundSignature.getArgumentType(0).getJavaType();
        Class<?> returnType = boundSignature.getReturnType().getJavaType();

        MethodHandle methodHandle = SWITCH_METHOD_HANDLE.asCollector(UnaryFunctionInterface[].class, switchCases);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.copyOf(argumentConventions),
                ImmutableList.copyOf(interfaces),
                methodHandle.asType(
                        methodHandle.type()
                                .changeParameterType(0, Primitives.wrap(valueType))
                                .changeReturnType(Primitives.unwrap(returnType))),
                Optional.empty());
    }

    public static Object switchCase(Object value, UnaryFunctionInterface[] cases)
    {
        for (UnaryFunctionInterface function : cases) {
            Object ret = function.apply(value);
            if (ret != null) {
                return ret;
            }
        }
        return null;
    }
}
