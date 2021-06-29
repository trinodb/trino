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
package io.trino.sql;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.FunctionInvoker;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.trino.type.FunctionType;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static java.lang.invoke.MethodHandleProxies.asInterfaceInstance;
import static java.util.Objects.requireNonNull;

public class InterpretedFunctionInvoker
{
    private final Metadata metadata;

    public InterpretedFunctionInvoker(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public Object invoke(ResolvedFunction function, ConnectorSession session, Object... arguments)
    {
        return invoke(function, session, Arrays.asList(arguments));
    }

    /**
     * Arguments must be the native container type for the corresponding SQL types.
     * <p>
     * Returns a value in the native container type corresponding to the declared SQL return type
     */
    public Object invoke(ResolvedFunction function, ConnectorSession session, List<Object> arguments)
    {
        FunctionMetadata functionMetadata = metadata.getFunctionMetadata(function);
        FunctionInvoker invoker = metadata.getScalarFunctionInvoker(function, getInvocationConvention(function, functionMetadata));
        return invoke(functionMetadata, invoker, session, arguments);
    }

    public static Object invoke(FunctionMetadata functionMetadata, FunctionInvoker invoker, ConnectorSession session, List<Object> arguments)
    {
        MethodHandle method = invoker.getMethodHandle();

        List<Object> actualArguments = new ArrayList<>();

        // handle function on instance method, to allow use of fields
        if (invoker.getInstanceFactory().isPresent()) {
            try {
                actualArguments.add(invoker.getInstanceFactory().get().invoke());
            }
            catch (Throwable throwable) {
                throw propagate(throwable);
            }
        }

        // add session
        if (method.type().parameterCount() > actualArguments.size() && method.type().parameterType(actualArguments.size()) == ConnectorSession.class) {
            actualArguments.add(session);
        }

        int lambdaArgumentIndex = 0;
        for (int i = 0; i < arguments.size(); i++) {
            Object argument = arguments.get(i);

            // if argument is null and function does not handle nulls, result is null
            if (argument == null && !functionMetadata.getArgumentDefinitions().get(i).isNullable()) {
                return null;
            }

            if (functionMetadata.getSignature().getArgumentTypes().get(i).getBase().equals(FunctionType.NAME)) {
                argument = asInterfaceInstance(invoker.getLambdaInterfaces().get(lambdaArgumentIndex), (MethodHandle) argument);
                lambdaArgumentIndex++;
            }

            actualArguments.add(argument);
        }

        try {
            return method.invokeWithArguments(actualArguments);
        }
        catch (Throwable throwable) {
            throw propagate(throwable);
        }
    }

    private static InvocationConvention getInvocationConvention(ResolvedFunction function, FunctionMetadata functionMetadata)
    {
        ImmutableList.Builder<InvocationArgumentConvention> argumentConventions = ImmutableList.builder();
        for (int i = 0; i < functionMetadata.getArgumentDefinitions().size(); i++) {
            if (function.getSignature().getArgumentTypes().get(i) instanceof FunctionType) {
                argumentConventions.add(FUNCTION);
            }
            else if (functionMetadata.getArgumentDefinitions().get(i).isNullable()) {
                argumentConventions.add(BOXED_NULLABLE);
            }
            else {
                argumentConventions.add(NEVER_NULL);
            }
        }

        return new InvocationConvention(
                argumentConventions.build(),
                functionMetadata.isNullable() ? NULLABLE_RETURN : FAIL_ON_NULL,
                true,
                true);
    }

    private static RuntimeException propagate(Throwable throwable)
    {
        if (throwable instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        throwIfUnchecked(throwable);
        throw new RuntimeException(throwable);
    }
}
