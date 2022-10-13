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
package io.trino.operator.annotations;

import io.trino.metadata.FunctionBinding;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleProxies;

import static java.lang.invoke.MethodHandles.dropArguments;
import static java.util.Objects.requireNonNull;

public abstract class ScalarImplementationDependency
        implements ImplementationDependency
{
    private final InvocationConvention invocationConvention;
    private final Class<?> type;

    protected ScalarImplementationDependency(InvocationConvention invocationConvention, Class<?> type)
    {
        this.invocationConvention = requireNonNull(invocationConvention, "invocationConvention is null");
        this.type = requireNonNull(type, "type is null");
        if (invocationConvention.supportsInstanceFactory()) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " does not support instance functions");
        }
    }

    public InvocationConvention getInvocationConvention()
    {
        return invocationConvention;
    }

    public Class<?> getType()
    {
        return type;
    }

    protected abstract ScalarFunctionImplementation getImplementation(FunctionBinding functionBinding, FunctionDependencies functionDependencies, InvocationConvention invocationConvention);

    @Override
    public Object resolve(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        MethodHandle methodHandle = getImplementation(functionBinding, functionDependencies, invocationConvention).getMethodHandle();
        if (invocationConvention.supportsSession() && !methodHandle.type().parameterType(0).equals(ConnectorSession.class)) {
            methodHandle = dropArguments(methodHandle, 0, ConnectorSession.class);
        }
        if (type == MethodHandle.class) {
            return methodHandle;
        }
        return MethodHandleProxies.asInterfaceInstance(type, methodHandle);
    }

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();
}
