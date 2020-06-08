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
package io.prestosql.operator.annotations;

import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.function.InvocationConvention;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class ScalarImplementationDependency
        implements ImplementationDependency
{
    private final Optional<InvocationConvention> invocationConvention;

    protected ScalarImplementationDependency(Optional<InvocationConvention> invocationConvention)
    {
        this.invocationConvention = requireNonNull(invocationConvention, "invocationConvention is null");
        if (invocationConvention.map(InvocationConvention::supportsInstanceFactor).orElse(false)) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " does not support instance functions");
        }
    }

    protected abstract ResolvedFunction getResolvedFunction(BoundVariables boundVariables, Metadata metadata);

    @Override
    public MethodHandle resolve(BoundVariables boundVariables, Metadata metadata)
    {
        ResolvedFunction resolvedFunction = getResolvedFunction(boundVariables, metadata);
        return metadata.getScalarFunctionInvoker(resolvedFunction, invocationConvention).getMethodHandle();
    }

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();
}
