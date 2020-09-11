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
package io.prestosql.operator.window;

import io.prestosql.metadata.Signature;
import io.prestosql.operator.aggregation.LambdaProvider;
import io.prestosql.spi.function.WindowFunction;
import io.prestosql.type.FunctionType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class AbstractWindowFunctionSupplier
        implements WindowFunctionSupplier
{
    private final Signature signature;
    private final String description;
    private final List<Class<?>> lambdaInterfaces;

    protected AbstractWindowFunctionSupplier(Signature signature, String description, List<Class<?>> lambdaInterfaces)
    {
        this.signature = requireNonNull(signature, "signature is null");
        this.description = description;
        this.lambdaInterfaces = lambdaInterfaces;
    }

    @Override
    public final Signature getSignature()
    {
        return signature;
    }

    @Override
    public final String getDescription()
    {
        return description;
    }

    @Override
    public List<Class<?>> getLambdaInterfaces()
    {
        return lambdaInterfaces;
    }

    @Override
    public final WindowFunction createWindowFunction(List<Integer> argumentChannels, boolean ignoreNulls, List<LambdaProvider> lambdaProviders)
    {
        requireNonNull(argumentChannels, "inputs is null");

        long argumentCount = signature.getArgumentTypes().stream()
                .filter(type -> !type.getBase().equalsIgnoreCase(FunctionType.NAME))
                .count();

        checkArgument(argumentChannels.size() == argumentCount,
                "Expected %s arguments for function %s, but got %s",
                argumentCount,
                signature.getName(),
                argumentChannels.size());

        return newWindowFunction(argumentChannels, ignoreNulls, lambdaProviders);
    }

    /**
     * Create window function instance using the supplied arguments.  The
     * inputs have already validated.
     */
    protected abstract WindowFunction newWindowFunction(List<Integer> inputs, boolean ignoreNulls, List<LambdaProvider> lambdaProviders);
}
