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
package io.trino.operator.window;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.trino.operator.aggregation.LambdaProvider;
import io.trino.spi.function.ValueWindowFunction;
import io.trino.spi.function.WindowFunction;

import java.lang.reflect.Constructor;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ReflectionWindowFunctionSupplier
        implements WindowFunctionSupplier
{
    private enum ConstructorType
    {
        NO_INPUTS,
        INPUTS,
        INPUTS_IGNORE_NULLS
    }

    private final int argumentCount;
    private final Constructor<? extends WindowFunction> constructor;
    private final ConstructorType constructorType;

    public ReflectionWindowFunctionSupplier(int argumentCount, Class<? extends WindowFunction> type)
    {
        this.argumentCount = argumentCount;
        try {
            Constructor<? extends WindowFunction> constructor;
            ConstructorType constructorType;

            if (argumentCount == 0) {
                constructor = type.getConstructor();
                constructorType = ConstructorType.NO_INPUTS;
            }
            else if (ValueWindowFunction.class.isAssignableFrom(type)) {
                try {
                    constructor = type.getConstructor(List.class, boolean.class);
                    constructorType = ConstructorType.INPUTS_IGNORE_NULLS;
                }
                catch (NoSuchMethodException e) {
                    // Fallback to default constructor.
                    constructor = type.getConstructor(List.class);
                    constructorType = ConstructorType.INPUTS;
                }
            }
            else {
                constructor = type.getConstructor(List.class);
                constructorType = ConstructorType.INPUTS;
            }

            this.constructor = constructor;
            this.constructorType = constructorType;
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Class<?>> getLambdaInterfaces()
    {
        return ImmutableList.of();
    }

    @Override
    public WindowFunction createWindowFunction(List<Integer> argumentChannels, boolean ignoreNulls, List<LambdaProvider> lambdaProviders)
    {
        requireNonNull(argumentChannels, "inputs is null");
        checkArgument(argumentChannels.size() == argumentCount, "Expected %s arguments, but got %s", argumentCount, argumentChannels.size());

        try {
            switch (constructorType) {
                case NO_INPUTS:
                    return constructor.newInstance();
                case INPUTS:
                    return constructor.newInstance(argumentChannels);
                case INPUTS_IGNORE_NULLS:
                    return constructor.newInstance(argumentChannels, ignoreNulls);
            }
            throw new VerifyException("Unhandled constructor type: " + constructorType);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
