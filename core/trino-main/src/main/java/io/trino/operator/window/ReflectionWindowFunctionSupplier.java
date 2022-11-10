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
import io.trino.spi.function.WindowFunction;
import io.trino.spi.function.WindowFunctionSupplier;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ReflectionWindowFunctionSupplier
        implements WindowFunctionSupplier
{
    private final int argumentCount;
    private final Constructor<? extends WindowFunction> constructor;
    private final ConstructorType constructorType;

    public ReflectionWindowFunctionSupplier(int argumentCount, Class<? extends WindowFunction> type)
    {
        this.argumentCount = argumentCount;

        Constructor<? extends WindowFunction> constructor = null;
        ConstructorType constructorType = null;

        for (ConstructorType constructorTypeValue : ConstructorType.values()) {
            constructorType = constructorTypeValue;
            constructor = constructorType.tryGetConstructor(type).orElse(null);
            if (constructor != null) {
                break;
            }
        }

        checkArgument(constructor != null, "No constructor found for type: %s", type.getName());

        this.constructor = constructor;
        this.constructorType = constructorType;
    }

    @Override
    public List<Class<?>> getLambdaInterfaces()
    {
        return ImmutableList.of();
    }

    @Override
    public WindowFunction createWindowFunction(boolean ignoreNulls, List<Supplier<Object>> lambdaProviders)
    {
        requireNonNull(lambdaProviders, "lambdaProviders is null");
        checkArgument(lambdaProviders.isEmpty(), "lambdaProviders is not empty");

        List<Integer> argumentChannels = IntStream.range(0, argumentCount).boxed().collect(toImmutableList());
        try {
            switch (constructorType) {
                case NO_INPUTS:
                    return constructor.newInstance();
                case IGNORE_NULLS:
                    return constructor.newInstance(ignoreNulls);
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

    private enum ConstructorType
    {
        INPUTS_IGNORE_NULLS {
            @Override
            Optional<Constructor<? extends WindowFunction>> tryGetConstructor(Class<? extends WindowFunction> type)
            {
                return optionalConstructor(type, List.class, boolean.class);
            }
        },
        INPUTS {
            @Override
            Optional<Constructor<? extends WindowFunction>> tryGetConstructor(Class<? extends WindowFunction> type)
            {
                return optionalConstructor(type, List.class);
            }
        },
        IGNORE_NULLS {
            @Override
            Optional<Constructor<? extends WindowFunction>> tryGetConstructor(Class<? extends WindowFunction> type)
            {
                return optionalConstructor(type, boolean.class);
            }
        },
        NO_INPUTS {
            @Override
            Optional<Constructor<? extends WindowFunction>> tryGetConstructor(Class<? extends WindowFunction> type)
            {
                return optionalConstructor(type);
            }
        };

        abstract Optional<Constructor<? extends WindowFunction>> tryGetConstructor(Class<? extends WindowFunction> type);

        private static Optional<Constructor<? extends WindowFunction>> optionalConstructor(Class<? extends WindowFunction> type, Class<?>... parameterTypes)
        {
            try {
                return Optional.of(type.getConstructor(parameterTypes));
            }
            catch (NoSuchMethodException e) {
                return Optional.empty();
            }
        }
    }
}
