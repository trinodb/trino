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

import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionNullability;
import io.trino.operator.aggregation.AggregationMetadata;
import io.trino.operator.aggregation.WindowAccumulator;
import io.trino.spi.function.WindowFunction;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.function.Supplier;

import static io.trino.operator.aggregation.AccumulatorCompiler.generateWindowAccumulatorClass;
import static java.util.Objects.requireNonNull;

public class AggregationWindowFunctionSupplier
        implements WindowFunctionSupplier
{
    private final Constructor<? extends WindowAccumulator> constructor;
    private final boolean hasRemoveInput;
    private final List<Class<?>> lambdaInterfaces;

    public AggregationWindowFunctionSupplier(BoundSignature boundSignature, AggregationMetadata aggregationMetadata, FunctionNullability functionNullability)
    {
        requireNonNull(boundSignature, "boundSignature is null");
        requireNonNull(aggregationMetadata, "aggregationMetadata is null");
        constructor = generateWindowAccumulatorClass(boundSignature, aggregationMetadata, functionNullability);
        hasRemoveInput = aggregationMetadata.getRemoveInputFunction().isPresent();
        lambdaInterfaces = aggregationMetadata.getLambdaInterfaces();
    }

    @Override
    public List<Class<?>> getLambdaInterfaces()
    {
        return lambdaInterfaces;
    }

    @Override
    public WindowFunction createWindowFunction(boolean ignoreNulls, List<Supplier<Object>> lambdaProviders)
    {
        return new AggregateWindowFunction(() -> createWindowAccumulator(lambdaProviders), hasRemoveInput);
    }

    private WindowAccumulator createWindowAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        try {
            return constructor.newInstance(lambdaProviders);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
