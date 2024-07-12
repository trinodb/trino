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

import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.function.WindowFunction;
import io.trino.spi.function.WindowFunctionSupplier;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.trino.operator.aggregation.AccumulatorCompiler.generateWindowAccumulatorClass;
import static java.util.Objects.requireNonNull;

public class AggregationWindowFunctionSupplier
        implements WindowFunctionSupplier
{
    private final Function<List<Supplier<Object>>, WindowAccumulator> windowAccumulatorFactory;
    private final boolean hasRemoveInput;
    private final List<Class<?>> lambdaInterfaces;

    public AggregationWindowFunctionSupplier(BoundSignature boundSignature, AggregationImplementation aggregationImplementation, FunctionNullability functionNullability)
    {
        requireNonNull(boundSignature, "boundSignature is null");
        requireNonNull(aggregationImplementation, "aggregationMetadata is null");
        windowAccumulatorFactory = generateWindowAccumulatorClass(boundSignature, aggregationImplementation, functionNullability);
        hasRemoveInput = aggregationImplementation.getWindowAccumulator().isPresent();
        lambdaInterfaces = aggregationImplementation.getLambdaInterfaces();
    }

    @Override
    public List<Class<?>> getLambdaInterfaces()
    {
        return lambdaInterfaces;
    }

    @Override
    public WindowFunction createWindowFunction(boolean ignoreNulls, List<Supplier<Object>> lambdaProviders)
    {
        return new AggregateWindowFunction(() -> windowAccumulatorFactory.apply(lambdaProviders), hasRemoveInput);
    }

    public WindowAccumulator createWindowAccumulator(List<Supplier<Object>> lambdaProviders)
    {
        return windowAccumulatorFactory.apply(lambdaProviders);
    }
}
