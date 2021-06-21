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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.trino.operator.aggregation.LambdaProvider;
import io.trino.operator.window.FrameInfo;
import io.trino.operator.window.WindowFunctionSupplier;
import io.trino.spi.function.WindowFunction;
import io.trino.spi.type.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class WindowFunctionDefinition
{
    private final WindowFunctionSupplier functionSupplier;
    private final Type type;
    private final Optional<FrameInfo> frameInfo;
    private final List<Integer> argumentChannels;
    private final boolean ignoreNulls;
    private final List<LambdaProvider> lambdaProviders;

    public static WindowFunctionDefinition window(WindowFunctionSupplier functionSupplier, Type type, FrameInfo frameInfo, boolean ignoreNulls, List<LambdaProvider> lambdaProviders, List<Integer> inputs)
    {
        return new WindowFunctionDefinition(functionSupplier, type, Optional.of(frameInfo), ignoreNulls, lambdaProviders, inputs);
    }

    public static WindowFunctionDefinition window(WindowFunctionSupplier functionSupplier, Type type, FrameInfo frameInfo, boolean ignoreNulls, List<LambdaProvider> lambdaProviders, Integer... inputs)
    {
        return window(functionSupplier, type, frameInfo, ignoreNulls, lambdaProviders, Arrays.asList(inputs));
    }

    /**
     * Create a WindowFunctionDefinition without the FrameInfo.
     * This method is used for window functions in pattern recognition context.
     * The corresponding FrameInfo is common to all window functions, and it is
     * a property of PatternRecognitionPartition.
     */
    public static WindowFunctionDefinition window(WindowFunctionSupplier functionSupplier, Type type, boolean ignoreNulls, List<LambdaProvider> lambdaProviders, List<Integer> inputs)
    {
        return new WindowFunctionDefinition(functionSupplier, type, Optional.empty(), ignoreNulls, lambdaProviders, inputs);
    }

    WindowFunctionDefinition(WindowFunctionSupplier functionSupplier, Type type, Optional<FrameInfo> frameInfo, boolean ignoreNulls, List<LambdaProvider> lambdaProviders, List<Integer> argumentChannels)
    {
        requireNonNull(functionSupplier, "functionSupplier is null");
        requireNonNull(type, "type is null");
        requireNonNull(frameInfo, "frameInfo is null");
        requireNonNull(lambdaProviders, "lambdaProviders is null");
        requireNonNull(argumentChannels, "argumentChannels is null");

        this.functionSupplier = functionSupplier;
        this.type = type;
        this.frameInfo = frameInfo;
        this.ignoreNulls = ignoreNulls;
        this.lambdaProviders = lambdaProviders;
        this.argumentChannels = ImmutableList.copyOf(argumentChannels);
    }

    public Optional<FrameInfo> getFrameInfo()
    {
        return frameInfo;
    }

    public Type getType()
    {
        return type;
    }

    public WindowFunction createWindowFunction()
    {
        return functionSupplier.createWindowFunction(argumentChannels, ignoreNulls, lambdaProviders);
    }
}
