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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AggregationMetadata
{
    private final MethodHandle inputFunction;
    private final Optional<MethodHandle> removeInputFunction;
    private final Optional<MethodHandle> combineFunction;
    private Optional<MethodHandle> isNullFunction = Optional.empty();
    private final MethodHandle outputFunction;
    private final List<AccumulatorStateDescriptor<?>> accumulatorStateDescriptors;
    private final List<Class<?>> lambdaInterfaces;

    public AggregationMetadata(
            MethodHandle inputFunction,
            Optional<MethodHandle> removeInputFunction,
            Optional<MethodHandle> combineFunction,
            MethodHandle outputFunction,
            List<AccumulatorStateDescriptor<?>> accumulatorStateDescriptors)
    {
        this(
                inputFunction,
                removeInputFunction,
                combineFunction,
                outputFunction,
                accumulatorStateDescriptors,
                ImmutableList.of());
    }

    public AggregationMetadata(
            MethodHandle inputFunction,
            Optional<MethodHandle> removeInputFunction,
            Optional<MethodHandle> combineFunction,
            MethodHandle outputFunction,
            List<AccumulatorStateDescriptor<?>> accumulatorStateDescriptors,
            List<Class<?>> lambdaInterfaces)
    {
        this.inputFunction = requireNonNull(inputFunction, "inputFunction is null");
        this.removeInputFunction = requireNonNull(removeInputFunction, "removeInputFunction is null");
        this.combineFunction = requireNonNull(combineFunction, "combineFunction is null");
        this.outputFunction = requireNonNull(outputFunction, "outputFunction is null");
        this.accumulatorStateDescriptors = requireNonNull(accumulatorStateDescriptors, "accumulatorStateDescriptors is null");
        this.lambdaInterfaces = ImmutableList.copyOf(requireNonNull(lambdaInterfaces, "lambdaInterfaces is null"));
    }

    public AggregationMetadata(
            MethodHandle inputFunction,
            Optional<MethodHandle> removeInputFunction,
            Optional<MethodHandle> combineFunction,
            MethodHandle outputFunction,
            List<AccumulatorStateDescriptor<?>> accumulatorStateDescriptors,
            List<Class<?>> lambdaInterfaces,
            Optional<MethodHandle> isNullFunction)
    {
        this.inputFunction = requireNonNull(inputFunction, "inputFunction is null");
        this.removeInputFunction = requireNonNull(removeInputFunction, "removeInputFunction is null");
        this.combineFunction = requireNonNull(combineFunction, "combineFunction is null");
        this.outputFunction = requireNonNull(outputFunction, "outputFunction is null");
        this.accumulatorStateDescriptors = requireNonNull(accumulatorStateDescriptors, "accumulatorStateDescriptors is null");
        this.lambdaInterfaces = ImmutableList.copyOf(requireNonNull(lambdaInterfaces, "lambdaInterfaces is null"));
        this.isNullFunction = isNullFunction;
    }

    public MethodHandle getInputFunction()
    {
        return inputFunction;
    }

    public Optional<MethodHandle> getRemoveInputFunction()
    {
        return removeInputFunction;
    }

    public Optional<MethodHandle> getCombineFunction()
    {
        return combineFunction;
    }

    public Optional<MethodHandle> getIsNullFunction()
    {
        return isNullFunction;
    }

    public MethodHandle getOutputFunction()
    {
        return outputFunction;
    }

    public List<AccumulatorStateDescriptor<?>> getAccumulatorStateDescriptors()
    {
        return accumulatorStateDescriptors;
    }

    public List<Class<?>> getLambdaInterfaces()
    {
        return lambdaInterfaces;
    }

    public static class AccumulatorStateDescriptor<T extends AccumulatorState>
    {
        private final Class<T> stateInterface;
        private final AccumulatorStateSerializer<T> serializer;
        private final AccumulatorStateFactory<T> factory;

        public AccumulatorStateDescriptor(Class<T> stateInterface, AccumulatorStateSerializer<T> serializer, AccumulatorStateFactory<T> factory)
        {
            this.stateInterface = requireNonNull(stateInterface, "stateInterface is null");
            this.serializer = requireNonNull(serializer, "serializer is null");
            this.factory = requireNonNull(factory, "factory is null");
        }

        // this is only used to verify method interfaces
        public Class<T> getStateInterface()
        {
            return stateInterface;
        }

        public AccumulatorStateSerializer<T> getSerializer()
        {
            return serializer;
        }

        public AccumulatorStateFactory<T> getFactory()
        {
            return factory;
        }
    }
}
