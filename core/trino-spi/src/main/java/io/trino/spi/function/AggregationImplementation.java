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
package io.trino.spi.function;

import io.trino.spi.Experimental;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Experimental(eta = "2022-10-31")
public class AggregationImplementation
{
    private final MethodHandle inputFunction;
    private final Optional<MethodHandle> removeInputFunction;
    private final Optional<MethodHandle> combineFunction;
    private final MethodHandle outputFunction;
    private final List<AccumulatorStateDescriptor<?>> accumulatorStateDescriptors;
    private final List<Class<?>> lambdaInterfaces;

    private AggregationImplementation(
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
        this.lambdaInterfaces = List.copyOf(requireNonNull(lambdaInterfaces, "lambdaInterfaces is null"));
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

        private AccumulatorStateDescriptor(Class<T> stateInterface, AccumulatorStateSerializer<T> serializer, AccumulatorStateFactory<T> factory)
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

        public static <T extends AccumulatorState> Builder<T> builder(Class<T> stateInterface)
        {
            return new Builder<>(stateInterface);
        }

        public static class Builder<T extends AccumulatorState>
        {
            private final Class<T> stateInterface;
            private AccumulatorStateSerializer<T> serializer;
            private AccumulatorStateFactory<T> factory;

            private Builder(Class<T> stateInterface)
            {
                this.stateInterface = requireNonNull(stateInterface, "stateInterface is null");
            }

            public Builder<T> serializer(AccumulatorStateSerializer<T> serializer)
            {
                this.serializer = serializer;
                return this;
            }

            public Builder<T> factory(AccumulatorStateFactory<T> factory)
            {
                this.factory = factory;
                return this;
            }

            public AccumulatorStateDescriptor<T> build()
            {
                return new AccumulatorStateDescriptor<>(stateInterface, serializer, factory);
            }
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private MethodHandle inputFunction;
        private Optional<MethodHandle> removeInputFunction = Optional.empty();
        private Optional<MethodHandle> combineFunction = Optional.empty();
        private MethodHandle outputFunction;
        private List<AccumulatorStateDescriptor<?>> accumulatorStateDescriptors = new ArrayList<>();
        private List<Class<?>> lambdaInterfaces = List.of();

        private Builder() {}

        public Builder inputFunction(MethodHandle inputFunction)
        {
            this.inputFunction = requireNonNull(inputFunction, "inputFunction is null");
            return this;
        }

        public Builder removeInputFunction(MethodHandle removeInputFunction)
        {
            this.removeInputFunction = Optional.of(requireNonNull(removeInputFunction, "removeInputFunction is null"));
            return this;
        }

        public Builder combineFunction(MethodHandle combineFunction)
        {
            this.combineFunction = Optional.of(requireNonNull(combineFunction, "combineFunction is null"));
            return this;
        }

        public Builder outputFunction(MethodHandle outputFunction)
        {
            this.outputFunction = requireNonNull(outputFunction, "outputFunction is null");
            return this;
        }

        public <T extends AccumulatorState> Builder accumulatorStateDescriptor(Class<T> stateInterface, AccumulatorStateSerializer<T> serializer, AccumulatorStateFactory<T> factory)
        {
            this.accumulatorStateDescriptors.add(AccumulatorStateDescriptor.builder(stateInterface)
                    .serializer(serializer)
                    .factory(factory)
                    .build());
            return this;
        }

        public Builder accumulatorStateDescriptors(List<AccumulatorStateDescriptor<?>> accumulatorStateDescriptors)
        {
            requireNonNull(accumulatorStateDescriptors, "accumulatorStateDescriptors is null");

            this.accumulatorStateDescriptors = new ArrayList<>();
            this.accumulatorStateDescriptors.addAll(accumulatorStateDescriptors);
            return this;
        }

        public Builder lambdaInterfaces(Class<?>... lambdaInterfaces)
        {
            return lambdaInterfaces(List.of(lambdaInterfaces));
        }

        public Builder lambdaInterfaces(List<Class<?>> lambdaInterfaces)
        {
            this.lambdaInterfaces = List.copyOf(requireNonNull(lambdaInterfaces, "lambdaInterfaces is null"));
            return this;
        }

        public AggregationImplementation build()
        {
            return new AggregationImplementation(
                    inputFunction,
                    removeInputFunction,
                    combineFunction,
                    outputFunction,
                    accumulatorStateDescriptors,
                    lambdaInterfaces);
        }
    }
}
