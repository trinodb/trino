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
import io.trino.Session;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionNullability;
import io.trino.operator.PagesIndex;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.aggregation.AccumulatorCompiler.generateAccumulatorFactoryBinder;
import static java.util.Objects.requireNonNull;

public final class InternalAggregationFunction
{
    private final List<Class<?>> lambdaInterfaces;
    private final AccumulatorFactoryBinder factory;

    public InternalAggregationFunction(BoundSignature boundSignature, AggregationMetadata aggregationMetadata, FunctionNullability functionNullability)
    {
        requireNonNull(boundSignature, "boundSignature is null");
        requireNonNull(aggregationMetadata, "aggregationMetadata is null");
        this.factory = generateAccumulatorFactoryBinder(boundSignature, aggregationMetadata, functionNullability);
        this.lambdaInterfaces = ImmutableList.copyOf(aggregationMetadata.getLambdaInterfaces());

        verifyInputFunctionSignature(boundSignature, aggregationMetadata);
        verifyCombineFunction(aggregationMetadata);
        verifyExactOutputFunction(aggregationMetadata);
    }

    public List<Class<?>> getLambdaInterfaces()
    {
        return lambdaInterfaces;
    }

    public AccumulatorFactory bind(List<Integer> inputChannels, Optional<Integer> maskChannel)
    {
        return factory.bind(
                inputChannels,
                maskChannel,
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                null,
                false,
                null,
                null,
                ImmutableList.of(),
                null);
    }

    public AccumulatorFactory bind(
            List<Integer> inputChannels,
            Optional<Integer> maskChannel,
            List<Type> sourceTypes,
            List<Integer> orderByChannels,
            List<SortOrder> orderings,
            PagesIndex.Factory pagesIndexFactory,
            boolean distinct,
            JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators,
            List<LambdaProvider> lambdaProviders,
            Session session)
    {
        return factory.bind(inputChannels, maskChannel, sourceTypes, orderByChannels, orderings, pagesIndexFactory, distinct, joinCompiler, blockTypeOperators, lambdaProviders, session);
    }

    private static void verifyInputFunctionSignature(BoundSignature boundSignature, AggregationMetadata aggregationMetadata)
    {
        MethodHandle inputFunction = aggregationMetadata.getInputFunction();

        int inputParameterCount = boundSignature.getArgumentTypes().size() - aggregationMetadata.getLambdaInterfaces().size();

        int expectedParameterCount = aggregationMetadata.getAccumulatorStateDescriptors().size() +
                inputParameterCount +
                (inputParameterCount > 0 ? 1 : 0) +
                aggregationMetadata.getLambdaInterfaces().size();
        checkArgument(
                inputFunction.type().parameterList().size() == expectedParameterCount,
                "Expected input function to have %s input arguments, but it has %s arguments",
                expectedParameterCount,
                inputFunction.type().parameterList().size());

        List<AccumulatorStateDescriptor<?>> accumulatorStateDescriptors = aggregationMetadata.getAccumulatorStateDescriptors();
        int parameterIndex = 0;
        for (AccumulatorStateDescriptor<?> accumulatorStateDescriptor : accumulatorStateDescriptors) {
            verifyMethodParameterType(inputFunction, parameterIndex, accumulatorStateDescriptor.getStateInterface(), "state");
            parameterIndex++;
        }
        for (int inputParameter = 0; inputParameter < inputParameterCount; inputParameter++) {
            verifyMethodParameterType(inputFunction, parameterIndex, Block.class, "input");
            parameterIndex++;
        }
        if (inputParameterCount > 0) {
            verifyMethodParameterType(inputFunction, parameterIndex, int.class, "position");
            parameterIndex++;
        }
        for (Class<?> lambdaInterface : aggregationMetadata.getLambdaInterfaces()) {
            verifyMethodParameterType(inputFunction, parameterIndex, lambdaInterface, "function");
        }
    }

    private static void verifyCombineFunction(AggregationMetadata aggregationMetadata)
    {
        MethodHandle combineFunction = aggregationMetadata.getCombineFunction();
        Class<?>[] parameterTypes = combineFunction.type().parameterArray();
        List<Class<?>> lambdaInterfaces = aggregationMetadata.getLambdaInterfaces();
        List<AccumulatorStateDescriptor<?>> stateDescriptors = aggregationMetadata.getAccumulatorStateDescriptors();
        checkArgument(
                parameterTypes.length == stateDescriptors.size() * 2 + lambdaInterfaces.size(),
                "Number of arguments for combine function must be 2 times the size of states plus number of lambda channels.");

        for (int i = 0; i < stateDescriptors.size() * 2; i++) {
            checkArgument(
                    parameterTypes[i].equals(stateDescriptors.get(i % stateDescriptors.size()).getStateInterface()),
                    "Type for Parameter index %s is unexpected. Arguments for combine function must appear in the order of state1, state2, ..., otherState1, otherState2, ...",
                    i);
        }

        for (int i = 0; i < lambdaInterfaces.size(); i++) {
            verifyMethodParameterType(combineFunction, i + stateDescriptors.size() * 2, lambdaInterfaces.get(i), "function");
        }
    }

    private static void verifyExactOutputFunction(AggregationMetadata aggregationMetadata)
    {
        Class<?>[] parameterTypes = aggregationMetadata.getOutputFunction().type().parameterArray();
        List<AccumulatorStateDescriptor<?>> stateDescriptors = aggregationMetadata.getAccumulatorStateDescriptors();
        checkArgument(parameterTypes.length == stateDescriptors.size() + 1, "Number of arguments for combine function must be exactly one plus than number of states.");
        for (int i = 0; i < stateDescriptors.size(); i++) {
            checkArgument(parameterTypes[i].equals(stateDescriptors.get(i).getStateInterface()), "Type for Parameter index %s is unexpected", i);
        }
        checkArgument(Arrays.stream(parameterTypes).filter(type -> type.equals(BlockBuilder.class)).count() == 1, "Output function must take exactly one BlockBuilder parameter");
    }

    private static void verifyMethodParameterType(MethodHandle method, int index, Class<?> javaType, String sqlTypeDisplayName)
    {
        checkArgument(method.type().parameterType(index).isAssignableFrom(javaType),
                "Expected method %s parameter %s type to be %s (%s)",
                method,
                index,
                javaType.getName(),
                sqlTypeDisplayName);
    }
}
