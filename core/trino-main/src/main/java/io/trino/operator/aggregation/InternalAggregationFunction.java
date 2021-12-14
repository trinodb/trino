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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.BoundSignature;
import io.trino.operator.PagesIndex;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.AggregationMetadata.AggregationParameterKind;
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
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.aggregation.AccumulatorCompiler.generateAccumulatorFactoryBinder;
import static io.trino.operator.aggregation.AggregationMetadata.AggregationParameterKind.STATE;
import static java.util.Objects.requireNonNull;

public final class InternalAggregationFunction
{
    private static final Set<Class<?>> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(long.class, double.class, boolean.class);

    private final List<Class<?>> lambdaInterfaces;
    private final AccumulatorFactoryBinder factory;

    public InternalAggregationFunction(BoundSignature boundSignature, AggregationMetadata aggregationMetadata)
    {
        requireNonNull(boundSignature, "boundSignature is null");
        requireNonNull(aggregationMetadata, "aggregationMetadata is null");
        this.factory = generateAccumulatorFactoryBinder(boundSignature, aggregationMetadata);
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
        List<AggregationParameterKind> inputParameterKinds = aggregationMetadata.getInputParameterKinds();
        List<Class<?>> parameters = inputFunction.type().parameterList();
        List<Class<?>> lambdaInterfaces = aggregationMetadata.getLambdaInterfaces();

        checkArgument(parameters.size() == inputParameterKinds.size() + lambdaInterfaces.size(),
                "Expected input to have %s input arguments, but it has %s arguments", inputParameterKinds.size() + lambdaInterfaces.size(), parameters.size());

        List<AccumulatorStateDescriptor<?>> accumulatorStateDescriptors = aggregationMetadata.getAccumulatorStateDescriptors();
        checkArgument(inputParameterKinds.stream().filter(STATE::equals).count() == accumulatorStateDescriptors
                .size(), "Number of state parameter in input function must be the same as size of stateDescriptors");
        checkArgument(inputParameterKinds.get(0) == STATE, "First parameter must be state");

        // verify data channels
        int stateIndex = 0;
        int parameterIndex = 0;
        for (int i = 0; i < inputParameterKinds.size(); i++) {
            AggregationParameterKind parameterKind = inputParameterKinds.get(i);
            switch (parameterKind) {
                case STATE:
                    checkArgument(accumulatorStateDescriptors.get(stateIndex).getStateInterface() == parameters.get(i),
                            "State argument must be of type %s", accumulatorStateDescriptors.get(stateIndex).getStateInterface());
                    stateIndex++;
                    break;
                case BLOCK_INPUT_CHANNEL:
                case NULLABLE_BLOCK_INPUT_CHANNEL:
                    checkArgument(parameters.get(i) == Block.class, "Parameter must be Block if it has @BlockPosition");
                    parameterIndex++;
                    break;
                case INPUT_CHANNEL:
                    checkArgument(!parameters.get(i).isPrimitive() || SUPPORTED_PRIMITIVE_TYPES.contains(parameters.get(i)),
                            "Unsupported type: %s", parameters.get(i).getSimpleName());
                    verifyMethodParameterType(inputFunction, i, boundSignature.getArgumentTypes().get(parameterIndex));
                    parameterIndex++;
                    break;
                case BLOCK_INDEX:
                    checkArgument(parameters.get(i) == int.class, "Block index parameter must be an int");
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported parameter: " + parameterKind);
            }
        }
        checkArgument(stateIndex == accumulatorStateDescriptors.size(), "Input function only has %s states, expected: %s", stateIndex, accumulatorStateDescriptors.size());

        // verify lambda channels
        for (int i = 0; i < lambdaInterfaces.size(); i++) {
            verifyMethodParameterType(inputFunction, i + inputParameterKinds.size(), lambdaInterfaces.get(i), "function");
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

    private static void verifyMethodParameterType(MethodHandle inputFunction, int i, Type type)
    {
        verifyMethodParameterType(inputFunction, i, type.getJavaType(), type.getDisplayName());
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
