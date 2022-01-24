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
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionNullability;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.AggregationNode.Step;

import java.util.List;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.GroupByHashFactoryTestUtils.createGroupByHashFactory;
import static io.trino.operator.aggregation.AccumulatorCompiler.generateAccumulatorFactory;
import static java.util.Objects.requireNonNull;

public class TestingAggregationFunction
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private final List<Type> parameterTypes;
    private final Type intermediateType;
    private final Type finalType;

    private final AccumulatorFactory factory;
    private final DistinctAccumulatorFactory distinctFactory;

    public TestingAggregationFunction(BoundSignature signature, FunctionNullability functionNullability, AggregationMetadata aggregationMetadata)
    {
        this.parameterTypes = signature.getArgumentTypes();
        List<Type> intermediateTypes = aggregationMetadata.getAccumulatorStateDescriptors().stream()
                .map(stateDescriptor -> stateDescriptor.getSerializer().getSerializedType())
                .collect(toImmutableList());
        intermediateType = (intermediateTypes.size() == 1) ? getOnlyElement(intermediateTypes) : RowType.anonymous(intermediateTypes);
        this.finalType = signature.getReturnType();
        this.factory = generateAccumulatorFactory(signature, aggregationMetadata, functionNullability);
        distinctFactory = new DistinctAccumulatorFactory(
                factory,
                parameterTypes,
                createGroupByHashFactory(TYPE_OPERATORS),
                TEST_SESSION);
    }

    public TestingAggregationFunction(List<Type> parameterTypes, List<Type> intermediateTypes, Type finalType, AccumulatorFactory factory)
    {
        this.parameterTypes = ImmutableList.copyOf(requireNonNull(parameterTypes, "parameterTypes is null"));
        requireNonNull(intermediateTypes, "intermediateTypes is null");
        this.intermediateType = (intermediateTypes.size() == 1) ? getOnlyElement(intermediateTypes) : RowType.anonymous(intermediateTypes);
        this.finalType = requireNonNull(finalType, "finalType is null");
        this.factory = requireNonNull(factory, "factory is null");
        distinctFactory = new DistinctAccumulatorFactory(
                factory,
                parameterTypes,
                createGroupByHashFactory(TYPE_OPERATORS),
                TEST_SESSION);
    }

    public int getParameterCount()
    {
        return parameterTypes.size();
    }

    public List<Type> getParameterTypes()
    {
        return parameterTypes;
    }

    public Type getIntermediateType()
    {
        return intermediateType;
    }

    public Type getFinalType()
    {
        return finalType;
    }

    public AggregatorFactory createAggregatorFactory(Step step, List<Integer> inputChannels, OptionalInt maskChannel)
    {
        return createAggregatorFactory(step, inputChannels, maskChannel, factory);
    }

    public AggregatorFactory createDistinctAggregatorFactory(Step step, List<Integer> inputChannels, OptionalInt maskChannel)
    {
        return createAggregatorFactory(step, inputChannels, maskChannel, distinctFactory);
    }

    private AggregatorFactory createAggregatorFactory(Step step, List<Integer> inputChannels, OptionalInt maskChannel, AccumulatorFactory distinctFactory)
    {
        return new AggregatorFactory(
                distinctFactory,
                step,
                intermediateType,
                finalType,
                inputChannels,
                OptionalInt.empty(), // TODO lysy:
                maskChannel,
                true,
                ImmutableList.of());
    }
}
