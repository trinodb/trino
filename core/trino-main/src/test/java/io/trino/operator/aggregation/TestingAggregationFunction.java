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
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.BoundSignature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static java.util.Objects.requireNonNull;

public class TestingAggregationFunction
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private final InternalAggregationFunction function;
    private final List<Type> parameterTypes;
    private final List<TypeSignature> intermediateTypes;
    private final Type finalType;

    public TestingAggregationFunction(BoundSignature signature, AggregationFunctionMetadata aggregationFunctionMetadata, InternalAggregationFunction function)
    {
        this.parameterTypes = signature.getArgumentTypes();
        this.intermediateTypes = requireNonNull(aggregationFunctionMetadata, "aggregationFunctionMetadata is null").getIntermediateTypes();
        this.finalType = signature.getReturnType();
        this.function = requireNonNull(function, "function is null");
    }

    public int getParameterCount()
    {
        return parameterTypes.size();
    }

    public List<Type> getParameterTypes()
    {
        return parameterTypes;
    }

    public List<TypeSignature> getIntermediateType()
    {
        return intermediateTypes;
    }

    public Type getFinalType()
    {
        return finalType;
    }

    public AccumulatorFactory bind(List<Integer> inputChannels, Optional<Integer> maskChannel)
    {
        return function.bind(inputChannels, maskChannel);
    }

    public AccumulatorFactory bindDistinct(
            List<Integer> inputChannels,
            List<Type> sourceTypes,
            Optional<Integer> maskChannel)
    {
        return function.bind(
                inputChannels,
                maskChannel,
                sourceTypes,
                ImmutableList.of(),
                ImmutableList.of(),
                null,
                true, // distinct
                new JoinCompiler(TYPE_OPERATORS),
                new BlockTypeOperators(TYPE_OPERATORS),
                ImmutableList.of(),
                TEST_SESSION);
    }
}
