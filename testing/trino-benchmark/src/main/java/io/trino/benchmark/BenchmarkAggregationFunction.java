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
package io.trino.benchmark;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.aggregation.AccumulatorFactory;
import io.trino.operator.aggregation.AggregationMetadata;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.AggregationNode.Step;

import java.util.List;
import java.util.OptionalInt;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.aggregation.AccumulatorCompiler.generateAccumulatorFactory;

public class BenchmarkAggregationFunction
{
    private final Type intermediateType;
    private final AccumulatorFactory accumulatorFactory;
    private final Type finalType;

    public BenchmarkAggregationFunction(ResolvedFunction resolvedFunction, AggregationMetadata aggregationMetadata)
    {
        BoundSignature signature = resolvedFunction.getSignature();
        intermediateType = getOnlyElement(aggregationMetadata.getAccumulatorStateDescriptors()).getSerializer().getSerializedType();
        finalType = signature.getReturnType();
        accumulatorFactory = generateAccumulatorFactory(signature, aggregationMetadata, resolvedFunction.getFunctionNullability());
    }

    public AggregatorFactory bind(List<Integer> inputChannels)
    {
        return new AggregatorFactory(
                accumulatorFactory,
                Step.SINGLE,
                intermediateType,
                finalType,
                inputChannels,
                OptionalInt.empty(), // TODO lysy:
                OptionalInt.empty(),
                true,
                ImmutableList.of());
    }
}
