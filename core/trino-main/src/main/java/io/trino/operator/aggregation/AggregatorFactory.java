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
import io.trino.operator.AggregationMetrics;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.AggregationNode.Step;

import java.util.List;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AggregatorFactory
{
    private final AccumulatorFactory accumulatorFactory;
    private final Step step;
    private final Type intermediateType;
    private final Type finalType;
    private final List<Integer> inputChannels;
    private final OptionalInt maskChannel;
    private final boolean spillable;
    private final List<Supplier<Object>> lambdaProviders;

    public AggregatorFactory(
            AccumulatorFactory accumulatorFactory,
            Step step,
            Type intermediateType,
            Type finalType,
            List<Integer> inputChannels,
            OptionalInt maskChannel,
            boolean spillable,
            List<Supplier<Object>> lambdaProviders)
    {
        this.accumulatorFactory = requireNonNull(accumulatorFactory, "accumulatorFactory is null");
        this.step = requireNonNull(step, "step is null");
        this.intermediateType = requireNonNull(intermediateType, "intermediateType is null");
        this.finalType = requireNonNull(finalType, "finalType is null");
        this.inputChannels = ImmutableList.copyOf(requireNonNull(inputChannels, "inputChannels is null"));
        this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
        this.spillable = spillable;
        this.lambdaProviders = ImmutableList.copyOf(requireNonNull(lambdaProviders, "lambdaProviders is null"));

        checkArgument(step.isInputRaw() || inputChannels.size() == 1, "expected 1 input channel for intermediate aggregation");
    }

    public Aggregator createAggregator(AggregationMetrics metrics)
    {
        Accumulator accumulator;
        if (step.isInputRaw()) {
            accumulator = accumulatorFactory.createAccumulator(lambdaProviders);
        }
        else {
            accumulator = accumulatorFactory.createIntermediateAccumulator(lambdaProviders);
        }
        return new Aggregator(accumulator, step, intermediateType, finalType, inputChannels, maskChannel, accumulatorFactory.createAggregationMaskBuilder(), metrics);
    }

    public GroupedAggregator createGroupedAggregator(AggregationMetrics metrics)
    {
        GroupedAccumulator accumulator;
        if (step.isInputRaw()) {
            accumulator = accumulatorFactory.createGroupedAccumulator(lambdaProviders);
        }
        else {
            accumulator = accumulatorFactory.createGroupedIntermediateAccumulator(lambdaProviders);
        }
        return new GroupedAggregator(accumulator, step, intermediateType, finalType, inputChannels, maskChannel, accumulatorFactory.createAggregationMaskBuilder(), metrics);
    }

    public GroupedAggregator createUnspillGroupedAggregator(Step step, int inputChannel, AggregationMetrics metrics)
    {
        GroupedAccumulator accumulator;
        if (step.isInputRaw()) {
            accumulator = accumulatorFactory.createGroupedAccumulator(lambdaProviders);
        }
        else {
            accumulator = accumulatorFactory.createGroupedIntermediateAccumulator(lambdaProviders);
        }
        return new GroupedAggregator(accumulator, step, intermediateType, finalType, ImmutableList.of(inputChannel), maskChannel, accumulatorFactory.createAggregationMaskBuilder(), metrics);
    }

    public boolean isSpillable()
    {
        return spillable;
    }
}
