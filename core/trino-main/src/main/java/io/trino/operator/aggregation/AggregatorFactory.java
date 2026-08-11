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
import java.util.Optional;
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
    private final Optional<AccumulatorFactory> unspillAccumulatorFactory;
    private final List<Supplier<Object>> lambdaProviders;

    public AggregatorFactory(
            AccumulatorFactory accumulatorFactory,
            Step step,
            Type intermediateType,
            Type finalType,
            List<Integer> inputChannels,
            OptionalInt maskChannel,
            boolean spillable,
            Optional<AccumulatorFactory> unspillAccumulatorFactory,
            List<Supplier<Object>> lambdaProviders)
    {
        this.accumulatorFactory = requireNonNull(accumulatorFactory, "accumulatorFactory is null");
        this.step = requireNonNull(step, "step is null");
        this.intermediateType = requireNonNull(intermediateType, "intermediateType is null");
        this.finalType = requireNonNull(finalType, "finalType is null");
        this.inputChannels = ImmutableList.copyOf(requireNonNull(inputChannels, "inputChannels is null"));
        this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
        this.spillable = spillable;
        this.unspillAccumulatorFactory = requireNonNull(unspillAccumulatorFactory, "unspillAccumulatorFactory is null");
        this.lambdaProviders = ImmutableList.copyOf(requireNonNull(lambdaProviders, "lambdaProviders is null"));

        checkArgument(step.isInputRaw() || inputChannels.size() == 1, "expected 1 input channel for intermediate aggregation");
        checkArgument(accumulatorFactory.isLegacyDecomposition() || !spillable || unspillAccumulatorFactory.isPresent(),
                "unspillAccumulatorFactory is required for a spillable aggregation with declared decomposition");
    }

    public Type getOutputType()
    {
        // Note: this must match Aggregator#getType() and GroupedAggregator#getType()
        return step.isOutputPartial() ? intermediateType : finalType;
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
        if (!accumulatorFactory.isLegacyDecomposition()) {
            // With a declared decomposition there is no combine to merge serialized states. Instead, the
            // spilled intermediate state is consumed as raw input by the function resolved over the
            // intermediate type, which shares the state representation with this aggregation.
            AccumulatorFactory unspillFactory = unspillAccumulatorFactory.orElseThrow();
            // The requested step consumes intermediate input; the unspill accumulator consumes it as raw
            // input instead, while producing the same intermediate or final output
            Step rawInputStep = step.isOutputPartial() ? Step.PARTIAL : Step.SINGLE;
            return new GroupedAggregator(
                    unspillFactory.createGroupedAccumulator(lambdaProviders),
                    rawInputStep,
                    intermediateType,
                    finalType,
                    ImmutableList.of(inputChannel),
                    OptionalInt.empty(),
                    unspillFactory.createAggregationMaskBuilder(),
                    metrics);
        }

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
