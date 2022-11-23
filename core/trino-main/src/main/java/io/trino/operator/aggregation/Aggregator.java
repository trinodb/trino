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

import com.google.common.primitives.Ints;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.AggregationNode.Step;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Aggregator
{
    private final Accumulator accumulator;
    private final Step step;
    private final Type intermediateType;
    private final Type finalType;
    private final int[] inputChannels;
    private final OptionalInt maskChannel;

    public Aggregator(Accumulator accumulator, Step step, Type intermediateType, Type finalType, List<Integer> inputChannels, OptionalInt maskChannel)
    {
        this.accumulator = requireNonNull(accumulator, "accumulator is null");
        this.step = requireNonNull(step, "step is null");
        this.intermediateType = requireNonNull(intermediateType, "intermediateType is null");
        this.finalType = requireNonNull(finalType, "finalType is null");
        this.inputChannels = Ints.toArray(requireNonNull(inputChannels, "inputChannels is null"));
        this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
        checkArgument(step.isInputRaw() || inputChannels.size() == 1, "expected 1 input channel for intermediate aggregation");
    }

    public Type getType()
    {
        if (step.isOutputPartial()) {
            return intermediateType;
        }
        return finalType;
    }

    public void processPage(Page page)
    {
        if (step.isInputRaw()) {
            accumulator.addInput(page.getColumns(inputChannels), getMaskBlock(page));
        }
        else {
            accumulator.addIntermediate(page.getBlock(inputChannels[0]));
        }
    }

    private Optional<Block> getMaskBlock(Page page)
    {
        if (maskChannel.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(page.getBlock(maskChannel.getAsInt()));
    }

    public void evaluate(BlockBuilder blockBuilder)
    {
        if (step.isOutputPartial()) {
            accumulator.evaluateIntermediate(blockBuilder);
        }
        else {
            accumulator.evaluateFinal(blockBuilder);
        }
    }

    public long getEstimatedSize()
    {
        return accumulator.getEstimatedSize();
    }
}
