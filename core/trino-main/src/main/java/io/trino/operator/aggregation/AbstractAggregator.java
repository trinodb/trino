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
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.AggregationNode;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class AbstractAggregator
{
    private AggregationNode.Step step;
    private final Type intermediateType;
    private final Type finalType;
    private final int[] aggregationRawInputChannels;
    private final OptionalInt intermediateStateChannel;
    private final OptionalInt rawInputMaskChannel;
    private final OptionalInt maskChannel;

    AbstractAggregator(
            AggregationNode.Step step,
            Type intermediateType,
            Type finalType,
            List<Integer> aggregationRawInputChannels,
            OptionalInt intermediateStateChannel,
            OptionalInt rawInputMaskChannel,
            OptionalInt maskChannel)
    {
        this.step = requireNonNull(step, "step is null");
        this.intermediateType = requireNonNull(intermediateType, "intermediateType is null");
        this.finalType = requireNonNull(finalType, "finalType is null");
        this.aggregationRawInputChannels = Ints.toArray(requireNonNull(aggregationRawInputChannels, "aggregationRawInputChannels is null"));
        this.intermediateStateChannel = requireNonNull(intermediateStateChannel, "intermediateStateChannel is null");
        this.rawInputMaskChannel = requireNonNull(rawInputMaskChannel, "rawInputMaskChannel is null");
        this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
        checkArgument(step.isInputRaw() || intermediateStateChannel.isPresent(), "expected intermediateStateChannel for intermediate aggregation but got %s ", intermediateStateChannel);
    }

    protected void processPage(Page page, AccumulatorWrapper accumulator)
    {
        if (step.isInputRaw()) {
            accumulator.addInput(page.getColumns(aggregationRawInputChannels), getMaskBlock(page));
            return;
        }

        if (rawInputMaskChannel.isEmpty()) {
            // process partially aggregated data
            accumulator.addIntermediate(page.getBlock(intermediateStateChannel.getAsInt()));
            return;
        }
        Block rawInputMaskBlock = page.getBlock(rawInputMaskChannel.getAsInt());
        if (rawInputMaskBlock instanceof RunLengthEncodedBlock) {
            if (rawInputMaskBlock.isNull(0)) {
                // process partially aggregated data
                accumulator.addIntermediate(page.getBlock(intermediateStateChannel.getAsInt()));
            }
            else {
                // process raw data=
                accumulator.addInput(page.getColumns(aggregationRawInputChannels), getMaskBlock(page));
            }
            return;
        }

        // rawInputMaskBlock has potentially mixed partially aggregated and raw data
        Block maskBlock = getMaskBlock(page)
                .map(mask -> andMasks(mask, rawInputMaskBlock))
                .orElse(rawInputMaskBlock);

        accumulator.addInput(page.getColumns(aggregationRawInputChannels), Optional.of(maskBlock));

        accumulator.addIntermediate(filterByNull(rawInputMaskBlock), page.getBlock(intermediateStateChannel.getAsInt()));
    }

    public Type getType()
    {
        if (step.isOutputPartial()) {
            return intermediateType;
        }
        else {
            return finalType;
        }
    }

    // todo this should return a new GroupedAggregator instead of modifying the existing object
    public void setSpillOutput()
    {
        step = AggregationNode.Step.partialOutput(step);
    }

    public Type getSpillType()
    {
        return intermediateType;
    }

    protected boolean isOutputPartial()
    {
        return step.isOutputPartial();
    }

    private Optional<Block> getMaskBlock(Page page)
    {
        if (maskChannel.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(page.getBlock(maskChannel.getAsInt()));
    }

    private static Block andMasks(Block mask1, Block mask2)
    {
        int positionCount = mask1.getPositionCount();
        byte[] mask = new byte[positionCount];
        for (int i = 0; i < positionCount; i++) {
            mask[i] = (byte) ((!mask1.isNull(i) && mask1.getByte(i, 0) == 1 && !mask2.isNull(i) && mask2.getByte(i, 0) == 1) ? 1 : 0);
        }
        return new ByteArrayBlock(positionCount, Optional.empty(), mask);
    }

    private static IntArrayList filterByNull(Block mask)
    {
        int positions = mask.getPositionCount();

        IntArrayList ids = new IntArrayList(positions);
        for (int i = 0; i < positions; ++i) {
            if (mask.isNull(i)) {
                ids.add(i);
            }
        }

        return ids;
    }

    public interface AccumulatorWrapper
    {
        void addInput(Page page, Optional<Block> maskBlock);

        void addIntermediate(Block block);

        void addIntermediate(IntArrayList positions, Block block);
    }
}
