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
import io.trino.operator.GroupByIdBlock;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Step;
import org.apache.commons.math3.util.Pair;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class GroupedAggregator
{
    private final GroupedAccumulator accumulator;
    private AggregationNode.Step step;
    private final Type intermediateType;
    private final Type finalType;
    private final int[] inputChannels;
    private final int intermediateStateChannel;
    private final OptionalInt useRawInputChannel;
    private final OptionalInt maskChannel;

    public GroupedAggregator(GroupedAccumulator accumulator, Step step, Type intermediateType, Type finalType, List<Integer> inputChannels, int intermediateStateChannel, OptionalInt useRawInputChannel, OptionalInt maskChannel)
    {
        this.accumulator = requireNonNull(accumulator, "accumulator is null");
        this.step = requireNonNull(step, "step is null");
        this.intermediateType = requireNonNull(intermediateType, "intermediateType is null");
        this.finalType = requireNonNull(finalType, "finalType is null");
        this.inputChannels = Ints.toArray(requireNonNull(inputChannels, "inputChannels is null"));
        this.intermediateStateChannel = intermediateStateChannel;
        this.useRawInputChannel = requireNonNull(useRawInputChannel, "useRawInputChannel is null");
        this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
//        checkArgument(step.isInputRaw() || inputChannels.size() == 1, "expected 1 input channel for intermediate aggregation");
        checkArgument(step.isInputRaw() || intermediateStateChannel != -1, "expected intermediateStateChannel for intermediate aggregation but got %s ", intermediateStateChannel);
    }

    public long getEstimatedSize()
    {
        return accumulator.getEstimatedSize();
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

    public void processPage(GroupByIdBlock groupIds, Page page)
    {
        if (step.isInputRaw()) {
            accumulator.addInput(groupIds, page.getColumns(inputChannels), getMaskBlock(page));
            return;
        }

        if (useRawInputChannel.isEmpty()) {
            // process grouped data
            accumulator.addIntermediate(groupIds, page.getBlock(intermediateStateChannel));
            return;
        }
        Block useRawInputBlock = page.getBlock(useRawInputChannel.getAsInt());
        if (useRawInputBlock instanceof RunLengthEncodedBlock) {
            if (useRawInputBlock.isNull(0)) {
                // process grouped data
                accumulator.addIntermediate(groupIds, page.getBlock(intermediateStateChannel));
            }
            else {
                // process raw data
                accumulator.addInput(groupIds, page.getColumns(inputChannels), getMaskBlock(page));
            }
            return;
        }

        // useRawInputBlock has potentially mixed grouped and raw data
        Optional<Block> maskBlock = Optional.of((getMaskBlock(page).map(mask -> andMasks(mask, useRawInputBlock)).orElse(useRawInputBlock)));
        accumulator.addInput(groupIds, page.getColumns(inputChannels), maskBlock);
        Pair<Block, GroupByIdBlock> filtered = filterByNull(page.getBlock(intermediateStateChannel), groupIds, useRawInputBlock);
        accumulator.addIntermediate(filtered.getSecond(), filtered.getFirst());
    }

    private Block andMasks(Block mask1, Block mask2)
    {
        int positionCount = mask1.getPositionCount();
        byte[] mask = new byte[positionCount];
        for (int i = 0; i < positionCount; i++) {
            mask[i] = (byte) ((!mask1.isNull(i) && mask1.getByte(i, 0) == 1 && !mask2.isNull(i) && mask2.getByte(i, 0) == 1) ? 1 : 0);
        }
        return new ByteArrayBlock(positionCount, Optional.empty(), mask);
    }

    private Optional<Block> getMaskBlock(Page page)
    {
        if (maskChannel.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(page.getBlock(maskChannel.getAsInt()));
    }

    public void prepareFinal()
    {
        accumulator.prepareFinal();
    }

    public void evaluate(int groupId, BlockBuilder output)
    {
        if (step.isOutputPartial()) {
            accumulator.evaluateIntermediate(groupId, output);
        }
        else {
            accumulator.evaluateFinal(groupId, output);
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

    private static Pair<Block, GroupByIdBlock> filterByNull(Block block, GroupByIdBlock groupByIdBlock, Block mask)
    {
        int positions = mask.getPositionCount();

        int[] ids = new int[positions];
        int next = 0;
        for (int i = 0; i < ids.length; ++i) {
            if (mask.isNull(i)) {
                ids[next++] = i;
            }
        }

        if (next == ids.length) {
            return Pair.create(block, groupByIdBlock); // no rows were eliminated by the filter
        }
        return Pair.create(
                block.getPositions(ids, 0, next),
                new GroupByIdBlock(groupByIdBlock.getGroupCount(), groupByIdBlock.getPositions(ids, 0, next)));
    }
}
