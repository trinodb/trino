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

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class InterpretedAggregationMaskBuilder
        implements AggregationMaskBuilder
{
    private final List<ChannelNullCheck> nullChecks;
    private int[] selectedPositions = new int[0];

    public InterpretedAggregationMaskBuilder(int... nonNullArguments)
    {
        this.nullChecks = Arrays.stream(nonNullArguments)
                .mapToObj(ChannelNullCheck::new)
                .collect(toImmutableList());
    }

    @Override
    public AggregationMask buildAggregationMask(Page arguments, Optional<Block> optionalMaskBlock)
    {
        int positionCount = arguments.getPositionCount();

        // if page is empty, we are done
        if (positionCount == 0) {
            return AggregationMask.createSelectNone(positionCount);
        }

        Block maskBlock = optionalMaskBlock.orElse(null);
        boolean hasMaskBlock = maskBlock != null;
        boolean maskBlockMayHaveNull = hasMaskBlock && maskBlock.mayHaveNull();
        if (maskBlock instanceof RunLengthEncodedBlock rle) {
            if (!testMaskBlock(rle.getValue(), maskBlockMayHaveNull, 0)) {
                return AggregationMask.createSelectNone(positionCount);
            }
            // mask block is always true, so do not evaluate mask block
            hasMaskBlock = false;
            maskBlockMayHaveNull = false;
        }

        for (ChannelNullCheck nullCheck : nullChecks) {
            nullCheck.reset(arguments);
            if (nullCheck.isAlwaysNull()) {
                return AggregationMask.createSelectNone(positionCount);
            }
        }

        // if there is no mask block, and all non-null arguments do not have nulls, we are done
        if (!hasMaskBlock && nullChecks.stream().noneMatch(ChannelNullCheck::mayHaveNull)) {
            return AggregationMask.createSelectAll(positionCount);
        }

        // grow the selection array if necessary
        int[] selectedPositions = this.selectedPositions;
        if (selectedPositions.length < positionCount) {
            selectedPositions = new int[positionCount];
            this.selectedPositions = selectedPositions;
        }

        // add all positions that pass the tests
        int selectedPositionsIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = i;
            if (testMaskBlock(maskBlock, maskBlockMayHaveNull, position) && nullChecks.stream().allMatch(arg -> arg.isNotNull(position))) {
                selectedPositions[selectedPositionsIndex] = position;
                selectedPositionsIndex++;
            }
        }
        return AggregationMask.createSelectedPositions(positionCount, selectedPositions, selectedPositionsIndex);
    }

    private static boolean testMaskBlock(Block block, boolean mayHaveNulls, int position)
    {
        if (block == null) {
            return true;
        }
        if (mayHaveNulls && block.isNull(position)) {
            return false;
        }
        return block.getByte(position, 0) != 0;
    }

    private static final class ChannelNullCheck
    {
        private final int channel;
        private Block block;
        private boolean mayHaveNull;

        public ChannelNullCheck(int channel)
        {
            this.channel = channel;
        }

        public void reset(Page arguments)
        {
            block = arguments.getBlock(channel);
            mayHaveNull = block.mayHaveNull();
        }

        public boolean mayHaveNull()
        {
            return mayHaveNull;
        }

        private boolean isAlwaysNull()
        {
            if (block instanceof RunLengthEncodedBlock rle) {
                return rle.getValue().isNull(0);
            }
            return false;
        }

        private boolean isNotNull(int position)
        {
            return !mayHaveNull || !block.isNull(position);
        }
    }
}
