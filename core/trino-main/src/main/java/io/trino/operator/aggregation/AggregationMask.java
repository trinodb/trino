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
import jakarta.annotation.Nullable;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class AggregationMask
{
    private static final int[] NO_SELECTED_POSITIONS = new int[0];

    private int positionCount;
    private int[] selectedPositions = NO_SELECTED_POSITIONS;
    private int selectedPositionCount;

    public static AggregationMask createSelectNone(int positionCount)
    {
        return createSelectedPositions(positionCount, NO_SELECTED_POSITIONS, 0);
    }

    public static AggregationMask createSelectAll(int positionCount)
    {
        return new AggregationMask(positionCount);
    }

    /**
     * Creates a mask with the given selected positions. Selected positions must be sorted in ascending order.
     */
    public static AggregationMask createSelectedPositions(int positionCount, int[] selectedPositions, int selectedPositionCount)
    {
        return new AggregationMask(positionCount, selectedPositions, selectedPositionCount);
    }

    private AggregationMask(int positionCount)
    {
        reset(positionCount);
    }

    private AggregationMask(int positionCount, int[] selectedPositions, int selectedPositionCount)
    {
        checkArgument(positionCount >= 0, "positionCount is negative");
        checkArgument(selectedPositionCount >= 0, "selectedPositionCount is negative");
        checkArgument(selectedPositionCount <= positionCount, "selectedPositionCount cannot be greater than positionCount");
        requireNonNull(selectedPositions, "selectedPositions is null");
        checkArgument(selectedPositions.length >= selectedPositionCount, "selectedPosition is smaller than selectedPositionCount");

        reset(positionCount);
        this.selectedPositions = selectedPositions;
        this.selectedPositionCount = selectedPositionCount;
    }

    public void reset(int positionCount)
    {
        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = positionCount;
        this.selectedPositionCount = positionCount;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public boolean isSelectAll()
    {
        return positionCount == selectedPositionCount;
    }

    public boolean isSelectNone()
    {
        return selectedPositionCount == 0;
    }

    public Page filterPage(Page page)
    {
        if (isSelectAll()) {
            return page;
        }
        if (isSelectNone()) {
            return page.getRegion(0, 0);
        }
        return page.getPositions(Arrays.copyOf(selectedPositions, selectedPositionCount), 0, selectedPositionCount);
    }

    /**
     * Do not use this to filter a page, as the underlying array can change, and this will change the page after the filtering.
     */
    public int getSelectedPositionCount()
    {
        return selectedPositionCount;
    }

    public int[] getSelectedPositions()
    {
        checkState(!isSelectAll(), "getSelectedPositions not available when in selectAll mode");
        return selectedPositions;
    }

    public void unselectNullPositions(Block block)
    {
        unselectPositions(block, false);
    }

    public void applyMaskBlock(@Nullable Block maskBlock)
    {
        if (maskBlock != null) {
            unselectPositions(maskBlock, true);
        }
    }

    private void unselectPositions(Block block, boolean shouldTestValues)
    {
        int positionCount = block.getPositionCount();
        checkArgument(positionCount == this.positionCount, "Block position count does not match current position count");
        if (isSelectNone()) {
            return;
        }

        // short circuit if there are no nulls, and we are not testing the value
        if (!block.mayHaveNull() && !shouldTestValues) {
            // all positions selected, so change nothing
            return;
        }

        if (block instanceof RunLengthEncodedBlock) {
            if (test(block, 0, shouldTestValues)) {
                // all positions selected, so change nothing
                return;
            }
            // no positions selected
            selectedPositionCount = 0;
            return;
        }

        if (positionCount == selectedPositionCount) {
            if (selectedPositions.length < positionCount) {
                selectedPositions = new int[positionCount];
            }

            // add all positions that pass the test
            int selectedPositionsIndex = 0;
            for (int position = 0; position < positionCount; position++) {
                if (test(block, position, shouldTestValues)) {
                    selectedPositions[selectedPositionsIndex] = position;
                    selectedPositionsIndex++;
                }
            }
            selectedPositionCount = selectedPositionsIndex;
            return;
        }

        // keep only the positions that pass the test
        int originalIndex = 0;
        int newIndex = 0;
        for (; originalIndex < selectedPositionCount; originalIndex++) {
            int position = selectedPositions[originalIndex];
            if (test(block, position, shouldTestValues)) {
                selectedPositions[newIndex] = position;
                newIndex++;
            }
        }
        selectedPositionCount = newIndex;
    }

    private static boolean test(Block block, int position, boolean testValue)
    {
        if (block.isNull(position)) {
            return false;
        }
        if (testValue && block.getByte(position, 0) == 0) {
            return false;
        }
        return true;
    }
}
