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
package io.trino.operator.join;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfIntArray;

/**
 * Maps a flat row number to the block that contains it and the position within that block, from the
 * per-block position counts alone, so position resolution reads small directories instead of a large
 * per-row address array. {@code blockStarts} is the
 * prefix sum of the counts; {@code windowToBlock} maps each window of {@code 2^windowShift} row numbers
 * to the block it starts in, so a lookup is one array read plus a short forward scan. {@code windowShift}
 * is chosen at construction as the smallest that fits {@code windowToBlock} within
 * {@link #WINDOW_TO_BLOCK_BUDGET_BYTES}, giving the densest directory the budget allows. The expected scan
 * is about {@code 2^windowShift / average block rows} steps, so small builds get a one-step-or-less lookup.
 */
public final class BlockPositionIndex
{
    private static final int INSTANCE_SIZE = instanceSize(BlockPositionIndex.class);
    // Memory budget for the windowToBlock directory; the window shift is the smallest that keeps the directory within it.
    private static final int WINDOW_TO_BLOCK_BUDGET_BYTES = 512 * 1024;

    private final int windowShift;
    private final int[] blockStarts;
    private final int[] windowToBlock;

    public BlockPositionIndex(IntArrayList positionCounts)
    {
        int blockCount = positionCounts.size();
        blockStarts = new int[blockCount + 1];
        for (int blockIndex = 0; blockIndex < blockCount; blockIndex++) {
            blockStarts[blockIndex + 1] = blockStarts[blockIndex] + positionCounts.getInt(blockIndex);
        }
        int positionCount = blockStarts[blockCount];

        windowShift = windowShift(positionCount);
        int windowCount = windowCount(positionCount, windowShift);
        windowToBlock = new int[windowCount];
        int blockIndex = 0;
        for (int window = 0; window < windowCount; window++) {
            long windowStartRow = ((long) window) << windowShift;
            while (blockIndex + 1 < blockCount && blockStarts[blockIndex + 1] <= windowStartRow) {
                blockIndex++;
            }
            windowToBlock[window] = blockIndex;
        }
    }

    /**
     * @param rowNumber must satisfy {@code 0 <= rowNumber < positionCount}
     */
    public int decodeBlockIndex(int rowNumber)
    {
        int blockIndex = windowToBlock[rowNumber >>> windowShift];
        while (rowNumber >= blockStarts[blockIndex + 1]) {
            blockIndex++;
        }
        return blockIndex;
    }

    public int decodePosition(int rowNumber, int blockIndex)
    {
        return rowNumber - blockStarts[blockIndex];
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(blockStarts) + sizeOf(windowToBlock);
    }

    public static long getEstimatedRetainedSizeInBytes(int blockCount, int positionCount)
    {
        int windowShift = windowShift(positionCount);
        return INSTANCE_SIZE + sizeOfIntArray(blockCount + 1) + sizeOfIntArray(windowCount(positionCount, windowShift));
    }

    // Smallest shift whose windowToBlock fits the budget; smaller shift means a denser directory and shorter scans.
    private static int windowShift(int positionCount)
    {
        int maxWindowEntries = WINDOW_TO_BLOCK_BUDGET_BYTES / Integer.BYTES;
        int windowShift = 0;
        if (positionCount > 0) {
            while ((((positionCount - 1) >>> windowShift) + 1) > maxWindowEntries) {
                windowShift++;
            }
        }
        return windowShift;
    }

    private static int windowCount(int positionCount, int windowShift)
    {
        if (positionCount == 0) {
            return 0;
        }
        return ((positionCount - 1) >>> windowShift) + 1;
    }
}
