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
package io.trino.spi.function;

import io.trino.spi.block.BlockBuilder;

public abstract class ValueWindowFunction
        implements WindowFunction
{
    protected WindowIndex windowIndex;

    private int currentPosition;

    @Override
    public final void reset(WindowIndex windowIndex)
    {
        this.windowIndex = windowIndex;
        this.currentPosition = 0;

        reset();
    }

    @Override
    public final void processRow(BlockBuilder output, int peerGroupStart, int peerGroupEnd, int frameStart, int frameEnd, int excludedStart, int excludedEnd, int keptRow)
    {
        processRow(output, frameStart, frameEnd, currentPosition, excludedStart, excludedEnd, keptRow);

        currentPosition++;
    }

    /**
     * Reset state for a new partition (including the first one).
     */
    public void reset()
    {
        // subclasses can override
    }

    /**
     * Process a row by outputting the result of the window function.
     * <p>
     * The window frame is the contiguous range of positions {@code [frameStart, frameEnd]}
     * with the positions {@code [excludedStart, excludedEnd]} removed, as directed by the
     * {@code EXCLUDE} clause of the frame specification. The excluded range is empty (excludes
     * nothing) when {@code excludedStart > excludedEnd}. {@code keptRow}, when non-negative, is
     * a single position within the excluded range that remains part of the frame (expressing
     * {@code EXCLUDE TIES}).
     *
     * @param output the {@link BlockBuilder} to use for writing the output row
     * @param frameStart the position of the first row in the window frame
     * @param frameEnd the position of the last row in the window frame
     * @param currentPosition the current position for this row
     * @param excludedStart the position of the first excluded row, or {@code 0} if no rows are excluded
     * @param excludedEnd the position of the last excluded row, or {@code -1} if no rows are excluded
     * @param keptRow a position within the excluded range that remains part of the frame, or {@code -1} if none
     */
    public abstract void processRow(BlockBuilder output, int frameStart, int frameEnd, int currentPosition, int excludedStart, int excludedEnd, int keptRow);
}
