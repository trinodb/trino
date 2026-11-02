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

public interface WindowFunction
{
    /**
     * Reset state for a new partition (including the first one).
     *
     * @param windowIndex the window index which contains sorted values for the partition
     */
    void reset(WindowIndex windowIndex);

    /**
     * Process a row by outputting the result of the window function.
     * <p>
     * This method provides information about the ordering peer group. A peer group is all
     * of the rows that are peers within the specified ordering. Rows are peers if they
     * compare equal to each other using the specified ordering expression. The ordering
     * of rows within a peer group is undefined (otherwise they would not be peers).
     * <p>
     * The window frame is the contiguous range of positions {@code [frameStart, frameEnd]}
     * with the positions {@code [excludedStart, excludedEnd]} removed, as directed by the
     * {@code EXCLUDE} clause of the frame specification. The excluded range is a contiguous
     * sub-range of the frame; when it is empty ({@code excludedStart > excludedEnd}) no
     * positions are excluded. {@code keptRow}, when non-negative, is a single position that
     * falls within the excluded range but is nevertheless part of the frame (this expresses
     * {@code EXCLUDE TIES}, which removes the current row's peers but keeps the current row).
     *
     * @param output the {@link BlockBuilder} to use for writing the output row
     * @param peerGroupStart the position of the first row in the peer group
     * @param peerGroupEnd the position of the last row in the peer group
     * @param frameStart the position of the first row in the window frame
     * @param frameEnd the position of the last row in the window frame
     * @param excludedStart the position of the first excluded row, or {@code 0} if no rows are excluded
     * @param excludedEnd the position of the last excluded row, or {@code -1} if no rows are excluded
     * @param keptRow a position within the excluded range that remains part of the frame, or {@code -1} if none
     */
    void processRow(BlockBuilder output, int peerGroupStart, int peerGroupEnd, int frameStart, int frameEnd, int excludedStart, int excludedEnd, int keptRow);
}
