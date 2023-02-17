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
package io.trino.operator.output;

import io.trino.spi.block.Block;
import it.unimi.dsi.fastutil.ints.IntArrayList;

public interface PositionsAppender
{
    void append(IntArrayList positions, Block source);

    /**
     * Appends the specified value positionCount times.
     * The result is the same as with using {@link PositionsAppender#append(IntArrayList, Block)} with
     * positions list [0...positionCount -1] but with possible performance optimizations.
     */
    void appendRle(Block value, int rlePositionCount);

    /**
     * Appends single position. The implementation must be conceptually equal to
     * {@code append(IntArrayList.wrap(new int[] {position}), source)} but may be optimized.
     * Caller should avoid using this method if {@link #append(IntArrayList, Block)} can be used
     * as appending positions one by one can be significantly slower and may not support features
     * like pushing RLE through the appender.
     */
    void append(int position, Block source);

    /**
     * Creates the block from the appender data.
     * After this, appender is reset to the initial state, and it is ready to build a new block.
     */
    Block build();

    /**
     * Returns number of bytes retained by this instance in memory including over-allocations.
     */
    long getRetainedSizeInBytes();

    /**
     * Returns the size of memory in bytes used by this appender.
     */
    long getSizeInBytes();
}
