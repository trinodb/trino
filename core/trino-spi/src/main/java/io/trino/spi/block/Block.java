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
package io.trino.spi.block;

import java.util.function.ObjLongConsumer;

import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;

public sealed interface Block
        permits DictionaryBlock, RunLengthEncodedBlock, ValueBlock
{
    /**
     * Gets the value at the specified position as a single element block.  The method
     * must copy the data into a new block.
     * <p>
     * This method is useful for operators that hold on to a single value without
     * holding on to the entire block.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    ValueBlock getSingleValueBlock(int position);

    /**
     * Returns the number of positions in this block.
     */
    int getPositionCount();

    /**
     * Returns the estimated size of this block as if were fully expanded.
     * This size includes the extra space to represent null values.
     * For example, the size of an RLE block is the size of the repeated value
     * times the number of positions in the block. The size of a dictionary
     * block is the average size of a dictionary entry times the number of
     * positions in the block.
     */
    long getSizeInBytes();

    /**
     * Returns the size of {@code block.getRegion(position, length)}.
     */
    long getRegionSizeInBytes(int position, int length);

    /**
     * Returns the retained size of this block in memory, including over-allocations.
     * This method is called from the inner most execution loop and must be fast.
     */
    long getRetainedSizeInBytes();

    /**
     * Returns the estimated in memory data size for stats of position.
     * Do not use it for other purpose.
     */
    long getEstimatedDataSizeForStats(int position);

    /**
     * {@code consumer} visits each of the internal data container and accepts the size for it.
     * This method can be helpful in cases such as memory counting for internal data structure.
     * Also, the method should be non-recursive, only visit the elements at the top level,
     * and specifically should not call retainedBytesForEachPart on nested blocks
     * {@code consumer} should be called at least once with the current block and
     * must include the instance size of the current block
     */
    void retainedBytesForEachPart(ObjLongConsumer<Object> consumer);

    /**
     * Create a new block from the current block by keeping the same elements only with respect
     * to {@code positions} that starts at {@code offset} and has length of {@code length}. The
     * implementation may return a view over the data in this block or may return a copy, and the
     * implementation is allowed to retain the positions array for use in the view.
     */
    default Block getPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        return DictionaryBlock.createInternal(offset, length, this, positions, randomDictionaryId());
    }

    /**
     * Returns a block containing the specified positions.
     * Positions to copy are stored in a subarray within {@code positions} array
     * that starts at {@code offset} and has length of {@code length}.
     * All specified positions must be valid for this block.
     * <p>
     * The returned block must be a compact representation of the original block.
     */
    Block copyPositions(int[] positions, int offset, int length);

    /**
     * Returns a block starting at the specified position and extends for the
     * specified length.  The specified region must be entirely contained
     * within this block.
     * <p>
     * The region can be a view over this block.  If this block is released
     * the region block may also be released.  If the region block is released
     * this block may also be released.
     */
    Block getRegion(int positionOffset, int length);

    /**
     * Returns a block starting at the specified position and extends for the
     * specified length.  The specified region must be entirely contained
     * within this block.
     * <p>
     * The region returned must be a compact representation of the original block, unless their internal
     * representation will be exactly the same. This method is useful for
     * operators that hold on to a range of values without holding on to the
     * entire block.
     */
    Block copyRegion(int position, int length);

    /**
     * Is it possible the block may have a null value?  If false, the block cannot contain
     * a null, but if true, the block may or may not have a null.
     */
    default boolean mayHaveNull()
    {
        return true;
    }

    /**
     * Does this block have a null value? This method is expected to be O(N).
     */
    default boolean hasNull()
    {
        for (int i = 0; i < getPositionCount(); i++) {
            if (isNull(i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Is the specified position null?
     *
     * @throws IllegalArgumentException if this position is not valid. The method may return false
     * without throwing exception when there are no nulls in the block, even if the position is invalid
     */
    boolean isNull(int position);

    /**
     * Returns a block that contains a copy of the contents of the current block, and an appended null at the end. The
     * original block will not be modified. The purpose of this method is to leverage the contents of a block and the
     * structure of the implementation to efficiently produce a copy of the block with a NULL element inserted - so that
     * it can be used as a dictionary. This method is expected to be invoked on completely built {@link Block} instances
     * i.e. not on in-progress block builders.
     */
    Block copyWithAppendedNull();

    /**
     * Returns the underlying value block underlying this block.
     */
    ValueBlock getUnderlyingValueBlock();

    /**
     * Returns the position in the underlying value block corresponding to the specified position in this block.
     */
    int getUnderlyingValuePosition(int position);
}
