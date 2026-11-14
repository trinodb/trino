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

import io.trino.spi.Unstable;
import io.trino.spi.block.Block;
import io.trino.spi.block.ValueBlock;

import static java.util.Arrays.copyOfRange;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

/**
 * Positions of a page that must be evaluated.
 * <p>
 * The positions are either a contiguous range starting at {@link #offset()}, or
 * a list stored in {@link #positions()} starting at index {@link #offset()}.
 * In both cases, {@link #size()} is the number of selected positions.
 */
@Unstable
public final class SelectedPositions
{
    private static final int[] EMPTY_POSITIONS = new int[0];

    private final boolean list;
    private final int[] positions;
    private final int offset;
    private final int size;

    public static SelectedPositions positionsRange(int offset, int size)
    {
        return new SelectedPositions(false, EMPTY_POSITIONS, offset, size);
    }

    /**
     * The positions array is not copied and must not be modified.
     */
    public static SelectedPositions positionsList(int[] positions, int offset, int size)
    {
        requireNonNull(positions, "positions is null");
        checkFromIndexSize(offset, size, positions.length);
        return new SelectedPositions(true, positions, offset, size);
    }

    private SelectedPositions(boolean list, int[] positions, int offset, int size)
    {
        if (offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
        this.list = list;
        this.positions = positions;
        this.offset = offset;
        this.size = size;
    }

    public boolean isList()
    {
        return list;
    }

    /**
     * Returns the array containing the selected positions, starting at index {@link #offset()}.
     * The array must not be modified.
     *
     * @throws IllegalStateException if the positions are a range
     */
    public int[] positions()
    {
        if (!list) {
            throw new IllegalStateException("Selected positions are a range");
        }
        return positions;
    }

    /**
     * Returns the first selected position for a range, or the index of the first
     * selected position in {@link #positions()} for a list.
     */
    public int offset()
    {
        return offset;
    }

    public int size()
    {
        return size;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    /**
     * Returns the selected position at the specified index, which must be between
     * zero and {@link #size()}, exclusive.
     */
    public int position(int index)
    {
        checkIndex(index, size);
        if (list) {
            return positions[offset + index];
        }
        return offset + index;
    }

    /**
     * Returns a block containing the selected positions of the specified block, without
     * copying the values of the block.
     */
    public Block select(Block block)
    {
        if (list) {
            if (block instanceof ValueBlock) {
                // The view retains the positions array, which the caller may reuse for the next page
                return block.getPositions(copyOfRange(positions, offset, offset + size), 0, size);
            }
            return block.getPositions(positions, offset, size);
        }
        if (offset == 0 && size == block.getPositionCount()) {
            return block;
        }
        return block.getRegion(offset, size);
    }

    @Override
    public String toString()
    {
        if (list) {
            return "SelectedPositions{list, offset=%s, size=%s}".formatted(offset, size);
        }
        return "SelectedPositions{range, offset=%s, size=%s}".formatted(offset, size);
    }
}
