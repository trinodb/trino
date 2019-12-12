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
package io.prestosql.spi.block;

import static io.prestosql.spi.block.BlockUtil.checkValidRegion;
import static java.util.Objects.requireNonNull;

public class SelectedPositions
{
    private final boolean isList;
    private final int[] positions;
    private final int offset;
    private final int size;

    public static SelectedPositions positionsList(int[] positions, int offset, int size)
    {
        return new SelectedPositions(true, positions, offset, size);
    }

    public static SelectedPositions positionsRange(int offset, int size)
    {
        return new SelectedPositions(false, new int[0], offset, size);
    }

    private SelectedPositions(boolean isList, int[] positions, int offset, int size)
    {
        this.isList = isList;
        this.positions = requireNonNull(positions, "positions is null");
        this.offset = offset;
        this.size = size;

        if (offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size is negative");
        }

        if (isList) {
            checkValidRegion(positions.length, offset, size);
        }
    }

    public boolean isList()
    {
        return isList;
    }

    public boolean isRange()
    {
        return !isList;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public int[] getPositions()
    {
        if (!isList) {
            throw new IllegalStateException("SelectedPositions is a range");
        }
        return positions;
    }

    public int getOffset()
    {
        return offset;
    }

    public int size()
    {
        return size;
    }

    public SelectedPositions subRange(int start, int end)
    {
        checkValidRegion(size, start, end - start);

        int newOffset = this.offset + start;
        int newLength = end - start;
        return new SelectedPositions(isList, positions, newOffset, newLength);
    }
}
