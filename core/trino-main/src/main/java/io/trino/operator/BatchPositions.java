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
package io.trino.operator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class BatchPositions
{
    private final boolean isList;
    private final int[] positions;
    private final int offset;
    private final int size;

    public static BatchPositions list(int[] positions, int offset, int size)
    {
        return new BatchPositions(true, positions, offset, size);
    }

    public static BatchPositions range(int offset, int size)
    {
        return new BatchPositions(false, new int[0], offset, size);
    }

    private BatchPositions(boolean isList, int[] positions, int offset, int size)
    {
        this.isList = isList;
        this.positions = requireNonNull(positions, "positions is null");
        this.offset = offset;
        this.size = size;

        checkArgument(offset >= 0, "offset is negative");
        checkArgument(size >= 0, "size is negative");
    }

    public boolean isRange()
    {
        return !isList;
    }

    public boolean isList()
    {
        return isList;
    }

    public int[] positions()
    {
        checkState(isList, "BatchPositions is a range");
        return positions;
    }

    public int offset()
    {
        return offset;
    }

    public int size()
    {
        return size;
    }
}
