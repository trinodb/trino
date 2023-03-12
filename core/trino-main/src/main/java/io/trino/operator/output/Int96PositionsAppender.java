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
import io.trino.spi.block.Int96ArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Arrays;
import java.util.Optional;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.output.PositionsAppenderUtil.calculateBlockResetSize;
import static io.trino.operator.output.PositionsAppenderUtil.calculateNewArraySize;
import static java.lang.Math.max;

public class Int96PositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = instanceSize(Int96PositionsAppender.class);
    private static final Block NULL_VALUE_BLOCK = new Int96ArrayBlock(1, Optional.of(new boolean[] {true}), new long[1], new int[1]);

    private boolean initialized;
    private int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    // it is assumed that these arrays are the same length
    private boolean[] valueIsNull = new boolean[0];
    private long[] high = new long[0];
    private int[] low = new int[0];

    private long retainedSizeInBytes;
    private long sizeInBytes;

    public Int96PositionsAppender(int expectedEntries)
    {
        this.initialEntryCount = max(expectedEntries, 1);

        updateRetainedSize();
    }

    @Override
    // TODO: Make PositionsAppender work performant with different block types (https://github.com/trinodb/trino/issues/13267)
    public void append(IntArrayList positions, Block block)
    {
        if (positions.isEmpty()) {
            return;
        }
        int[] positionArray = positions.elements();
        int positionsSize = positions.size();
        ensureCapacity(positionCount + positionsSize);

        if (block.mayHaveNull()) {
            for (int i = 0; i < positionsSize; i++) {
                int position = positionArray[i];
                boolean isNull = block.isNull(position);
                int positionIndex = positionCount + i;
                if (isNull) {
                    valueIsNull[positionIndex] = true;
                    hasNullValue = true;
                }
                else {
                    high[positionIndex] = block.getLong(position, 0);
                    low[positionIndex] = block.getInt(position, SIZE_OF_LONG);
                    hasNonNullValue = true;
                }
            }
            positionCount += positionsSize;
        }
        else {
            for (int i = 0; i < positionsSize; i++) {
                int position = positionArray[i];
                high[positionCount + i] = block.getLong(position, 0);
                low[positionCount + i] = block.getInt(position, SIZE_OF_LONG);
            }
            positionCount += positionsSize;
            hasNonNullValue = true;
        }

        updateSize(positionsSize);
    }

    @Override
    public void appendRle(Block block, int rlePositionCount)
    {
        if (rlePositionCount == 0) {
            return;
        }
        int sourcePosition = 0;
        ensureCapacity(positionCount + rlePositionCount);
        if (block.isNull(sourcePosition)) {
            Arrays.fill(valueIsNull, positionCount, positionCount + rlePositionCount, true);
            hasNullValue = true;
        }
        else {
            long valueHigh = block.getLong(sourcePosition, 0);
            int valueLow = block.getInt(sourcePosition, SIZE_OF_LONG);
            for (int i = 0; i < rlePositionCount; i++) {
                high[positionCount + i] = valueHigh;
                low[positionCount + i] = valueLow;
            }
            hasNonNullValue = true;
        }
        positionCount += rlePositionCount;

        updateSize(rlePositionCount);
    }

    @Override
    public void append(int sourcePosition, Block source)
    {
        ensureCapacity(positionCount + 1);
        if (source.isNull(sourcePosition)) {
            valueIsNull[positionCount] = true;
            hasNullValue = true;
        }
        else {
            high[positionCount] = source.getLong(sourcePosition, 0);
            low[positionCount] = source.getInt(sourcePosition, SIZE_OF_LONG);

            hasNonNullValue = true;
        }
        positionCount++;

        updateSize(1);
    }

    @Override
    public Block build()
    {
        Block result;
        if (hasNonNullValue) {
            result = new Int96ArrayBlock(positionCount, hasNullValue ? Optional.of(valueIsNull) : Optional.empty(), high, low);
        }
        else {
            result = RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positionCount);
        }
        reset();
        return result;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    private void reset()
    {
        initialEntryCount = calculateBlockResetSize(positionCount);
        initialized = false;
        valueIsNull = new boolean[0];
        high = new long[0];
        low = new int[0];
        positionCount = 0;
        sizeInBytes = 0;
        hasNonNullValue = false;
        hasNullValue = false;
        updateRetainedSize();
    }

    private void ensureCapacity(int capacity)
    {
        if (valueIsNull.length >= capacity) {
            return;
        }

        int newSize;
        if (initialized) {
            newSize = calculateNewArraySize(valueIsNull.length);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }
        newSize = Math.max(newSize, capacity);

        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        high = Arrays.copyOf(high, newSize);
        low = Arrays.copyOf(low, newSize);
        updateRetainedSize();
    }

    private void updateSize(long positionsSize)
    {
        sizeInBytes += Int96ArrayBlock.SIZE_IN_BYTES_PER_POSITION * positionsSize;
    }

    private void updateRetainedSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(high) + sizeOf(low);
    }
}
