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
import io.trino.spi.block.Fixed12Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Arrays;
import java.util.Optional;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.output.PositionsAppenderUtil.calculateBlockResetSize;
import static io.trino.operator.output.PositionsAppenderUtil.calculateNewArraySize;
import static java.lang.Math.max;

public class Fixed12PositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = instanceSize(Fixed12PositionsAppender.class);
    private static final Block NULL_VALUE_BLOCK = new Fixed12Block(1, Optional.of(new boolean[] {true}), new int[3]);

    private boolean initialized;
    private int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    // it is assumed that these arrays are the same length
    private boolean[] valueIsNull = new boolean[0];
    private int[] values = new int[0];

    private long retainedSizeInBytes;
    private long sizeInBytes;

    public Fixed12PositionsAppender(int expectedEntries)
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
                if (isNull) {
                    valueIsNull[positionCount + i] = true;
                    hasNullValue = true;
                }
                else {
                    int valuesIndex = (positionCount + i) * 3;
                    values[valuesIndex] = block.getInt(position, 0);
                    values[valuesIndex + 1] = block.getInt(position, SIZE_OF_INT);
                    values[valuesIndex + 2] = block.getInt(position, SIZE_OF_INT + SIZE_OF_INT);
                    hasNonNullValue = true;
                }
            }
            positionCount += positionsSize;
        }
        else {
            for (int i = 0; i < positionsSize; i++) {
                int position = positionArray[i];
                int valuesIndex = (positionCount + i) * 3;
                values[valuesIndex] = block.getInt(position, 0);
                values[valuesIndex + 1] = block.getInt(position, SIZE_OF_INT);
                values[valuesIndex + 2] = block.getInt(position, SIZE_OF_INT + SIZE_OF_INT);
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
            int valueHigh = block.getInt(sourcePosition, 0);
            int valueMid = block.getInt(sourcePosition, SIZE_OF_INT);
            int valueLow = block.getInt(sourcePosition, SIZE_OF_INT + SIZE_OF_INT);
            int positionIndex = positionCount * 3;
            for (int i = 0; i < rlePositionCount; i++) {
                values[positionIndex] = valueHigh;
                values[positionIndex + 1] = valueMid;
                values[positionIndex + 2] = valueLow;
                positionIndex += 3;
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
            int positionIndex = positionCount * 3;
            values[positionIndex] = source.getInt(sourcePosition, 0);
            values[positionIndex + 1] = source.getInt(sourcePosition, SIZE_OF_INT);
            values[positionIndex + 2] = source.getInt(sourcePosition, SIZE_OF_INT + SIZE_OF_INT);
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
            result = new Fixed12Block(positionCount, hasNullValue ? Optional.of(valueIsNull) : Optional.empty(), values);
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
        values = new int[0];
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
        values = Arrays.copyOf(values, newSize * 3);
        updateRetainedSize();
    }

    private void updateSize(long positionsSize)
    {
        sizeInBytes += Fixed12Block.SIZE_IN_BYTES_PER_POSITION * positionsSize;
    }

    private void updateRetainedSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
    }
}
