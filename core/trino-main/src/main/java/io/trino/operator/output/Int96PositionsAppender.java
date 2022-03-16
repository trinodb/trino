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
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.BlockUtil;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.Int96ArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Optional;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.calculateBlockResetSize;
import static java.lang.Math.max;

public class Int96PositionsAppender
        implements BlockTypeAwarePositionsAppender
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Int96PositionsAppender.class).instanceSize();
    private static final Block NULL_VALUE_BLOCK = new Int96ArrayBlock(1, Optional.of(new boolean[] {true}), new long[1], new int[1]);

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private final int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    // it is assumed that these arrays are the same length
    private boolean[] valueIsNull = new boolean[0];
    private long[] high = new long[0];
    private int[] low = new int[0];

    private long retainedSizeInBytes;

    public Int96PositionsAppender(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);

        updateDataSize();
    }

    @Override
    public void append(IntArrayList positions, Block block)
    {
        int[] positionArray = positions.elements();
        int newPositionCount = positions.size();
        ensureCapacity(positionCount + newPositionCount);

        if (block.mayHaveNull()) {
            for (int i = 0; i < newPositionCount; i++) {
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
            this.positionCount += newPositionCount;
        }
        else {
            for (int i = 0; i < newPositionCount; i++) {
                int position = positionArray[i];
                high[positionCount + i] = block.getLong(position, 0);
                low[positionCount + i] = block.getInt(position, SIZE_OF_LONG);
            }
            positionCount += newPositionCount;
            this.hasNonNullValue = true;
        }

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Int96ArrayBlock.SIZE_IN_BYTES_PER_POSITION * newPositionCount);
        }
    }

    @Override
    public void appendDictionary(IntArrayList positions, DictionaryBlock block)
    {
        int[] positionArray = positions.elements();
        int newPositionCount = positions.size();
        ensureCapacity(positionCount + newPositionCount);
        if (block.mayHaveNull()) {
            for (int i = 0; i < newPositionCount; i++) {
                int position = positionArray[i];
                if (block.isNull(position)) {
                    valueIsNull[positionCount] = true;
                    hasNullValue = true;
                }
                else {
                    high[positionCount] = block.getLong(position, 0);
                    low[positionCount] = block.getInt(position, SIZE_OF_LONG);
                    hasNonNullValue = true;
                }
                positionCount++;
            }
        }
        else {
            for (int i = 0; i < newPositionCount; i++) {
                int position = positionArray[i];
                high[positionCount] = block.getLong(position, 0);
                low[positionCount] = block.getInt(position, SIZE_OF_LONG);
                positionCount++;
            }
            hasNonNullValue = true;
        }

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Int96ArrayBlock.SIZE_IN_BYTES_PER_POSITION * newPositionCount);
        }
    }

    @Override
    public void appendRle(RunLengthEncodedBlock block)
    {
        int rlePositionCount = block.getPositionCount();
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

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Int96ArrayBlock.SIZE_IN_BYTES_PER_POSITION * rlePositionCount);
        }
    }

    @Override
    public void appendRow(Block source, int position)
    {
        if (source.isNull(position)) {
            appendNull();
        }
        else {
            writeInt96(source.getLong(position, 0), source.getInt(position, SIZE_OF_LONG));
        }
    }

    private void appendNull()
    {
        ensureCapacity(positionCount + 1);

        valueIsNull[positionCount] = true;

        hasNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Int96ArrayBlock.SIZE_IN_BYTES_PER_POSITION);
        }
    }

    private void writeInt96(long high, int low)
    {
        ensureCapacity(positionCount + 1);

        this.high[positionCount] = high;
        this.low[positionCount] = low;

        hasNonNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Int96ArrayBlock.SIZE_IN_BYTES_PER_POSITION);
        }
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, positionCount);
        }
        return new Int96ArrayBlock(positionCount, hasNullValue ? Optional.of(valueIsNull) : Optional.empty(), high, low);
    }

    @Override
    public BlockTypeAwarePositionsAppender newStateLike(@Nullable BlockBuilderStatus blockBuilderStatus)
    {
        return new Int96PositionsAppender(blockBuilderStatus, calculateBlockResetSize(positionCount));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    private void ensureCapacity(int capacity)
    {
        if (valueIsNull.length >= capacity) {
            return;
        }

        int newSize;
        if (initialized) {
            newSize = BlockUtil.calculateNewArraySize(valueIsNull.length);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }
        newSize = Math.max(newSize, capacity);

        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        high = Arrays.copyOf(high, newSize);
        low = Arrays.copyOf(low, newSize);
        updateDataSize();
    }

    private void updateDataSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(high) + sizeOf(low);
        if (blockBuilderStatus != null) {
            retainedSizeInBytes += BlockBuilderStatus.INSTANCE_SIZE;
        }
    }
}
