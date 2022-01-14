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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.calculateBlockResetSize;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.BlockUtil.countUsedPositions;
import static io.trino.spi.block.Int96ArrayBlock.INT96_BYTES;
import static java.lang.Math.max;

public class Int96ArrayBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Int96ArrayBlockBuilder.class).instanceSize();
    private static final Block NULL_VALUE_BLOCK = new Int96ArrayBlock(0, 1, new boolean[] {true}, new long[1], new int[1]);

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

    private int entryPositionCount;

    public Int96ArrayBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);

        updateDataSize();
    }

    @Override
    public BlockBuilder writeLong(long high)
    {
        if (entryPositionCount != 0) {
            throw new IllegalArgumentException("long can only be written at the beginning of the entry");
        }

        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }

        this.high[positionCount] = high;
        hasNonNullValue = true;
        entryPositionCount++;
        return this;
    }

    @Override
    public BlockBuilder writeInt(int low)
    {
        if (entryPositionCount != 1) {
            throw new IllegalArgumentException("int can only be written at the end of the entry");
        }

        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }

        this.low[positionCount] = low;
        hasNonNullValue = true;
        entryPositionCount++;
        return this;
    }

    @Override
    public BlockBuilder closeEntry()
    {
        if (entryPositionCount != 2) {
            throw new IllegalStateException("Expected entry size to be exactly " + INT96_BYTES + " bytes but was " + (entryPositionCount * SIZE_OF_LONG));
        }

        positionCount++;
        entryPositionCount = 0;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + INT96_BYTES);
        }
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (entryPositionCount != 0) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }

        valueIsNull[positionCount] = true;

        hasNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + INT96_BYTES);
        }
        return this;
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, positionCount);
        }
        return new Int96ArrayBlock(0, positionCount, hasNullValue ? valueIsNull : null, high, low);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        return new Int96ArrayBlockBuilder(blockBuilderStatus, calculateBlockResetSize(positionCount));
    }

    private void growCapacity()
    {
        int newSize;
        if (initialized) {
            newSize = BlockUtil.calculateNewArraySize(valueIsNull.length);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }

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

    @Override
    public long getSizeInBytes()
    {
        return (INT96_BYTES + Byte.BYTES) * (long) positionCount;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return (INT96_BYTES + Byte.BYTES) * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        return (INT96_BYTES + Byte.BYTES) * (long) countUsedPositions(positions);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : INT96_BYTES;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(high, sizeOf(high));
        consumer.accept(low, sizeOf(low));
        consumer.accept(valueIsNull, sizeOf(valueIsNull));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be 0");
        }

        return high[position];
    }

    @Override
    public int getInt(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 8) {
            throw new IllegalArgumentException("offset must be 8");
        }

        return low[position];
    }

    @Override
    public boolean mayHaveNull()
    {
        return hasNullValue;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return valueIsNull[position];
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new Int96ArrayBlock(
                0,
                1,
                valueIsNull[position] ? new boolean[] {true} : null,
                new long[] {high[position]},
                new int[] {low[position]});
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, length);
        }
        boolean[] newValueIsNull = null;
        if (hasNullValue) {
            newValueIsNull = new boolean[length];
        }
        long[] newHigh = new long[length];
        int[] newLow = new int[length];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (hasNullValue) {
                newValueIsNull[i] = valueIsNull[position];
            }
            newHigh[i] = high[position];
            newLow[i] = low[position];
        }
        return new Int96ArrayBlock(0, length, newValueIsNull, newHigh, newLow);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, length);
        }
        return new Int96ArrayBlock(positionOffset, length, hasNullValue ? valueIsNull : null, high, low);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, length);
        }
        boolean[] newValueIsNull = null;
        if (hasNullValue) {
            newValueIsNull = compactArray(valueIsNull, positionOffset, length);
        }
        long[] newHigh = compactArray(high, positionOffset, length);
        int[] newLow = compactArray(low, positionOffset, length);
        return new Int96ArrayBlock(0, length, newValueIsNull, newHigh, newLow);
    }

    @Override
    public String getEncodingName()
    {
        return Int96ArrayBlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Int96ArrayBlockBuilder{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    Slice getHighSlice()
    {
        return Slices.wrappedLongArray(high, 0, positionCount);
    }

    Slice getLowSlice()
    {
        return Slices.wrappedIntArray(low, 0, positionCount);
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
