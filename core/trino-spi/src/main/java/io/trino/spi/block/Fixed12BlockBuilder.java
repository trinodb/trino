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

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkReadablePosition;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.Fixed12Block.FIXED12_BYTES;
import static io.trino.spi.block.Fixed12Block.decodeFixed12First;
import static io.trino.spi.block.Fixed12Block.encodeFirst;
import static java.lang.Math.max;

public class Fixed12BlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(Fixed12BlockBuilder.class);
    private static final Block NULL_VALUE_BLOCK = new Fixed12Block(0, 1, new boolean[] {true}, new int[3]);

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private final int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    // it is assumed that these arrays are the same length
    private boolean[] valueIsNull = new boolean[0];
    private int[] values = new int[0];

    private long retainedSizeInBytes;

    private int entryPositionCount;

    public Fixed12BlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);

        updateDataSize();
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        if (entryPositionCount != 0) {
            throw new IllegalArgumentException("long can only be written at the beginning of the entry");
        }

        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }

        encodeFirst(value, values, positionCount);
        entryPositionCount += 2;

        hasNonNullValue = true;
        return this;
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }

        values[(positionCount * 3) + entryPositionCount] = value;
        entryPositionCount++;

        hasNonNullValue = true;
        return this;
    }

    @Override
    public BlockBuilder closeEntry()
    {
        if (entryPositionCount != 3) {
            throw new IllegalStateException("Expected entry size to be exactly " + FIXED12_BYTES + " bytes but was " + (entryPositionCount * SIZE_OF_INT));
        }

        positionCount++;
        entryPositionCount = 0;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + FIXED12_BYTES);
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
            blockBuilderStatus.addBytes(Byte.BYTES + FIXED12_BYTES);
        }
        return this;
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positionCount);
        }
        return new Fixed12Block(0, positionCount, hasNullValue ? valueIsNull : null, values);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        return new Fixed12BlockBuilder(blockBuilderStatus, expectedEntries);
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
        values = Arrays.copyOf(values, newSize * 3);
        updateDataSize();
    }

    private void updateDataSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
        if (blockBuilderStatus != null) {
            retainedSizeInBytes += BlockBuilderStatus.INSTANCE_SIZE;
        }
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.of(Fixed12Block.SIZE_IN_BYTES_PER_POSITION);
    }

    @Override
    public long getSizeInBytes()
    {
        return Fixed12Block.SIZE_IN_BYTES_PER_POSITION * (long) positionCount;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return Fixed12Block.SIZE_IN_BYTES_PER_POSITION * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int selectedPositionsCount)
    {
        return Fixed12Block.SIZE_IN_BYTES_PER_POSITION * (long) selectedPositionsCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : FIXED12_BYTES;
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(values, sizeOf(values));
        consumer.accept(valueIsNull, sizeOf(valueIsNull));
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(this, position);
        if (offset != 0) {
            // If needed, we can add support for offset 4
            throw new IllegalArgumentException("offset must be 0");
        }
        return decodeFixed12First(values, position);
    }

    @Override
    public int getInt(int position, int offset)
    {
        checkReadablePosition(this, position);
        if (offset == 0) {
            return values[position * 3];
        }
        if (offset == 4) {
            return values[(position * 3) + 1];
        }
        if (offset == 8) {
            return values[(position * 3) + 2];
        }
        throw new IllegalArgumentException("offset must be 0, 4, or 8");
    }

    @Override
    public boolean mayHaveNull()
    {
        return hasNullValue;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(this, position);
        return valueIsNull[position];
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(this, position);
        int index = position * 3;
        return new Fixed12Block(
                0,
                1,
                isNull(position) ? new boolean[] {true} : null,
                new int[] {values[index], values[index + 1], values[index + 2]});
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, length);
        }
        boolean[] newValueIsNull = null;
        if (hasNullValue) {
            newValueIsNull = new boolean[length];
        }
        int[] newValues = new int[length * 3];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(this, position);
            if (newValueIsNull != null) {
                newValueIsNull[i] = valueIsNull[position];
            }
            int valuesIndex = (position) * 3;
            int newValuesIndex = i * 3;
            newValues[newValuesIndex] = values[valuesIndex];
            newValues[newValuesIndex + 1] = values[valuesIndex + 1];
            newValues[newValuesIndex + 2] = values[valuesIndex + 2];
        }
        return new Fixed12Block(0, length, newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, length);
        }
        return new Fixed12Block(positionOffset, length, hasNullValue ? valueIsNull : null, values);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, length);
        }
        boolean[] newValueIsNull = null;
        if (hasNullValue) {
            newValueIsNull = compactArray(valueIsNull, positionOffset, length);
        }
        int[] newValues = compactArray(values, positionOffset * 3, length * 3);
        return new Fixed12Block(0, length, newValueIsNull, newValues);
    }

    @Override
    public String getEncodingName()
    {
        return Fixed12BlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Fixed12BlockBuilder{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    Slice getValuesSlice()
    {
        return Slices.wrappedIntArray(values, 0, positionCount * 3);
    }
}
