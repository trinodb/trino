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

import jakarta.annotation.Nullable;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.clearBits;
import static io.trino.spi.block.Bitmap.copyBits;
import static io.trino.spi.block.Bitmap.hasSetBit;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Bitmap.setBits;
import static io.trino.spi.block.BlockUtil.calculateNewArraySize;
import static java.lang.Math.max;
import static java.util.Objects.checkIndex;

public class LongArrayBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(LongArrayBlockBuilder.class);
    private static final Block NULL_VALUE_BLOCK = new LongArrayBlock(0, 1, new long[] {0}, new long[1]);

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private final int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    @Nullable
    private long[] valueIsValid;
    private long[] values = new long[0];

    private long retainedSizeInBytes;

    public LongArrayBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);

        updateRetainedSize();
    }

    public BlockBuilder writeLong(long value)
    {
        ensureCapacity(positionCount + 1);

        values[positionCount] = value;
        if (valueIsValid != null) {
            set(valueIsValid, 0, positionCount);
        }

        hasNonNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(LongArrayBlock.SIZE_IN_BYTES_PER_POSITION);
        }
        return this;
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        ensureCapacity(positionCount + 1);

        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        if (longArrayBlock.isNull(position)) {
            initializeValidityForFirstNull();
            clear(valueIsValid, 0, positionCount);
            hasNullValue = true;
        }
        else {
            values[positionCount] = longArrayBlock.getLong(position);
            if (valueIsValid != null) {
                set(valueIsValid, 0, positionCount);
            }
            hasNonNullValue = true;
        }
        positionCount++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(LongArrayBlock.SIZE_IN_BYTES_PER_POSITION);
        }
    }

    @Override
    public void appendRepeated(ValueBlock block, int position, int count)
    {
        if (count == 0) {
            return;
        }
        if (count == 1) {
            append(block, position);
            return;
        }

        ensureCapacity(positionCount + count);

        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        if (longArrayBlock.isNull(position)) {
            initializeValidityForFirstNull();
            clearBits(valueIsValid, 0, positionCount, count);
            hasNullValue = true;
        }
        else {
            long value = longArrayBlock.getLong(position);
            Arrays.fill(values, positionCount, positionCount + count, value);
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, count);
            }
            hasNonNullValue = true;
        }
        positionCount += count;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(count * LongArrayBlock.SIZE_IN_BYTES_PER_POSITION);
        }
    }

    @Override
    public void appendRange(ValueBlock block, int offset, int length)
    {
        if (length == 0) {
            return;
        }
        if (length == 1) {
            append(block, offset);
            return;
        }

        ensureCapacity(positionCount + length);

        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        int rawOffset = longArrayBlock.getRawValuesOffset();

        long[] rawValues = longArrayBlock.getRawValues();
        System.arraycopy(rawValues, rawOffset + offset, values, positionCount, length);

        long[] rawValueIsValid = longArrayBlock.getRawValueIsValid();
        if (rawValueIsValid == null || !hasUnsetBit(rawValueIsValid, rawOffset + offset, length)) {
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, length);
            }
            hasNonNullValue = true;
        }
        else {
            initializeValidityForFirstNull();
            copyBits(rawValueIsValid, rawOffset + offset, valueIsValid, positionCount, length);
            hasNullValue = true;
            hasNonNullValue |= hasSetBit(rawValueIsValid, rawOffset + offset, length);
        }
        positionCount += length;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(length * LongArrayBlock.SIZE_IN_BYTES_PER_POSITION);
        }
    }

    @Override
    public void appendPositions(ValueBlock block, int[] positions, int offset, int length)
    {
        if (length == 0) {
            return;
        }
        if (length == 1) {
            append(block, positions[offset]);
            return;
        }

        ensureCapacity(positionCount + length);

        LongArrayBlock longArrayBlock = (LongArrayBlock) block;
        int rawOffset = longArrayBlock.getRawValuesOffset();
        long[] rawValues = longArrayBlock.getRawValues();
        long[] rawValueIsValid = longArrayBlock.getRawValueIsValid();
        for (int i = 0; i < length; i++) {
            int rawPosition = positions[offset + i] + rawOffset;
            values[positionCount + i] = rawValues[rawPosition];
        }
        if (rawValueIsValid == null || !hasUnsetBit(rawValueIsValid, rawOffset, positions, offset, length)) {
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, length);
            }
            hasNonNullValue = true;
        }
        else {
            initializeValidityForFirstNull();
            copyBits(rawValueIsValid, rawOffset, positions, offset, valueIsValid, positionCount, length);
            hasNullValue = true;
            hasNonNullValue |= hasSetBit(rawValueIsValid, rawOffset, positions, offset, length);
        }
        positionCount += length;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(length * LongArrayBlock.SIZE_IN_BYTES_PER_POSITION);
        }
    }

    @Override
    public BlockBuilder appendNull()
    {
        ensureCapacity(positionCount + 1);

        initializeValidityForFirstNull();

        clear(valueIsValid, 0, positionCount);

        hasNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(LongArrayBlock.SIZE_IN_BYTES_PER_POSITION);
        }
        return this;
    }

    @Override
    public void resetTo(int position)
    {
        checkIndex(position, positionCount + 1);
        positionCount = position;
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positionCount);
        }
        return buildValueBlock();
    }

    @Override
    public LongArrayBlock buildValueBlock()
    {
        return new LongArrayBlock(0, positionCount, hasNullValue ? valueIsValid : null, values);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        return new LongArrayBlockBuilder(blockBuilderStatus, expectedEntries);
    }

    private void ensureCapacity(int capacity)
    {
        if (values.length >= capacity) {
            return;
        }

        int newSize;
        if (initialized) {
            newSize = calculateNewArraySize(capacity);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }
        newSize = max(newSize, capacity);

        if (valueIsValid != null) {
            valueIsValid = Bitmap.ensureCapacity(valueIsValid, newSize);
        }
        values = Arrays.copyOf(values, newSize);
        updateRetainedSize();
    }

    private boolean initializeValidityForFirstNull()
    {
        if (valueIsValid != null) {
            return false;
        }
        valueIsValid = Bitmap.allocateWords(values.length, false);
        setBits(valueIsValid, 0, 0, positionCount);
        updateRetainedSize();
        return true;
    }

    private void updateRetainedSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsValid) + sizeOf(values);
        if (blockBuilderStatus != null) {
            retainedSizeInBytes += BlockBuilderStatus.INSTANCE_SIZE;
        }
    }

    @Override
    public long getSizeInBytes()
    {
        return LongArrayBlock.SIZE_IN_BYTES_PER_POSITION * (long) positionCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("LongArrayBlockBuilder{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }
}
