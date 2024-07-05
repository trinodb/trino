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

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.calculateNewArraySize;
import static java.lang.Math.max;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.util.Objects.checkIndex;

public class LongArrayBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(LongArrayBlockBuilder.class);
    private static final Block NULL_VALUE_BLOCK = new LongArrayBlock(MemorySegment.ofArray(new byte[] {1}), MemorySegment.ofArray(new long[1]));

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private final int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    // it is assumed that these arrays are the same length
    private byte[] valueIsNull = new byte[0];
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
            valueIsNull[positionCount] = 1;
            hasNullValue = true;
        }
        else {
            values[positionCount] = longArrayBlock.getLong(position);
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
            Arrays.fill(valueIsNull, positionCount, positionCount + count, (byte) 1);
            hasNullValue = true;
        }
        else {
            long value = longArrayBlock.getLong(position);
            Arrays.fill(values, positionCount, positionCount + count, value);
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

        MemorySegment.copy(longArrayBlock.getValues(), JAVA_LONG, offset * 8L, values, positionCount, length);

        MemorySegment nulls = longArrayBlock.getValueIsNull();
        if (nulls != null) {
            MemorySegment.copy(nulls, JAVA_BYTE, offset, valueIsNull, positionCount, length);
            if (!hasNullValue && !hasNonNullValue) {
                for (int i = 0; i < length; i++) {
                    if (nulls.get(JAVA_BYTE, offset + i) != 0) {
                        hasNullValue = true;
                    }
                    else {
                        hasNonNullValue = true;
                    }
                }
            }
        }
        else {
            hasNonNullValue = true;
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
        MemorySegment blockValues = longArrayBlock.getValues();
        MemorySegment blockNulls = longArrayBlock.getValueIsNull();
        if (blockNulls != null) {
            for (int i = 0; i < length; i++) {
                int position = positions[offset + i];
                if (blockNulls.get(JAVA_BYTE, position) != 0) {
                    valueIsNull[positionCount + i] = 1;
                    hasNullValue = true;
                }
                else {
                    values[positionCount + i] = blockValues.getAtIndex(JAVA_LONG, position);
                    hasNonNullValue = true;
                }
            }
        }
        else {
            for (int i = 0; i < length; i++) {
                int position = positions[offset + i];
                values[positionCount + i] = blockValues.getAtIndex(JAVA_LONG, position);
            }
            hasNonNullValue = true;
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

        valueIsNull[positionCount] = 1;

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
        return new LongArrayBlock(hasNullValue ? MemorySegment.ofArray(valueIsNull).asSlice(0, positionCount) : null,
                MemorySegment.ofArray(values).asSlice(0, positionCount * 8L));
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

        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        values = Arrays.copyOf(values, newSize);
        updateRetainedSize();
    }

    private void updateRetainedSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
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
