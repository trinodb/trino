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
import static io.trino.spi.block.BlockUtil.calculateNewArraySize;
import static io.trino.spi.block.Fixed12Block.FIXED12_BYTES;
import static io.trino.spi.block.Fixed12Block.encodeFixed12;
import static java.lang.Math.max;
import static java.util.Objects.checkIndex;

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

    public Fixed12BlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);

        updateRetainedSize();
    }

    public void writeFixed12(long first, int second)
    {
        ensureCapacity(positionCount + 1);

        encodeFixed12(first, second, values, positionCount);

        hasNonNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + FIXED12_BYTES);
        }
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        ensureCapacity(positionCount + 1);

        Fixed12Block fixed12Block = (Fixed12Block) block;
        if (fixed12Block.isNull(position)) {
            valueIsNull[positionCount] = true;
            hasNullValue = true;
        }
        else {
            int[] rawValues = fixed12Block.getRawValues();
            int rawValuePosition = (fixed12Block.getRawOffset() + position) * 3;

            int positionIndex = positionCount * 3;
            values[positionIndex] = rawValues[rawValuePosition];
            values[positionIndex + 1] = rawValues[rawValuePosition + 1];
            values[positionIndex + 2] = rawValues[rawValuePosition + 2];
            hasNonNullValue = true;
        }
        positionCount++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Fixed12Block.SIZE_IN_BYTES_PER_POSITION);
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

        Fixed12Block fixed12Block = (Fixed12Block) block;
        if (fixed12Block.isNull(position)) {
            Arrays.fill(valueIsNull, positionCount, positionCount + count, true);
            hasNullValue = true;
        }
        else {
            int[] rawValues = fixed12Block.getRawValues();
            int rawValuePosition = (fixed12Block.getRawOffset() + position) * 3;
            int valueFirst = rawValues[rawValuePosition];
            int valueSecond = rawValues[rawValuePosition + 1];
            int valueThird = rawValues[rawValuePosition + 2];

            int positionIndex = positionCount * 3;
            for (int i = 0; i < count; i++) {
                values[positionIndex] = valueFirst;
                values[positionIndex + 1] = valueSecond;
                values[positionIndex + 2] = valueThird;
                positionIndex += 3;
            }

            hasNonNullValue = true;
        }
        positionCount += count;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(count * Fixed12Block.SIZE_IN_BYTES_PER_POSITION);
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

        Fixed12Block fixed12Block = (Fixed12Block) block;
        int rawOffset = fixed12Block.getRawOffset();

        int[] rawValues = fixed12Block.getRawValues();
        System.arraycopy(rawValues, (rawOffset + offset) * 3, values, positionCount * 3, length * 3);

        boolean[] rawValueIsNull = fixed12Block.getRawValueIsNull();
        if (rawValueIsNull != null) {
            for (int i = 0; i < length; i++) {
                if (rawValueIsNull[rawOffset + offset + i]) {
                    valueIsNull[positionCount + i] = true;
                    hasNullValue = true;
                }
                else {
                    hasNonNullValue = true;
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

        Fixed12Block fixed12Block = (Fixed12Block) block;
        int rawOffset = fixed12Block.getRawOffset();
        int[] rawValues = fixed12Block.getRawValues();
        boolean[] rawValueIsNull = fixed12Block.getRawValueIsNull();

        int positionIndex = positionCount * 3;
        if (rawValueIsNull != null) {
            for (int i = 0; i < length; i++) {
                int rawPosition = positions[offset + i] + rawOffset;
                if (rawValueIsNull[rawPosition]) {
                    valueIsNull[positionCount + i] = true;
                    hasNullValue = true;
                }
                else {
                    int rawValuePosition = rawPosition * 3;
                    values[positionIndex] = rawValues[rawValuePosition];
                    values[positionIndex + 1] = rawValues[rawValuePosition + 1];
                    values[positionIndex + 2] = rawValues[rawValuePosition + 2];
                    hasNonNullValue = true;
                }
                positionIndex += 3;
            }
        }
        else {
            for (int i = 0; i < length; i++) {
                int rawPosition = positions[offset + i] + rawOffset;
                int rawValuePosition = rawPosition * 3;
                values[positionIndex] = rawValues[rawValuePosition];
                values[positionIndex + 1] = rawValues[rawValuePosition + 1];
                values[positionIndex + 2] = rawValues[rawValuePosition + 2];
                positionIndex += 3;
            }
            hasNonNullValue = true;
        }
        positionCount += length;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(length * Fixed12Block.SIZE_IN_BYTES_PER_POSITION);
        }
    }

    @Override
    public BlockBuilder appendNull()
    {
        ensureCapacity(positionCount + 1);

        valueIsNull[positionCount] = true;

        hasNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Byte.BYTES + FIXED12_BYTES);
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
    public Fixed12Block buildValueBlock()
    {
        return new Fixed12Block(0, positionCount, hasNullValue ? valueIsNull : null, values);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        return new Fixed12BlockBuilder(blockBuilderStatus, expectedEntries);
    }

    private void ensureCapacity(int capacity)
    {
        if (valueIsNull.length >= capacity) {
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
        values = Arrays.copyOf(values, newSize * 3);
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
        return Fixed12Block.SIZE_IN_BYTES_PER_POSITION * (long) positionCount;
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
        StringBuilder sb = new StringBuilder("Fixed12BlockBuilder{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }
}
