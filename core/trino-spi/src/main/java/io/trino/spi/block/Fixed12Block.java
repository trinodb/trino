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

import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkReadablePosition;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.BlockUtil.copyIsNullAndAppendNull;
import static io.trino.spi.block.BlockUtil.ensureCapacity;

public final class Fixed12Block
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(Fixed12Block.class);
    public static final int FIXED12_BYTES = Long.BYTES + Integer.BYTES;
    public static final int SIZE_IN_BYTES_PER_POSITION = FIXED12_BYTES + Byte.BYTES;

    private final int positionOffset;
    private final int positionCount;
    @Nullable
    private final boolean[] valueIsNull;
    private final int[] values;

    private final long retainedSizeInBytes;

    public Fixed12Block(int positionCount, Optional<boolean[]> valueIsNull, int[] values)
    {
        this(0, positionCount, valueIsNull.orElse(null), values);
    }

    Fixed12Block(int positionOffset, int positionCount, boolean[] valueIsNull, int[] values)
    {
        if (positionOffset < 0) {
            throw new IllegalArgumentException("positionOffset is negative");
        }
        this.positionOffset = positionOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length - (positionOffset * 3) < positionCount * 3) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;

        if (valueIsNull != null && valueIsNull.length - positionOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.of(SIZE_IN_BYTES_PER_POSITION);
    }

    @Override
    public long getSizeInBytes()
    {
        return SIZE_IN_BYTES_PER_POSITION * (long) positionCount;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return SIZE_IN_BYTES_PER_POSITION * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int selectedPositionsCount)
    {
        return (long) SIZE_IN_BYTES_PER_POSITION * selectedPositionsCount;
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
        if (valueIsNull != null) {
            consumer.accept(valueIsNull, sizeOf(valueIsNull));
        }
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    public int getInt(int position, int offset)
    {
        checkReadablePosition(this, position);
        if (offset == 0) {
            return values[(position + positionOffset) * 3];
        }
        if (offset == 4) {
            return values[((position + positionOffset) * 3) + 1];
        }
        if (offset == 8) {
            return values[((position + positionOffset) * 3) + 2];
        }
        throw new IllegalArgumentException("offset must be 0, 4, or 8");
    }

    public long getFixed12First(int position)
    {
        checkReadablePosition(this, position);
        return decodeFixed12First(values, position + positionOffset);
    }

    public int getFixed12Second(int position)
    {
        return decodeFixed12Second(values, position + positionOffset);
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsNull != null;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(this, position);
        return valueIsNull != null && valueIsNull[position + positionOffset];
    }

    @Override
    public Fixed12Block getSingleValueBlock(int position)
    {
        checkReadablePosition(this, position);
        int index = (position + positionOffset) * 3;
        return new Fixed12Block(
                0,
                1,
                isNull(position) ? new boolean[] {true} : null,
                new int[] {values[index], values[index + 1], values[index + 2]});
    }

    @Override
    public Fixed12Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }
        int[] newValues = new int[length * 3];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(this, position);
            if (valueIsNull != null) {
                newValueIsNull[i] = valueIsNull[position + positionOffset];
            }
            int valuesIndex = (position + positionOffset) * 3;
            int newValuesIndex = i * 3;
            newValues[newValuesIndex] = values[valuesIndex];
            newValues[newValuesIndex + 1] = values[valuesIndex + 1];
            newValues[newValuesIndex + 2] = values[valuesIndex + 2];
        }
        return new Fixed12Block(0, length, newValueIsNull, newValues);
    }

    @Override
    public Fixed12Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new Fixed12Block(positionOffset + this.positionOffset, length, valueIsNull, values);
    }

    @Override
    public Fixed12Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        positionOffset += this.positionOffset;
        boolean[] newValueIsNull = valueIsNull == null ? null : compactArray(valueIsNull, positionOffset, length);
        int[] newValues = compactArray(values, positionOffset * 3, length * 3);

        if (newValueIsNull == valueIsNull && newValues == values) {
            return this;
        }
        return new Fixed12Block(0, length, newValueIsNull, newValues);
    }

    @Override
    public Fixed12Block copyWithAppendedNull()
    {
        boolean[] newValueIsNull = copyIsNullAndAppendNull(valueIsNull, positionOffset, positionCount);
        int[] newValues = ensureCapacity(values, (positionOffset + positionCount + 1) * 3);
        return new Fixed12Block(positionOffset, positionCount + 1, newValueIsNull, newValues);
    }

    @Override
    public Fixed12Block getUnderlyingValueBlock()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return "Fixed12Block{positionCount=" + getPositionCount() + '}';
    }

    @Override
    public Optional<ByteArrayBlock> getNulls()
    {
        return BlockUtil.getNulls(valueIsNull, positionOffset, positionCount);
    }

    /**
     * At position * 3 in the values, write a little endian long followed by a little endian int.
     */
    public static void encodeFixed12(long first, int second, int[] values, int position)
    {
        int entryPosition = position * 3;
        values[entryPosition] = (int) first;
        values[entryPosition + 1] = (int) (first >>> 32);
        values[entryPosition + 2] = second;
    }

    /**
     * At position * 3 in the values, read a little endian long.
     */
    public static long decodeFixed12First(int[] values, int position)
    {
        int offset = position * 3;
        long high32 = (long) values[offset + 1] << 32;
        long low32 = values[offset] & 0xFFFF_FFFFL;
        return high32 | low32;
    }

    /**
     * At position * 3 + 8 in the values, read a little endian int.
     */
    public static int decodeFixed12Second(int[] values, int position)
    {
        int offset = position * 3;
        return values[offset + 2];
    }

    int getRawOffset()
    {
        return positionOffset;
    }

    @Nullable
    boolean[] getRawValueIsNull()
    {
        return valueIsNull;
    }

    int[] getRawValues()
    {
        return values;
    }
}
