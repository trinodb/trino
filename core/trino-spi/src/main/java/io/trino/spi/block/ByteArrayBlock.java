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

public final class ByteArrayBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(ByteArrayBlock.class);
    public static final int SIZE_IN_BYTES_PER_POSITION = Byte.BYTES + Byte.BYTES;

    private final int arrayOffset;
    private final int positionCount;
    @Nullable
    private final boolean[] valueIsNull;
    private final byte[] values;

    private final long retainedSizeInBytes;

    public ByteArrayBlock(int positionCount, Optional<boolean[]> valueIsNull, byte[] values)
    {
        this(0, positionCount, valueIsNull.orElse(null), values);
    }

    ByteArrayBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, byte[] values)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = arrayOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;

        if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        retainedSizeInBytes = (INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values));
    }

    /**
     * Gets the raw byte array that keeps the actual data values.
     */
    public byte[] getRawValues()
    {
        return values;
    }

    /**
     * Gets the offset into raw byte array where the data values start.
     */
    public int getRawValuesOffset()
    {
        return arrayOffset;
    }

    @Override
    public long getSizeInBytes()
    {
        return SIZE_IN_BYTES_PER_POSITION * (long) positionCount;
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.of(SIZE_IN_BYTES_PER_POSITION);
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
        return isNull(position) ? 0 : Byte.BYTES;
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

    public byte getByte(int position)
    {
        checkReadablePosition(this, position);
        return values[position + arrayOffset];
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsNull != null;
    }

    @Override
    public boolean hasNull()
    {
        if (valueIsNull == null) {
            return false;
        }
        for (int i = 0; i < positionCount; i++) {
            if (valueIsNull[i + arrayOffset]) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(this, position);
        return valueIsNull != null && valueIsNull[position + arrayOffset];
    }

    @Override
    public ByteArrayBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(this, position);
        return new ByteArrayBlock(
                0,
                1,
                isNull(position) ? new boolean[] {true} : null,
                new byte[] {values[position + arrayOffset]});
    }

    @Override
    public ByteArrayBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }
        byte[] newValues = new byte[length];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(this, position);
            if (valueIsNull != null) {
                newValueIsNull[i] = valueIsNull[position + arrayOffset];
            }
            newValues[i] = values[position + arrayOffset];
        }
        return new ByteArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public ByteArrayBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new ByteArrayBlock(positionOffset + arrayOffset, length, valueIsNull, values);
    }

    @Override
    public ByteArrayBlock copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        positionOffset += arrayOffset;
        boolean[] newValueIsNull = valueIsNull == null ? null : compactArray(valueIsNull, positionOffset, length);
        byte[] newValues = compactArray(values, positionOffset, length);

        if (newValueIsNull == valueIsNull && newValues == values) {
            return this;
        }
        return new ByteArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public ByteArrayBlock copyWithAppendedNull()
    {
        boolean[] newValueIsNull = copyIsNullAndAppendNull(valueIsNull, arrayOffset, positionCount);
        byte[] newValues = ensureCapacity(values, arrayOffset + positionCount + 1);

        return new ByteArrayBlock(arrayOffset, positionCount + 1, newValueIsNull, newValues);
    }

    @Override
    public ByteArrayBlock getUnderlyingValueBlock()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return "ByteArrayBlock{positionCount=" + getPositionCount() + '}';
    }

    @Override
    public Optional<ByteArrayBlock> getNulls()
    {
        return BlockUtil.getNulls(valueIsNull, arrayOffset, positionCount);
    }

    Slice getValuesSlice()
    {
        return Slices.wrappedBuffer(values, arrayOffset, positionCount);
    }

    boolean[] getRawValueIsNull()
    {
        return valueIsNull;
    }
}
