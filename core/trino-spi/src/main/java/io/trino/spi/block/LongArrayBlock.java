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
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.compactMemorySegment;
import static io.trino.spi.block.BlockUtil.copyIsNullAndAppendNull;
import static io.trino.spi.block.BlockUtil.expandValueWithNullValue;
import static java.lang.Math.toIntExact;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.util.Objects.requireNonNull;

public final class LongArrayBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(LongArrayBlock.class);
    public static final int SIZE_IN_BYTES_PER_POSITION = Long.BYTES + Byte.BYTES;

    @Nullable
    private final MemorySegment valueIsNull;
    private final MemorySegment values;

    private final long retainedSizeInBytes;

    public LongArrayBlock(int positionCount, Optional<byte[]> valueIsNull, long[] values)
    {
        this(valueIsNull.map(byteArray -> MemorySegment.ofArray(byteArray).asSlice(0, positionCount)).orElse(null),
                MemorySegment.ofArray(values).asSlice(0, positionCount * 8L));
    }

    LongArrayBlock(MemorySegment valueIsNull, MemorySegment values)
    {
        this.values = requireNonNull(values, "values is null");
        if (!(values.heapBase().orElse(null) instanceof long[])) {
            throw new IllegalArgumentException("values is not backed by a long[] array");
        }
        if (values.byteSize() % 8 != 0) {
            throw new IllegalArgumentException("values is not a multiple of 8");
        }

        if (valueIsNull != null && valueIsNull.byteSize() != values.byteSize() / 8) {
            throw new IllegalArgumentException("valueIsNull length does not match values length");
        }
        this.valueIsNull = valueIsNull;

        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(values) + (valueIsNull == null ? 0 : sizeOf(valueIsNull));
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.of(SIZE_IN_BYTES_PER_POSITION);
    }

    @Override
    public long getSizeInBytes()
    {
        return SIZE_IN_BYTES_PER_POSITION * (long) getPositionCount();
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
        return isNull(position) ? 0 : Long.BYTES;
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
        return toIntExact(values.byteSize() / 8);
    }

    public long getLong(int position)
    {
        return values.getAtIndex(JAVA_LONG, position);
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsNull != null;
    }

    @Override
    public boolean isNull(int position)
    {
        return valueIsNull != null && valueIsNull.get(JAVA_BYTE, position) != 0;
    }

    @Override
    public LongArrayBlock getSingleValueBlock(int position)
    {
        return new LongArrayBlock(
                isNull(position) ? MemorySegment.ofArray(new byte[] {1}) : null,
                MemorySegment.ofArray(new long[] {getLong(position)}));
    }

    @Override
    public LongArrayBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        byte[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new byte[length];
        }
        long[] newValues = new long[length];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            if (valueIsNull != null) {
                newValueIsNull[i] = valueIsNull.get(JAVA_BYTE, position);
            }
            newValues[i] = values.getAtIndex(JAVA_LONG, position);
        }
        return new LongArrayBlock(newValueIsNull == null ? null : MemorySegment.ofArray(newValueIsNull), MemorySegment.ofArray(newValues));
    }

    @Override
    public LongArrayBlock getRegion(int positionOffset, int length)
    {
        return new LongArrayBlock(
                valueIsNull == null ? null : valueIsNull.asSlice(positionOffset, length),
                values.asSlice(positionOffset * 8L, length * 8L));
    }

    @Override
    public LongArrayBlock copyRegion(int positionOffset, int length)
    {
        MemorySegment newValueIsNull = valueIsNull == null ? null : compactMemorySegment(valueIsNull, positionOffset, length);
        MemorySegment newValues = compactMemorySegment(values, positionOffset, length);

        if (newValueIsNull == valueIsNull && newValues == values) {
            return this;
        }
        return new LongArrayBlock(newValueIsNull, newValues);
    }

    @Override
    public String getEncodingName()
    {
        return LongArrayBlockEncoding.NAME;
    }

    @Override
    public LongArrayBlock copyWithAppendedNull()
    {
        MemorySegment newValueIsNull = copyIsNullAndAppendNull(valueIsNull, getPositionCount());
        MemorySegment newValues = expandValueWithNullValue(values);
        return new LongArrayBlock(newValueIsNull, newValues);
    }

    @Override
    public LongArrayBlock getUnderlyingValueBlock()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return "LongArrayBlock{positionCount=" + getPositionCount() + '}';
    }

    @Override
    public Optional<ByteArrayBlock> getNulls()
    {
        return BlockUtil.getNulls(valueIsNull);
    }

    @Nullable
    MemorySegment getValueIsNull()
    {
        return valueIsNull;
    }

    MemorySegment getValues()
    {
        return values;
    }
}
