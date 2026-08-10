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
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.Bitmap.checkBitRange;
import static io.trino.spi.block.Bitmap.compactBitmap;
import static io.trino.spi.block.Bitmap.copyBitmapAndAppendUnset;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidPosition;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.BlockUtil.ensureCapacity;

public final class LongArrayBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(LongArrayBlock.class);
    public static final int SIZE_IN_BYTES_PER_POSITION = Long.BYTES + Byte.BYTES;

    private final int arrayOffset;
    private final int positionCount;
    @Nullable
    private final long[] valueIsValid;
    private final long[] values;

    private final long retainedSizeInBytes;

    public LongArrayBlock(int positionCount, Optional<long[]> valueIsValid, long[] values)
    {
        this(0, positionCount, valueIsValid.orElse(null), values);
    }

    LongArrayBlock(int arrayOffset, int positionCount, long[] valueIsValid, long[] values)
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

        checkBitRange(valueIsValid, arrayOffset, positionCount);
        this.valueIsValid = valueIsValid;

        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsValid) + sizeOf(values);
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
        if (valueIsValid != null) {
            consumer.accept(valueIsValid, sizeOf(valueIsValid));
        }
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    public long getLong(int position)
    {
        checkValidPosition(position, positionCount);
        return values[position + arrayOffset];
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsValid != null;
    }

    @Override
    public boolean hasNull()
    {
        return Bitmap.hasUnsetBit(valueIsValid, arrayOffset, positionCount);
    }

    @Override
    public boolean isNull(int position)
    {
        if (!mayHaveNull()) {
            return false;
        }
        checkValidPosition(position, positionCount);
        return !Bitmap.isSet(valueIsValid, arrayOffset, position);
    }

    @Override
    public LongArrayBlock getSingleValueBlock(int position)
    {
        checkValidPosition(position, positionCount);
        return new LongArrayBlock(
                0,
                1,
                isNull(position) ? new long[] {0} : null,
                new long[] {values[position + arrayOffset]});
    }

    @Override
    public LongArrayBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        long[] newValueIsValid = null;
        if (valueIsValid != null) {
            newValueIsValid = new long[Bitmap.wordsForBits(length)];
        }
        long[] newValues = new long[length];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkValidPosition(position, positionCount);
            if (valueIsValid != null && Bitmap.isSet(valueIsValid, arrayOffset, position)) {
                set(newValueIsValid, 0, i);
            }
            newValues[i] = values[position + arrayOffset];
        }
        return new LongArrayBlock(0, length, Bitmap.hasUnsetBit(newValueIsValid, 0, length) ? newValueIsValid : null, newValues);
    }

    @Override
    public LongArrayBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new LongArrayBlock(positionOffset + arrayOffset, length, valueIsValid, values);
    }

    @Override
    public LongArrayBlock copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        positionOffset += arrayOffset;
        long[] newValueIsValid = compactBitmap(valueIsValid, positionOffset, length);
        long[] newValues = compactArray(values, positionOffset, length);

        if (newValueIsValid == valueIsValid && newValues == values) {
            return this;
        }
        return new LongArrayBlock(0, length, newValueIsValid, newValues);
    }

    @Override
    public LongArrayBlock copyWithAppendedNull()
    {
        long[] newValueIsValid = copyBitmapAndAppendUnset(valueIsValid, arrayOffset, positionCount);
        long[] newValues = ensureCapacity(values, arrayOffset + positionCount + 1);

        return new LongArrayBlock(arrayOffset, positionCount + 1, newValueIsValid, newValues);
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
    public Optional<Bitmap> getValidityBitmap()
    {
        if (valueIsValid == null) {
            return Optional.empty();
        }
        return Optional.of(new Bitmap(valueIsValid, arrayOffset, positionCount));
    }

    public int getRawValuesOffset()
    {
        return arrayOffset;
    }

    public long[] getRawValues()
    {
        return values;
    }

    /// Returns raw validity bitmap words using the [Bitmap] encoding, or null if all positions are valid.
    ///
    /// The returned array is raw block storage. Use [getValidityBitmap()] unless the caller already has the matching
    /// raw bit offset.
    @Nullable
    public long[] getRawValueIsValid()
    {
        return valueIsValid;
    }
}
