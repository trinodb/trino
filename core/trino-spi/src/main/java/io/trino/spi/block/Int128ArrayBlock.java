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

import io.trino.spi.type.Int128;
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

public final class Int128ArrayBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(Int128ArrayBlock.class);
    public static final int INT128_BYTES = Long.BYTES + Long.BYTES;
    public static final int SIZE_IN_BYTES_PER_POSITION = INT128_BYTES + Byte.BYTES;

    private final int positionOffset;
    private final int positionCount;
    @Nullable
    private final long[] valueIsValid;
    private final long[] values;

    private final long retainedSizeInBytes;

    public Int128ArrayBlock(int positionCount, Optional<long[]> valueIsValid, long[] values)
    {
        this(0, positionCount, valueIsValid.orElse(null), values);
    }

    Int128ArrayBlock(int positionOffset, int positionCount, long[] valueIsValid, long[] values)
    {
        if (positionOffset < 0) {
            throw new IllegalArgumentException("positionOffset is negative");
        }
        this.positionOffset = positionOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length - (positionOffset * 2) < positionCount * 2) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }
        this.values = values;

        checkBitRange(valueIsValid, positionOffset, positionCount);
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
        return isNull(position) ? 0 : INT128_BYTES;
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

    public Int128 getInt128(int position)
    {
        checkValidPosition(position, positionCount);
        int offset = (position + positionOffset) * 2;
        return Int128.valueOf(values[offset], values[offset + 1]);
    }

    public long getInt128High(int position)
    {
        checkValidPosition(position, positionCount);
        return values[(position + positionOffset) * 2];
    }

    public long getInt128Low(int position)
    {
        checkValidPosition(position, positionCount);
        return values[((position + positionOffset) * 2) + 1];
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsValid != null;
    }

    @Override
    public boolean hasNull()
    {
        return Bitmap.hasUnsetBit(valueIsValid, positionOffset, positionCount);
    }

    @Override
    public boolean isNull(int position)
    {
        if (!mayHaveNull()) {
            return false;
        }
        checkValidPosition(position, positionCount);
        return !Bitmap.isSet(valueIsValid, positionOffset, position);
    }

    @Override
    public Int128ArrayBlock getSingleValueBlock(int position)
    {
        checkValidPosition(position, positionCount);
        return new Int128ArrayBlock(
                0,
                1,
                isNull(position) ? new long[] {0} : null,
                new long[] {
                        values[(position + positionOffset) * 2],
                        values[((position + positionOffset) * 2) + 1],
                });
    }

    @Override
    public Int128ArrayBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        long[] newValueIsValid = null;
        if (valueIsValid != null) {
            newValueIsValid = new long[Bitmap.wordsForBits(length)];
        }
        long[] newValues = new long[length * 2];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkValidPosition(position, positionCount);
            if (valueIsValid != null && Bitmap.isSet(valueIsValid, positionOffset, position)) {
                set(newValueIsValid, 0, i);
            }
            newValues[i * 2] = values[(position + positionOffset) * 2];
            newValues[(i * 2) + 1] = values[((position + positionOffset) * 2) + 1];
        }
        return new Int128ArrayBlock(0, length, Bitmap.hasUnsetBit(newValueIsValid, 0, length) ? newValueIsValid : null, newValues);
    }

    @Override
    public Int128ArrayBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new Int128ArrayBlock(positionOffset + this.positionOffset, length, valueIsValid, values);
    }

    @Override
    public Int128ArrayBlock copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        positionOffset += this.positionOffset;
        long[] newValueIsValid = compactBitmap(valueIsValid, positionOffset, length);
        long[] newValues = compactArray(values, positionOffset * 2, length * 2);

        if (newValueIsValid == valueIsValid && newValues == values) {
            return this;
        }
        return new Int128ArrayBlock(0, length, newValueIsValid, newValues);
    }

    @Override
    public Int128ArrayBlock copyWithAppendedNull()
    {
        long[] newValueIsValid = copyBitmapAndAppendUnset(valueIsValid, positionOffset, positionCount);
        long[] newValues = ensureCapacity(values, (positionOffset + positionCount + 1) * 2);
        return new Int128ArrayBlock(positionOffset, positionCount + 1, newValueIsValid, newValues);
    }

    @Override
    public Int128ArrayBlock getUnderlyingValueBlock()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return "Int128ArrayBlock{positionCount=" + getPositionCount() + '}';
    }

    @Override
    public Optional<Bitmap> getValidityBitmap()
    {
        if (valueIsValid == null) {
            return Optional.empty();
        }
        return Optional.of(new Bitmap(valueIsValid, positionOffset, positionCount));
    }

    public int getRawOffset()
    {
        return positionOffset;
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

    public long[] getRawValues()
    {
        return values;
    }
}
