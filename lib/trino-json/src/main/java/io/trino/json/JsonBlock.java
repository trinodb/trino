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
package io.trino.json;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.ValueBlock;
import jakarta.annotation.Nullable;

import java.util.Optional;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.String.format;

/// Backing block for `JsonType`. Values are held as bytes in one buffer, addressed by an offsets
/// table — the same shape as a variable-width block, so a JSON column is contiguous in memory and
/// costs no object per value.
///
/// Each position holds a self-contained payload: either the typed encoding or raw JSON text, told
/// apart by the leading byte. Text read from a connector is therefore stored as it arrived and is
/// parsed only if something looks inside it.
public final class JsonBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(JsonBlock.class);

    private final Slice slice;
    private final int[] offsets;
    @Nullable
    private final boolean[] valueIsNull;
    private final int arrayOffset;
    private final int positionCount;

    private final long sizeInBytes;
    private final long retainedSizeInBytes;

    public JsonBlock(int positionCount, Optional<boolean[]> valueIsNull, int[] offsets, Slice slice)
    {
        this(0, positionCount, valueIsNull.orElse(null), offsets, slice);
    }

    JsonBlock(int arrayOffset, int positionCount, @Nullable boolean[] valueIsNull, int[] offsets, Slice slice)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        if (offsets.length - arrayOffset < positionCount + 1) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }
        if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }
        this.arrayOffset = arrayOffset;
        this.positionCount = positionCount;
        this.offsets = offsets;
        this.slice = slice;
        this.valueIsNull = valueIsNull;

        sizeInBytes = (offsets[arrayOffset + positionCount] - offsets[arrayOffset]) + ((long) positionCount * (Integer.BYTES + Byte.BYTES));
        retainedSizeInBytes = INSTANCE_SIZE + slice.getRetainedSize() + sizeOf(offsets) + sizeOf(valueIsNull);
    }

    /// The value at the given position, as a view over this block's buffer. Copies no bytes.
    public Json getJson(int position)
    {
        checkReadable(position);
        return Json.wrap(getRawSlice(position));
    }

    /// The bytes of the value at the given position — a view into this block's buffer.
    public Slice getRawSlice(int position)
    {
        int index = position + arrayOffset;
        return slice.slice(offsets[index], offsets[index + 1] - offsets[index]);
    }

    public Slice getRawSlice()
    {
        return slice;
    }

    public int[] getRawOffsets()
    {
        return offsets;
    }

    @Nullable
    public boolean[] getRawValueIsNull()
    {
        return valueIsNull;
    }

    public int getArrayOffset()
    {
        return arrayOffset;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        checkValidRegion(positionCount, position, length);
        int index = position + arrayOffset;
        return (offsets[index + length] - offsets[index]) + ((long) length * (Integer.BYTES + Byte.BYTES));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        if (isNull(position)) {
            return 0;
        }
        int index = position + arrayOffset;
        return offsets[index + 1] - offsets[index];
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(slice, slice.getRetainedSize());
        consumer.accept(offsets, sizeOf(offsets));
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
        if (!mayHaveNull()) {
            return false;
        }
        checkReadable(position);
        return valueIsNull[position + arrayOffset];
    }

    @Override
    public JsonBlock getSingleValueBlock(int position)
    {
        checkReadable(position);
        int index = position + arrayOffset;
        int start = offsets[index];
        int length = offsets[index + 1] - start;
        boolean[] isNull = isNull(position) ? new boolean[] {true} : null;
        return new JsonBlock(0, 1, isNull, new int[] {0, length}, slice.slice(start, length));
    }

    @Override
    public JsonBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int totalLength = 0;
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i] + arrayOffset;
            totalLength += offsets[position + 1] - offsets[position];
        }

        int[] newOffsets = new int[length + 1];
        boolean[] newValueIsNull = valueIsNull == null ? null : new boolean[length];
        SliceOutput output = Slices.allocate(totalLength).getOutput();
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i] + arrayOffset;
            newOffsets[i] = output.size();
            if (newValueIsNull != null && valueIsNull[position]) {
                newValueIsNull[i] = true;
            }
            output.writeBytes(slice, offsets[position], offsets[position + 1] - offsets[position]);
        }
        newOffsets[length] = output.size();
        return new JsonBlock(0, length, newValueIsNull, newOffsets, output.slice());
    }

    @Override
    public JsonBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);
        return new JsonBlock(positionOffset + arrayOffset, length, valueIsNull, offsets, slice);
    }

    @Override
    public JsonBlock copyRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);
        int index = positionOffset + arrayOffset;
        int start = offsets[index];
        int end = offsets[index + length];

        int[] newOffsets = new int[length + 1];
        for (int i = 0; i <= length; i++) {
            newOffsets[i] = offsets[index + i] - start;
        }
        return new JsonBlock(0, length, compactIsNull(valueIsNull, index, length), newOffsets, slice.slice(start, end - start));
    }

    @Override
    public JsonBlock copyWithAppendedNull()
    {
        int base = offsets[arrayOffset];
        int end = offsets[arrayOffset + positionCount];

        int[] newOffsets = new int[positionCount + 2];
        for (int i = 0; i <= positionCount; i++) {
            newOffsets[i] = offsets[arrayOffset + i] - base;
        }
        newOffsets[positionCount + 1] = newOffsets[positionCount];

        boolean[] newValueIsNull = new boolean[positionCount + 1];
        if (valueIsNull != null) {
            System.arraycopy(valueIsNull, arrayOffset, newValueIsNull, 0, positionCount);
        }
        newValueIsNull[positionCount] = true;

        return new JsonBlock(0, positionCount + 1, newValueIsNull, newOffsets, slice.slice(base, end - base));
    }

    @Override
    public JsonBlock getUnderlyingValueBlock()
    {
        return this;
    }

    @Override
    public Optional<Bitmap> getValidityBitmap()
    {
        if (valueIsNull == null) {
            return Optional.empty();
        }
        long[] words = Bitmap.allocateWords(positionCount, true);
        for (int i = 0; i < positionCount; i++) {
            if (valueIsNull[i + arrayOffset]) {
                Bitmap.clear(words, 0, i);
            }
        }
        return Optional.of(new Bitmap(words, 0, positionCount));
    }

    @Override
    public String toString()
    {
        return "JsonBlock{positionCount=" + positionCount + '}';
    }

    private void checkReadable(int position)
    {
        if (position < 0 || position >= positionCount) {
            throw new IllegalArgumentException(format("Invalid position %s in block with %s positions", position, positionCount));
        }
    }

    private static void checkArrayRange(int[] array, int offset, int length)
    {
        if (offset < 0 || length < 0 || offset + length > array.length) {
            throw new IndexOutOfBoundsException(format("Invalid offset %s and length %s in array with %s elements", offset, length, array.length));
        }
    }

    private static void checkValidRegion(int positionCount, int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException(format("Invalid position %s and length %s in block with %s positions", positionOffset, length, positionCount));
        }
    }

    @Nullable
    private static boolean[] compactIsNull(@Nullable boolean[] isNull, int index, int length)
    {
        if (isNull == null) {
            return null;
        }
        boolean[] compacted = new boolean[length];
        System.arraycopy(isNull, index, compacted, 0, length);
        return compacted;
    }
}
