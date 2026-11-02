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
import static io.airlift.slice.Slices.EMPTY_SLICE;
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
    private final long[] valueIsValid;
    private final int arrayOffset;
    private final int positionCount;

    private final long sizeInBytes;
    private final long retainedSizeInBytes;

    public JsonBlock(int positionCount, Optional<long[]> valueIsValid, int[] offsets, Slice slice)
    {
        this(0, positionCount, valueIsValid.orElse(null), offsets, slice);
    }

    JsonBlock(int arrayOffset, int positionCount, @Nullable long[] valueIsValid, int[] offsets, Slice slice)
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
        if (valueIsValid != null && (long) arrayOffset + positionCount > (long) valueIsValid.length * Long.SIZE) {
            throw new IllegalArgumentException("validity bitmap is shorter than the region");
        }
        this.arrayOffset = arrayOffset;
        this.positionCount = positionCount;
        this.offsets = offsets;
        this.slice = slice;
        this.valueIsValid = valueIsValid;

        sizeInBytes = (offsets[arrayOffset + positionCount] - offsets[arrayOffset]) + ((long) positionCount * (Integer.BYTES + Byte.BYTES));
        retainedSizeInBytes = INSTANCE_SIZE + slice.getRetainedSize() + sizeOf(offsets) + sizeOf(valueIsValid);
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
    public long[] getRawValueIsValid()
    {
        return valueIsValid;
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
        checkReadable(position);
        return !Bitmap.isSet(valueIsValid, arrayOffset, position);
    }

    @Override
    public JsonBlock getSingleValueBlock(int position)
    {
        checkReadable(position);
        if (isNull(position)) {
            return new JsonBlock(0, 1, new long[] {0}, new int[] {0, 0}, EMPTY_SLICE);
        }
        int index = position + arrayOffset;
        int start = offsets[index];
        int length = offsets[index + 1] - start;
        return new JsonBlock(0, 1, null, new int[] {0, length}, slice.copy(start, length));
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
        long[] newValueIsValid = valueIsValid == null ? null : Bitmap.allocateWords(length, true);
        SliceOutput output = Slices.allocate(totalLength).getOutput();
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i] + arrayOffset;
            newOffsets[i] = output.size();
            if (newValueIsValid != null && !Bitmap.isSet(valueIsValid, 0, position)) {
                Bitmap.clear(newValueIsValid, 0, i);
            }
            output.writeBytes(slice, offsets[position], offsets[position + 1] - offsets[position]);
        }
        newOffsets[length] = output.size();
        return new JsonBlock(0, length, newValueIsValid, newOffsets, output.slice());
    }

    @Override
    public JsonBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);
        return new JsonBlock(positionOffset + arrayOffset, length, valueIsValid, offsets, slice);
    }

    @Override
    public JsonBlock copyRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);
        int index = positionOffset + arrayOffset;
        int start = offsets[index];
        int end = offsets[index + length];

        Slice newSlice = slice;
        if (!slice.isCompact() || start != 0 || end != slice.length()) {
            newSlice = slice.copy(start, end - start);
        }
        int[] newOffsets = offsets;
        if (index != 0 || offsets.length != length + 1 || start != 0) {
            newOffsets = new int[length + 1];
            for (int i = 1; i <= length; i++) {
                newOffsets[i] = offsets[index + i] - start;
            }
        }
        long[] newValueIsValid = compactValidity(valueIsValid, index, length);

        if (newSlice == slice && newOffsets == offsets && newValueIsValid == valueIsValid) {
            return this;
        }
        return new JsonBlock(0, length, newValueIsValid, newOffsets, newSlice);
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

        long[] newValueIsValid = Bitmap.allocateWords(positionCount + 1, true);
        if (valueIsValid != null) {
            Bitmap.copyBits(valueIsValid, arrayOffset, newValueIsValid, 0, positionCount);
        }
        Bitmap.clear(newValueIsValid, 0, positionCount);

        return new JsonBlock(0, positionCount + 1, newValueIsValid, newOffsets, slice.slice(base, end - base));
    }

    @Override
    public JsonBlock getUnderlyingValueBlock()
    {
        return this;
    }

    @Override
    public Optional<Bitmap> getValidityBitmap()
    {
        if (valueIsValid == null) {
            return Optional.empty();
        }
        return Optional.of(new Bitmap(valueIsValid, arrayOffset, positionCount));
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
    private static long[] compactValidity(@Nullable long[] valueIsValid, int index, int length)
    {
        if (valueIsValid == null) {
            return null;
        }
        if (index == 0 && Bitmap.wordsForBits(length) == valueIsValid.length) {
            return valueIsValid;
        }
        long[] compacted = Bitmap.allocateWords(length, false);
        Bitmap.copyBits(valueIsValid, index, compacted, 0, length);
        return compacted;
    }
}
