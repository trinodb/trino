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
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import jakarta.annotation.Nullable;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkReadablePosition;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.BlockUtil.compactOffsets;
import static io.trino.spi.block.BlockUtil.compactSlice;
import static io.trino.spi.block.BlockUtil.copyIsNullAndAppendNull;
import static io.trino.spi.block.BlockUtil.copyOffsetsAndAppendNull;

public final class VariableWidthBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(VariableWidthBlock.class);

    private final int arrayOffset;
    private final int positionCount;
    private final Slice slice;
    private final int[] offsets;
    @Nullable
    private final boolean[] valueIsNull;

    private final long retainedSizeInBytes;
    private final long sizeInBytes;

    public VariableWidthBlock(int positionCount, Slice slice, int[] offsets, Optional<boolean[]> valueIsNull)
    {
        this(0, positionCount, slice, offsets, valueIsNull.orElse(null));
    }

    VariableWidthBlock(int arrayOffset, int positionCount, Slice slice, int[] offsets, boolean[] valueIsNull)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = arrayOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (slice == null) {
            throw new IllegalArgumentException("slice is null");
        }
        this.slice = slice;

        if (offsets.length - arrayOffset < (positionCount + 1)) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }
        this.offsets = offsets;

        if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        sizeInBytes = offsets[arrayOffset + positionCount] - offsets[arrayOffset] + ((Integer.BYTES + Byte.BYTES) * (long) positionCount);
        retainedSizeInBytes = INSTANCE_SIZE + slice.getRetainedSize() + sizeOf(valueIsNull) + sizeOf(offsets);
    }

    /**
     * Gets the raw {@link Slice} that keeps the actual data bytes.
     */
    public Slice getRawSlice()
    {
        return slice;
    }

    /**
     * Gets the offset of the value at the {@code position} in the {@link Slice} returned by {@link #getRawSlice()}.
     */
    public int getRawSliceOffset(int position)
    {
        checkReadablePosition(this, position);
        return getPositionOffset(position);
    }

    int getPositionOffset(int position)
    {
        return offsets[position + arrayOffset];
    }

    public int getSliceLength(int position)
    {
        checkReadablePosition(this, position);
        return getPositionOffset(position + 1) - getPositionOffset(position);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.empty(); // size varies per element and is not fixed
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return offsets[arrayOffset + position + length] - offsets[arrayOffset + position] + ((Integer.BYTES + Byte.BYTES) * (long) length);
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int selectedPositionsCount)
    {
        if (selectedPositionsCount == 0) {
            return 0;
        }
        if (selectedPositionsCount == positionCount) {
            return getSizeInBytes();
        }
        long sizeInBytes = 0;
        for (int i = 0; i < positions.length; ++i) {
            if (positions[i]) {
                sizeInBytes += offsets[arrayOffset + i + 1] - offsets[arrayOffset + i];
            }
        }
        return sizeInBytes + (Integer.BYTES + Byte.BYTES) * (long) selectedPositionsCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : getSliceLength(position);
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

    public Slice getSlice(int position)
    {
        checkReadablePosition(this, position);
        int offset = offsets[position + arrayOffset];
        int length = offsets[position + 1 + arrayOffset] - offset;
        return slice.slice(offset, length);
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
    public VariableWidthBlock getSingleValueBlock(int position)
    {
        if (isNull(position)) {
            return new VariableWidthBlock(0, 1, EMPTY_SLICE, new int[] {0, 0}, new boolean[] {true});
        }

        int offset = getPositionOffset(position);
        int entrySize = getSliceLength(position);

        Slice copy = slice.copy(offset, entrySize);

        return new VariableWidthBlock(0, 1, copy, new int[] {0, copy.length()}, null);
    }

    @Override
    public VariableWidthBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);
        if (length == 0) {
            return new VariableWidthBlock(0, 0, EMPTY_SLICE, new int[1], null);
        }

        int[] newOffsets = new int[length + 1];
        int finalLength = 0;
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            finalLength += getSliceLength(position);
            newOffsets[i + 1] = finalLength;
        }

        SliceOutput newSlice = Slices.allocate(finalLength).getOutput();
        boolean[] newValueIsNull = null;
        int firstPosition = positions[offset];
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
            newValueIsNull[0] = valueIsNull[firstPosition + arrayOffset];
        }
        int currentStart = getPositionOffset(firstPosition);
        int currentEnd = getPositionOffset(firstPosition + 1);
        for (int i = 1; i < length; i++) {
            int position = positions[offset + i];
            if (valueIsNull != null) {
                newValueIsNull[i] = valueIsNull[position + arrayOffset];
            }
            // Null positions must have valid offsets for getSliceLength to work correctly on the next non-null position
            int currentOffset = getPositionOffset(position);
            if (currentOffset != currentEnd) {
                // Copy last continuous range of bytes and update currentStart to start new range
                newSlice.writeBytes(slice, currentStart, currentEnd - currentStart);
                currentStart = currentOffset;
            }
            currentEnd = getPositionOffset(position + 1);
        }
        // Copy last range of bytes
        newSlice.writeBytes(slice, currentStart, currentEnd - currentStart);
        return new VariableWidthBlock(0, length, newSlice.slice(), newOffsets, newValueIsNull);
    }

    @Override
    public VariableWidthBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new VariableWidthBlock(positionOffset + arrayOffset, length, slice, offsets, valueIsNull);
    }

    @Override
    public VariableWidthBlock copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        positionOffset += arrayOffset;

        int[] newOffsets = compactOffsets(offsets, positionOffset, length);
        Slice newSlice = compactSlice(slice, offsets[positionOffset], newOffsets[length]);
        boolean[] newValueIsNull = valueIsNull == null ? null : compactArray(valueIsNull, positionOffset, length);

        if (newOffsets == offsets && newSlice == slice && newValueIsNull == valueIsNull) {
            return this;
        }
        return new VariableWidthBlock(0, length, newSlice, newOffsets, newValueIsNull);
    }

    @Override
    public VariableWidthBlock copyWithAppendedNull()
    {
        boolean[] newValueIsNull = copyIsNullAndAppendNull(valueIsNull, arrayOffset, positionCount);
        int[] newOffsets = copyOffsetsAndAppendNull(offsets, arrayOffset, positionCount);

        return new VariableWidthBlock(arrayOffset, positionCount + 1, slice, newOffsets, newValueIsNull);
    }

    int getRawArrayBase()
    {
        return arrayOffset;
    }

    int[] getRawOffsets()
    {
        return offsets;
    }

    boolean[] getRawValueIsNull()
    {
        return valueIsNull;
    }

    @Override
    public VariableWidthBlock getUnderlyingValueBlock()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return "VariableWidthBlock{positionCount=" + getPositionCount() + ", slice=" + slice + '}';
    }

    @Override
    public Optional<ByteArrayBlock> getNulls()
    {
        return BlockUtil.getNulls(valueIsNull, arrayOffset, positionCount);
    }
}
