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
import static io.trino.spi.block.BlockUtil.compactOffsets;
import static io.trino.spi.block.BlockUtil.copyOffsetsAndAppendNull;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ArrayBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(ArrayBlock.class);

    private final int arrayOffset;
    private final int positionCount;
    @Nullable
    private final long[] valueIsValid;
    private final Block values;
    private final int[] offsets;

    private volatile long sizeInBytes;
    private final long retainedSizeInBytes;

    /**
     * Create an array block directly from columnar nulls, values, and offsets into the values.
     * A null array must have no entries.
     */
    public static ArrayBlock fromElementBlock(int positionCount, Optional<long[]> valueIsValidOptional, int[] arrayOffset, Block values)
    {
        long[] valueIsValid = valueIsValidOptional.orElse(null);
        validateConstructorArguments(0, positionCount, valueIsValid, arrayOffset, values);
        // for performance reasons per element checks are only performed on the public construction
        for (int i = 0; i < positionCount; i++) {
            int offset = arrayOffset[i];
            int length = arrayOffset[i + 1] - offset;
            if (length < 0) {
                throw new IllegalArgumentException(format("Offset is not monotonically ascending. offsets[%s]=%s, offsets[%s]=%s", i, arrayOffset[i], i + 1, arrayOffset[i + 1]));
            }
            if (valueIsValid != null && !Bitmap.isSet(valueIsValid, 0, i) && length != 0) {
                throw new IllegalArgumentException("A null array must have zero entries");
            }
        }
        return new ArrayBlock(0, positionCount, valueIsValid, arrayOffset, values);
    }

    /**
     * Create an array block directly without per-element validations.
     */
    static ArrayBlock createArrayBlockInternal(int arrayOffset, int positionCount, @Nullable long[] valueIsValid, int[] offsets, Block values)
    {
        validateConstructorArguments(arrayOffset, positionCount, valueIsValid, offsets, values);
        return new ArrayBlock(arrayOffset, positionCount, valueIsValid, offsets, values);
    }

    private static void validateConstructorArguments(int arrayOffset, int positionCount, @Nullable long[] valueIsValid, int[] offsets, Block values)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        checkBitRange(valueIsValid, arrayOffset, positionCount);

        requireNonNull(offsets, "offsets is null");
        if (offsets.length - arrayOffset < positionCount + 1) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }

        requireNonNull(values, "values is null");
    }

    /**
     * Use createArrayBlockInternal or fromElementBlock instead of this method.  The caller of this method is assumed to have
     * validated the arguments with validateConstructorArguments.
     */
    private ArrayBlock(int arrayOffset, int positionCount, @Nullable long[] valueIsValid, int[] offsets, Block values)
    {
        // caller must check arguments with validateConstructorArguments
        this.arrayOffset = arrayOffset;
        this.positionCount = positionCount;
        this.valueIsValid = valueIsValid;
        this.offsets = offsets;
        this.values = requireNonNull(values);

        sizeInBytes = -1;
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(offsets) + sizeOf(valueIsValid);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        if (sizeInBytes < 0) {
            calculateSize();
        }
        return sizeInBytes;
    }

    private void calculateSize()
    {
        int valueStart = offsets[arrayOffset];
        int valueEnd = offsets[arrayOffset + positionCount];
        sizeInBytes = values.getRegionSizeInBytes(valueStart, valueEnd - valueStart) + getBaseSizeInBytes();
    }

    private long getBaseSizeInBytes()
    {
        return (Integer.BYTES + Byte.BYTES) * (long) this.positionCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes + values.getRetainedSizeInBytes();
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(values, values.getRetainedSizeInBytes());
        consumer.accept(offsets, sizeOf(offsets));
        if (valueIsValid != null) {
            consumer.accept(valueIsValid, sizeOf(valueIsValid));
        }
        consumer.accept(this, INSTANCE_SIZE);
    }

    public Block getElementsBlock()
    {
        int start = offsets[arrayOffset];
        int end = offsets[arrayOffset + positionCount];
        return values.getRegion(start, end - start);
    }

    public Block getRawElementBlock()
    {
        return values;
    }

    public int[] getRawOffsets()
    {
        return offsets;
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

    public int getOffsetBase()
    {
        return arrayOffset;
    }

    /**
     * Creates a projection by replacing the visible elements for this array block.
     * The replacement elements must correspond to {@link #getElementsBlock()}, not {@link #getRawElementBlock()}.
     * If this block is zero-aligned, the existing offsets and validity bitmap are reused.
     * Otherwise, the visible offsets and validity bits are normalized and the returned block has an offset base of zero.
     */
    public ArrayBlock createProjection(Block newVisibleElements)
    {
        requireNonNull(newVisibleElements, "newVisibleElements is null");

        int visibleElementCount = offsets[arrayOffset + positionCount] - offsets[arrayOffset];
        if (newVisibleElements.getPositionCount() != visibleElementCount) {
            throw new IllegalArgumentException(format("newVisibleElements must have the same position count as getElementsBlock(); expected %s but got %s", visibleElementCount, newVisibleElements.getPositionCount()));
        }

        if (arrayOffset == 0 && offsets[0] == 0) {
            return new ArrayBlock(0, positionCount, valueIsValid, offsets, newVisibleElements);
        }

        int[] newOffsets = new int[positionCount + 1];
        for (int i = 1; i <= positionCount; i++) {
            newOffsets[i] = offsets[arrayOffset + i] - offsets[arrayOffset];
        }
        long[] newValueIsValid = compactBitmap(valueIsValid, arrayOffset, positionCount);
        return new ArrayBlock(0, positionCount, newValueIsValid, newOffsets, newVisibleElements);
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
    public String toString()
    {
        return "ArrayBlock{positionCount=" + getPositionCount() + '}';
    }

    @Override
    public ArrayBlock copyWithAppendedNull()
    {
        long[] newValueIsValid = copyBitmapAndAppendUnset(valueIsValid, arrayOffset, getPositionCount());
        int[] newOffsets = copyOffsetsAndAppendNull(offsets, arrayOffset, getPositionCount());

        return createArrayBlockInternal(
                arrayOffset,
                getPositionCount() + 1,
                newValueIsValid,
                newOffsets,
                values);
    }

    @Override
    public ArrayBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int[] newOffsets = new int[length + 1];
        newOffsets[0] = 0;
        long[] newValueIsValid = valueIsValid == null ? null : new long[Bitmap.wordsForBits(length)];

        IntArrayList valuesPositions = new IntArrayList();
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkValidPosition(position, positionCount);
            if (valueIsValid != null && !Bitmap.isSet(valueIsValid, arrayOffset, position)) {
                newOffsets[i + 1] = newOffsets[i];
            }
            else {
                if (newValueIsValid != null) {
                    set(newValueIsValid, 0, i);
                }
                int valuesStartOffset = offsets[position + arrayOffset];
                int valuesEndOffset = offsets[position + 1 + arrayOffset];
                int valuesLength = valuesEndOffset - valuesStartOffset;

                newOffsets[i + 1] = newOffsets[i] + valuesLength;

                for (int elementIndex = valuesStartOffset; elementIndex < valuesEndOffset; elementIndex++) {
                    valuesPositions.add(elementIndex);
                }
            }
        }
        Block newValues = values.copyPositions(valuesPositions.elements(), 0, valuesPositions.size());
        return createArrayBlockInternal(0, length, Bitmap.hasUnsetBit(newValueIsValid, 0, length) ? newValueIsValid : null, newOffsets, newValues);
    }

    @Override
    public ArrayBlock getRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        return createArrayBlockInternal(
                position + arrayOffset,
                length,
                valueIsValid,
                offsets,
                values);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int valueStart = offsets[arrayOffset + position];
        int valueEnd = offsets[arrayOffset + position + length];

        return values.getRegionSizeInBytes(valueStart, valueEnd - valueStart) + ((Integer.BYTES + Byte.BYTES) * (long) length);
    }

    @Override
    public ArrayBlock copyRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int startValueOffset = offsets[position + arrayOffset];
        int endValueOffset = offsets[position + length + arrayOffset];
        Block newValues = values.copyRegion(startValueOffset, endValueOffset - startValueOffset);

        int[] newOffsets = compactOffsets(offsets, position + arrayOffset, length);
        long[] newValueIsValid = compactBitmap(valueIsValid, position + arrayOffset, length);

        if (newValues == values && newOffsets == offsets && newValueIsValid == valueIsValid) {
            return this;
        }
        return createArrayBlockInternal(0, length, newValueIsValid, newOffsets, newValues);
    }

    public Block getArray(int position)
    {
        checkValidPosition(position, positionCount);
        int startValueOffset = offsets[position + arrayOffset];
        int endValueOffset = offsets[position + 1 + arrayOffset];
        return values.getRegion(startValueOffset, endValueOffset - startValueOffset);
    }

    @Override
    public ArrayBlock getSingleValueBlock(int position)
    {
        checkValidPosition(position, positionCount);

        int startValueOffset = offsets[position + arrayOffset];
        int valueLength = offsets[position + 1 + arrayOffset] - startValueOffset;
        Block newValues = values.copyRegion(startValueOffset, valueLength);

        return createArrayBlockInternal(
                0,
                1,
                isNull(position) ? new long[] {0} : null,
                new int[] {0, valueLength},
                newValues);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkValidPosition(position, positionCount);

        if (isNull(position)) {
            return 0;
        }

        int startValueOffset = offsets[position + arrayOffset];
        int endValueOffset = offsets[position + 1 + arrayOffset];

        Block rawElementBlock = values;
        long size = 0;
        for (int i = startValueOffset; i < endValueOffset; i++) {
            size += rawElementBlock.getEstimatedDataSizeForStats(i);
        }
        return size;
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
    public ArrayBlock getUnderlyingValueBlock()
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

    public <T> T apply(ArrayBlockFunction<T> function, int position)
    {
        checkValidPosition(position, positionCount);

        int startValueOffset = offsets[position + arrayOffset];
        int endValueOffset = offsets[position + 1 + arrayOffset];
        return function.apply(values, startValueOffset, endValueOffset - startValueOffset);
    }

    public interface ArrayBlockFunction<T>
    {
        T apply(Block block, int startPosition, int length);
    }
}
