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

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkReadablePosition;
import static io.trino.spi.block.BlockUtil.checkValidPositions;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.BlockUtil.compactOffsets;
import static io.trino.spi.block.BlockUtil.copyIsNullAndAppendNull;
import static io.trino.spi.block.BlockUtil.copyOffsetsAndAppendNull;
import static io.trino.spi.block.BlockUtil.countAndMarkSelectedPositionsFromOffsets;
import static io.trino.spi.block.BlockUtil.countSelectedPositionsFromOffsets;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class ArrayBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(ArrayBlock.class);

    private final int arrayOffset;
    private final int positionCount;
    private final boolean[] valueIsNull;
    private final Block values;
    private final int[] offsets;

    private volatile long sizeInBytes;
    private final long retainedSizeInBytes;

    /**
     * Create an array block directly from columnar nulls, values, and offsets into the values.
     * A null array must have no entries.
     */
    public static Block fromElementBlock(int positionCount, Optional<boolean[]> valueIsNullOptional, int[] arrayOffset, Block values)
    {
        boolean[] valueIsNull = valueIsNullOptional.orElse(null);
        validateConstructorArguments(0, positionCount, valueIsNull, arrayOffset, values);
        // for performance reasons per element checks are only performed on the public construction
        for (int i = 0; i < positionCount; i++) {
            int offset = arrayOffset[i];
            int length = arrayOffset[i + 1] - offset;
            if (length < 0) {
                throw new IllegalArgumentException(format("Offset is not monotonically ascending. offsets[%s]=%s, offsets[%s]=%s", i, arrayOffset[i], i + 1, arrayOffset[i + 1]));
            }
            if (valueIsNull != null && valueIsNull[i] && length != 0) {
                throw new IllegalArgumentException("A null array must have zero entries");
            }
        }
        return new ArrayBlock(0, positionCount, valueIsNull, arrayOffset, values);
    }

    /**
     * Create an array block directly without per element validations.
     */
    static ArrayBlock createArrayBlockInternal(int arrayOffset, int positionCount, @Nullable boolean[] valueIsNull, int[] offsets, Block values)
    {
        validateConstructorArguments(arrayOffset, positionCount, valueIsNull, offsets, values);
        return new ArrayBlock(arrayOffset, positionCount, valueIsNull, offsets, values);
    }

    private static void validateConstructorArguments(int arrayOffset, int positionCount, @Nullable boolean[] valueIsNull, int[] offsets, Block values)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }

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
    private ArrayBlock(int arrayOffset, int positionCount, @Nullable boolean[] valueIsNull, int[] offsets, Block values)
    {
        // caller must check arguments with validateConstructorArguments
        this.arrayOffset = arrayOffset;
        this.positionCount = positionCount;
        this.valueIsNull = valueIsNull;
        this.offsets = offsets;
        this.values = requireNonNull(values);

        sizeInBytes = -1;
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(offsets) + sizeOf(valueIsNull);
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
            if (!values.isLoaded()) {
                return getBaseSizeInBytes();
            }
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
        if (valueIsNull != null) {
            consumer.accept(valueIsNull, sizeOf(valueIsNull));
        }
        consumer.accept(this, INSTANCE_SIZE);
    }

    protected Block getRawElementBlock()
    {
        return values;
    }

    protected int[] getOffsets()
    {
        return offsets;
    }

    protected int getOffsetBase()
    {
        return arrayOffset;
    }

    @Override
    public final List<Block> getChildren()
    {
        return singletonList(values);
    }

    @Override
    public String getEncodingName()
    {
        return ArrayBlockEncoding.NAME;
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsNull != null;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ArrayBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean isLoaded()
    {
        return values.isLoaded();
    }

    @Override
    public ArrayBlock getLoadedBlock()
    {
        Block loadedValuesBlock = values.getLoadedBlock();

        if (loadedValuesBlock == values) {
            return this;
        }
        return createArrayBlockInternal(
                arrayOffset,
                positionCount,
                valueIsNull,
                offsets,
                loadedValuesBlock);
    }

    @Override
    public ArrayBlock copyWithAppendedNull()
    {
        boolean[] newValueIsNull = copyIsNullAndAppendNull(valueIsNull, arrayOffset, getPositionCount());
        int[] newOffsets = copyOffsetsAndAppendNull(offsets, arrayOffset, getPositionCount());

        return createArrayBlockInternal(
                arrayOffset,
                getPositionCount() + 1,
                newValueIsNull,
                newOffsets,
                values);
    }

    @Override
    public ArrayBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int[] newOffsets = new int[length + 1];
        newOffsets[0] = 0;
        boolean[] newValueIsNull = valueIsNull == null ? null : new boolean[length];

        IntArrayList valuesPositions = new IntArrayList();
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            if (newValueIsNull != null && isNull(position)) {
                newValueIsNull[i] = true;
                newOffsets[i + 1] = newOffsets[i];
            }
            else {
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
        return createArrayBlockInternal(0, length, newValueIsNull, newOffsets, newValues);
    }

    @Override
    public ArrayBlock getRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        return createArrayBlockInternal(
                position + arrayOffset,
                length,
                valueIsNull,
                offsets,
                values);
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.empty(); // size per position varies based on the number of entries in each array
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
    public final long getPositionsSizeInBytes(boolean[] positions, int selectedArrayPositions)
    {
        int positionCount = getPositionCount();
        checkValidPositions(positions, positionCount);
        if (selectedArrayPositions == 0) {
            return 0;
        }
        if (selectedArrayPositions == positionCount) {
            return getSizeInBytes();
        }

        Block rawElementBlock = values;
        OptionalInt fixedPerElementSizeInBytes = rawElementBlock.fixedSizeInBytesPerPosition();
        int[] offsets = this.offsets;
        int offsetBase = arrayOffset;
        long sizeInBytes;

        if (fixedPerElementSizeInBytes.isPresent()) {
            sizeInBytes = fixedPerElementSizeInBytes.getAsInt() * (long) countSelectedPositionsFromOffsets(positions, offsets, offsetBase);
        }
        else if (rawElementBlock instanceof RunLengthEncodedBlock) {
            // RLE blocks don't have a fixed-size per position, but accept null for the position array
            sizeInBytes = rawElementBlock.getPositionsSizeInBytes(null, countSelectedPositionsFromOffsets(positions, offsets, offsetBase));
        }
        else {
            boolean[] selectedElements = new boolean[rawElementBlock.getPositionCount()];
            int selectedElementCount = countAndMarkSelectedPositionsFromOffsets(positions, offsets, offsetBase, selectedElements);
            sizeInBytes = rawElementBlock.getPositionsSizeInBytes(selectedElements, selectedElementCount);
        }
        return sizeInBytes + ((Integer.BYTES + Byte.BYTES) * (long) selectedArrayPositions);
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
        boolean[] valueIsNull = this.valueIsNull;
        boolean[] newValueIsNull;
        newValueIsNull = valueIsNull == null ? null : compactArray(valueIsNull, position + arrayOffset, length);

        if (newValues == values && newOffsets == offsets && newValueIsNull == valueIsNull) {
            return this;
        }
        return createArrayBlockInternal(0, length, newValueIsNull, newOffsets, newValues);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        if (clazz != Block.class) {
            throw new IllegalArgumentException("clazz must be Block.class");
        }
        checkReadablePosition(this, position);

        int startValueOffset = offsets[position + arrayOffset];
        int endValueOffset = offsets[position + 1 + arrayOffset];
        return clazz.cast(values.getRegion(startValueOffset, endValueOffset - startValueOffset));
    }

    @Override
    public ArrayBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(this, position);

        int startValueOffset = offsets[position + arrayOffset];
        int valueLength = offsets[position + 1 + arrayOffset] - startValueOffset;
        Block newValues = values.copyRegion(startValueOffset, valueLength);

        return createArrayBlockInternal(
                0,
                1,
                isNull(position) ? new boolean[] {true} : null,
                new int[] {0, valueLength},
                newValues);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkReadablePosition(this, position);

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
        checkReadablePosition(this, position);
        boolean[] valueIsNull = this.valueIsNull;
        return valueIsNull != null && valueIsNull[position + arrayOffset];
    }

    public <T> T apply(ArrayBlockFunction<T> function, int position)
    {
        checkReadablePosition(this, position);

        int startValueOffset = offsets[position + arrayOffset];
        int endValueOffset = offsets[position + 1 + arrayOffset];
        return function.apply(values, startValueOffset, endValueOffset - startValueOffset);
    }

    public interface ArrayBlockFunction<T>
    {
        T apply(Block block, int startPosition, int length);
    }
}
