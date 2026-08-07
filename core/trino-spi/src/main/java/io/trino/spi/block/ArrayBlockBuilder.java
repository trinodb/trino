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

import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.ArrayBlock.createArrayBlockInternal;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.copyBits;
import static io.trino.spi.block.Bitmap.hasSetBit;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Bitmap.setBits;
import static io.trino.spi.block.BlockUtil.calculateNewArraySize;
import static java.lang.Math.max;
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

public class ArrayBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(ArrayBlockBuilder.class);
    private static final int SIZE_IN_BYTES_PER_POSITION = Integer.BYTES + Byte.BYTES;

    private int positionCount;

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private final int initialEntryCount;

    private int[] offsets = new int[1];
    @Nullable
    private long[] valueIsValid;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    private final BlockBuilder values;
    private boolean currentEntryOpened;

    private long retainedSizeInBytes;

    /**
     * Caller of this constructor is responsible for making sure `valuesBlock` is constructed with the same `blockBuilderStatus` as the one in the argument
     */
    public ArrayBlockBuilder(BlockBuilder valuesBlock, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(blockBuilderStatus,
                valuesBlock,
                expectedEntries);
    }

    public ArrayBlockBuilder(Type elementType, BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        this(blockBuilderStatus,
                elementType.createBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry),
                expectedEntries);
    }

    public ArrayBlockBuilder(Type elementType, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(blockBuilderStatus,
                elementType.createBlockBuilder(blockBuilderStatus, expectedEntries),
                expectedEntries);
    }

    /**
     * Caller of this private constructor is responsible for making sure `values` is constructed with the same `blockBuilderStatus` as the one in the argument
     */
    private ArrayBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, BlockBuilder values, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.values = requireNonNull(values, "values is null");
        this.initialEntryCount = max(expectedEntries, 1);

        updateRetainedSize();
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return values.getSizeInBytes() + (SIZE_IN_BYTES_PER_POSITION * (long) positionCount);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes + values.getRetainedSizeInBytes();
    }

    public <E extends Throwable> void buildEntry(ArrayValueBuilder<E> builder)
            throws E
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was opened");
        }

        currentEntryOpened = true;
        builder.build(values);
        entryAdded(false);
        currentEntryOpened = false;
    }

    public ArrayEntryBuilder buildEntry()
    {
        return new ArrayEntryBuilderImplementation();
    }

    private class ArrayEntryBuilderImplementation
            implements ArrayEntryBuilder
    {
        private boolean entryBuilt;

        public ArrayEntryBuilderImplementation()
        {
            if (currentEntryOpened) {
                throw new IllegalStateException("Expected current entry to be closed but was opened");
            }
            currentEntryOpened = true;
        }

        @Override
        public BlockBuilder getElementBuilder()
        {
            if (entryBuilt || !currentEntryOpened) {
                throw new IllegalStateException("Entry has already been built");
            }
            return values;
        }

        @Override
        public void build()
        {
            if (entryBuilt || !currentEntryOpened) {
                throw new IllegalStateException("Entry has already been built");
            }
            entryBuilt = true;
            entryAdded(false);
            currentEntryOpened = false;
        }
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        ArrayBlock arrayBlock = (ArrayBlock) block;
        if (block.isNull(position)) {
            entryAdded(true);
            return;
        }

        int offsetBase = arrayBlock.getOffsetBase();
        int[] offsets = arrayBlock.getRawOffsets();
        int startOffset = offsets[offsetBase + position];
        int length = offsets[offsetBase + position + 1] - startOffset;

        values.appendBlockRange(arrayBlock.getRawElementBlock(), startOffset, length);
        entryAdded(false);
    }

    @Override
    public void appendRepeated(ValueBlock block, int position, int count)
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }
        for (int i = 0; i < count; i++) {
            append(block, position);
        }
    }

    @Override
    public void appendRange(ValueBlock block, int offset, int length)
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        ensureCapacity(positionCount + length);

        ArrayBlock arrayBlock = (ArrayBlock) block;

        int rawOffsetBase = arrayBlock.getOffsetBase();
        int[] rawOffsets = arrayBlock.getRawOffsets();
        int startOffset = rawOffsets[rawOffsetBase + offset];
        int endOffset = rawOffsets[rawOffsetBase + offset + length];

        values.appendBlockRange(arrayBlock.getRawElementBlock(), startOffset, endOffset - startOffset);

        // update offsets for copied data
        for (int i = 0; i < length; i++) {
            int entrySize = rawOffsets[rawOffsetBase + offset + i + 1] - rawOffsets[rawOffsetBase + offset + i];
            offsets[positionCount + i + 1] = offsets[positionCount + i] + entrySize;
        }

        long[] rawValueIsValid = arrayBlock.getRawValueIsValid();
        if (rawValueIsValid == null || !hasUnsetBit(rawValueIsValid, rawOffsetBase + offset, length)) {
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, length);
            }
            hasNonNullValue = true;
        }
        else {
            initializeValidityForFirstNull();
            copyBits(rawValueIsValid, rawOffsetBase + offset, valueIsValid, positionCount, length);
            hasNullValue = true;
            hasNonNullValue |= hasSetBit(rawValueIsValid, rawOffsetBase + offset, length);
        }

        positionCount += length;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(length * SIZE_IN_BYTES_PER_POSITION);
        }
    }

    @Override
    public void appendPositions(ValueBlock block, int[] positions, int offset, int length)
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }
        for (int i = 0; i < length; i++) {
            append(block, positions[offset + i]);
        }
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        entryAdded(true);
        return this;
    }

    @Override
    public void resetTo(int position)
    {
        checkIndex(position, positionCount + 1);
        currentEntryOpened = false;
        positionCount = position;
        values.resetTo(offsets[positionCount]);
    }

    private void entryAdded(boolean isNull)
    {
        ensureCapacity(positionCount + 1);

        offsets[positionCount + 1] = values.getPositionCount();
        if (isNull) {
            initializeValidityForFirstNull();
            clear(valueIsValid, 0, positionCount);
        }
        else if (valueIsValid != null) {
            set(valueIsValid, 0, positionCount);
        }
        hasNullValue |= isNull;
        hasNonNullValue |= !isNull;
        positionCount++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(SIZE_IN_BYTES_PER_POSITION);
        }
    }

    private void ensureCapacity(int capacity)
    {
        if (offsets.length > capacity) {
            return;
        }

        int newSize;
        if (initialized) {
            newSize = calculateNewArraySize(capacity);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }
        newSize = max(newSize, capacity);

        if (valueIsValid != null) {
            valueIsValid = Bitmap.ensureCapacity(valueIsValid, newSize);
        }
        offsets = Arrays.copyOf(offsets, newSize + 1);
        updateRetainedSize();
    }

    private boolean initializeValidityForFirstNull()
    {
        if (valueIsValid != null) {
            return false;
        }
        valueIsValid = Bitmap.allocateWords(offsets.length - 1, false);
        setBits(valueIsValid, 0, 0, positionCount);
        updateRetainedSize();
        return true;
    }

    private void updateRetainedSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsValid) + sizeOf(offsets);
        if (blockBuilderStatus != null) {
            retainedSizeInBytes += BlockBuilderStatus.INSTANCE_SIZE;
        }
    }

    @Override
    public Block build()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        if (!hasNonNullValue) {
            return nullRle(positionCount);
        }
        return buildValueBlock();
    }

    @Override
    public ValueBlock buildValueBlock()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        return createArrayBlockInternal(0, positionCount, hasNullValue ? valueIsValid : null, offsets, values.build());
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        return new ArrayBlockBuilder(blockBuilderStatus, values.newBlockBuilderLike(blockBuilderStatus), expectedEntries);
    }

    @Override
    public String toString()
    {
        return "ArrayBlockBuilder{" +
                "positionCount=" + getPositionCount() +
                '}';
    }

    private Block nullRle(int positionCount)
    {
        ArrayBlock nullValueBlock = createArrayBlockInternal(0, 1, new long[] {0}, new int[] {0, 0}, values.newBlockBuilderLike(null).build());
        return RunLengthEncodedBlock.create(nullValueBlock, positionCount);
    }
}
