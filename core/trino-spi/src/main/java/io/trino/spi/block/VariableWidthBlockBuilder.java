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
import io.trino.spi.TrinoException;
import jakarta.annotation.Nullable;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.clearBits;
import static io.trino.spi.block.Bitmap.copyBits;
import static io.trino.spi.block.Bitmap.hasSetBit;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Bitmap.setBits;
import static io.trino.spi.block.BlockUtil.MAX_ARRAY_SIZE;
import static io.trino.spi.block.BlockUtil.calculateBlockResetBytes;
import static io.trino.spi.block.BlockUtil.calculateNewArraySize;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkIndex;

public class VariableWidthBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(VariableWidthBlockBuilder.class);
    static final Block NULL_VALUE_BLOCK = new VariableWidthBlock(0, 1, EMPTY_SLICE, new int[] {0, 0}, new long[] {0});
    private static final int SIZE_IN_BYTES_PER_POSITION = Integer.BYTES + Byte.BYTES;

    private final BlockBuilderStatus blockBuilderStatus;

    private final int initialEntryCount;
    private final int initialSliceOutputSize;

    private byte[] bytes = new byte[0];

    private boolean hasNullValue;
    private boolean hasNonNullValue;
    @Nullable
    private long[] valueIsValid;
    private int[] offsets = new int[1];

    private int positionCount;

    private long arraysRetainedSizeInBytes;

    public VariableWidthBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytes)
    {
        this.blockBuilderStatus = blockBuilderStatus;

        initialEntryCount = expectedEntries;
        initialSliceOutputSize = min(expectedBytes, MAX_ARRAY_SIZE);

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
        return offsets[positionCount] + SIZE_IN_BYTES_PER_POSITION * (long) positionCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + arraysRetainedSizeInBytes;
        if (blockBuilderStatus != null) {
            size += BlockBuilderStatus.INSTANCE_SIZE;
        }
        return size;
    }

    public VariableWidthBlockBuilder writeEntry(Slice source)
    {
        return writeEntry(source, 0, source.length());
    }

    public VariableWidthBlockBuilder writeEntry(Slice source, int sourceIndex, int length)
    {
        ensureFreeSpace(length);
        source.getBytes(sourceIndex, bytes, offsets[positionCount], length);
        entryAdded(length, false);
        return this;
    }

    public VariableWidthBlockBuilder writeEntry(byte[] source, int sourceIndex, int length)
    {
        ensureFreeSpace(length);
        System.arraycopy(source, sourceIndex, bytes, offsets[positionCount], length);
        entryAdded(length, false);
        return this;
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        ensureCapacity(positionCount + 1);

        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
        int bytesWritten = 0;
        if (variableWidthBlock.isNull(position)) {
            initializeValidityForFirstNull();
            clear(valueIsValid, 0, positionCount);
            hasNullValue = true;
        }
        else {
            int rawArrayBase = variableWidthBlock.getRawArrayBase();
            int[] rawOffsets = variableWidthBlock.getRawOffsets();
            int startValueOffset = rawOffsets[rawArrayBase + position];
            int endValueOffset = rawOffsets[rawArrayBase + position + 1];
            int length = endValueOffset - startValueOffset;
            ensureFreeSpace(length);

            Slice rawSlice = variableWidthBlock.getRawSlice();

            rawSlice.getBytes(startValueOffset, bytes, offsets[positionCount], length);

            bytesWritten = length;
            if (valueIsValid != null) {
                set(valueIsValid, 0, positionCount);
            }
            hasNonNullValue = true;
        }
        offsets[positionCount + 1] = offsets[positionCount] + bytesWritten;
        positionCount++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(SIZE_IN_BYTES_PER_POSITION + bytesWritten);
        }
    }

    @Override
    public void appendRepeated(ValueBlock block, int position, int count)
    {
        if (count == 0) {
            return;
        }
        if (count == 1) {
            append(block, position);
            return;
        }

        ensureCapacity(positionCount + count);

        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
        int bytesWritten = 0;
        if (variableWidthBlock.isNull(position)) {
            initializeValidityForFirstNull();
            clearBits(valueIsValid, 0, positionCount, count);
            Arrays.fill(offsets, positionCount + 1, positionCount + count + 1, offsets[positionCount]);
            hasNullValue = true;
        }
        else {
            int rawArrayBase = variableWidthBlock.getRawArrayBase();
            int[] rawOffsets = variableWidthBlock.getRawOffsets();
            int startValueOffset = rawOffsets[rawArrayBase + position];
            int endValueOffset = rawOffsets[rawArrayBase + position + 1];
            int length = endValueOffset - startValueOffset;

            if (length > 0) {
                bytesWritten = toIntExact((long) length * count);
                ensureFreeSpace(bytesWritten);

                // copy in the value
                Slice rawSlice = variableWidthBlock.getRawSlice();
                int currentOffset = offsets[positionCount];
                rawSlice.getBytes(startValueOffset, bytes, currentOffset, length);

                // repeatedly duplicate the written vales, doubling the number of values copied each time
                int duplicatedBytes = length;
                while (duplicatedBytes * 2 <= bytesWritten) {
                    System.arraycopy(bytes, currentOffset, bytes, currentOffset + duplicatedBytes, duplicatedBytes);
                    duplicatedBytes = duplicatedBytes * 2;
                }
                // copy the remaining values
                System.arraycopy(bytes, currentOffset, bytes, currentOffset + duplicatedBytes, bytesWritten - duplicatedBytes);

                // set the offsets
                int previousOffset = currentOffset;
                for (int i = 0; i < count; i++) {
                    previousOffset += length;
                    offsets[positionCount + i + 1] = previousOffset;
                }
                if (valueIsValid != null) {
                    setBits(valueIsValid, 0, positionCount, count);
                }
            }
            else {
                // zero length array
                Arrays.fill(offsets, positionCount + 1, positionCount + count + 1, offsets[positionCount]);
                if (valueIsValid != null) {
                    setBits(valueIsValid, 0, positionCount, count);
                }
            }

            hasNonNullValue = true;
        }
        positionCount += count;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(count * SIZE_IN_BYTES_PER_POSITION + bytesWritten);
        }
    }

    @Override
    public void appendRange(ValueBlock block, int offset, int length)
    {
        if (length == 0) {
            return;
        }
        if (length == 1) {
            append(block, offset);
            return;
        }

        ensureCapacity(positionCount + length);

        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
        int rawArrayBase = variableWidthBlock.getRawArrayBase();
        int[] rawOffsets = variableWidthBlock.getRawOffsets();
        int startValueOffset = rawOffsets[rawArrayBase + offset];
        int totalSize = rawOffsets[rawArrayBase + offset + length] - startValueOffset;
        // grow the buffer for the new data
        ensureFreeSpace(totalSize);

        Slice sourceSlice = variableWidthBlock.getRawSlice();
        sourceSlice.getBytes(startValueOffset, bytes, offsets[positionCount], totalSize);

        // update offsets for copied data
        int offsetDelta = offsets[positionCount] - rawOffsets[rawArrayBase + offset];
        for (int i = 0; i < length; i++) {
            offsets[positionCount + i + 1] = rawOffsets[rawArrayBase + offset + i + 1] + offsetDelta;
        }

        // update nulls
        long[] rawValueIsValid = variableWidthBlock.getRawValueIsValid();
        if (rawValueIsValid == null || !hasUnsetBit(rawValueIsValid, rawArrayBase + offset, length)) {
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, length);
            }
            hasNonNullValue = true;
        }
        else {
            initializeValidityForFirstNull();
            copyBits(rawValueIsValid, rawArrayBase + offset, valueIsValid, positionCount, length);
            hasNullValue = true;
            hasNonNullValue |= hasSetBit(rawValueIsValid, rawArrayBase + offset, length);
        }
        positionCount += length;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(length * SIZE_IN_BYTES_PER_POSITION);
        }
    }

    @Override
    public void appendPositions(ValueBlock block, int[] positions, int offset, int length)
    {
        if (length == 0) {
            return;
        }
        if (length == 1) {
            append(block, positions[offset]);
            return;
        }

        ensureCapacity(positionCount + length);

        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
        int rawArrayBase = variableWidthBlock.getRawArrayBase();
        int[] rawOffsets = variableWidthBlock.getRawOffsets();

        // update the offsets and compute the total size
        int initialOffset = offsets[positionCount];
        int totalSize = 0;
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            totalSize += rawOffsets[rawArrayBase + position + 1] - rawOffsets[rawArrayBase + position];
            offsets[positionCount + i + 1] = initialOffset + totalSize;
        }
        // grow the buffer for the new data
        ensureFreeSpace(totalSize);

        // copy values to buffer
        Slice rawSlice = variableWidthBlock.getRawSlice();
        byte[] sourceBytes = rawSlice.byteArray();
        int sourceBytesOffset = rawSlice.byteArrayOffset();
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            int sourceStart = rawOffsets[rawArrayBase + position];
            int sourceLength = rawOffsets[rawArrayBase + position + 1] - sourceStart;
            System.arraycopy(sourceBytes, sourceBytesOffset + sourceStart, bytes, offsets[positionCount + i], sourceLength);
            totalSize += sourceLength;
        }

        // update nulls
        long[] rawValueIsValid = variableWidthBlock.getRawValueIsValid();
        if (rawValueIsValid == null || !hasUnsetBit(rawValueIsValid, rawArrayBase, positions, offset, length)) {
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, length);
            }
            hasNonNullValue = true;
        }
        else {
            initializeValidityForFirstNull();
            copyBits(rawValueIsValid, rawArrayBase, positions, offset, valueIsValid, positionCount, length);
            hasNullValue = true;
            hasNonNullValue |= hasSetBit(rawValueIsValid, rawArrayBase, positions, offset, length);
        }
        positionCount += length;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(length * SIZE_IN_BYTES_PER_POSITION + totalSize);
        }
    }

    @Override
    public BlockBuilder appendNull()
    {
        hasNullValue = true;
        entryAdded(0, true);
        return this;
    }

    @Override
    public void resetTo(int position)
    {
        checkIndex(position, positionCount + 1);
        positionCount = position;
    }

    private void entryAdded(int bytesWritten, boolean isNull)
    {
        ensureCapacity(positionCount + 1);

        if (isNull) {
            initializeValidityForFirstNull();
            clear(valueIsValid, 0, positionCount);
        }
        else if (valueIsValid != null) {
            set(valueIsValid, 0, positionCount);
        }
        offsets[positionCount + 1] = offsets[positionCount] + bytesWritten;

        positionCount++;
        hasNonNullValue |= !isNull;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(SIZE_IN_BYTES_PER_POSITION + bytesWritten);
        }
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positionCount);
        }
        return buildValueBlock();
    }

    @Override
    public VariableWidthBlock buildValueBlock()
    {
        return new VariableWidthBlock(0, positionCount, Slices.wrappedBuffer(bytes, 0, offsets[positionCount]), offsets, hasNullValue ? valueIsValid : null);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        int currentSizeInBytes = positionCount == 0 ? positionCount : (getOffset(positionCount) - getOffset(0));
        return new VariableWidthBlockBuilder(blockBuilderStatus, expectedEntries, calculateBlockResetBytes(currentSizeInBytes));
    }

    private int getOffset(int position)
    {
        return offsets[position];
    }

    private void ensureCapacity(int capacity)
    {
        if (offsets.length > capacity) {
            return;
        }

        int newSize = calculateNewArraySize(capacity, initialEntryCount);
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

    private void ensureFreeSpace(int extraBytesCapacity)
    {
        long requiredSize = (long) offsets[positionCount] + extraBytesCapacity;
        if (requiredSize > MAX_ARRAY_SIZE) {
            throw new TrinoException(
                    GENERIC_INSUFFICIENT_RESOURCES,
                    "Block byte size exceeds limit of " + MAX_ARRAY_SIZE + " bytes");
        }
        if (bytes.length >= requiredSize) {
            return;
        }

        int newSize = calculateNewArraySize((int) requiredSize, initialSliceOutputSize);
        bytes = Arrays.copyOf(bytes, newSize);
        updateRetainedSize();
    }

    private void updateRetainedSize()
    {
        arraysRetainedSizeInBytes = sizeOf(valueIsValid) + sizeOf(offsets) + sizeOf(bytes);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("VariableWidthBlockBuilder{");
        sb.append("positionCount=").append(positionCount);
        sb.append(", size=").append(offsets[positionCount]);
        sb.append('}');
        return sb.toString();
    }
}
