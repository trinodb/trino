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
package io.trino.operator.output;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Arrays;
import java.util.Optional;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.operator.output.PositionsAppenderUtil.MAX_ARRAY_SIZE;
import static io.trino.operator.output.PositionsAppenderUtil.calculateBlockResetBytes;
import static io.trino.operator.output.PositionsAppenderUtil.calculateBlockResetSize;
import static io.trino.operator.output.PositionsAppenderUtil.calculateNewArraySize;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class SlicePositionsAppender
        implements PositionsAppender
{
    private static final int EXPECTED_BYTES_PER_ENTRY = 32;
    private static final int INSTANCE_SIZE = instanceSize(SlicePositionsAppender.class);
    private static final Block NULL_VALUE_BLOCK = new VariableWidthBlock(1, EMPTY_SLICE, new int[] {0, 0}, Optional.of(new boolean[] {true}));

    private boolean initialized;
    private int initialEntryCount;
    private int initialBytesSize;

    private byte[] bytes = new byte[0];

    private boolean hasNullValue;
    private boolean hasNonNullValue;
    // it is assumed that the offsets array is one position longer than the valueIsNull array
    private boolean[] valueIsNull = new boolean[0];
    private int[] offsets = new int[1];

    private int positionCount;

    private long retainedSizeInBytes;
    private long sizeInBytes;

    public SlicePositionsAppender(int expectedEntries, long maxPageSizeInBytes)
    {
        this(expectedEntries, getExpectedBytes(maxPageSizeInBytes, expectedEntries));
    }

    public SlicePositionsAppender(int expectedEntries, int expectedBytes)
    {
        initialEntryCount = expectedEntries;
        initialBytesSize = min(expectedBytes, MAX_ARRAY_SIZE);

        updateRetainedSize();
    }

    @Override
    // TODO: Make PositionsAppender work performant with different block types (https://github.com/trinodb/trino/issues/13267)
    public void append(IntArrayList positions, Block block)
    {
        if (positions.isEmpty()) {
            return;
        }
        ensurePositionCapacity(positionCount + positions.size());
        if (block instanceof VariableWidthBlock variableWidthBlock) {
            int newByteCount = 0;
            int[] lengths = new int[positions.size()];
            int[] sourceOffsets = new int[positions.size()];
            int[] positionArray = positions.elements();

            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    int length = variableWidthBlock.getSliceLength(position);
                    lengths[i] = length;
                    sourceOffsets[i] = variableWidthBlock.getRawSliceOffset(position);
                    newByteCount += length;
                    boolean isNull = block.isNull(position);
                    valueIsNull[positionCount + i] = isNull;
                    offsets[positionCount + i + 1] = offsets[positionCount + i] + length;
                    hasNullValue |= isNull;
                    hasNonNullValue |= !isNull;
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    int length = variableWidthBlock.getSliceLength(position);
                    lengths[i] = length;
                    sourceOffsets[i] = variableWidthBlock.getRawSliceOffset(position);
                    newByteCount += length;
                    offsets[positionCount + i + 1] = offsets[positionCount + i] + length;
                }
                hasNonNullValue = true;
            }
            copyBytes(variableWidthBlock.getRawSlice(), lengths, sourceOffsets, positions.size(), newByteCount);
        }
        else {
            appendGenericBlock(positions, block);
        }
    }

    @Override
    public void appendRle(Block block, int rlePositionCount)
    {
        if (rlePositionCount == 0) {
            return;
        }
        ensurePositionCapacity(positionCount + rlePositionCount);
        if (block.isNull(0)) {
            Arrays.fill(valueIsNull, positionCount, positionCount + rlePositionCount, true);
            Arrays.fill(offsets, positionCount + 1, positionCount + rlePositionCount + 1, getCurrentOffset());
            positionCount += rlePositionCount;

            hasNullValue = true;
            updateSize(rlePositionCount, 0);
        }
        else {
            hasNonNullValue = true;
            duplicateBytes(block.getSlice(0, 0, block.getSliceLength(0)), rlePositionCount);
        }
    }

    @Override
    public void append(int position, Block source)
    {
        ensurePositionCapacity(positionCount + 1);
        if (source.isNull(position)) {
            valueIsNull[positionCount] = true;
            offsets[positionCount + 1] = getCurrentOffset();
            positionCount++;

            hasNullValue = true;
            updateSize(1, 0);
        }
        else {
            hasNonNullValue = true;
            int currentOffset = getCurrentOffset();
            int sliceLength = source.getSliceLength(position);
            Slice slice = source.getSlice(position, 0, sliceLength);

            ensureExtraBytesCapacity(sliceLength);

            slice.getBytes(0, bytes, currentOffset, sliceLength);

            offsets[positionCount + 1] = currentOffset + sliceLength;

            positionCount++;
            updateSize(1, sliceLength);
        }
    }

    @Override
    public Block build()
    {
        Block result;
        if (hasNonNullValue) {
            result = new VariableWidthBlock(
                    positionCount,
                    Slices.wrappedBuffer(bytes, 0, getCurrentOffset()),
                    offsets,
                    hasNullValue ? Optional.of(valueIsNull) : Optional.empty());
        }
        else {
            result = RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positionCount);
        }
        reset();
        return result;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    private void copyBytes(Slice rawSlice, int[] lengths, int[] sourceOffsets, int count, int newByteCount)
    {
        ensureExtraBytesCapacity(newByteCount);

        if (rawSlice.hasByteArray()) {
            byte[] base = rawSlice.byteArray();
            int byteArrayOffset = rawSlice.byteArrayOffset();
            for (int i = 0; i < count; i++) {
                System.arraycopy(base, byteArrayOffset + sourceOffsets[i], bytes, offsets[positionCount + i], lengths[i]);
            }
        }
        else {
            for (int i = 0; i < count; i++) {
                rawSlice.getBytes(sourceOffsets[i], bytes, offsets[positionCount + i], lengths[i]);
            }
        }

        positionCount += count;
        updateSize(count, newByteCount);
    }

    /**
     * Copy all bytes from {@code slice} to {@code count} consecutive positions in the {@link #bytes} array.
     */
    private void duplicateBytes(Slice slice, int count)
    {
        int length = slice.length();
        int newByteCount = toIntExact((long) count * length);
        int startOffset = getCurrentOffset();
        ensureExtraBytesCapacity(newByteCount);

        duplicateBytes(slice, bytes, startOffset, count);

        int currentStartOffset = startOffset + length;
        for (int i = 0; i < count; i++) {
            offsets[positionCount + i + 1] = currentStartOffset;
            currentStartOffset += length;
        }

        positionCount += count;
        updateSize(count, newByteCount);
    }

    /**
     * Copy {@code length} bytes from {@code slice}, starting at offset {@code sourceOffset} to {@code count} consecutive positions in the {@link #bytes} array.
     */
    @VisibleForTesting
    static void duplicateBytes(Slice slice, byte[] bytes, int startOffset, int count)
    {
        int length = slice.length();
        if (length == 0) {
            // nothing to copy
            return;
        }
        // copy slice to the first position
        slice.getBytes(0, bytes, startOffset, length);
        int totalDuplicatedBytes = count * length;
        int duplicatedBytes = length;
        // copy every byte copied so far, doubling the number of bytes copied on evey iteration
        while (duplicatedBytes * 2 <= totalDuplicatedBytes) {
            System.arraycopy(bytes, startOffset, bytes, startOffset + duplicatedBytes, duplicatedBytes);
            duplicatedBytes = duplicatedBytes * 2;
        }
        // copy the leftover
        System.arraycopy(bytes, startOffset, bytes, startOffset + duplicatedBytes, totalDuplicatedBytes - duplicatedBytes);
    }

    private void appendGenericBlock(IntArrayList positions, Block block)
    {
        int newByteCount = 0;
        for (int i = 0; i < positions.size(); i++) {
            int position = positions.getInt(i);
            if (block.isNull(position)) {
                offsets[positionCount + 1] = offsets[positionCount];
                valueIsNull[positionCount] = true;
                hasNullValue = true;
            }
            else {
                int length = block.getSliceLength(position);
                ensureExtraBytesCapacity(length);
                Slice slice = block.getSlice(position, 0, length);
                slice.getBytes(0, bytes, offsets[positionCount], length);
                offsets[positionCount + 1] = offsets[positionCount] + length;
                hasNonNullValue = true;
                newByteCount += length;
            }
            positionCount++;
        }
        updateSize(positions.size(), newByteCount);
    }

    private void reset()
    {
        initialEntryCount = calculateBlockResetSize(positionCount);
        initialBytesSize = calculateBlockResetBytes(getCurrentOffset());
        initialized = false;
        valueIsNull = new boolean[0];
        offsets = new int[1];
        bytes = new byte[0];
        positionCount = 0;
        sizeInBytes = 0;
        hasNonNullValue = false;
        hasNullValue = false;
        updateRetainedSize();
    }

    private int getCurrentOffset()
    {
        return offsets[positionCount];
    }

    private void updateSize(long positionsSize, int bytesWritten)
    {
        sizeInBytes += (SIZE_OF_BYTE + SIZE_OF_INT) * positionsSize + bytesWritten;
    }

    private void ensureExtraBytesCapacity(int extraBytesCapacity)
    {
        int totalBytesCapacity = getCurrentOffset() + extraBytesCapacity;
        if (bytes.length < totalBytesCapacity) {
            int newBytesLength = Math.max(bytes.length, initialBytesSize);
            if (totalBytesCapacity > newBytesLength) {
                newBytesLength = Math.max(totalBytesCapacity, calculateNewArraySize(newBytesLength));
            }
            bytes = Arrays.copyOf(bytes, newBytesLength);
            updateRetainedSize();
        }
    }

    private void ensurePositionCapacity(int capacity)
    {
        if (valueIsNull.length < capacity) {
            int newSize;
            if (initialized) {
                newSize = calculateNewArraySize(valueIsNull.length);
            }
            else {
                newSize = initialEntryCount;
                initialized = true;
            }
            newSize = Math.max(newSize, capacity);

            valueIsNull = Arrays.copyOf(valueIsNull, newSize);
            offsets = Arrays.copyOf(offsets, newSize + 1);
            updateRetainedSize();
        }
    }

    private void updateRetainedSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(offsets) + sizeOf(bytes);
    }

    private static int getExpectedBytes(long maxPageSizeInBytes, int expectedPositions)
    {
        // it is guaranteed Math.min will not overflow; safe to cast
        return (int) min((long) expectedPositions * EXPECTED_BYTES_PER_ENTRY, maxPageSizeInBytes);
    }
}
