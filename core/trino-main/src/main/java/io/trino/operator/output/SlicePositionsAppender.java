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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.operator.output.PositionsAppenderUtil.MAX_ARRAY_SIZE;
import static io.trino.operator.output.PositionsAppenderUtil.calculateBlockResetBytes;
import static io.trino.operator.output.PositionsAppenderUtil.calculateBlockResetSize;
import static io.trino.operator.output.PositionsAppenderUtil.calculateNewArraySize;
import static java.lang.Math.min;

public class SlicePositionsAppender
        implements PositionsAppender
{
    private static final int EXPECTED_BYTES_PER_ENTRY = 32;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SlicePositionsAppender.class).instanceSize();
    private static final Block NULL_VALUE_BLOCK = new VariableWidthBlock(1, EMPTY_SLICE, new int[] {0, 0}, Optional.of(new boolean[] {true}));

    private boolean initialized;
    private int initialEntryCount;
    private int initialBytesSize;

    private byte[] bytes = new byte[0];
    private int currentOffset;

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
    public void append(IntArrayList positions, Block block)
    {
        if (positions.isEmpty()) {
            return;
        }
        // performance of this method depends on block being always the same, flat type
        checkArgument(block instanceof VariableWidthBlock);
        ensurePositionCapacity(positionCount + positions.size());
        int[] positionArray = positions.elements();
        int newByteCount = 0;
        int[] lengths = new int[positions.size()];

        if (block.mayHaveNull()) {
            for (int i = 0; i < positions.size(); i++) {
                int position = positionArray[i];
                if (block.isNull(position)) {
                    offsets[positionCount + i + 1] = offsets[positionCount + i];
                    valueIsNull[positionCount + i] = true;
                    hasNullValue = true;
                }
                else {
                    int length = block.getSliceLength(position);
                    lengths[i] = length;
                    newByteCount += length;
                    offsets[positionCount + i + 1] = offsets[positionCount + i] + length;
                    hasNonNullValue = true;
                }
            }
        }
        else {
            for (int i = 0; i < positions.size(); i++) {
                int position = positionArray[i];
                int length = block.getSliceLength(position);
                lengths[i] = length;
                newByteCount += length;
                offsets[positionCount + i + 1] = offsets[positionCount + i] + length;
            }
            hasNonNullValue = true;
        }
        copyBytes(block, lengths, positionArray, positions.size(), offsets, positionCount, newByteCount);
    }

    @Override
    public void appendRle(RunLengthEncodedBlock block)
    {
        int rlePositionCount = block.getPositionCount();
        if (rlePositionCount == 0) {
            return;
        }
        int sourcePosition = 0;
        ensurePositionCapacity(positionCount + rlePositionCount);
        if (block.isNull(sourcePosition)) {
            int offset = offsets[positionCount];
            Arrays.fill(valueIsNull, positionCount, positionCount + rlePositionCount, true);
            Arrays.fill(offsets, positionCount + 1, positionCount + rlePositionCount + 1, offset);
            positionCount += rlePositionCount;

            hasNullValue = true;
            updateSize(rlePositionCount, 0);
        }
        else {
            int startOffset = offsets[positionCount];
            hasNonNullValue = true;
            duplicateBytes(block.getValue(), sourcePosition, rlePositionCount, startOffset);
        }
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, positionCount);
        }
        VariableWidthBlock result = new VariableWidthBlock(
                positionCount,
                Slices.wrappedBuffer(bytes, 0, currentOffset),
                offsets,
                hasNullValue ? Optional.of(valueIsNull) : Optional.empty());
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

    private void copyBytes(Block block, int[] lengths, int[] positions, int count, int[] targetOffsets, int targetOffsetsIndex, int newByteCount)
    {
        ensureBytesCapacity(currentOffset + newByteCount);

        for (int i = 0; i < count; i++) {
            int position = positions[i];
            if (!block.isNull(position)) {
                int length = lengths[i];
                Slice slice = block.getSlice(position, 0, length);
                slice.getBytes(0, bytes, targetOffsets[targetOffsetsIndex + i], length);
            }
        }

        positionCount += count;
        currentOffset += newByteCount;
        updateSize(count, newByteCount);
    }

    /**
     * Copy {@code length} bytes from {@code block}, at position {@code position} to {@code count} consecutive positions in the {@link #bytes} array.
     */
    private void duplicateBytes(Block block, int position, int count, int startOffset)
    {
        int length = block.getSliceLength(position);
        int newByteCount = count * length;
        ensureBytesCapacity(currentOffset + newByteCount);

        Slice slice = block.getSlice(position, 0, length);
        for (int i = 0; i < count; i++) {
            slice.getBytes(0, bytes, startOffset + (i * length), length);
            offsets[positionCount + i + 1] = startOffset + ((i + 1) * length);
        }

        positionCount += count;
        currentOffset += newByteCount;
        updateSize(count, newByteCount);
    }

    private void reset()
    {
        initialEntryCount = calculateBlockResetSize(positionCount);
        initialBytesSize = calculateBlockResetBytes(currentOffset);
        initialized = false;
        valueIsNull = new boolean[0];
        offsets = new int[1];
        bytes = new byte[0];
        positionCount = 0;
        currentOffset = 0;
        sizeInBytes = 0;
        hasNonNullValue = false;
        hasNullValue = false;
        updateRetainedSize();
    }

    private void updateSize(long positionsSize, int bytesWritten)
    {
        sizeInBytes += (SIZE_OF_BYTE + SIZE_OF_INT) * positionsSize + bytesWritten;
    }

    private void ensureBytesCapacity(int bytesCapacity)
    {
        if (bytes.length < bytesCapacity) {
            int newBytesLength = Math.max(bytes.length, initialBytesSize);
            if (bytesCapacity > newBytesLength) {
                newBytesLength = Math.max(bytesCapacity, calculateNewArraySize(newBytesLength));
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
