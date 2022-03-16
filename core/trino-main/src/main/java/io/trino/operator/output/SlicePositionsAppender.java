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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.BlockUtil;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Optional;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.spi.block.BlockUtil.MAX_ARRAY_SIZE;
import static io.trino.spi.block.BlockUtil.calculateBlockResetBytes;
import static io.trino.spi.block.BlockUtil.calculateBlockResetSize;
import static io.trino.spi.type.AbstractVariableWidthType.EXPECTED_BYTES_PER_ENTRY;
import static java.lang.Math.min;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class SlicePositionsAppender
        implements BlockTypeAwarePositionsAppender
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SlicePositionsAppender.class).instanceSize();
    private static final Block NULL_VALUE_BLOCK = new VariableWidthBlock(1, EMPTY_SLICE, new int[] {0, 0}, Optional.of(new boolean[] {true}));

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;

    private boolean initialized;
    private final int initialEntryCount;
    private int initialBytesSize;

    private byte[] bytes = new byte[0];
    private int currentOffset;

    private boolean hasNullValue;
    private boolean hasNonNullValue;
    // it is assumed that the offsets array is one position longer than the valueIsNull array
    private boolean[] valueIsNull = new boolean[0];
    private int[] offsets = new int[1];

    private int positionCount;

    private long arraysRetainedSizeInBytes;
    private final BlockBuilder fakeBlockBuilder;

    public SlicePositionsAppender(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedPositions)
    {
        this(blockBuilderStatus, expectedPositions, getExpectedBytes(blockBuilderStatus, expectedPositions));
    }

    public SlicePositionsAppender(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytes)
    {
        this.blockBuilderStatus = blockBuilderStatus;

        initialEntryCount = expectedEntries;
        initialBytesSize = min(expectedBytes, MAX_ARRAY_SIZE);

        updateArraysDataSize();
        fakeBlockBuilder = new VariableWidthBlockBuilder(null, 0, 0)
        {
            @Override
            public BlockBuilder writeBytes(Slice source, int sourceIndex, int length)
            {
                SlicePositionsAppender.this.writeBytes(source, sourceIndex, length);
                return this;
            }
        };
    }

    @Override
    public void append(IntArrayList positions, Block block)
    {
        ensurePositionCapacity(positionCount + positions.size());
        int[] positionArray = positions.elements();
        int newByteCount = 0;
        int notNullCount = 0;
        int[] lengths = new int[positions.size()];
        int[] offsets = new int[positions.size()];
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
        Slice rawSlice = variableWidthBlock.getRawSlice(0);

        if (block.mayHaveNull()) {
            int[] targetOffsets = new int[positions.size() + 1];
            targetOffsets[0] = this.offsets[positionCount];
            for (int i = 0; i < positions.size(); i++) {
                int position = positionArray[i];
                if (variableWidthBlock.isNull(position)) {
                    this.offsets[positionCount + i + 1] = this.offsets[positionCount + i];
                    valueIsNull[positionCount + i] = true;
                }
                else {
                    int length = variableWidthBlock.getSliceLength(position);
                    int offset = variableWidthBlock.getPositionOffset(position);
                    lengths[notNullCount] = length;
                    offsets[notNullCount] = offset;
                    targetOffsets[notNullCount + 1] = targetOffsets[notNullCount] + length;
                    newByteCount += length;
                    notNullCount++;
                    this.offsets[positionCount + i + 1] = this.offsets[positionCount + i] + length;
                }
            }
            int nullCount = positions.size() - notNullCount;
            hasNullValue |= nullCount > 0;
            hasNonNullValue |= notNullCount > 0;
            positionCount += nullCount;
            copyBytes(rawSlice, lengths, offsets, notNullCount, targetOffsets, 0, newByteCount);
        }
        else {
            newByteCount = extractLengths(positions, positionArray, variableWidthBlock, lengths, offsets);
            notNullCount = positions.size();
            hasNonNullValue |= notNullCount > 0;
            copyBytes(rawSlice, lengths, offsets, notNullCount, this.offsets, positionCount, newByteCount);
        }
        // update BlockBuilderStatus for null values
        updateBlockBuilderStatus(positions.size() - notNullCount, 0);
    }

    private void copyBytes(Slice rawSlice, int[] lengths, int[] sourceOffsets, int count, int[] targetOffsets, int targetOffsetsIndex, int newByteCount)
    {
        ensureBytesCapacity(currentOffset + newByteCount);
        Object base = rawSlice.getBase();

        if (base instanceof byte[] && rawSlice.getAddress() == ARRAY_BYTE_BASE_OFFSET) {
            for (int i = 0; i < count; i++) {
                int length = lengths[i];
                int offset = sourceOffsets[i];
                System.arraycopy(base, offset, bytes, targetOffsets[targetOffsetsIndex + i], length);
            }
        }
        else {
            for (int i = 0; i < count; i++) {
                int length = lengths[i];
                int offset = sourceOffsets[i];
                rawSlice.getBytes(offset, bytes, targetOffsets[targetOffsetsIndex + i], length);
            }
        }
        positionCount += count;
        currentOffset += newByteCount;
        updateBlockBuilderStatus(count, newByteCount);
    }

    private int extractLengths(IntArrayList positions, int[] positionArray, VariableWidthBlock variableWidthBlock, int[] lengths, int[] offsets)
    {
        int newByteCount = 0;
        for (int i = 0; i < positions.size(); i++) {
            int position = positionArray[i];
            int length = variableWidthBlock.getSliceLength(position);
            int offset = variableWidthBlock.getPositionOffset(position);
            lengths[i] = length;
            offsets[i] = offset;
            newByteCount += length;
            this.offsets[positionCount + i + 1] = this.offsets[positionCount + i] + length;
        }
        return newByteCount;
    }

    @Override
    public void appendDictionary(IntArrayList positions, DictionaryBlock block)
    {
        ensurePositionCapacity(positionCount + positions.size());
        int[] positionArray = positions.elements();
        int newByteCount = 0;
        int notNullCount = 0;
        int[] lengths = new int[positions.size()];
        int[] offsets = new int[positions.size()];
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block.getDictionary();
        Slice rawSlice = variableWidthBlock.getRawSlice(0);

        if (block.mayHaveNull()) {
            int[] targetOffsets = new int[positions.size() + 1];
            targetOffsets[0] = this.offsets[positionCount];
            for (int i = 0; i < positions.size(); i++) {
                int position = block.getId(positionArray[i]);
                if (variableWidthBlock.isNull(position)) {
                    this.offsets[positionCount + i + 1] = this.offsets[positionCount + i];
                    valueIsNull[positionCount + i] = true;
                }
                else {
                    int length = variableWidthBlock.getSliceLength(position);
                    int offset = variableWidthBlock.getPositionOffset(position);
                    lengths[notNullCount] = length;
                    offsets[notNullCount] = offset;
                    targetOffsets[notNullCount + 1] = targetOffsets[notNullCount] + length;
                    newByteCount += length;
                    notNullCount++;
                    this.offsets[positionCount + i + 1] = this.offsets[positionCount + i] + length;
                }
            }
            int nullCount = positions.size() - notNullCount;
            hasNullValue |= nullCount > 0;
            hasNonNullValue |= notNullCount > 0;
            positionCount += nullCount;
            copyBytes(rawSlice, lengths, offsets, notNullCount, targetOffsets, 0, newByteCount);
        }
        else {
            for (int i = 0; i < positions.size(); i++) {
                int position = block.getId(positionArray[i]);
                int length = variableWidthBlock.getSliceLength(position);
                int offset = variableWidthBlock.getPositionOffset(position);
                lengths[i] = length;
                offsets[i] = offset;
                newByteCount += length;
                this.offsets[positionCount + i + 1] = this.offsets[positionCount + i] + length;
            }
            notNullCount = positions.size();
            hasNonNullValue |= notNullCount > 0;
            copyBytes(rawSlice, lengths, offsets, notNullCount, this.offsets, positionCount, newByteCount);
        }
        // update BlockBuilderStatus for null values
        updateBlockBuilderStatus(positions.size() - notNullCount, 0);
    }

    @Override
    public void appendRle(RunLengthEncodedBlock block)
    {
        int rlePositionCount = block.getPositionCount();
        int sourcePosition = 0;
        ensurePositionCapacity(positionCount + rlePositionCount);
        if (block.isNull(sourcePosition)) {
            int offset = this.offsets[positionCount];
            Arrays.fill(valueIsNull, positionCount, positionCount + rlePositionCount, true);
            Arrays.fill(offsets, positionCount + 1, positionCount + rlePositionCount + 1, offset);
            positionCount += rlePositionCount;

            hasNullValue = true;
            updateBlockBuilderStatus(rlePositionCount, 0);
        }
        else {
            VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block.getValue();
            Slice rawSlice = variableWidthBlock.getRawSlice(0);

            int length = variableWidthBlock.getSliceLength(sourcePosition);
            int offset = variableWidthBlock.getPositionOffset(sourcePosition);

            int startOffset = offsets[positionCount];
            hasNonNullValue = true;
            duplicateBytes(rawSlice, offset, length, rlePositionCount, startOffset);
        }
    }

    /**
     * Copy {@code length} bytes from {@code rawSlice}, starting at offset {@code sourceOffset} to {@code count} consecutive positions in the {@link #bytes} array.
     */
    private void duplicateBytes(Slice rawSlice, int sourceOffset, int length, int count, int startOffset)
    {
        int newByteCount = count * length;
        ensureBytesCapacity(currentOffset + newByteCount);
        Object base = rawSlice.getBase();

        if (base instanceof byte[] && rawSlice.getAddress() == ARRAY_BYTE_BASE_OFFSET) {
            for (int i = 0; i < count; i++) {
                System.arraycopy(base, sourceOffset, bytes, startOffset + i * length, length);
                this.offsets[positionCount + i + 1] = startOffset + ((i + 1) * length);
            }
        }
        else {
            for (int i = 0; i < count; i++) {
                rawSlice.getBytes(sourceOffset, bytes, startOffset + i * length, length);
                this.offsets[positionCount + i + 1] = startOffset + ((i + 1) * length);
            }
        }
        positionCount += count;
        currentOffset += newByteCount;
        updateBlockBuilderStatus(count, newByteCount);
    }

    @Override
    public void appendRow(Block source, int position)
    {
        if (source.isNull(position)) {
            appendNull();
        }
        else {
            source.writeBytesTo(position, 0, source.getSliceLength(position), fakeBlockBuilder);
        }
    }

    private void appendNull()
    {
        ensurePositionCapacity(positionCount + 1);
        hasNullValue = true;
        entryAdded(0, true);
    }

    private void writeBytes(Slice source, int sourceIndex, int length)
    {
        ensureCapacity(positionCount + 1, currentOffset + length);
        source.getBytes(sourceIndex, bytes, currentOffset, length);
        hasNonNullValue = true;
        entryAdded(length, false);
    }

    private void entryAdded(int bytesWritten, boolean isNull)
    {
        valueIsNull[positionCount] = isNull;
        offsets[positionCount + 1] = currentOffset + bytesWritten;
        currentOffset += bytesWritten;
        positionCount++;

        updateBlockBuilderStatus(1, bytesWritten);
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, positionCount);
        }
        return new VariableWidthBlock(positionCount, Slices.wrappedBuffer(bytes, 0, currentOffset), offsets, hasNullValue ? Optional.of(valueIsNull) : Optional.empty());
    }

    @Override
    public BlockTypeAwarePositionsAppender newStateLike(@Nullable BlockBuilderStatus blockBuilderStatus)
    {
        int currentSizeInBytes = positionCount == 0 ? positionCount : (offsets[positionCount] - offsets[0]);
        return new SlicePositionsAppender(blockBuilderStatus, calculateBlockResetSize(positionCount), calculateBlockResetBytes(currentSizeInBytes));
    }

    private static int getExpectedBytes(BlockBuilderStatus blockBuilderStatus, int expectedPositions)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }

        // it is guaranteed Math.min will not overflow; safe to cast
        return (int) min((long) expectedPositions * EXPECTED_BYTES_PER_ENTRY, maxBlockSizeInBytes);
    }

    private void updateBlockBuilderStatus(int count, int bytesWritten)
    {
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes((SIZE_OF_BYTE + SIZE_OF_INT) * count + bytesWritten);
        }
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

    private void ensureCapacity(int capacity, int bytesCapacity)
    {
        ensurePositionCapacity(capacity);
        ensureBytesCapacity(bytesCapacity);
    }

    private void ensureBytesCapacity(int bytesCapacity)
    {
        if (bytes.length < bytesCapacity) {
            int newBytesLength = Math.max(bytes.length, initialBytesSize);
            if (bytesCapacity > newBytesLength) {
                newBytesLength = Math.max(bytesCapacity, BlockUtil.calculateNewArraySize(newBytesLength));
            }
            bytes = Arrays.copyOf(bytes, newBytesLength);
        }
    }

    private void ensurePositionCapacity(int capacity)
    {
        if (valueIsNull.length < capacity) {
            int newSize;
            if (initialized) {
                newSize = BlockUtil.calculateNewArraySize(valueIsNull.length);
            }
            else {
                newSize = initialEntryCount;
                initialized = true;
            }
            newSize = Math.max(newSize, capacity);

            valueIsNull = Arrays.copyOf(valueIsNull, newSize);
            offsets = Arrays.copyOf(offsets, newSize + 1);
            updateArraysDataSize();
        }
    }

    private void updateArraysDataSize()
    {
        arraysRetainedSizeInBytes = sizeOf(valueIsNull) + sizeOf(offsets) + sizeOf(bytes);
    }
}
