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
import jakarta.annotation.Nullable;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.spi.block.BlockUtil.MAX_ARRAY_SIZE;
import static io.trino.spi.block.BlockUtil.calculateBlockResetBytes;
import static io.trino.spi.block.BlockUtil.calculateNewArraySize;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class VariableWidthBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(VariableWidthBlockBuilder.class);
    private static final Block NULL_VALUE_BLOCK = new VariableWidthBlock(0, 1, EMPTY_SLICE, new int[]{0, 0}, new boolean[]{true});
    private static final int SIZE_IN_BYTES_PER_POSITION = Integer.BYTES + Byte.BYTES;

    private final BlockBuilderStatus blockBuilderStatus;

    private final int initialEntryCount;
    private final int initialSliceOutputSize;

    private byte[] bytes = new byte[0];

    private boolean hasNullValue;
    private boolean hasNonNullValue;
    // it is assumed that the offsets array is one position longer than the valueIsNull array
    private boolean[] valueIsNull = new boolean[0];
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
        long size = INSTANCE_SIZE + sizeOf(bytes) + arraysRetainedSizeInBytes;
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
    public BlockBuilder appendNull()
    {
        hasNullValue = true;
        entryAdded(0, true);
        return this;
    }

    private void entryAdded(int bytesWritten, boolean isNull)
    {
        if (valueIsNull.length <= positionCount) {
            growCapacity();
        }

        valueIsNull[positionCount] = isNull;
        offsets[positionCount + 1] = offsets[positionCount] + bytesWritten;

        positionCount++;
        hasNonNullValue |= !isNull;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(SIZE_OF_BYTE + SIZE_OF_INT + bytesWritten);
        }
    }

    private void growCapacity()
    {
        int newSize = calculateNewArraySize(valueIsNull.length, initialEntryCount);
        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        offsets = Arrays.copyOf(offsets, newSize + 1);
        updateRetainedSize();
    }

    private void ensureFreeSpace(int extraBytesCapacity)
    {
        int requiredSize = offsets[positionCount] + extraBytesCapacity;
        if (bytes.length < requiredSize) {
            int newBytesLength = max(bytes.length, initialSliceOutputSize);
            if (requiredSize > newBytesLength) {
                newBytesLength = max(requiredSize, calculateNewArraySize(newBytesLength));
            }
            bytes = Arrays.copyOf(bytes, newBytesLength);
            updateRetainedSize();
        }
    }

    private void updateRetainedSize()
    {
        arraysRetainedSizeInBytes = sizeOf(valueIsNull) + sizeOf(offsets);
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
        return new VariableWidthBlock(0, positionCount, Slices.wrappedBuffer(bytes, 0, offsets[positionCount]), offsets, hasNullValue ? valueIsNull : null);
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
