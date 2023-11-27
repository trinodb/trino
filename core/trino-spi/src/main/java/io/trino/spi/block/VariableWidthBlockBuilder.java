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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import jakarta.annotation.Nullable;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.spi.block.BlockUtil.MAX_ARRAY_SIZE;
import static io.trino.spi.block.BlockUtil.calculateBlockResetBytes;
import static java.lang.Math.min;

public class VariableWidthBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(VariableWidthBlockBuilder.class);
    private static final Block NULL_VALUE_BLOCK = new VariableWidthBlock(0, 1, EMPTY_SLICE, new int[] {0, 0}, new boolean[] {true});

    private final BlockBuilderStatus blockBuilderStatus;

    private boolean initialized;
    private final int initialEntryCount;
    private final int initialSliceOutputSize;

    private SliceOutput sliceOutput = new DynamicSliceOutput(0);

    private boolean hasNullValue;
    private boolean hasNonNullValue;
    // it is assumed that the offsets array is one position longer than the valueIsNull array
    private boolean[] valueIsNull = new boolean[0];
    private int[] offsets = new int[1];

    private int positions;

    private long arraysRetainedSizeInBytes;

    public VariableWidthBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytes)
    {
        this.blockBuilderStatus = blockBuilderStatus;

        initialEntryCount = expectedEntries;
        initialSliceOutputSize = min(expectedBytes, MAX_ARRAY_SIZE);

        updateArraysDataSize();
    }

    @Override
    public int getPositionCount()
    {
        return positions;
    }

    @Override
    public long getSizeInBytes()
    {
        long arraysSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) positions;
        return sliceOutput.size() + arraysSizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sliceOutput.getRetainedSize() + arraysRetainedSizeInBytes;
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
        if (!initialized) {
            initializeCapacity();
        }

        sliceOutput.writeBytes(source, sourceIndex, length);
        entryAdded(length, false);
        return this;
    }

    public VariableWidthBlockBuilder writeEntry(byte[] source, int sourceIndex, int length)
    {
        if (!initialized) {
            initializeCapacity();
        }

        sliceOutput.writeBytes(source, sourceIndex, length);
        entryAdded(length, false);
        return this;
    }

    public <E extends Throwable> void buildEntry(VariableWidthEntryBuilder<E> builder)
            throws E
    {
        if (!initialized) {
            initializeCapacity();
        }

        int start = sliceOutput.size();
        builder.build(sliceOutput);
        int length = sliceOutput.size() - start;
        entryAdded(length, false);
    }

    public interface VariableWidthEntryBuilder<E extends Throwable>
    {
        void build(SliceOutput output)
                throws E;
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
        if (!initialized) {
            initializeCapacity();
        }
        if (valueIsNull.length <= positions) {
            growCapacity();
        }

        valueIsNull[positions] = isNull;
        offsets[positions + 1] = sliceOutput.size();

        positions++;
        hasNonNullValue |= !isNull;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(SIZE_OF_BYTE + SIZE_OF_INT + bytesWritten);
        }
    }

    private void growCapacity()
    {
        int newSize = BlockUtil.calculateNewArraySize(valueIsNull.length);
        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        offsets = Arrays.copyOf(offsets, newSize + 1);
        updateArraysDataSize();
    }

    private void initializeCapacity()
    {
        if (positions != 0) {
            throw new IllegalStateException(getClass().getSimpleName() + " was used before initialization");
        }
        initialized = true;
        valueIsNull = new boolean[initialEntryCount];
        offsets = new int[initialEntryCount + 1];
        sliceOutput = new DynamicSliceOutput(initialSliceOutputSize);
        updateArraysDataSize();
    }

    private void updateArraysDataSize()
    {
        arraysRetainedSizeInBytes = sizeOf(valueIsNull) + sizeOf(offsets);
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positions);
        }
        return buildValueBlock();
    }

    @Override
    public VariableWidthBlock buildValueBlock()
    {
        return new VariableWidthBlock(0, positions, sliceOutput.slice(), offsets, hasNullValue ? valueIsNull : null);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        int currentSizeInBytes = positions == 0 ? positions : (getOffset(positions) - getOffset(0));
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
        sb.append("positionCount=").append(positions);
        sb.append(", size=").append(sliceOutput.size());
        sb.append('}');
        return sb.toString();
    }
}
