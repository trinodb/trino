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
import static io.trino.spi.block.BlockUtil.copyIsNullAndAppendNull;
import static io.trino.spi.block.BlockUtil.copyOffsetsAndAppendNull;
import static io.trino.spi.block.BlockUtil.ensureBlocksAreLoaded;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RowBlock
        extends AbstractRowBlock
{
    private static final int INSTANCE_SIZE = instanceSize(RowBlock.class);

    private final int startOffset;
    private final int positionCount;

    private final boolean[] rowIsNull;
    private final int[] fieldBlockOffsets;
    private final Block[] fieldBlocks;

    private volatile long sizeInBytes = -1;
    private final long retainedSizeInBytes;

    /**
     * Create a row block directly from columnar nulls and field blocks.
     */
    public static Block fromFieldBlocks(int positionCount, Optional<boolean[]> rowIsNullOptional, Block[] fieldBlocks)
    {
        boolean[] rowIsNull = rowIsNullOptional.orElse(null);
        int[] fieldBlockOffsets = null;
        if (rowIsNull != null) {
            // Check for nulls when computing field block offsets
            fieldBlockOffsets = new int[positionCount + 1];
            fieldBlockOffsets[0] = 0;
            for (int position = 0; position < positionCount; position++) {
                fieldBlockOffsets[position + 1] = fieldBlockOffsets[position] + (rowIsNull[position] ? 0 : 1);
            }
            // fieldBlockOffsets is positionCount + 1 in length
            if (fieldBlockOffsets[positionCount] == positionCount) {
                // No nulls encountered, discard the null mask
                rowIsNull = null;
                fieldBlockOffsets = null;
            }
        }

        validateConstructorArguments(0, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
        return new RowBlock(0, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
    }

    /**
     * Create a row block directly without per element validations.
     */
    static RowBlock createRowBlockInternal(int startOffset, int positionCount, @Nullable boolean[] rowIsNull, @Nullable int[] fieldBlockOffsets, Block[] fieldBlocks)
    {
        validateConstructorArguments(startOffset, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
        return new RowBlock(startOffset, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
    }

    private static void validateConstructorArguments(int startOffset, int positionCount, @Nullable boolean[] rowIsNull, @Nullable int[] fieldBlockOffsets, Block[] fieldBlocks)
    {
        if (startOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        if (rowIsNull != null && rowIsNull.length - startOffset < positionCount) {
            throw new IllegalArgumentException("rowIsNull length is less than positionCount");
        }

        if ((rowIsNull == null) != (fieldBlockOffsets == null)) {
            throw new IllegalArgumentException("When rowIsNull is (non) null then fieldBlockOffsets should be (non) null as well");
        }

        if (fieldBlockOffsets != null && fieldBlockOffsets.length - startOffset < positionCount + 1) {
            throw new IllegalArgumentException("fieldBlockOffsets length is less than positionCount");
        }

        requireNonNull(fieldBlocks, "fieldBlocks is null");

        if (fieldBlocks.length <= 0) {
            throw new IllegalArgumentException("Number of fields in RowBlock must be positive");
        }

        int firstFieldBlockPositionCount = fieldBlocks[0].getPositionCount();
        for (int i = 1; i < fieldBlocks.length; i++) {
            if (firstFieldBlockPositionCount != fieldBlocks[i].getPositionCount()) {
                throw new IllegalArgumentException(format("length of field blocks differ: field 0: %s, block %s: %s", firstFieldBlockPositionCount, i, fieldBlocks[i].getPositionCount()));
            }
        }
    }

    /**
     * Use createRowBlockInternal or fromFieldBlocks instead of this method.  The caller of this method is assumed to have
     * validated the arguments with validateConstructorArguments.
     */
    private RowBlock(int startOffset, int positionCount, @Nullable boolean[] rowIsNull, @Nullable int[] fieldBlockOffsets, Block[] fieldBlocks)
    {
        super(fieldBlocks.length);

        this.startOffset = startOffset;
        this.positionCount = positionCount;
        this.rowIsNull = rowIsNull;
        this.fieldBlockOffsets = fieldBlockOffsets;
        this.fieldBlocks = fieldBlocks;

        this.retainedSizeInBytes = INSTANCE_SIZE + sizeOf(fieldBlockOffsets) + sizeOf(rowIsNull);
    }

    @Override
    protected Block[] getRawFieldBlocks()
    {
        return fieldBlocks;
    }

    @Override
    @Nullable
    protected int[] getFieldBlockOffsets()
    {
        return fieldBlockOffsets;
    }

    @Override
    protected int getOffsetBase()
    {
        return startOffset;
    }

    @Override
    @Nullable
    protected boolean[] getRowIsNull()
    {
        return rowIsNull;
    }

    @Override
    public boolean mayHaveNull()
    {
        return rowIsNull != null;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        if (sizeInBytes >= 0) {
            return sizeInBytes;
        }

        long sizeInBytes = getBaseSizeInBytes();
        boolean hasUnloadedBlocks = false;

        int startFieldBlockOffset = fieldBlockOffsets != null ? fieldBlockOffsets[startOffset] : startOffset;
        int endFieldBlockOffset = fieldBlockOffsets != null ? fieldBlockOffsets[startOffset + positionCount] : startOffset + positionCount;
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;

        for (Block fieldBlock : fieldBlocks) {
            sizeInBytes += fieldBlock.getRegionSizeInBytes(startFieldBlockOffset, fieldBlockLength);
            hasUnloadedBlocks = hasUnloadedBlocks || !fieldBlock.isLoaded();
        }

        if (!hasUnloadedBlocks) {
            this.sizeInBytes = sizeInBytes;
        }
        return sizeInBytes;
    }

    private long getBaseSizeInBytes()
    {
        return (Integer.BYTES + Byte.BYTES) * (long) positionCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = this.retainedSizeInBytes;
        for (Block fieldBlock : fieldBlocks) {
            retainedSizeInBytes += fieldBlock.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        for (int i = 0; i < numFields; i++) {
            consumer.accept(fieldBlocks[i], fieldBlocks[i].getRetainedSizeInBytes());
        }
        if (fieldBlockOffsets != null) {
            consumer.accept(fieldBlockOffsets, sizeOf(fieldBlockOffsets));
        }
        if (rowIsNull != null) {
            consumer.accept(rowIsNull, sizeOf(rowIsNull));
        }
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public String toString()
    {
        return format("RowBlock{numFields=%d, positionCount=%d}", numFields, getPositionCount());
    }

    @Override
    public boolean isLoaded()
    {
        for (Block fieldBlock : fieldBlocks) {
            if (!fieldBlock.isLoaded()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Block getLoadedBlock()
    {
        Block[] loadedFieldBlocks = ensureBlocksAreLoaded(fieldBlocks);
        if (loadedFieldBlocks == fieldBlocks) {
            // All blocks are already loaded
            return this;
        }
        return createRowBlockInternal(
                startOffset,
                positionCount,
                rowIsNull,
                fieldBlockOffsets,
                loadedFieldBlocks);
    }

    @Override
    public Block copyWithAppendedNull()
    {
        boolean[] newRowIsNull = copyIsNullAndAppendNull(getRowIsNull(), getOffsetBase(), getPositionCount());

        int[] newOffsets;
        if (getFieldBlockOffsets() == null) {
            int desiredLength = getOffsetBase() + positionCount + 2;
            newOffsets = new int[desiredLength];
            newOffsets[getOffsetBase()] = getOffsetBase();
            for (int position = getOffsetBase(); position < getOffsetBase() + positionCount; position++) {
                // Since there are no nulls in the original array, new offsets are the same as previous ones
                newOffsets[position + 1] = newOffsets[position] + 1;
            }

            // Null does not change offset
            newOffsets[desiredLength - 1] = newOffsets[desiredLength - 2];
        }
        else {
            newOffsets = copyOffsetsAndAppendNull(getFieldBlockOffsets(), getOffsetBase(), getPositionCount());
        }

        return createRowBlockInternal(getOffsetBase(), getPositionCount() + 1, newRowIsNull, newOffsets, getRawFieldBlocks());
    }
}
