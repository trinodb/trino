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
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.RowBlock.createRowBlockInternal;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RowBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(RowBlockBuilder.class);

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;

    private int positionCount;
    private int[] fieldBlockOffsets;
    private boolean[] rowIsNull;
    private final BlockBuilder[] fieldBlockBuilders;
    private final List<BlockBuilder> fieldBlockBuildersList;

    private boolean currentEntryOpened;
    private boolean hasNullRow;
    private boolean hasNonNullRow;

    public RowBlockBuilder(List<Type> fieldTypes, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(
                blockBuilderStatus,
                createFieldBlockBuilders(fieldTypes, blockBuilderStatus, expectedEntries),
                new int[expectedEntries + 1],
                new boolean[expectedEntries]);
    }

    private RowBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, BlockBuilder[] fieldBlockBuilders, int[] fieldBlockOffsets, boolean[] rowIsNull)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.positionCount = 0;
        this.fieldBlockOffsets = requireNonNull(fieldBlockOffsets, "fieldBlockOffsets is null");
        this.rowIsNull = requireNonNull(rowIsNull, "rowIsNull is null");
        this.fieldBlockBuilders = requireNonNull(fieldBlockBuilders, "fieldBlockBuilders is null");
        this.fieldBlockBuildersList = List.of(fieldBlockBuilders);
    }

    private static BlockBuilder[] createFieldBlockBuilders(List<Type> fieldTypes, BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        // Stream API should not be used since constructor can be called in performance sensitive sections
        BlockBuilder[] fieldBlockBuilders = new BlockBuilder[fieldTypes.size()];
        for (int i = 0; i < fieldTypes.size(); i++) {
            fieldBlockBuilders[i] = fieldTypes.get(i).createBlockBuilder(blockBuilderStatus, expectedEntries);
        }
        return fieldBlockBuilders;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        long sizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) positionCount;
        for (BlockBuilder fieldBlockBuilder : fieldBlockBuilders) {
            sizeInBytes += fieldBlockBuilder.getSizeInBytes();
        }
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sizeOf(fieldBlockOffsets) + sizeOf(rowIsNull);
        for (BlockBuilder fieldBlockBuilder : fieldBlockBuilders) {
            size += fieldBlockBuilder.getRetainedSizeInBytes();
        }
        if (blockBuilderStatus != null) {
            size += BlockBuilderStatus.INSTANCE_SIZE;
        }
        return size;
    }

    public <E extends Throwable> void buildEntry(RowValueBuilder<E> builder)
            throws E
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was opened");
        }

        currentEntryOpened = true;
        builder.build(fieldBlockBuildersList);
        entryAdded(false);
        currentEntryOpened = false;
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

    private void entryAdded(boolean isNull)
    {
        if (rowIsNull.length <= positionCount) {
            int newSize = BlockUtil.calculateNewArraySize(rowIsNull.length);
            rowIsNull = Arrays.copyOf(rowIsNull, newSize);
            fieldBlockOffsets = Arrays.copyOf(fieldBlockOffsets, newSize + 1);
        }

        if (isNull) {
            fieldBlockOffsets[positionCount + 1] = fieldBlockOffsets[positionCount];
        }
        else {
            fieldBlockOffsets[positionCount + 1] = fieldBlockOffsets[positionCount] + 1;
        }
        rowIsNull[positionCount] = isNull;
        hasNullRow |= isNull;
        hasNonNullRow |= !isNull;
        positionCount++;

        for (int i = 0; i < fieldBlockBuilders.length; i++) {
            if (fieldBlockBuilders[i].getPositionCount() != fieldBlockOffsets[positionCount]) {
                throw new IllegalStateException(format("field %s has unexpected position count. Expected: %s, actual: %s", i, fieldBlockOffsets[positionCount], fieldBlockBuilders[i].getPositionCount()));
            }
        }

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES);
        }
    }

    @Override
    public Block build()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        if (!hasNonNullRow) {
            return nullRle(positionCount);
        }
        return buildValueBlock();
    }

    @Override
    public RowBlock buildValueBlock()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }

        Block[] fieldBlocks = new Block[fieldBlockBuilders.length];
        for (int i = 0; i < fieldBlockBuilders.length; i++) {
            fieldBlocks[i] = fieldBlockBuilders[i].build();
        }
        return createRowBlockInternal(0, positionCount, hasNullRow ? rowIsNull : null, hasNullRow ? fieldBlockOffsets : null, fieldBlocks);
    }

    @Override
    public String toString()
    {
        return format("RowBlockBuilder{numFields=%d, positionCount=%d", fieldBlockBuilders.length, getPositionCount());
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        BlockBuilder[] newBlockBuilders = new BlockBuilder[fieldBlockBuilders.length];
        for (int i = 0; i < fieldBlockBuilders.length; i++) {
            newBlockBuilders[i] = fieldBlockBuilders[i].newBlockBuilderLike(blockBuilderStatus);
        }
        return new RowBlockBuilder(blockBuilderStatus, newBlockBuilders, new int[expectedEntries + 1], new boolean[expectedEntries]);
    }

    private Block nullRle(int length)
    {
        Block[] fieldBlocks = new Block[fieldBlockBuilders.length];
        for (int i = 0; i < fieldBlockBuilders.length; i++) {
            fieldBlocks[i] = fieldBlockBuilders[i].newBlockBuilderLike(null).build();
        }

        RowBlock nullRowBlock = createRowBlockInternal(0, 1, new boolean[] {true}, new int[] {0, 0}, fieldBlocks);
        return RunLengthEncodedBlock.create(nullRowBlock, length);
    }
}
