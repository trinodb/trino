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
                new boolean[expectedEntries]);
    }

    private RowBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, BlockBuilder[] fieldBlockBuilders, boolean[] rowIsNull)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.positionCount = 0;
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
        long size = INSTANCE_SIZE + sizeOf(rowIsNull);
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
    public void append(ValueBlock block, int position)
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        RowBlock rowBlock = (RowBlock) block;
        if (block.isNull(position)) {
            appendNull();
            return;
        }

        List<Block> fieldBlocks = rowBlock.getFieldBlocks();
        for (int fieldId = 0; fieldId < fieldBlockBuilders.length; fieldId++) {
            appendToField(fieldBlocks.get(fieldId), position, fieldBlockBuilders[fieldId]);
        }
        entryAdded(false);
    }

    private static void appendToField(Block fieldBlock, int position, BlockBuilder fieldBlockBuilder)
    {
        fieldBlock = fieldBlock.getLoadedBlock();
        if (fieldBlock instanceof RunLengthEncodedBlock rleBlock) {
            fieldBlockBuilder.append(rleBlock.getValue(), 0);
        }
        else if (fieldBlock instanceof DictionaryBlock dictionaryBlock) {
            fieldBlockBuilder.append(dictionaryBlock.getDictionary(), dictionaryBlock.getId(position));
        }
        else if (fieldBlock instanceof ValueBlock valueBlock) {
            fieldBlockBuilder.append(valueBlock, position);
        }
        else {
            throw new IllegalArgumentException("Unsupported block type " + fieldBlock.getClass().getSimpleName());
        }
    }

    @Override
    public void appendRange(ValueBlock block, int offset, int length)
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }
        if (length == 0) {
            return;
        }

        RowBlock rowBlock = (RowBlock) block;
        ensureCapacity(positionCount + length);

        List<Block> fieldBlocks = rowBlock.getFieldBlocks();
        for (int fieldId = 0; fieldId < fieldBlockBuilders.length; fieldId++) {
            appendRangeToField(fieldBlocks.get(fieldId), offset, length, fieldBlockBuilders[fieldId]);
        }

        boolean[] rawRowIsNull = rowBlock.getRawRowIsNull();
        if (rawRowIsNull != null) {
            for (int i = 0; i < length; i++) {
                if (rawRowIsNull[offset + i]) {
                    rowIsNull[positionCount + i] = true;
                    hasNullRow = true;
                }
                else {
                    hasNonNullRow = true;
                }
            }
        }
        else {
            hasNonNullRow = true;
        }
        positionCount += length;
    }

    private static void appendRangeToField(Block fieldBlock, int offset, int length, BlockBuilder fieldBlockBuilder)
    {
        fieldBlock = fieldBlock.getLoadedBlock();
        if (fieldBlock instanceof RunLengthEncodedBlock rleBlock) {
            fieldBlockBuilder.appendRepeated(rleBlock.getValue(), 0, length);
        }
        else if (fieldBlock instanceof DictionaryBlock dictionaryBlock) {
            int[] rawIds = dictionaryBlock.getRawIds();
            int rawIdsOffset = dictionaryBlock.getRawIdsOffset();
            fieldBlockBuilder.appendPositions(dictionaryBlock.getDictionary(), rawIds, rawIdsOffset + offset, length);
        }
        else if (fieldBlock instanceof ValueBlock valueBlock) {
            fieldBlockBuilder.appendRange(valueBlock, offset, length);
        }
        else {
            throw new IllegalArgumentException("Unsupported block type " + fieldBlock.getClass().getSimpleName());
        }
    }

    @Override
    public void appendRepeated(ValueBlock block, int position, int count)
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }
        if (count == 0) {
            return;
        }

        RowBlock rowBlock = (RowBlock) block;
        ensureCapacity(positionCount + count);

        List<Block> fieldBlocks = rowBlock.getFieldBlocks();
        for (int fieldId = 0; fieldId < fieldBlockBuilders.length; fieldId++) {
            appendRepeatedToField(fieldBlocks.get(fieldId), position, count, fieldBlockBuilders[fieldId]);
        }

        if (rowBlock.isNull(position)) {
            Arrays.fill(rowIsNull, positionCount, positionCount + count, true);
            hasNullRow = true;
        }
        else {
            hasNonNullRow = true;
        }

        positionCount += count;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES);
        }
    }

    private static void appendRepeatedToField(Block fieldBlock, int position, int count, BlockBuilder fieldBlockBuilder)
    {
        fieldBlock = fieldBlock.getLoadedBlock();
        if (fieldBlock instanceof RunLengthEncodedBlock rleBlock) {
            fieldBlockBuilder.appendRepeated(rleBlock.getValue(), 0, count);
        }
        else if (fieldBlock instanceof DictionaryBlock dictionaryBlock) {
            fieldBlockBuilder.appendRepeated(dictionaryBlock.getDictionary(), dictionaryBlock.getId(position), count);
        }
        else if (fieldBlock instanceof ValueBlock valueBlock) {
            fieldBlockBuilder.appendRepeated(valueBlock, position, count);
        }
        else {
            throw new IllegalArgumentException("Unsupported block type " + fieldBlock.getClass().getSimpleName());
        }
    }

    @Override
    public void appendPositions(ValueBlock block, int[] positions, int offset, int length)
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }
        if (length == 0) {
            return;
        }

        RowBlock rowBlock = (RowBlock) block;
        ensureCapacity(positionCount + length);

        List<Block> fieldBlocks = rowBlock.getFieldBlocks();
        for (int fieldId = 0; fieldId < fieldBlockBuilders.length; fieldId++) {
            appendPositionsToField(fieldBlocks.get(fieldId), positions, offset, length, fieldBlockBuilders[fieldId]);
        }

        boolean[] rawRowIsNull = rowBlock.getRawRowIsNull();
        if (rawRowIsNull != null) {
            for (int i = 0; i < length; i++) {
                if (rawRowIsNull[positions[offset + i]]) {
                    rowIsNull[positionCount + i] = true;
                    hasNullRow = true;
                }
                else {
                    hasNonNullRow = true;
                }
            }
        }
        else {
            hasNonNullRow = true;
        }
        positionCount += length;
    }

    private static void appendPositionsToField(Block fieldBlock, int[] positions, int offset, int length, BlockBuilder fieldBlockBuilder)
    {
        fieldBlock = fieldBlock.getLoadedBlock();
        if (fieldBlock instanceof RunLengthEncodedBlock rleBlock) {
            fieldBlockBuilder.appendRepeated(rleBlock.getValue(), 0, length);
        }
        else if (fieldBlock instanceof DictionaryBlock dictionaryBlock) {
            int[] newPositions = new int[length];
            for (int i = 0; i < newPositions.length; i++) {
                newPositions[i] = dictionaryBlock.getId(positions[offset + i]);
            }
            fieldBlockBuilder.appendPositions(dictionaryBlock.getDictionary(), newPositions, 0, length);
        }
        else if (fieldBlock instanceof ValueBlock valueBlock) {
            fieldBlockBuilder.appendPositions(valueBlock, positions, offset, length);
        }
        else {
            throw new IllegalArgumentException("Unsupported block type " + fieldBlock.getClass().getSimpleName());
        }
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        for (BlockBuilder fieldBlockBuilder : fieldBlockBuilders) {
            fieldBlockBuilder.appendNull();
        }

        entryAdded(true);
        return this;
    }

    private void entryAdded(boolean isNull)
    {
        ensureCapacity(positionCount + 1);

        rowIsNull[positionCount] = isNull;
        hasNullRow |= isNull;
        hasNonNullRow |= !isNull;
        positionCount++;

        for (int i = 0; i < fieldBlockBuilders.length; i++) {
            if (fieldBlockBuilders[i].getPositionCount() != positionCount) {
                throw new IllegalStateException(format("field %s has unexpected position count. Expected: %s, actual: %s", i, positionCount, fieldBlockBuilders[i].getPositionCount()));
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
        return createRowBlockInternal(positionCount, hasNullRow ? rowIsNull : null, fieldBlocks);
    }

    private void ensureCapacity(int capacity)
    {
        if (rowIsNull.length >= capacity) {
            return;
        }

        // todo add lazy initialize
        int newSize = BlockUtil.calculateNewArraySize(rowIsNull.length);
        rowIsNull = Arrays.copyOf(rowIsNull, newSize);
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
        return new RowBlockBuilder(blockBuilderStatus, newBlockBuilders, new boolean[expectedEntries]);
    }

    private Block nullRle(int length)
    {
        Block[] fieldBlocks = new Block[fieldBlockBuilders.length];
        for (int i = 0; i < fieldBlockBuilders.length; i++) {
            fieldBlocks[i] = fieldBlockBuilders[i].newBlockBuilderLike(null).appendNull().build();
        }

        RowBlock nullRowBlock = createRowBlockInternal(1, new boolean[] {true}, fieldBlocks);
        return RunLengthEncodedBlock.create(nullRowBlock, length);
    }
}
