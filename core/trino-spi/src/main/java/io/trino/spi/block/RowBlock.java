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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.arraySame;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkReadablePosition;
import static io.trino.spi.block.BlockUtil.checkValidPositions;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.BlockUtil.ensureBlocksAreLoaded;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class RowBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(RowBlock.class);

    private final int positionCount;
    @Nullable
    private final boolean[] rowIsNull;
    /**
     * Field blocks have the same position count as this row block. The field value of a null row must be null.
     */
    private final Block[] fieldBlocks;
    private final List<Block> fieldBlocksList;
    private final int fixedSizePerRow;

    private volatile long sizeInBytes = -1;

    /**
     * Create a row block directly from field blocks. The returned RowBlock will not contain any null rows, although the fields may contain null values.
     */
    public static RowBlock fromFieldBlocks(int positionCount, Block[] fieldBlocks)
    {
        return createRowBlockInternal(positionCount, null, fieldBlocks);
    }

    /**
     * Create a row block directly from field blocks that are not null-suppressed. The field value of a null row must be null.
     */
    public static RowBlock fromNotNullSuppressedFieldBlocks(int positionCount, Optional<boolean[]> rowIsNullOptional, Block[] fieldBlocks)
    {
        // verify that field values for null rows are null
        if (rowIsNullOptional.isPresent()) {
            boolean[] rowIsNull = rowIsNullOptional.get();
            checkArrayRange(rowIsNull, 0, positionCount);

            for (int fieldIndex = 0; fieldIndex < fieldBlocks.length; fieldIndex++) {
                Block field = fieldBlocks[fieldIndex];
                // LazyBlock may not have loaded the field yet
                if (!(field instanceof LazyBlock lazyBlock) || lazyBlock.isLoaded()) {
                    for (int position = 0; position < positionCount; position++) {
                        if (rowIsNull[position] && !field.isNull(position)) {
                            throw new IllegalArgumentException(format("Field value for null row must be null: field %s, position %s", fieldIndex, position));
                        }
                    }
                }
            }
        }
        return createRowBlockInternal(positionCount, rowIsNullOptional.orElse(null), fieldBlocks);
    }

    static RowBlock createRowBlockInternal(int positionCount, @Nullable boolean[] rowIsNull, Block[] fieldBlocks)
    {
        int fixedSize = Byte.BYTES;
        for (Block fieldBlock : fieldBlocks) {
            OptionalInt fieldFixedSize = fieldBlock.fixedSizeInBytesPerPosition();
            if (fieldFixedSize.isEmpty()) {
                // found a block without a single per-position size
                fixedSize = -1;
                break;
            }
            fixedSize += fieldFixedSize.getAsInt();
        }

        return new RowBlock(positionCount, rowIsNull, fieldBlocks, fixedSize);
    }

    /**
     * Use createRowBlockInternal or fromFieldBlocks instead of this method. The caller of this method is assumed to have
     * validated the arguments with validateConstructorArguments.
     */
    private RowBlock(int positionCount, @Nullable boolean[] rowIsNull, Block[] fieldBlocks, int fixedSizePerRow)
    {
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        if (rowIsNull != null && rowIsNull.length < positionCount) {
            throw new IllegalArgumentException("rowIsNull length is less than positionCount");
        }

        requireNonNull(fieldBlocks, "fieldBlocks is null");
        if (fieldBlocks.length == 0) {
            throw new IllegalArgumentException("Row block must contain at least one field");
        }

        for (int i = 0; i < fieldBlocks.length; i++) {
            if (positionCount != fieldBlocks[i].getPositionCount()) {
                throw new IllegalArgumentException("Expected field %s to have %s positions but has %s positions".formatted(i, positionCount, fieldBlocks[i].getPositionCount()));
            }
        }

        this.positionCount = positionCount;
        this.rowIsNull = positionCount == 0 ? null : rowIsNull;
        this.fieldBlocks = fieldBlocks;
        this.fieldBlocksList = List.of(fieldBlocks);
        this.fixedSizePerRow = fixedSizePerRow;
    }

    Block[] getRawFieldBlocks()
    {
        return fieldBlocks;
    }

    public List<Block> getFieldBlocks()
    {
        return fieldBlocksList;
    }

    public Block getFieldBlock(int fieldIndex)
    {
        return fieldBlocks[fieldIndex];
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

        long sizeInBytes = Byte.BYTES * (long) positionCount;
        boolean hasUnloadedBlocks = false;

        for (Block fieldBlock : fieldBlocks) {
            sizeInBytes += fieldBlock.getSizeInBytes();
            hasUnloadedBlocks = hasUnloadedBlocks || !fieldBlock.isLoaded();
        }

        if (!hasUnloadedBlocks) {
            this.sizeInBytes = sizeInBytes;
        }
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = INSTANCE_SIZE + sizeOf(rowIsNull);
        for (Block fieldBlock : fieldBlocks) {
            retainedSizeInBytes += fieldBlock.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        for (Block fieldBlock : fieldBlocks) {
            consumer.accept(fieldBlock, fieldBlock.getRetainedSizeInBytes());
        }
        if (rowIsNull != null) {
            consumer.accept(rowIsNull, sizeOf(rowIsNull));
        }
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public String toString()
    {
        return format("RowBlock{fieldCount=%d, positionCount=%d}", fieldBlocks.length, positionCount);
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
        return new RowBlock(positionCount, rowIsNull, loadedFieldBlocks, fixedSizePerRow);
    }

    @Override
    public RowBlock copyWithAppendedNull()
    {
        boolean[] newRowIsNull;
        if (rowIsNull != null) {
            newRowIsNull = Arrays.copyOf(rowIsNull, positionCount + 1);
        }
        else {
            newRowIsNull = new boolean[positionCount + 1];
        }
        // mark the (new) last element as null
        newRowIsNull[positionCount] = true;

        Block[] newBlocks = new Block[fieldBlocks.length];
        for (int i = 0; i < fieldBlocks.length; i++) {
            newBlocks[i] = fieldBlocks[i].copyWithAppendedNull();
        }
        return new RowBlock(positionCount + 1, newRowIsNull, newBlocks, fixedSizePerRow);
    }

    @Override
    public List<Block> getChildren()
    {
        return fieldBlocksList;
    }

    @Override
    public String getEncodingName()
    {
        return RowBlockEncoding.NAME;
    }

    @Override
    public RowBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        Block[] newBlocks = new Block[fieldBlocks.length];
        for (int i = 0; i < newBlocks.length; i++) {
            newBlocks[i] = fieldBlocks[i].copyPositions(positions, offset, length);
        }

        boolean[] newRowIsNull = null;
        if (rowIsNull != null) {
            newRowIsNull = new boolean[length];
            for (int i = 0; i < length; i++) {
                newRowIsNull[i] = rowIsNull[positions[offset + i]];
            }
        }

        return new RowBlock(length, newRowIsNull, newBlocks, fixedSizePerRow);
    }

    @Override
    public RowBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);

        // This copies the null array, but this dramatically simplifies this class.
        // Without a copy here, we would need a null array offset, and that would mean that the
        // null array would be offset while the field blocks are not offset, which is confusing.
        boolean[] newRowIsNull = rowIsNull == null ? null : compactArray(rowIsNull, positionOffset, length);
        Block[] newBlocks = new Block[fieldBlocks.length];
        for (int i = 0; i < newBlocks.length; i++) {
            newBlocks[i] = fieldBlocks[i].getRegion(positionOffset, length);
        }
        return new RowBlock(length, newRowIsNull, newBlocks, fixedSizePerRow);
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return fixedSizePerRow > 0 ? OptionalInt.of(fixedSizePerRow) : OptionalInt.empty();
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        checkValidRegion(positionCount, position, length);

        long regionSizeInBytes = Byte.BYTES * (long) length;
        for (Block fieldBlock : fieldBlocks) {
            regionSizeInBytes += fieldBlock.getRegionSizeInBytes(position, length);
        }
        return regionSizeInBytes;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int selectedRowPositions)
    {
        checkValidPositions(positions, positionCount);
        if (selectedRowPositions == 0) {
            return 0;
        }
        if (selectedRowPositions == positionCount) {
            return getSizeInBytes();
        }

        if (fixedSizePerRow > 0) {
            return fixedSizePerRow * (long) selectedRowPositions;
        }

        long sizeInBytes = Byte.BYTES * (long) selectedRowPositions;
        for (Block fieldBlock : fieldBlocks) {
            sizeInBytes += fieldBlock.getPositionsSizeInBytes(positions, selectedRowPositions);
        }
        return sizeInBytes;
    }

    @Override
    public RowBlock copyRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);

        Block[] newBlocks = new Block[fieldBlocks.length];
        for (int i = 0; i < fieldBlocks.length; i++) {
            newBlocks[i] = fieldBlocks[i].copyRegion(positionOffset, length);
        }

        boolean[] newRowIsNull = rowIsNull == null ? null : compactArray(rowIsNull, positionOffset, length);
        if (newRowIsNull == rowIsNull && arraySame(newBlocks, fieldBlocks)) {
            return this;
        }
        return new RowBlock(length, newRowIsNull, newBlocks, fixedSizePerRow);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        if (clazz != SqlRow.class) {
            throw new IllegalArgumentException("clazz must be SqlRow.class");
        }
        return clazz.cast(getRow(position));
    }

    public SqlRow getRow(int position)
    {
        checkReadablePosition(this, position);
        if (isNull(position)) {
            throw new IllegalStateException("Position is null");
        }
        return new SqlRow(position, fieldBlocks);
    }

    @Override
    public RowBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(this, position);

        Block[] newBlocks = new Block[fieldBlocks.length];
        for (int i = 0; i < fieldBlocks.length; i++) {
            newBlocks[i] = fieldBlocks[i].getSingleValueBlock(position);
        }
        boolean[] newRowIsNull = isNull(position) ? new boolean[] {true} : null;
        return new RowBlock(1, newRowIsNull, newBlocks, fixedSizePerRow);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkReadablePosition(this, position);

        if (isNull(position)) {
            return 0;
        }

        long size = 0;
        for (Block fieldBlock : fieldBlocks) {
            size += fieldBlock.getEstimatedDataSizeForStats(position);
        }
        return size;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(this, position);
        return rowIsNull != null && rowIsNull[position];
    }

    /**
     * Returns the row fields from the specified block. The block maybe a LazyBlock, RunLengthEncodedBlock, or
     * DictionaryBlock, but the underlying block must be a RowBlock. The returned field blocks will be the same
     * length as the specified block, which means they are not null suppressed.
     */
    public static List<Block> getRowFieldsFromBlock(Block block)
    {
        // if the block is lazy, be careful to not materialize the nested blocks
        if (block instanceof LazyBlock lazyBlock) {
            block = lazyBlock.getBlock();
        }

        if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
            RowBlock rowBlock = (RowBlock) runLengthEncodedBlock.getValue();
            return rowBlock.fieldBlocksList.stream()
                    .map(fieldBlock -> RunLengthEncodedBlock.create(fieldBlock, runLengthEncodedBlock.getPositionCount()))
                    .toList();
        }
        if (block instanceof DictionaryBlock dictionaryBlock) {
            RowBlock rowBlock = (RowBlock) dictionaryBlock.getDictionary();
            return rowBlock.fieldBlocksList.stream()
                    .map(dictionaryBlock::createProjection)
                    .toList();
        }
        if (block instanceof RowBlock) {
            return ((RowBlock) block).getFieldBlocks();
        }
        throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
    }

    /**
     * Returns the row fields from the specified block with null rows suppressed. The block maybe a LazyBlock, RunLengthEncodedBlock, or
     * DictionaryBlock, but the underlying block must be a RowBlock. The returned field blocks will not be the same
     * length as the specified block if it contains null rows.
     */
    public static List<Block> getNullSuppressedRowFieldsFromBlock(Block block)
    {
        // if the block is lazy, be careful to not materialize the nested blocks
        if (block instanceof LazyBlock lazyBlock) {
            block = lazyBlock.getBlock();
        }

        if (!block.mayHaveNull()) {
            return getRowFieldsFromBlock(block);
        }

        if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
            RowBlock rowBlock = (RowBlock) runLengthEncodedBlock.getValue();
            if (!rowBlock.isNull(0)) {
                throw new IllegalStateException("Expected run length encoded block value to be null");
            }
            // all values are null, so return a zero-length block of the correct type
            return rowBlock.fieldBlocksList.stream()
                    .map(fieldBlock -> fieldBlock.getRegion(0, 0))
                    .toList();
        }
        if (block instanceof DictionaryBlock dictionaryBlock) {
            int[] newIds = new int[dictionaryBlock.getPositionCount()];
            int idCount = 0;
            for (int position = 0; position < newIds.length; position++) {
                if (!dictionaryBlock.isNull(position)) {
                    newIds[idCount] = dictionaryBlock.getId(position);
                    idCount++;
                }
            }
            int nonNullPositionCount = idCount;
            RowBlock rowBlock = (RowBlock) dictionaryBlock.getDictionary();
            return rowBlock.fieldBlocksList.stream()
                    .map(field -> DictionaryBlock.create(nonNullPositionCount, field, newIds))
                    .toList();
        }
        if (block instanceof RowBlock rowBlock) {
            int[] nonNullPositions = new int[rowBlock.getPositionCount()];
            int idCount = 0;
            for (int position = 0; position < nonNullPositions.length; position++) {
                if (!rowBlock.isNull(position)) {
                    nonNullPositions[idCount] = position;
                    idCount++;
                }
            }
            int nonNullPositionCount = idCount;
            return rowBlock.fieldBlocksList.stream()
                    .map(field -> DictionaryBlock.create(nonNullPositionCount, field, nonNullPositions))
                    .toList();
        }
        throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
    }

    @Override
    public RowBlock getUnderlyingValueBlock()
    {
        return this;
    }
}
