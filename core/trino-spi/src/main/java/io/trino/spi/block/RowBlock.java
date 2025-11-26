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
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.arraySame;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkReadablePosition;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class RowBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(RowBlock.class);

    private final int startOffset;
    private final int positionCount;
    @Nullable
    private final boolean[] rowIsNull;
    /**
     * Field blocks have the same position count as this row block. The field value of a null row must be null.
     */
    private final Block[] fieldBlocks;

    private volatile long sizeInBytes = -1;
    private volatile long retainedSizeInBytes = -1;

    /**
     * Create a row block directly from field blocks. The returned RowBlock will not contain any null rows, although the fields may contain null values.
     */
    public static RowBlock fromFieldBlocks(int positionCount, Block[] fieldBlocks)
    {
        return createRowBlockInternal(0, positionCount, null, fieldBlocks);
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
                for (int position = 0; position < positionCount; position++) {
                    if (rowIsNull[position] && !field.isNull(position)) {
                        throw new IllegalArgumentException(format("Field value for null row must be null: field %s, position %s", fieldIndex, position));
                    }
                }
            }
        }
        return createRowBlockInternal(0, positionCount, rowIsNullOptional.orElse(null), fieldBlocks);
    }

    static RowBlock createRowBlockInternal(int startOffset, int positionCount, @Nullable boolean[] rowIsNull, Block[] fieldBlocks)
    {
        return new RowBlock(startOffset, positionCount, rowIsNull, fieldBlocks);
    }

    /**
     * Use createRowBlockInternal or fromFieldBlocks instead of this method. The caller of this method is assumed to have
     * validated the arguments with validateConstructorArguments.
     */
    private RowBlock(int startOffset, int positionCount, @Nullable boolean[] rowIsNull, Block[] fieldBlocks)
    {
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset is negative");
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        if (rowIsNull != null && rowIsNull.length - startOffset < positionCount) {
            throw new IllegalArgumentException("rowIsNull length is less than positionCount");
        }

        requireNonNull(fieldBlocks, "fieldBlocks is null");
        if (fieldBlocks.length == 0) {
            throw new IllegalArgumentException("Row block must contain at least one field");
        }

        int firstFieldBlockPositionCount = fieldBlocks[0].getPositionCount();
        for (int i = 1; i < fieldBlocks.length; i++) {
            if (firstFieldBlockPositionCount != fieldBlocks[i].getPositionCount()) {
                throw new IllegalArgumentException(format("length of field blocks differ: field 0: %s, block %s: %s", firstFieldBlockPositionCount, i, fieldBlocks[i].getPositionCount()));
            }
        }

        if (firstFieldBlockPositionCount - startOffset < positionCount) {
            throw new IllegalArgumentException("fieldBlock length is less than positionCount");
        }

        this.startOffset = startOffset;
        this.positionCount = positionCount;
        this.rowIsNull = positionCount == 0 ? null : rowIsNull;
        this.fieldBlocks = fieldBlocks;
    }

    /**
     * Returns the field blocks for this row block's visible region.
     * <p>
     * If {@code startOffset == 0} and the field blocks already have {@code positionCount}
     * positions, returns the underlying blocks as is. Otherwise, returns per-field
     * region views created via {@link Block#getRegion(int, int)} using this block's
     * {@code startOffset} and {@code positionCount}. No data is copied.
     * </p>
     *
     * @return unmodifiable list of blocks covering the visible region
     */
    public List<Block> getFieldBlocks()
    {
        if ((startOffset == 0) && (fieldBlocks[0].getPositionCount() == positionCount)) {
            return List.of(fieldBlocks);
        }
        return Arrays.stream(fieldBlocks).map(block -> block.getRegion(startOffset, positionCount)).toList();
    }

    public Block getFieldBlock(int fieldIndex)
    {
        if ((startOffset == 0) && (fieldBlocks[fieldIndex].getPositionCount() == positionCount)) {
            return fieldBlocks[fieldIndex];
        }
        return fieldBlocks[fieldIndex].getRegion(startOffset, positionCount);
    }

    public Block[] getRawFieldBlocks()
    {
        return fieldBlocks;
    }

    public Block getRawFieldBlock(int fieldIndex)
    {
        return fieldBlocks[fieldIndex];
    }

    public int getOffsetBase()
    {
        return startOffset;
    }

    @Override
    public boolean mayHaveNull()
    {
        return rowIsNull != null;
    }

    @Override
    public boolean hasNull()
    {
        if (rowIsNull == null) {
            return false;
        }
        for (int i = 0; i < positionCount; i++) {
            if (rowIsNull[startOffset + i]) {
                return true;
            }
        }
        return false;
    }

    @Nullable
    public boolean[] getRawRowIsNull()
    {
        return rowIsNull;
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
        for (Block fieldBlock : fieldBlocks) {
            sizeInBytes += fieldBlock.getRegionSizeInBytes(startOffset, positionCount);
        }
        this.sizeInBytes = sizeInBytes;
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = this.retainedSizeInBytes;
        if (retainedSizeInBytes < 0) {
            retainedSizeInBytes = INSTANCE_SIZE + sizeOf(fieldBlocks) + sizeOf(rowIsNull);
            for (Block fieldBlock : fieldBlocks) {
                retainedSizeInBytes += fieldBlock.getRetainedSizeInBytes();
            }
            this.retainedSizeInBytes = retainedSizeInBytes;
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
        consumer.accept(fieldBlocks, sizeOf(fieldBlocks));
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public String toString()
    {
        return format("RowBlock{startOffset=%d, fieldCount=%d, positionCount=%d}", startOffset, fieldBlocks.length, positionCount);
    }

    @Override
    public RowBlock copyWithAppendedNull()
    {
        boolean[] newRowIsNull;
        if (rowIsNull != null) {
            newRowIsNull = Arrays.copyOf(rowIsNull, startOffset + positionCount + 1);
        }
        else {
            newRowIsNull = new boolean[startOffset + positionCount + 1];
        }
        // mark the (new) last element as null
        newRowIsNull[startOffset + positionCount] = true;

        Block[] newBlocks = new Block[fieldBlocks.length];
        for (int i = 0; i < fieldBlocks.length; i++) {
            newBlocks[i] = fieldBlocks[i].copyWithAppendedNull();
        }
        return new RowBlock(startOffset, positionCount + 1, newRowIsNull, newBlocks);
    }

    @Override
    public RowBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        Block[] newBlocks = new Block[fieldBlocks.length];
        for (int i = 0; i < newBlocks.length; i++) {
            Block fieldBlock = fieldBlocks[i];
            // If the row block has a non-zero starting offset, we have to create a temporary field block starting
            // from the correct offset before copying positions
            if (startOffset != 0) {
                fieldBlock = fieldBlock.getRegion(startOffset, positionCount);
            }
            newBlocks[i] = fieldBlock.copyPositions(positions, offset, length);
        }

        boolean[] newRowIsNull = null;
        if (rowIsNull != null) {
            boolean hasNull = false;
            newRowIsNull = new boolean[length];
            for (int i = 0; i < length; i++) {
                boolean isNull = rowIsNull[startOffset + positions[offset + i]];
                newRowIsNull[i] = isNull;
                hasNull |= isNull;
            }
            if (!hasNull) {
                newRowIsNull = null;
            }
        }

        return new RowBlock(0, length, newRowIsNull, newBlocks);
    }

    @Override
    public RowBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);

        return new RowBlock(startOffset + positionOffset, length, rowIsNull, fieldBlocks);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        checkValidRegion(positionCount, position, length);

        long regionSizeInBytes = Byte.BYTES * (long) length;
        for (Block fieldBlock : fieldBlocks) {
            regionSizeInBytes += fieldBlock.getRegionSizeInBytes(startOffset + position, length);
        }
        return regionSizeInBytes;
    }

    @Override
    public RowBlock copyRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);

        Block[] newBlocks = new Block[fieldBlocks.length];
        for (int i = 0; i < fieldBlocks.length; i++) {
            newBlocks[i] = fieldBlocks[i].copyRegion(startOffset + positionOffset, length);
        }

        boolean[] newRowIsNull = rowIsNull == null ? null : compactArray(rowIsNull, startOffset + positionOffset, length);
        if (startOffset == 0 && newRowIsNull == rowIsNull && arraySame(newBlocks, fieldBlocks)) {
            return this;
        }
        return new RowBlock(0, length, newRowIsNull, newBlocks);
    }

    public SqlRow getRow(int position)
    {
        checkReadablePosition(this, position);
        if (isNull(position)) {
            throw new IllegalStateException("Position is null");
        }
        return new SqlRow(startOffset + position, fieldBlocks);
    }

    @Override
    public RowBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(this, position);

        Block[] newBlocks = new Block[fieldBlocks.length];
        for (int i = 0; i < fieldBlocks.length; i++) {
            newBlocks[i] = fieldBlocks[i].getSingleValueBlock(startOffset + position);
        }
        boolean[] newRowIsNull = isNull(position) ? new boolean[] {true} : null;
        return new RowBlock(0, 1, newRowIsNull, newBlocks);
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
            size += fieldBlock.getEstimatedDataSizeForStats(startOffset + position);
        }
        return size;
    }

    @Override
    public boolean isNull(int position)
    {
        if (!mayHaveNull()) {
            return false;
        }
        checkReadablePosition(this, position);
        return rowIsNull[startOffset + position];
    }

    /**
     * Returns the row fields from the specified block. The block maybe a RunLengthEncodedBlock, or
     * DictionaryBlock, but the underlying block must be a RowBlock. The returned field blocks will be the same
     * length as the specified block, which means they are not null suppressed.
     */
    public static List<Block> getRowFieldsFromBlock(Block block)
    {
        if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
            RowBlock rowBlock = (RowBlock) runLengthEncodedBlock.getValue();
            return rowBlock.getFieldBlocks().stream()
                    .map(fieldBlock -> RunLengthEncodedBlock.create(fieldBlock, runLengthEncodedBlock.getPositionCount()))
                    .toList();
        }
        if (block instanceof DictionaryBlock dictionaryBlock) {
            RowBlock rowBlock = (RowBlock) dictionaryBlock.getDictionary();
            return rowBlock.getFieldBlocks().stream()
                    .map(dictionaryBlock::createProjection)
                    .toList();
        }
        if (block instanceof RowBlock rowBlock) {
            return rowBlock.getFieldBlocks();
        }
        throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
    }

    /**
     * Returns the row fields from the specified block with null rows suppressed. The block maybe a RunLengthEncodedBlock, or
     * DictionaryBlock, but the underlying block must be a RowBlock. The returned field blocks will not be the same
     * length as the specified block if it contains null rows.
     */
    public static List<Block> getNullSuppressedRowFieldsFromBlock(Block block)
    {
        if (!block.mayHaveNull()) {
            return getRowFieldsFromBlock(block);
        }

        if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
            RowBlock rowBlock = (RowBlock) runLengthEncodedBlock.getValue();
            if (!rowBlock.isNull(0)) {
                throw new IllegalStateException("Expected run length encoded block value to be null");
            }
            // all values are null, so return a zero-length block of the correct type
            return rowBlock.getFieldBlocks().stream()
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
            return rowBlock.getFieldBlocks().stream()
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
            return rowBlock.getFieldBlocks().stream()
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

    @Override
    public Optional<ByteArrayBlock> getNulls()
    {
        return BlockUtil.getNulls(rowIsNull, startOffset, positionCount);
    }
}
