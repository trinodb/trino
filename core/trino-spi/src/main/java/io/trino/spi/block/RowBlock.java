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
import static io.trino.spi.block.Bitmap.checkBitRange;
import static io.trino.spi.block.Bitmap.compactBitmap;
import static io.trino.spi.block.Bitmap.copyBitmapAndAppendUnset;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.BlockUtil.arraySame;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidPosition;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class RowBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(RowBlock.class);

    private final int startOffset;
    private final int positionCount;
    @Nullable
    private final long[] valueIsValid;
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
    public static RowBlock fromNotNullSuppressedFieldBlocks(int positionCount, Optional<long[]> valueIsValidOptional, Block[] fieldBlocks)
    {
        // verify that field values for null rows are null
        if (valueIsValidOptional.isPresent()) {
            long[] valueIsValid = valueIsValidOptional.get();
            checkBitRange(valueIsValid, 0, positionCount);

            for (int fieldIndex = 0; fieldIndex < fieldBlocks.length; fieldIndex++) {
                Block field = fieldBlocks[fieldIndex];
                for (int position = 0; position < positionCount; position++) {
                    if (!Bitmap.isSet(valueIsValid, 0, position) && !field.isNull(position)) {
                        throw new IllegalArgumentException(format("Field value for null row must be null: field %s, position %s", fieldIndex, position));
                    }
                }
            }
        }
        return createRowBlockInternal(0, positionCount, valueIsValidOptional.orElse(null), fieldBlocks);
    }

    static RowBlock createRowBlockInternal(int startOffset, int positionCount, @Nullable long[] valueIsValid, Block[] fieldBlocks)
    {
        return new RowBlock(startOffset, positionCount, valueIsValid, fieldBlocks);
    }

    /**
     * Use createRowBlockInternal or fromFieldBlocks instead of this method. The caller of this method is assumed to have
     * validated the arguments with validateConstructorArguments.
     */
    private RowBlock(int startOffset, int positionCount, @Nullable long[] valueIsValid, Block[] fieldBlocks)
    {
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset is negative");
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        checkBitRange(valueIsValid, startOffset, positionCount);

        requireNonNull(fieldBlocks, "fieldBlocks is null");
        if (fieldBlocks.length > 0) {
            int firstFieldBlockPositionCount = fieldBlocks[0].getPositionCount();
            for (int i = 1; i < fieldBlocks.length; i++) {
                if (firstFieldBlockPositionCount != fieldBlocks[i].getPositionCount()) {
                    throw new IllegalArgumentException(format("length of field blocks differ: field 0: %s, block %s: %s", firstFieldBlockPositionCount, i, fieldBlocks[i].getPositionCount()));
                }
            }

            if (firstFieldBlockPositionCount - startOffset < positionCount) {
                throw new IllegalArgumentException("fieldBlock length is less than positionCount");
            }
        }

        this.startOffset = startOffset;
        this.positionCount = positionCount;
        this.valueIsValid = positionCount == 0 ? null : valueIsValid;
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

    /**
     * Creates a projection by replacing the visible field blocks for this row block.
     * The replacement fields must correspond to {@link #getFieldBlocks()}, not {@link #getRawFieldBlocks()}.
     * The replacement field count may differ from this block's field count.
     * If this block is zero-aligned, the existing validity bitmap is reused.
     * Otherwise, the visible validity bits are normalized and the returned block has an offset base of zero.
     */
    public RowBlock createProjection(Block[] newVisibleFieldBlocks)
    {
        requireNonNull(newVisibleFieldBlocks, "newVisibleFieldBlocks is null");

        for (int i = 0; i < newVisibleFieldBlocks.length; i++) {
            Block fieldBlock = newVisibleFieldBlocks[i];
            if (fieldBlock == null) {
                throw new NullPointerException(format("newVisibleFieldBlocks[%s] is null", i));
            }
            if (fieldBlock.getPositionCount() != positionCount) {
                throw new IllegalArgumentException(format("newVisibleFieldBlocks must have the same position count as this block; expected %s but field %s has %s", positionCount, i, fieldBlock.getPositionCount()));
            }
        }

        if (startOffset == 0) {
            return new RowBlock(0, positionCount, valueIsValid, newVisibleFieldBlocks);
        }

        long[] newValueIsValid = compactBitmap(valueIsValid, startOffset, positionCount);
        return new RowBlock(0, positionCount, newValueIsValid, newVisibleFieldBlocks);
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsValid != null;
    }

    @Override
    public boolean hasNull()
    {
        return Bitmap.hasUnsetBit(valueIsValid, startOffset, positionCount);
    }

    /// Returns raw validity bitmap words using the [Bitmap] encoding, or null if all positions are valid.
    ///
    /// The returned array is raw block storage. Use [getValidityBitmap()] unless the caller already has the matching
    /// raw bit offset.
    @Nullable
    public long[] getRawValueIsValid()
    {
        return valueIsValid;
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
            retainedSizeInBytes = INSTANCE_SIZE + sizeOf(fieldBlocks) + sizeOf(valueIsValid);
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
        if (valueIsValid != null) {
            consumer.accept(valueIsValid, sizeOf(valueIsValid));
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
        long[] newValueIsValid = copyBitmapAndAppendUnset(valueIsValid, startOffset, positionCount);
        Block[] newBlocks = new Block[fieldBlocks.length];
        for (int i = 0; i < fieldBlocks.length; i++) {
            newBlocks[i] = fieldBlocks[i].copyWithAppendedNull();
        }
        return new RowBlock(startOffset, positionCount + 1, newValueIsValid, newBlocks);
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

        long[] newValueIsValid = null;
        if (valueIsValid != null) {
            newValueIsValid = new long[Bitmap.wordsForBits(length)];
            for (int i = 0; i < length; i++) {
                if (Bitmap.isSet(valueIsValid, startOffset, positions[offset + i])) {
                    set(newValueIsValid, 0, i);
                }
            }
            if (!Bitmap.hasUnsetBit(newValueIsValid, 0, length)) {
                newValueIsValid = null;
            }
        }

        return new RowBlock(0, length, newValueIsValid, newBlocks);
    }

    @Override
    public RowBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);

        return new RowBlock(startOffset + positionOffset, length, valueIsValid, fieldBlocks);
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

        long[] newValueIsValid = compactBitmap(valueIsValid, startOffset + positionOffset, length);
        if (startOffset == 0 && newValueIsValid == valueIsValid && arraySame(newBlocks, fieldBlocks)) {
            return this;
        }
        return new RowBlock(0, length, newValueIsValid, newBlocks);
    }

    public SqlRow getRow(int position)
    {
        checkValidPosition(position, positionCount);
        if (isNull(position)) {
            throw new IllegalStateException("Position is null");
        }
        return new SqlRow(startOffset + position, fieldBlocks);
    }

    @Override
    public RowBlock getSingleValueBlock(int position)
    {
        checkValidPosition(position, positionCount);

        Block[] newBlocks = new Block[fieldBlocks.length];
        for (int i = 0; i < fieldBlocks.length; i++) {
            newBlocks[i] = fieldBlocks[i].getSingleValueBlock(startOffset + position);
        }
        long[] newValueIsValid = isNull(position) ? new long[] {0} : null;
        return new RowBlock(0, 1, newValueIsValid, newBlocks);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkValidPosition(position, positionCount);

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
        checkValidPosition(position, positionCount);
        return !Bitmap.isSet(valueIsValid, startOffset, position);
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
    public Optional<Bitmap> getValidityBitmap()
    {
        if (valueIsValid == null) {
            return Optional.empty();
        }
        return Optional.of(new Bitmap(valueIsValid, startOffset, positionCount));
    }
}
