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
import static io.trino.spi.block.BlockUtil.compactOffsets;
import static io.trino.spi.block.BlockUtil.copyIsNullAndAppendNull;
import static io.trino.spi.block.BlockUtil.copyOffsetsAndAppendNull;
import static io.trino.spi.block.BlockUtil.ensureBlocksAreLoaded;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RowBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(RowBlock.class);
    private final int numFields;

    private final int startOffset;
    private final int positionCount;

    private final boolean[] rowIsNull;
    private final int[] fieldBlockOffsets;
    private final Block[] fieldBlocks;
    private final List<Block> fieldBlocksList;

    private volatile long sizeInBytes = -1;
    private final long retainedSizeInBytes;

    /**
     * Create a row block directly from columnar nulls and field blocks.
     */
    public static RowBlock fromFieldBlocks(int positionCount, Optional<boolean[]> rowIsNullOptional, Block[] fieldBlocks)
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

        if (fieldBlocks.length == 0) {
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
        if (fieldBlocks.length == 0) {
            throw new IllegalArgumentException("Number of fields in RowBlock must be positive");
        }
        this.numFields = fieldBlocks.length;

        this.startOffset = startOffset;
        this.positionCount = positionCount;
        this.rowIsNull = rowIsNull;
        this.fieldBlockOffsets = fieldBlockOffsets;
        this.fieldBlocks = fieldBlocks;
        this.fieldBlocksList = List.of(fieldBlocks);

        this.retainedSizeInBytes = INSTANCE_SIZE + sizeOf(fieldBlockOffsets) + sizeOf(rowIsNull);
    }

    protected List<Block> getRawFieldBlocks()
    {
        return fieldBlocksList;
    }

    @Nullable
    protected int[] getFieldBlockOffsets()
    {
        return fieldBlockOffsets;
    }

    protected int getOffsetBase()
    {
        return startOffset;
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

        long sizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) positionCount;
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
    public RowBlock copyWithAppendedNull()
    {
        boolean[] newRowIsNull = copyIsNullAndAppendNull(rowIsNull, startOffset, getPositionCount());

        int[] newOffsets;
        if (fieldBlockOffsets == null) {
            int desiredLength = startOffset + positionCount + 2;
            newOffsets = new int[desiredLength];
            newOffsets[startOffset] = startOffset;
            for (int position = startOffset; position < startOffset + positionCount; position++) {
                // Since there are no nulls in the original array, new offsets are the same as previous ones
                newOffsets[position + 1] = newOffsets[position] + 1;
            }

            // Null does not change offset
            newOffsets[desiredLength - 1] = newOffsets[desiredLength - 2];
        }
        else {
            newOffsets = copyOffsetsAndAppendNull(fieldBlockOffsets, startOffset, getPositionCount());
        }

        return createRowBlockInternal(startOffset, getPositionCount() + 1, newRowIsNull, newOffsets, fieldBlocks);
    }

    @Override
    public final List<Block> getChildren()
    {
        return List.of(fieldBlocks);
    }

    // the offset in each field block, it can also be viewed as the "entry-based" offset in the RowBlock
    public final int getFieldBlockOffset(int position)
    {
        int[] offsets = fieldBlockOffsets;
        if (offsets != null) {
            return offsets[position + startOffset];
        }
        else {
            return position + startOffset;
        }
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

        int[] newOffsets = null;

        int[] fieldBlockPositions = new int[length];
        int fieldBlockPositionCount;
        boolean[] newRowIsNull;
        if (rowIsNull == null) {
            // No nulls are present
            newRowIsNull = null;
            for (int i = 0; i < fieldBlockPositions.length; i++) {
                int position = positions[offset + i];
                checkReadablePosition(this, position);
                fieldBlockPositions[i] = getFieldBlockOffset(position);
            }
            fieldBlockPositionCount = fieldBlockPositions.length;
        }
        else {
            newRowIsNull = new boolean[length];
            newOffsets = new int[length + 1];
            fieldBlockPositionCount = 0;
            for (int i = 0; i < length; i++) {
                newOffsets[i] = fieldBlockPositionCount;
                int position = positions[offset + i];
                boolean positionIsNull = isNull(position);
                newRowIsNull[i] = positionIsNull;
                fieldBlockPositions[fieldBlockPositionCount] = getFieldBlockOffset(position);
                fieldBlockPositionCount += positionIsNull ? 0 : 1;
            }
            // Record last offset position
            newOffsets[length] = fieldBlockPositionCount;
            if (fieldBlockPositionCount == length) {
                // No nulls encountered, discard the null mask and offsets
                newRowIsNull = null;
                newOffsets = null;
            }
        }

        Block[] newBlocks = new Block[numFields];
        Block[] rawBlocks = fieldBlocks;
        for (int i = 0; i < newBlocks.length; i++) {
            newBlocks[i] = rawBlocks[i].copyPositions(fieldBlockPositions, 0, fieldBlockPositionCount);
        }
        return createRowBlockInternal(0, length, newRowIsNull, newOffsets, newBlocks);
    }

    @Override
    public RowBlock getRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        return createRowBlockInternal(position + startOffset, length, rowIsNull, fieldBlockOffsets, fieldBlocks);
    }

    @Override
    public final OptionalInt fixedSizeInBytesPerPosition()
    {
        if (!mayHaveNull()) {
            // when null rows are present, we can't use the fixed field sizes to infer the correct
            // size for arbitrary position selection
            OptionalInt fieldSize = fixedSizeInBytesPerFieldPosition();
            if (fieldSize.isPresent()) {
                // must include the row block overhead in addition to the per position size in bytes
                return OptionalInt.of(fieldSize.getAsInt() + (Integer.BYTES + Byte.BYTES)); // offsets + rowIsNull
            }
        }
        return OptionalInt.empty();
    }

    /**
     * Returns the combined {@link Block#fixedSizeInBytesPerPosition()} value for all fields, assuming all
     * are fixed-size. If any field is not fixed size, then no value will be returned. This does <i>not</i>
     * include the size-per-position overhead associated with the {@link RowBlock} itself, only of
     * the constituent field members.
     */
    private OptionalInt fixedSizeInBytesPerFieldPosition()
    {
        Block[] rawFieldBlocks = fieldBlocks;
        int fixedSizePerRow = 0;
        for (int i = 0; i < numFields; i++) {
            OptionalInt fieldFixedSize = rawFieldBlocks[i].fixedSizeInBytesPerPosition();
            if (fieldFixedSize.isEmpty()) {
                return OptionalInt.empty(); // found a block without a single per-position size
            }
            fixedSizePerRow += fieldFixedSize.getAsInt();
        }
        return OptionalInt.of(fixedSizePerRow);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int endFieldBlockOffset = getFieldBlockOffset(position + length);
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;

        long regionSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) length;
        for (int i = 0; i < numFields; i++) {
            regionSizeInBytes += fieldBlocks[i].getRegionSizeInBytes(startFieldBlockOffset, fieldBlockLength);
        }
        return regionSizeInBytes;
    }

    @Override
    public final long getPositionsSizeInBytes(boolean[] positions, int selectedRowPositions)
    {
        int positionCount = getPositionCount();
        checkValidPositions(positions, positionCount);
        if (selectedRowPositions == 0) {
            return 0;
        }
        if (selectedRowPositions == positionCount) {
            return getSizeInBytes();
        }

        OptionalInt fixedSizePerFieldPosition = fixedSizeInBytesPerFieldPosition();
        if (fixedSizePerFieldPosition.isPresent()) {
            // All field blocks are fixed size per position, no specific position mapping is necessary
            int selectedFieldPositionCount = selectedRowPositions;
            boolean[] rowIsNull = this.rowIsNull;
            if (rowIsNull != null) {
                // Some positions in usedPositions may be null which must be removed from the selectedFieldPositionCount
                int offsetBase = startOffset;
                for (int i = 0; i < positions.length; i++) {
                    if (positions[i] && rowIsNull[i + offsetBase]) {
                        selectedFieldPositionCount--; // selected row is null, don't include it in the selected field positions
                    }
                }
                if (selectedFieldPositionCount < 0) {
                    throw new IllegalStateException("Invalid field position selection after nulls removed: " + selectedFieldPositionCount);
                }
            }
            return ((Integer.BYTES + Byte.BYTES) * (long) selectedRowPositions) + (fixedSizePerFieldPosition.getAsInt() * (long) selectedFieldPositionCount);
        }

        // Fall back to specific position size calculations
        return getSpecificPositionsSizeInBytes(positions, selectedRowPositions);
    }

    private long getSpecificPositionsSizeInBytes(boolean[] positions, int selectedRowPositions)
    {
        int positionCount = getPositionCount();
        int offsetBase = startOffset;
        boolean[] rowIsNull = this.rowIsNull;
        // No fixed width size per row, specific positions used must be tracked
        int totalFieldPositions = fieldBlocks[0].getPositionCount();
        boolean[] fieldPositions;
        int selectedFieldPositionCount;
        if (rowIsNull == null) {
            // No nulls, so the same number of positions are used
            selectedFieldPositionCount = selectedRowPositions;
            if (offsetBase == 0 && positionCount == totalFieldPositions) {
                // No need to adapt the positions array at all, reuse it directly
                fieldPositions = positions;
            }
            else {
                // no nulls present, so we can just shift the positions array into alignment with the elements block with other positions unused
                fieldPositions = new boolean[totalFieldPositions];
                System.arraycopy(positions, 0, fieldPositions, offsetBase, positions.length);
            }
        }
        else {
            fieldPositions = new boolean[totalFieldPositions];
            selectedFieldPositionCount = 0;
            for (int i = 0; i < positions.length; i++) {
                if (positions[i] && !rowIsNull[offsetBase + i]) {
                    selectedFieldPositionCount++;
                    fieldPositions[getFieldBlockOffset(i)] = true;
                }
            }
        }

        Block[] rawFieldBlocks = fieldBlocks;
        long sizeInBytes = ((Integer.BYTES + Byte.BYTES) * (long) selectedRowPositions); // offsets + rowIsNull
        for (int j = 0; j < numFields; j++) {
            sizeInBytes += rawFieldBlocks[j].getPositionsSizeInBytes(fieldPositions, selectedFieldPositionCount);
        }
        return sizeInBytes;
    }

    @Override
    public RowBlock copyRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int endFieldBlockOffset = getFieldBlockOffset(position + length);
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;
        Block[] newBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            newBlocks[i] = fieldBlocks[i].copyRegion(startFieldBlockOffset, fieldBlockLength);
        }
        int[] fieldBlockOffsets = this.fieldBlockOffsets;
        int[] newOffsets;
        if (fieldBlockOffsets == null) {
            newOffsets = null;
        }
        else {
            newOffsets = compactOffsets(fieldBlockOffsets, position + startOffset, length);
        }
        boolean[] rowIsNull = this.rowIsNull;
        boolean[] newRowIsNull;
        if (rowIsNull == null) {
            newRowIsNull = null;
        }
        else {
            newRowIsNull = compactArray(rowIsNull, position + startOffset, length);
        }

        if (arraySame(newBlocks, fieldBlocks) && newOffsets == fieldBlockOffsets && newRowIsNull == rowIsNull) {
            return this;
        }
        return createRowBlockInternal(0, length, newRowIsNull, newOffsets, newBlocks);
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
        return new SqlRow(getFieldBlockOffset(position), fieldBlocks);
    }

    @Override
    public RowBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(this, position);

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int endFieldBlockOffset = getFieldBlockOffset(position + 1);
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;
        Block[] newBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            newBlocks[i] = fieldBlocks[i].copyRegion(startFieldBlockOffset, fieldBlockLength);
        }
        boolean[] newRowIsNull = isNull(position) ? new boolean[] {true} : null;
        int[] newOffsets = isNull(position) ? new int[] {0, fieldBlockLength} : null;

        return createRowBlockInternal(0, 1, newRowIsNull, newOffsets, newBlocks);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkReadablePosition(this, position);

        if (isNull(position)) {
            return 0;
        }

        Block[] rawFieldBlocks = fieldBlocks;
        long size = 0;
        for (int i = 0; i < numFields; i++) {
            size += rawFieldBlocks[i].getEstimatedDataSizeForStats(getFieldBlockOffset(position));
        }
        return size;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(this, position);
        boolean[] rowIsNull = this.rowIsNull;
        if (rowIsNull == null) {
            return false;
        }
        return rowIsNull[position + startOffset];
    }

    @Override
    public RowBlock getUnderlyingValueBlock()
    {
        return this;
    }
}
