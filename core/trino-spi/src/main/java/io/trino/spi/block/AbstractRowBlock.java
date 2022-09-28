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

import javax.annotation.Nullable;

import java.util.List;
import java.util.OptionalInt;

import static io.trino.spi.block.BlockUtil.arraySame;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkReadablePosition;
import static io.trino.spi.block.BlockUtil.checkValidPositions;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.BlockUtil.compactOffsets;
import static io.trino.spi.block.RowBlock.createRowBlockInternal;

public abstract class AbstractRowBlock
        implements Block
{
    protected final int numFields;

    @Override
    public final List<Block> getChildren()
    {
        return List.of(getRawFieldBlocks());
    }

    protected abstract Block[] getRawFieldBlocks();

    @Nullable
    protected abstract int[] getFieldBlockOffsets();

    protected abstract int getOffsetBase();

    /**
     * @return the underlying rowIsNull array, or null when all rows are guaranteed to be non-null
     */
    @Nullable
    protected abstract boolean[] getRowIsNull();

    // the offset in each field block, it can also be viewed as the "entry-based" offset in the RowBlock
    public final int getFieldBlockOffset(int position)
    {
        int[] offsets = getFieldBlockOffsets();
        return offsets != null ? offsets[position + getOffsetBase()] : position + getOffsetBase();
    }

    protected AbstractRowBlock(int numFields)
    {
        if (numFields <= 0) {
            throw new IllegalArgumentException("Number of fields in RowBlock must be positive");
        }
        this.numFields = numFields;
    }

    @Override
    public String getEncodingName()
    {
        return RowBlockEncoding.NAME;
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int[] newOffsets = null;

        int[] fieldBlockPositions = new int[length];
        int fieldBlockPositionCount;
        boolean[] newRowIsNull;
        if (getRowIsNull() == null) {
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
        Block[] rawBlocks = getRawFieldBlocks();
        for (int i = 0; i < newBlocks.length; i++) {
            newBlocks[i] = rawBlocks[i].copyPositions(fieldBlockPositions, 0, fieldBlockPositionCount);
        }
        return createRowBlockInternal(0, length, newRowIsNull, newOffsets, newBlocks);
    }

    @Override
    public Block getRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        return createRowBlockInternal(position + getOffsetBase(), length, getRowIsNull(), getFieldBlockOffsets(), getRawFieldBlocks());
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
     * are fixed size. If any field is not fixed size, then no value will be returned. This does <i>not</i>
     * include the size-per-position overhead associated with the {@link AbstractRowBlock} itself, only of
     * the constituent field members.
     */
    private OptionalInt fixedSizeInBytesPerFieldPosition()
    {
        Block[] rawFieldBlocks = getRawFieldBlocks();
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
            regionSizeInBytes += getRawFieldBlocks()[i].getRegionSizeInBytes(startFieldBlockOffset, fieldBlockLength);
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
            boolean[] rowIsNull = getRowIsNull();
            if (rowIsNull != null) {
                // Some positions in usedPositions may be null which must be removed from the selectedFieldPositionCount
                int offsetBase = getOffsetBase();
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
        int offsetBase = getOffsetBase();
        boolean[] rowIsNull = getRowIsNull();
        // No fixed width size per row, specific positions used must be tracked
        int totalFieldPositions = getRawFieldBlocks()[0].getPositionCount();
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

        Block[] rawFieldBlocks = getRawFieldBlocks();
        long sizeInBytes = ((Integer.BYTES + Byte.BYTES) * (long) selectedRowPositions); // offsets + rowIsNull
        for (int j = 0; j < numFields; j++) {
            sizeInBytes += rawFieldBlocks[j].getPositionsSizeInBytes(fieldPositions, selectedFieldPositionCount);
        }
        return sizeInBytes;
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int endFieldBlockOffset = getFieldBlockOffset(position + length);
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;
        Block[] newBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            newBlocks[i] = getRawFieldBlocks()[i].copyRegion(startFieldBlockOffset, fieldBlockLength);
        }
        int[] fieldBlockOffsets = getFieldBlockOffsets();
        int[] newOffsets = fieldBlockOffsets == null ? null : compactOffsets(fieldBlockOffsets, position + getOffsetBase(), length);
        boolean[] rowIsNull = getRowIsNull();
        boolean[] newRowIsNull = rowIsNull == null ? null : compactArray(rowIsNull, position + getOffsetBase(), length);

        if (arraySame(newBlocks, getRawFieldBlocks()) && newOffsets == fieldBlockOffsets && newRowIsNull == rowIsNull) {
            return this;
        }
        return createRowBlockInternal(0, length, newRowIsNull, newOffsets, newBlocks);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        if (clazz != Block.class) {
            throw new IllegalArgumentException("clazz must be Block.class");
        }
        checkReadablePosition(this, position);

        return clazz.cast(new SingleRowBlock(getFieldBlockOffset(position), getRawFieldBlocks()));
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(this, position);

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int endFieldBlockOffset = getFieldBlockOffset(position + 1);
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;
        Block[] newBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            newBlocks[i] = getRawFieldBlocks()[i].copyRegion(startFieldBlockOffset, fieldBlockLength);
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

        Block[] rawFieldBlocks = getRawFieldBlocks();
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
        boolean[] rowIsNull = getRowIsNull();
        return rowIsNull != null && rowIsNull[position + getOffsetBase()];
    }
}
