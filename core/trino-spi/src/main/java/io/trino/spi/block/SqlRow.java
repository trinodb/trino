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
import io.airlift.slice.SliceOutput;

import java.util.List;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.block.BlockUtil.ensureBlocksAreLoaded;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SqlRow
        implements Block
{
    private static final int INSTANCE_SIZE = instanceSize(SqlRow.class);

    private final Block[] fieldBlocks;
    private final List<Block> fieldBlocksList;
    private final int rawIndex;

    public SqlRow(int rawIndex, Block[] fieldBlocks)
    {
        this.rawIndex = rawIndex;
        this.fieldBlocks = requireNonNull(fieldBlocks, "fieldBlocks is null");
        fieldBlocksList = List.of(fieldBlocks);
    }

    public int getFieldCount()
    {
        return fieldBlocks.length;
    }

    public int getRawIndex()
    {
        return rawIndex;
    }

    public Block getRawFieldBlock(int fieldIndex)
    {
        return fieldBlocks[fieldIndex];
    }

    public List<Block> getRawFieldBlocks()
    {
        return fieldBlocksList;
    }

    @Override
    public int getPositionCount()
    {
        return fieldBlocks.length;
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.empty();
    }

    @Override
    public long getSizeInBytes()
    {
        long sizeInBytes = 0;
        for (Block fieldBlock : fieldBlocks) {
            sizeInBytes += fieldBlock.getRegionSizeInBytes(rawIndex, 1);
        }
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = INSTANCE_SIZE;
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
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return SqlRowBlockEncoding.NAME;
    }

    @Override
    public Block copyWithAppendedNull()
    {
        throw new UnsupportedOperationException("SingleRowBlock does not support newBlockWithAppendedNull()");
    }

    @Override
    public String toString()
    {
        return format("SingleRowBlock{numFields=%d}", fieldBlocks.length);
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
        return new SqlRow(rawIndex, loadedFieldBlocks);
    }

    @Override
    public final List<Block> getChildren()
    {
        return List.of(fieldBlocks);
    }

    private void checkFieldIndex(int position)
    {
        if (position < 0 || position >= fieldBlocks.length) {
            throw new IllegalArgumentException("position is not valid: " + position);
        }
    }

    @Override
    public boolean isNull(int position)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].isNull(rawIndex);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].getByte(rawIndex, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].getShort(rawIndex, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].getInt(rawIndex, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].getLong(rawIndex, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].getSlice(rawIndex, offset, length);
    }

    @Override
    public void writeSliceTo(int position, int offset, int length, SliceOutput output)
    {
        checkFieldIndex(position);
        fieldBlocks[position].writeSliceTo(rawIndex, offset, length, output);
    }

    @Override
    public int getSliceLength(int position)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].getSliceLength(rawIndex);
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].compareTo(rawIndex, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].bytesEqual(rawIndex, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].bytesCompare(rawIndex, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].equals(rawIndex, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].hash(rawIndex, offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].getObject(rawIndex, clazz);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].getSingleValueBlock(rawIndex);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkFieldIndex(position);
        return fieldBlocks[position].getEstimatedDataSizeForStats(rawIndex);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int selectedPositionsCount)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        throw new UnsupportedOperationException();
    }
}
