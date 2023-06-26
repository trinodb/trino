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
import io.trino.spi.predicate.Utils;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkReadablePosition;
import static io.trino.spi.block.BlockUtil.checkValidPosition;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class RunLengthEncodedBlock
        implements Block
{
    private static final int INSTANCE_SIZE = instanceSize(RunLengthEncodedBlock.class);

    public static Block create(Type type, Object value, int positionCount)
    {
        Block block = Utils.nativeValueToBlock(type, value);
        if (block instanceof RunLengthEncodedBlock) {
            block = ((RunLengthEncodedBlock) block).getValue();
        }
        return create(block, positionCount);
    }

    public static Block create(Block value, int positionCount)
    {
        requireNonNull(value, "value is null");
        if (value.getPositionCount() != 1) {
            throw new IllegalArgumentException(format("Expected value to contain a single position but has %s positions", value.getPositionCount()));
        }

        if (positionCount == 0) {
            return value.copyRegion(0, 0);
        }
        if (positionCount == 1) {
            return value;
        }
        return new RunLengthEncodedBlock(value, positionCount);
    }

    private final Block value;
    private final int positionCount;

    private RunLengthEncodedBlock(Block value, int positionCount)
    {
        requireNonNull(value, "value is null");
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        if (positionCount < 2) {
            throw new IllegalArgumentException("positionCount must be at least 2");
        }

        // do not nest an RLE or Dictionary in an RLE
        if (value instanceof RunLengthEncodedBlock block) {
            this.value = block.getValue();
        }
        else if (value instanceof DictionaryBlock block) {
            Block dictionary = block.getDictionary();
            int id = block.getId(0);
            if (dictionary.getPositionCount() == 1 && id == 0) {
                this.value = dictionary;
            }
            else {
                this.value = dictionary.getRegion(id, 1);
            }
        }
        else {
            this.value = value;
        }

        this.positionCount = positionCount;
    }

    @Override
    public final List<Block> getChildren()
    {
        return singletonList(value);
    }

    public Block getValue()
    {
        return value;
    }

    /**
     * Positions count will always be at least 2
     */
    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.empty(); // size does not vary per position selected
    }

    @Override
    public long getSizeInBytes()
    {
        return value.getSizeInBytes();
    }

    @Override
    public long getLogicalSizeInBytes()
    {
        return positionCount * value.getLogicalSizeInBytes();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + value.getRetainedSizeInBytes();
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return value.getEstimatedDataSizeForStats(0);
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(value, value.getRetainedSizeInBytes());
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return RunLengthBlockEncoding.NAME;
    }

    @Override
    public Block getPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);
        for (int i = offset; i < offset + length; i++) {
            checkValidPosition(positions[i], positionCount);
        }
        return create(value, length);
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);
        for (int i = offset; i < offset + length; i++) {
            checkValidPosition(positions[i], positionCount);
        }
        return create(value.copyRegion(0, 1), length);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);
        return create(value, length);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return value.getSizeInBytes();
    }

    @Override
    public long getPositionsSizeInBytes(@Nullable boolean[] positions, int selectedPositionCount)
    {
        return value.getSizeInBytes();
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);
        return create(value.copyRegion(0, 1), length);
    }

    @Override
    public int getSliceLength(int position)
    {
        checkReadablePosition(this, position);
        return value.getSliceLength(0);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        checkReadablePosition(this, position);
        return value.getByte(0, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        checkReadablePosition(this, position);
        return value.getShort(0, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        checkReadablePosition(this, position);
        return value.getInt(0, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(this, position);
        return value.getLong(0, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        checkReadablePosition(this, position);
        return value.getSlice(0, offset, length);
    }

    @Override
    public void writeSliceTo(int position, int offset, int length, SliceOutput output)
    {
        checkReadablePosition(this, position);
        value.writeSliceTo(0, offset, length, output);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        checkReadablePosition(this, position);
        return value.getObject(0, clazz);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        checkReadablePosition(this, position);
        return value.bytesEqual(0, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        checkReadablePosition(this, position);
        return value.bytesCompare(0, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        checkReadablePosition(this, position);
        return value.equals(0, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        checkReadablePosition(this, position);
        return value.hash(0, offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        checkReadablePosition(this, leftPosition);
        return value.compareTo(0, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(this, position);
        return value;
    }

    @Override
    public boolean mayHaveNull()
    {
        return positionCount > 0 && value.isNull(0);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(this, position);
        return value.isNull(0);
    }

    @Override
    public Block copyWithAppendedNull()
    {
        if (value.isNull(0)) {
            return create(value, positionCount + 1);
        }

        Block dictionary = value.copyWithAppendedNull();
        int[] ids = new int[positionCount + 1];
        ids[positionCount] = 1;
        return DictionaryBlock.create(ids.length, dictionary, ids);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("{positionCount=").append(positionCount);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean isLoaded()
    {
        return value.isLoaded();
    }

    @Override
    public Block getLoadedBlock()
    {
        Block loadedValueBlock = value.getLoadedBlock();

        if (loadedValueBlock == value) {
            return this;
        }
        return create(loadedValueBlock, positionCount);
    }
}
