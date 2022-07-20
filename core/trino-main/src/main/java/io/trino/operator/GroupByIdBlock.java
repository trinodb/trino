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
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import io.trino.spi.block.Block;
import org.openjdk.jol.info.ClassLayout;

import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A block holding 'long' (8-byte) values and an additional group count value.
 * The implementation covers both standard and rle encoding through a simple
 * trick of a logical conjunction (&) of the position with a mask that is
 * full (111...111) for a standard encoding and empty (000...000) for a rle.
 * This way the rle block will always return the first position from the underlying array.
 * <p>
 * The implementation guarantees that every fetch of a single value will not
 * result in a virtual call and, as a result, may be safely inlined by the compiler.
 * <p>
 * The only limitation is that the block cannot be dictionary encoded, but this encoding
 * is not used within group by logic.
 */
public class GroupByIdBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupByIdBlock.class).instanceSize();
    private static final int SIZE_OF_ELEMENT = Long.BYTES;

    private static final int MASK_ARRAY = -1;
    private static final int MASK_RLE = 0;

    private final long groupCount;
    private final int positionCount;
    private final long[] groups;
    private final int mask;

    public static GroupByIdBlock ofArray(long groupCount, long[] groupIds)
    {
        return new GroupByIdBlock(groupCount, groupIds.length, groupIds, MASK_ARRAY);
    }

    public static GroupByIdBlock rle(long groupCount, long groupId, int length)
    {
        return new GroupByIdBlock(groupCount, length, new long[] {groupId}, MASK_RLE);
    }

    public static GroupByIdBlock ofBlock(long groupCount, Block block)
    {
        long[] groups = new long[block.getPositionCount()];
        for (int i = 0; i < block.getPositionCount(); i++) {
            groups[i] = block.getLong(i, 0);
        }
        return GroupByIdBlock.ofArray(groupCount, groups);
    }

    private GroupByIdBlock(long groupCount, int positionCount, long[] groups, int mask)
    {
        this.groupCount = groupCount;
        this.positionCount = positionCount;
        this.groups = requireNonNull(groups, "groups is null");
        this.mask = mask;
    }

    public long getGroupCount()
    {
        return groupCount;
    }

    public long getGroupId(int position)
    {
        checkReadablePosition(position);
        return groups[position & mask];
    }

    @VisibleForTesting
    boolean isRle()
    {
        return mask == MASK_RLE;
    }

    @Override
    public long getRegionSizeInBytes(int positionOffset, int length)
    {
        return isRle() ? SIZE_OF_ELEMENT : length * SIZE_OF_ELEMENT;
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return isRle() ? OptionalInt.empty() : OptionalInt.of(SIZE_OF_ELEMENT);
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int selectedPositionCount)
    {
        return (long) SIZE_OF_ELEMENT * selectedPositionCount;
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        throw new UnsupportedOperationException("GroupByIdBlock does not support copyRegion");
    }

    @Override
    public long getLong(int position, int offset)
    {
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero");
        }
        return getGroupId(position);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return GroupByIdBlock.rle(groupCount, getGroupId(position), 1);
    }

    @Override
    public boolean mayHaveNull()
    {
        return false;
    }

    @Override
    public boolean isNull(int position)
    {
        return false;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return getRegionSizeInBytes(0, positionCount);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(groups);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return SIZE_OF_ELEMENT;
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(groups, sizeOf(groups));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        throw new UnsupportedOperationException("GroupByIdBlock does not support serialization");
    }

    @Override
    public GroupByIdBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);
        if (isRle()) {
            return GroupByIdBlock.rle(groupCount, getGroupId(0), length);
        }
        long[] newGroupIds = new long[length];
        for (int i = 0; i < length; i++) {
            newGroupIds[i] = getGroupId(positions[i + offset]);
        }
        return GroupByIdBlock.ofArray(groupCount, newGroupIds);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        throw new UnsupportedOperationException("GroupByIdBlock does not support serialization");
    }

    @Override
    public Block copyWithAppendedNull()
    {
        throw new UnsupportedOperationException("GroupByIdBlock does not support newBlockWithAppendedNull()");
    }

    @Override
    public String toString()
    {
        return isRle() ?
                "RLE(" + getGroupId(0) + ")" :
                toStringHelper(this)
                        .add("groupCount", groupCount)
                        .add("positionCount", getPositionCount())
                        .toString();
    }

    @Override
    public boolean isLoaded()
    {
        return true;
    }

    @Override
    public Block getLoadedBlock()
    {
        return this;
    }

    @Override
    public Block getPositions(int[] positions, int offset, int length)
    {
        return copyPositions(positions, offset, length);
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException(format("Invalid position %s in block with %s positions", position, getPositionCount()));
        }
    }

    private static void checkArrayRange(int[] array, int offset, int length)
    {
        requireNonNull(array, "array is null");
        if (offset < 0 || length < 0 || offset + length > array.length) {
            throw new IndexOutOfBoundsException(format("Invalid offset %s and length %s in array with %s elements", offset, length, array.length));
        }
    }
}
