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
package io.prestosql.spi.type;

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.block.LongArrayBlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.connector.ConnectorSession;

import static io.prestosql.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.prestosql.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.prestosql.spi.type.TimeWithTimezoneTypes.hashShortTimeWithTimeZone;
import static io.prestosql.spi.type.TimeWithTimezoneTypes.normalizeNanos;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.String.format;

/**
 * Encodes time with time zone up to p = 9.
 */
class ShortTimeWithTimeZoneType
        extends TimeWithTimeZoneType
{
    public ShortTimeWithTimeZoneType(int precision)
    {
        super(precision, long.class);

        if (precision < 0 || precision > MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException(format("Precision must be in the range [0, %s]", MAX_SHORT_PRECISION));
        }
    }

    @Override
    public final int getFixedSize()
    {
        return Long.BYTES;
    }

    @Override
    public final long getLong(Block block, int position)
    {
        return block.getLong(position, 0);
    }

    @Override
    public final Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, getFixedSize());
    }

    @Override
    public final void writeLong(BlockBuilder blockBuilder, long value)
    {
        blockBuilder.writeLong(value).closeEntry();
    }

    @Override
    public final void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeLong(block.getLong(position, 0)).closeEntry();
        }
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long left = leftBlock.getLong(leftPosition, 0);
        long right = rightBlock.getLong(rightPosition, 0);

        return normalizeNanos(unpackTimeNanos(left), unpackOffsetMinutes(left)) == normalizeNanos(unpackTimeNanos(right), unpackOffsetMinutes(right));
    }

    @Override
    public long hash(Block block, int position)
    {
        long packedTime = block.getLong(position, 0);
        return hashShortTimeWithTimeZone(packedTime);
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long left = leftBlock.getLong(leftPosition, 0);
        long right = rightBlock.getLong(rightPosition, 0);

        return Long.compare(normalizeNanos(unpackTimeNanos(left), unpackOffsetMinutes(left)), normalizeNanos(unpackTimeNanos(right), unpackOffsetMinutes(right)));
    }

    @Override
    public final BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }
        return new LongArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / Long.BYTES));
    }

    @Override
    public final BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, Long.BYTES);
    }

    @Override
    public final BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new LongArrayBlockBuilder(null, positionCount);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        long value = block.getLong(position, 0);
        return SqlTimeWithTimeZone.newInstance(getPrecision(), unpackTimeNanos(value) * PICOSECONDS_PER_NANOSECOND, unpackOffsetMinutes(value));
    }
}
