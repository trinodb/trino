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

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.block.LongArrayBlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.connector.ConnectorSession;

import static java.lang.Long.rotateLeft;
import static java.lang.Math.multiplyExact;

/**
 * Encodes timestamps up to p = 6.
 *
 * For 0 <= p <= 3, the value is encoded as milliseconds from the 1970-01-01 00:00:00 epoch.
 * For 3 < p <= 6, the value is encoded as microseconds from the 1970-01-01 00:00:00 epoch.
 */
public class ShortTimestampType
        extends TimestampType
{
    public ShortTimestampType(int precision)
    {
        super(precision, long.class);
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
        long leftValue = leftBlock.getLong(leftPosition, 0);
        long rightValue = rightBlock.getLong(rightPosition, 0);
        return leftValue == rightValue;
    }

    @Override
    public long hash(Block block, int position)
    {
        return hash(block.getLong(position, 0));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = leftBlock.getLong(leftPosition, 0);
        long rightValue = rightBlock.getLong(rightPosition, 0);
        return Long.compare(leftValue, rightValue);
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

        if (getPrecision() <= 3) {
            value = scaleEpochMillisToMicros(value);
        }

        if (session.isLegacyTimestamp()) {
            return SqlTimestamp.newLegacyInstance(getPrecision(), value, 0, session.getTimeZoneKey());
        }
        else {
            return SqlTimestamp.newInstance(getPrecision(), value, 0);
        }
    }

    public static long hash(long value)
    {
        // xxhash64 mix
        return rotateLeft(value * 0xC2B2AE3D27D4EB4FL, 31) * 0x9E3779B185EBCA87L;
    }

    private static long scaleEpochMillisToMicros(long epochMillis)
    {
        return multiplyExact(epochMillis, 1000);
    }
}
