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
import io.prestosql.spi.block.Int96ArrayBlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.connector.ConnectorSession;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.String.format;

/**
 * The representation is a 96-bit value that contains the microseconds from the epoch
 * in the first long and the fractional increment in the remaining integer, as
 * a number of picoseconds additional to the epoch microsecond.
 */
class LongTimestampType
        extends TimestampType
{
    public LongTimestampType(int precision)
    {
        super(precision, LongTimestamp.class);

        if (precision < MAX_SHORT_PRECISION + 1 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(format("Precision must be in the range [%s, %s]", MAX_SHORT_PRECISION + 1, MAX_PRECISION));
        }
    }

    @Override
    public int getFixedSize()
    {
        return Long.BYTES + Integer.BYTES;
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }
        return new Int96ArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / getFixedSize()));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, getFixedSize());
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new Int96ArrayBlockBuilder(null, positionCount);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return compareTo(leftBlock, leftPosition, rightBlock, rightPosition) == 0;
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftEpochMicros = getEpochMicros(leftBlock, leftPosition);
        int leftFraction = getFraction(leftBlock, leftPosition);
        long rightEpochMicros = getEpochMicros(rightBlock, rightPosition);
        int rightFraction = getFraction(rightBlock, rightPosition);

        int value = Long.compare(leftEpochMicros, rightEpochMicros);
        if (value != 0) {
            return value;
        }
        return Integer.compare(leftFraction, rightFraction);
    }

    @Override
    public long hash(Block block, int position)
    {
        return TimestampTypes.hashLongTimestamp(getEpochMicros(block, position), getFraction(block, position));
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeLong(getEpochMicros(block, position));
            blockBuilder.writeInt(getFraction(block, position));
            blockBuilder.closeEntry();
        }
    }

    @Override
    public Object getObject(Block block, int position)
    {
        return new LongTimestamp(getEpochMicros(block, position), getFraction(block, position));
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        LongTimestamp timestamp = (LongTimestamp) value;
        write(blockBuilder, timestamp.getEpochMicros(), timestamp.getPicosOfMicro());
    }

    public void write(BlockBuilder blockBuilder, long epochMicros, int fraction)
    {
        blockBuilder.writeLong(epochMicros);
        blockBuilder.writeInt(fraction);
        blockBuilder.closeEntry();
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        long epochMicros = getEpochMicros(block, position);
        int fraction = getFraction(block, position);

        return SqlTimestamp.newInstance(getPrecision(), epochMicros, fraction);
    }

    private static long getEpochMicros(Block block, int position)
    {
        return block.getLong(position, 0);
    }

    private static int getFraction(Block block, int position)
    {
        return block.getInt(position, SIZE_OF_LONG);
    }
}
