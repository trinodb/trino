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
package io.trino.spi.type;

import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.ScalarOperator;

import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

/**
 * Encodes timestamps up to p = 3.
 * <p>
 * The value is encoded as milliseconds from the 1970-01-01 00:00:00 epoch.
 */
class ShortTimestampWithTimeZoneType
        extends TimestampWithTimeZoneType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(ShortTimestampWithTimeZoneType.class, lookup(), long.class);

    public ShortTimestampWithTimeZoneType(int precision)
    {
        super(precision, long.class);

        if (precision < 0 || precision > MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException(format("Precision must be in the range [0, %s]", MAX_SHORT_PRECISION));
        }
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
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
        return SqlTimestampWithTimeZone.newInstance(getPrecision(), unpackMillisUtc(value), 0, unpackZoneKey(value));
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(long left, long right)
    {
        long leftValue = unpackMillisUtc(left);
        long rightValue = unpackMillisUtc(right);
        return leftValue == rightValue;
    }

    @ScalarOperator(HASH_CODE)
    private static long hashCodeOperator(long value)
    {
        return AbstractLongType.hash(unpackMillisUtc(value));
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(long value)
    {
        return XxHash64.hash(unpackMillisUtc(value));
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(long left, long right)
    {
        return Long.compare(unpackMillisUtc(left), unpackMillisUtc(right));
    }

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(long left, long right)
    {
        return unpackMillisUtc(left) < unpackMillisUtc(right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(long left, long right)
    {
        return unpackMillisUtc(left) <= unpackMillisUtc(right);
    }
}
