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

import io.airlift.slice.XxHash64;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.ScalarOperator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.TimeWithTimeZoneTypes.normalizePackedTime;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

/**
 * Encodes time with time zone up to p = 9.
 */
final class ShortTimeWithTimeZoneType
        extends TimeWithTimeZoneType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(ShortTimeWithTimeZoneType.class, lookup(), long.class);
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    public ShortTimeWithTimeZoneType(int precision)
    {
        super(precision, long.class, LongArrayBlock.class);

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
    public int getFixedSize()
    {
        return Long.BYTES;
    }

    @Override
    public long getLong(Block block, int position)
    {
        return read((LongArrayBlock) block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        ((LongArrayBlockBuilder) blockBuilder).writeLong(value);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            writeLong(blockBuilder, getLong(block, position));
        }
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
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
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new LongArrayBlockBuilder(null, positionCount);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        long value = getLong(block, position);
        return SqlTimeWithTimeZone.newInstance(getPrecision(), unpackTimeNanos(value) * PICOSECONDS_PER_NANOSECOND, unpackOffsetMinutes(value));
    }

    @Override
    public int getFlatFixedSize()
    {
        return Long.BYTES;
    }

    @ScalarOperator(READ_VALUE)
    private static long read(@BlockPosition LongArrayBlock block, @BlockIndex int position)
    {
        return block.getLong(position);
    }

    @ScalarOperator(READ_VALUE)
    private static long readFlat(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            @FlatVariableOffset int unusedVariableSizeOffset)
    {
        return (long) LONG_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlat(
            long value,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] unusedVariableSizeSlice,
            int unusedVariableSizeOffset)
    {
        LONG_HANDLE.set(fixedSizeSlice, fixedSizeOffset, value);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(long leftPackedTime, long rightPackedTime)
    {
        return normalizePackedTime(leftPackedTime) == normalizePackedTime(rightPackedTime);
    }

    @ScalarOperator(HASH_CODE)
    private static long hashCodeOperator(long packedTime)
    {
        return AbstractLongType.hash(normalizePackedTime(packedTime));
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(long packedTime)
    {
        return XxHash64.hash(normalizePackedTime(packedTime));
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(long left, long right)
    {
        return Long.compare(normalizePackedTime(left), normalizePackedTime(right));
    }

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(long left, long right)
    {
        return normalizePackedTime(left) < normalizePackedTime(right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(long left, long right)
    {
        return normalizePackedTime(left) <= normalizePackedTime(right);
    }
}
