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
import io.trino.spi.block.Fixed12BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.ScalarOperator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Optional;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.rescale;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

/**
 * The representation is a 96-bit value that contains the milliseconds from the epoch + session key
 * in the first long and the fractional increment in the remaining integer, as a number of picoseconds
 * additional to the epoch millisecond.
 */
final class LongTimestampWithTimeZoneType
        extends TimestampWithTimeZoneType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(LongTimestampWithTimeZoneType.class, lookup(), LongTimestampWithTimeZone.class);
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    public LongTimestampWithTimeZoneType(int precision)
    {
        super(precision, LongTimestampWithTimeZone.class);

        if (precision < MAX_SHORT_PRECISION + 1 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(format("Precision must be in the range [%s, %s]", MAX_SHORT_PRECISION + 1, MAX_PRECISION));
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
        return new Fixed12BlockBuilder(
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
        return new Fixed12BlockBuilder(null, positionCount);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            write(blockBuilder, getPackedEpochMillis(block, position), getPicosOfMilli(block, position));
        }
    }

    @Override
    public Object getObject(Block block, int position)
    {
        long packedEpochMillis = getPackedEpochMillis(block, position);
        int picosOfMilli = getPicosOfMilli(block, position);

        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(unpackMillisUtc(packedEpochMillis), picosOfMilli, unpackZoneKey(packedEpochMillis));
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) value;

        write(blockBuilder, packDateTimeWithZone(timestamp.getEpochMillis(), timestamp.getTimeZoneKey()), timestamp.getPicosOfMilli());
    }

    private static void write(BlockBuilder blockBuilder, long packedDateTimeWithZone, int picosOfMilli)
    {
        ((Fixed12BlockBuilder) blockBuilder).writeFixed12(
                packedDateTimeWithZone,
                picosOfMilli);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        long packedEpochMillis = getPackedEpochMillis(block, position);
        int picosOfMilli = getPicosOfMilli(block, position);

        return SqlTimestampWithTimeZone.newInstance(getPrecision(), unpackMillisUtc(packedEpochMillis), picosOfMilli, unpackZoneKey(packedEpochMillis));
    }

    @Override
    public int getFlatFixedSize()
    {
        return Long.BYTES + Integer.BYTES;
    }

    @Override
    public Optional<Object> getPreviousValue(Object value)
    {
        LongTimestampWithTimeZone timestampWithTimeZone = (LongTimestampWithTimeZone) value;
        long epochMillis = timestampWithTimeZone.getEpochMillis();
        int picosOfMilli = timestampWithTimeZone.getPicosOfMilli();
        picosOfMilli -= toIntExact(rescale(1, 0, 12 - getPrecision()));
        if (picosOfMilli < 0) {
            if (epochMillis == Long.MIN_VALUE) {
                return Optional.empty();
            }
            epochMillis--;
            picosOfMilli += PICOSECONDS_PER_MILLISECOND;
        }
        // time zone doesn't matter for ordering
        return Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, UTC_KEY));
    }

    @Override
    public Optional<Object> getNextValue(Object value)
    {
        LongTimestampWithTimeZone timestampWithTimeZone = (LongTimestampWithTimeZone) value;
        long epochMillis = timestampWithTimeZone.getEpochMillis();
        int picosOfMilli = timestampWithTimeZone.getPicosOfMilli();
        picosOfMilli += toIntExact(rescale(1, 0, 12 - getPrecision()));
        if (picosOfMilli >= PICOSECONDS_PER_MILLISECOND) {
            if (epochMillis == Long.MAX_VALUE) {
                return Optional.empty();
            }
            epochMillis++;
            picosOfMilli -= PICOSECONDS_PER_MILLISECOND;
        }
        // time zone doesn't matter for ordering
        return Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, UTC_KEY));
    }

    private static long getPackedEpochMillis(Block block, int position)
    {
        return block.getLong(position, 0);
    }

    private static long getEpochMillis(Block block, int position)
    {
        return unpackMillisUtc(getPackedEpochMillis(block, position));
    }

    private static int getPicosOfMilli(Block block, int position)
    {
        return block.getInt(position, SIZE_OF_LONG);
    }

    @ScalarOperator(READ_VALUE)
    private static LongTimestampWithTimeZone readFlat(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice)
    {
        long packedEpochMillis = (long) LONG_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
        int picosOfMilli = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset + Long.BYTES);
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(unpackMillisUtc(packedEpochMillis), picosOfMilli, unpackZoneKey(packedEpochMillis));
    }

    @ScalarOperator(READ_VALUE)
    private static void readFlatToBlock(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            BlockBuilder blockBuilder)
    {
        write(blockBuilder,
                (long) LONG_HANDLE.get(fixedSizeSlice, fixedSizeOffset),
                (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset + Long.BYTES));
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlat(
            LongTimestampWithTimeZone value,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] unusedVariableSizeSlice,
            int unusedVariableSizeOffset)
    {
        LONG_HANDLE.set(fixedSizeSlice, fixedSizeOffset, packDateTimeWithZone(value.getEpochMillis(), value.getTimeZoneKey()));
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset + SIZE_OF_LONG, value.getPicosOfMilli());
    }

    @ScalarOperator(READ_VALUE)
    private static void writeBlockFlat(
            @BlockPosition Block block,
            @BlockIndex int position,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] unusedVariableSizeSlice,
            int unusedVariableSizeOffset)
    {
        LONG_HANDLE.set(fixedSizeSlice, fixedSizeOffset, getPackedEpochMillis(block, position));
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset + SIZE_OF_LONG, getPicosOfMilli(block, position));
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(LongTimestampWithTimeZone left, LongTimestampWithTimeZone right)
    {
        return equal(
                left.getEpochMillis(),
                left.getPicosOfMilli(),
                right.getEpochMillis(),
                right.getPicosOfMilli());
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        return equal(
                getEpochMillis(leftBlock, leftPosition),
                getPicosOfMilli(leftBlock, leftPosition),
                getEpochMillis(rightBlock, rightPosition),
                getPicosOfMilli(rightBlock, rightPosition));
    }

    private static boolean equal(long leftEpochMillis, int leftPicosOfMilli, long rightEpochMillis, int rightPicosOfMilli)
    {
        return leftEpochMillis == rightEpochMillis &&
                leftPicosOfMilli == rightPicosOfMilli;
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(LongTimestampWithTimeZone value)
    {
        return xxHash64(value.getEpochMillis(), value.getPicosOfMilli());
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(@BlockPosition Block block, @BlockIndex int position)
    {
        return xxHash64(
                getEpochMillis(block, position),
                getPicosOfMilli(block, position));
    }

    private static long xxHash64(long epochMillis, int picosOfMilli)
    {
        return XxHash64.hash(epochMillis) ^ XxHash64.hash(picosOfMilli);
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(LongTimestampWithTimeZone left, LongTimestampWithTimeZone right)
    {
        return comparison(left.getEpochMillis(), left.getPicosOfMilli(), right.getEpochMillis(), right.getPicosOfMilli());
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        return comparison(
                getEpochMillis(leftBlock, leftPosition),
                getPicosOfMilli(leftBlock, leftPosition),
                getEpochMillis(rightBlock, rightPosition),
                getPicosOfMilli(rightBlock, rightPosition));
    }

    private static int comparison(long leftEpochMillis, int leftPicosOfMilli, long rightEpochMillis, int rightPicosOfMilli)
    {
        int value = Long.compare(leftEpochMillis, rightEpochMillis);
        if (value != 0) {
            return value;
        }
        return Integer.compare(leftPicosOfMilli, rightPicosOfMilli);
    }

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(LongTimestampWithTimeZone left, LongTimestampWithTimeZone right)
    {
        return lessThan(left.getEpochMillis(), left.getPicosOfMilli(), right.getEpochMillis(), right.getPicosOfMilli());
    }

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        return lessThan(
                getEpochMillis(leftBlock, leftPosition),
                getPicosOfMilli(leftBlock, leftPosition),
                getEpochMillis(rightBlock, rightPosition),
                getPicosOfMilli(rightBlock, rightPosition));
    }

    private static boolean lessThan(long leftEpochMillis, int leftPicosOfMilli, long rightEpochMillis, int rightPicosOfMilli)
    {
        return (leftEpochMillis < rightEpochMillis) ||
                ((leftEpochMillis == rightEpochMillis) && (leftPicosOfMilli < rightPicosOfMilli));
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(LongTimestampWithTimeZone left, LongTimestampWithTimeZone right)
    {
        return lessThanOrEqual(left.getEpochMillis(), left.getPicosOfMilli(), right.getEpochMillis(), right.getPicosOfMilli());
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        return lessThanOrEqual(
                getEpochMillis(leftBlock, leftPosition),
                getPicosOfMilli(leftBlock, leftPosition),
                getEpochMillis(rightBlock, rightPosition),
                getPicosOfMilli(rightBlock, rightPosition));
    }

    private static boolean lessThanOrEqual(long leftEpochMillis, int leftPicosOfMilli, long rightEpochMillis, int rightPicosOfMilli)
    {
        return (leftEpochMillis < rightEpochMillis) ||
                ((leftEpochMillis == rightEpochMillis) && (leftPicosOfMilli <= rightPicosOfMilli));
    }
}
