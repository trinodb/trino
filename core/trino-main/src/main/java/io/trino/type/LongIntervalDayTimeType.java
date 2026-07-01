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
package io.trino.type;

import io.airlift.slice.XxHash64;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.Fixed12Block;
import io.trino.spi.block.Fixed12BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.type.IntervalField;
import io.trino.spi.type.TypeOperatorDeclaration;
import io.trino.spi.type.TypeOperators;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.invoke.MethodHandles.lookup;

/// The picosecond-backed form of a day-time interval, used when the fractional-seconds precision is 7
/// to 12. Stores a [LongInterval] — microseconds plus the picoseconds within that microsecond — in a
/// 96-bit [Fixed12Block].
final class LongIntervalDayTimeType
        extends IntervalDayTimeType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(LongIntervalDayTimeType.class, lookup(), LongInterval.class);
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    LongIntervalDayTimeType(IntervalField startField, IntervalField endField, int leadingPrecision, int fractionalPrecision)
    {
        super(startField, endField, leadingPrecision, fractionalPrecision, LongInterval.class, Fixed12Block.class);
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
    public int getFlatFixedSize()
    {
        return Long.BYTES + Integer.BYTES;
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
        return new Fixed12BlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / getFixedSize()));
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new Fixed12BlockBuilder(null, positionCount);
    }

    @Override
    public Object getObject(Block block, int position)
    {
        Fixed12Block valueBlock = (Fixed12Block) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return new LongInterval(getMicros(valueBlock, valuePosition), getFraction(valueBlock, valuePosition));
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        LongInterval interval = (LongInterval) value;
        write(blockBuilder, interval.getMicros(), interval.getPicosOfMicro());
    }

    private static void write(BlockBuilder blockBuilder, long micros, int fraction)
    {
        ((Fixed12BlockBuilder) blockBuilder).writeFixed12(micros, fraction);
    }

    @Override
    public Object getObjectValue(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        Fixed12Block valueBlock = (Fixed12Block) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return new SqlIntervalDayTime(getMicros(valueBlock, valuePosition), getFraction(valueBlock, valuePosition), getFractionalPrecision());
    }

    private static long getMicros(Fixed12Block block, int position)
    {
        return block.getFixed12First(position);
    }

    private static int getFraction(Fixed12Block block, int position)
    {
        return block.getFixed12Second(position);
    }

    @ScalarOperator(READ_VALUE)
    private static LongInterval readFlat(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            @FlatVariableOffset int unusedVariableSizeOffset)
    {
        return new LongInterval(
                (long) LONG_HANDLE.get(fixedSizeSlice, fixedSizeOffset),
                (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset + Long.BYTES));
    }

    @ScalarOperator(READ_VALUE)
    private static void readFlatToBlock(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            @FlatVariableOffset int unusedVariableSizeOffset,
            BlockBuilder blockBuilder)
    {
        write(blockBuilder,
                (long) LONG_HANDLE.get(fixedSizeSlice, fixedSizeOffset),
                (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset + SIZE_OF_LONG));
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlat(
            LongInterval value,
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            @FlatVariableOffset int unusedVariableSizeOffset)
    {
        LONG_HANDLE.set(fixedSizeSlice, fixedSizeOffset, value.getMicros());
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset + SIZE_OF_LONG, value.getPicosOfMicro());
    }

    @ScalarOperator(READ_VALUE)
    private static void writeBlockFlat(
            @BlockPosition Fixed12Block block,
            @BlockIndex int position,
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            @FlatVariableOffset int unusedVariableSizeOffset)
    {
        LONG_HANDLE.set(fixedSizeSlice, fixedSizeOffset, getMicros(block, position));
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset + SIZE_OF_LONG, getFraction(block, position));
    }

    @ScalarOperator(value = EQUAL)
    private static boolean equalOperator(LongInterval left, LongInterval right)
    {
        return equal(left.getMicros(), left.getPicosOfMicro(), right.getMicros(), right.getPicosOfMicro());
    }

    @ScalarOperator(value = EQUAL)
    private static boolean equalOperator(@BlockPosition Fixed12Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Fixed12Block rightBlock, @BlockIndex int rightPosition)
    {
        return equal(
                getMicros(leftBlock, leftPosition),
                getFraction(leftBlock, leftPosition),
                getMicros(rightBlock, rightPosition),
                getFraction(rightBlock, rightPosition));
    }

    @ScalarOperator(value = EQUAL)
    private static boolean equalOperator(
            @FlatFixed byte[] leftFixedSizeSlice,
            @FlatFixedOffset int leftFixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            @FlatVariableOffset int unusedVariableSizeOffset,
            @BlockPosition Fixed12Block rightBlock,
            @BlockIndex int rightPosition)
    {
        return equal(
                (long) LONG_HANDLE.get(leftFixedSizeSlice, leftFixedSizeOffset),
                (int) INT_HANDLE.get(leftFixedSizeSlice, leftFixedSizeOffset + SIZE_OF_LONG),
                getMicros(rightBlock, rightPosition),
                getFraction(rightBlock, rightPosition));
    }

    private static boolean equal(long leftMicros, int leftFraction, long rightMicros, int rightFraction)
    {
        return leftMicros == rightMicros && leftFraction == rightFraction;
    }

    @ScalarOperator(value = XX_HASH_64)
    private static long xxHash64Operator(LongInterval value)
    {
        return xxHash64(value.getMicros(), value.getPicosOfMicro());
    }

    @ScalarOperator(value = XX_HASH_64)
    private static long xxHash64Operator(@BlockPosition Fixed12Block block, @BlockIndex int position)
    {
        return xxHash64(getMicros(block, position), getFraction(block, position));
    }

    @ScalarOperator(value = XX_HASH_64)
    private static long xxHash64Operator(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            @FlatVariableOffset int unusedVariableSizeOffset)
    {
        return xxHash64(
                (long) LONG_HANDLE.get(fixedSizeSlice, fixedSizeOffset),
                (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset + SIZE_OF_LONG));
    }

    private static long xxHash64(long micros, int fraction)
    {
        return XxHash64.hash(micros) ^ XxHash64.hash(fraction);
    }

    @ScalarOperator(value = COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(LongInterval left, LongInterval right)
    {
        return comparison(left.getMicros(), left.getPicosOfMicro(), right.getMicros(), right.getPicosOfMicro());
    }

    @ScalarOperator(value = COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(@BlockPosition Fixed12Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Fixed12Block rightBlock, @BlockIndex int rightPosition)
    {
        return comparison(
                getMicros(leftBlock, leftPosition),
                getFraction(leftBlock, leftPosition),
                getMicros(rightBlock, rightPosition),
                getFraction(rightBlock, rightPosition));
    }

    private static int comparison(long leftMicros, int leftPicosOfMicro, long rightMicros, int rightPicosOfMicro)
    {
        int value = Long.compare(leftMicros, rightMicros);
        if (value != 0) {
            return value;
        }
        return Integer.compare(leftPicosOfMicro, rightPicosOfMicro);
    }

    @ScalarOperator(value = LESS_THAN)
    private static boolean lessThanOperator(LongInterval left, LongInterval right)
    {
        return lessThan(left.getMicros(), left.getPicosOfMicro(), right.getMicros(), right.getPicosOfMicro());
    }

    @ScalarOperator(value = LESS_THAN)
    private static boolean lessThanOperator(@BlockPosition Fixed12Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Fixed12Block rightBlock, @BlockIndex int rightPosition)
    {
        return lessThan(
                getMicros(leftBlock, leftPosition),
                getFraction(leftBlock, leftPosition),
                getMicros(rightBlock, rightPosition),
                getFraction(rightBlock, rightPosition));
    }

    private static boolean lessThan(long leftMicros, int leftPicosOfMicro, long rightMicros, int rightPicosOfMicro)
    {
        return (leftMicros < rightMicros) ||
                ((leftMicros == rightMicros) && (leftPicosOfMicro < rightPicosOfMicro));
    }

    @ScalarOperator(value = LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(LongInterval left, LongInterval right)
    {
        return lessThanOrEqual(left.getMicros(), left.getPicosOfMicro(), right.getMicros(), right.getPicosOfMicro());
    }

    @ScalarOperator(value = LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(@BlockPosition Fixed12Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Fixed12Block rightBlock, @BlockIndex int rightPosition)
    {
        return lessThanOrEqual(
                getMicros(leftBlock, leftPosition),
                getFraction(leftBlock, leftPosition),
                getMicros(rightBlock, rightPosition),
                getFraction(rightBlock, rightPosition));
    }

    private static boolean lessThanOrEqual(long leftMicros, int leftPicosOfMicro, long rightMicros, int rightPicosOfMicro)
    {
        return (leftMicros < rightMicros) ||
                ((leftMicros == rightMicros) && (leftPicosOfMicro <= rightPicosOfMicro));
    }
}
