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
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.Int128ArrayBlockBuilder;
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
import java.math.BigInteger;
import java.nio.ByteOrder;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.block.Int128ArrayBlock.INT128_BYTES;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.invoke.MethodHandles.lookup;

final class LongDecimalType
        extends DecimalType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(LongDecimalType.class, lookup(), Int128.class);
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    LongDecimalType(int precision, int scale)
    {
        super(precision, scale, Int128.class, Int128ArrayBlock.class);
        checkArgument(Decimals.MAX_SHORT_PRECISION < precision && precision <= Decimals.MAX_PRECISION, "Invalid precision: %s", precision);
        checkArgument(0 <= scale && scale <= precision, "Invalid scale for precision %s: %s", precision, scale);
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @Override
    public int getFixedSize()
    {
        return Int128.SIZE;
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
        return new Int128ArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / getFixedSize()));
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new Int128ArrayBlockBuilder(null, positionCount);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        Int128 value = getObject(block, position);
        BigInteger unscaledValue = value.toBigInteger();
        return new SqlDecimal(unscaledValue, getPrecision(), getScale());
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            Int128ArrayBlock valueBlock = (Int128ArrayBlock) block.getUnderlyingValueBlock();
            int valuePosition = block.getUnderlyingValuePosition(position);
            ((Int128ArrayBlockBuilder) blockBuilder).writeInt128(valueBlock.getInt128High(valuePosition), valueBlock.getInt128Low(valuePosition));
        }
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        Int128 decimal = (Int128) value;
        ((Int128ArrayBlockBuilder) blockBuilder).writeInt128(decimal.getHigh(), decimal.getLow());
    }

    @Override
    public Int128 getObject(Block block, int position)
    {
        return read((Int128ArrayBlock) block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
    }

    @Override
    public int getFlatFixedSize()
    {
        return INT128_BYTES;
    }

    @ScalarOperator(READ_VALUE)
    private static Int128 read(@BlockPosition Int128ArrayBlock block, @BlockIndex int position)
    {
        return block.getInt128(position);
    }

    @ScalarOperator(READ_VALUE)
    private static Int128 readFlat(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            @FlatVariableOffset int unusedVariableSizeOffset)
    {
        return Int128.valueOf(
                (long) LONG_HANDLE.get(fixedSizeSlice, fixedSizeOffset),
                (long) LONG_HANDLE.get(fixedSizeSlice, fixedSizeOffset + SIZE_OF_LONG));
    }

    @ScalarOperator(READ_VALUE)
    private static void readFlatToBlock(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            @FlatVariableOffset int unusedVariableSizeOffset,
            BlockBuilder blockBuilder)
    {
        ((Int128ArrayBlockBuilder) blockBuilder).writeInt128(
                (long) LONG_HANDLE.get(fixedSizeSlice, fixedSizeOffset),
                (long) LONG_HANDLE.get(fixedSizeSlice, fixedSizeOffset + SIZE_OF_LONG));
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlat(
            Int128 value,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] unusedVariableSizeSlice,
            int unusedVariableSizeOffset)
    {
        LONG_HANDLE.set(fixedSizeSlice, fixedSizeOffset, value.getHigh());
        LONG_HANDLE.set(fixedSizeSlice, fixedSizeOffset + SIZE_OF_LONG, value.getLow());
    }

    @ScalarOperator(READ_VALUE)
    private static void writeBlockToFlat(
            @BlockPosition Int128ArrayBlock block,
            @BlockIndex int position,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] unusedVariableSizeSlice,
            int unusedVariableSizeOffset)
    {
        LONG_HANDLE.set(fixedSizeSlice, fixedSizeOffset, block.getInt128High(position));
        LONG_HANDLE.set(fixedSizeSlice, fixedSizeOffset + SIZE_OF_LONG, block.getInt128Low(position));
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(Int128 left, Int128 right)
    {
        return left.equals(right);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(@BlockPosition Int128ArrayBlock leftBlock, @BlockIndex int leftPosition, @BlockPosition Int128ArrayBlock rightBlock, @BlockIndex int rightPosition)
    {
        return leftBlock.getInt128High(leftPosition) == rightBlock.getInt128High(rightPosition) &&
                leftBlock.getInt128Low(leftPosition) == rightBlock.getInt128Low(rightPosition);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(Int128 value)
    {
        return xxHash64(value.getHigh(), value.getLow());
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(@BlockPosition Int128ArrayBlock block, @BlockIndex int position)
    {
        return xxHash64(block.getInt128High(position), block.getInt128Low(position));
    }

    private static long xxHash64(long high, long low)
    {
        return XxHash64.hash(high) ^ XxHash64.hash(low);
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(Int128 left, Int128 right)
    {
        return left.compareTo(right);
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(@BlockPosition Int128ArrayBlock leftBlock, @BlockIndex int leftPosition, @BlockPosition Int128ArrayBlock rightBlock, @BlockIndex int rightPosition)
    {
        return Int128.compare(
                leftBlock.getInt128High(leftPosition),
                leftBlock.getInt128Low(leftPosition),
                rightBlock.getInt128High(rightPosition),
                rightBlock.getInt128Low(rightPosition));
    }
}
