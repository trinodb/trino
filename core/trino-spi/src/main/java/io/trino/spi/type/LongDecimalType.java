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
import io.trino.spi.HashUtils;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.Int128ArrayBlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.ScalarOperator;

import java.math.BigInteger;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.invoke.MethodHandles.lookup;

final class LongDecimalType
        extends DecimalType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(LongDecimalType.class, lookup(), Int128.class);

    LongDecimalType(int precision, int scale)
    {
        super(precision, scale, Int128.class);
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
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
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
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, getFixedSize());
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
        Int128 value = (Int128) getObject(block, position);
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
            blockBuilder.writeLong(block.getLong(position, 0));
            blockBuilder.writeLong(block.getLong(position, SIZE_OF_LONG));
            blockBuilder.closeEntry();
        }
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        Int128 decimal = (Int128) value;
        blockBuilder.writeLong(decimal.getHigh());
        blockBuilder.writeLong(decimal.getLow());
        blockBuilder.closeEntry();
    }

    @Override
    public Object getObject(Block block, int position)
    {
        return Int128.valueOf(
                block.getLong(position, 0),
                block.getLong(position, SIZE_OF_LONG));
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(Int128 left, Int128 right)
    {
        return left.equals(right);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        return leftBlock.getLong(leftPosition, 0) == rightBlock.getLong(rightPosition, 0) &&
                leftBlock.getLong(leftPosition, SIZE_OF_LONG) == rightBlock.getLong(rightPosition, SIZE_OF_LONG);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(Int128 value)
    {
        return xxHash64(value.getHigh(), value.getLow());
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(@BlockPosition Block block, @BlockIndex int position)
    {
        return xxHash64(block.getLong(position, 0), block.getLong(position, SIZE_OF_LONG));
    }

    private static long xxHash64(long low, long high)
    {
        return HashUtils.mix(XxHash64.hash(low), XxHash64.hash(high));
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(Int128 left, Int128 right)
    {
        return left.compareTo(right);
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        return Int128.compare(
                leftBlock.getLong(leftPosition, 0),
                leftBlock.getLong(leftPosition, SIZE_OF_LONG),
                rightBlock.getLong(rightPosition, 0),
                rightBlock.getLong(rightPosition, SIZE_OF_LONG));
    }
}
