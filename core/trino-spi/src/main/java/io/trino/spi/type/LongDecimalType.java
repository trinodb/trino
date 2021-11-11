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
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
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
import static io.trino.spi.block.Int128ArrayBlock.INT128_BYTES;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.Decimals.decodeUnscaledValue;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.compare;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.isNegative;
import static java.lang.invoke.MethodHandles.lookup;

final class LongDecimalType
        extends DecimalType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(LongDecimalType.class, lookup(), Slice.class);

    LongDecimalType(int precision, int scale)
    {
        super(precision, scale, Slice.class);
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
        return UNSCALED_DECIMAL_128_SLICE_LENGTH;
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
        Slice slice = getSlice(block, position);
        BigInteger unscaledValue = decodeUnscaledValue(slice);
        // sanity check: disallow "negative zero" in case we get a corrupted result from somewhere
        checkArgument(unscaledValue.signum() != 0 || !isNegative(slice), "The encoded long decimal is invalid: negative zero");
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
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        if (length != INT128_BYTES) {
            throw new IllegalStateException("Expected entry size to be exactly " + INT128_BYTES + " but was " + length);
        }
        blockBuilder.writeLong(value.getLong(offset));
        blockBuilder.writeLong(value.getLong(offset + SIZE_OF_LONG));
        blockBuilder.closeEntry();
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return Slices.wrappedLongArray(
                block.getLong(position, 0),
                block.getLong(position, SIZE_OF_LONG));
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(Slice left, Slice right)
    {
        return equal(
                left.getLong(0),
                left.getLong(SIZE_OF_LONG),
                right.getLong(0),
                right.getLong(SIZE_OF_LONG));
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        return equal(
                leftBlock.getLong(leftPosition, 0),
                leftBlock.getLong(leftPosition, SIZE_OF_LONG),
                rightBlock.getLong(rightPosition, 0),
                rightBlock.getLong(rightPosition, SIZE_OF_LONG));
    }

    private static boolean equal(long leftLow, long leftHigh, long rightLow, long rightHigh)
    {
        return leftLow == rightLow && leftHigh == rightHigh;
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(Slice value)
    {
        return xxHash64(value.getLong(0), value.getLong(SIZE_OF_LONG));
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(@BlockPosition Block block, @BlockIndex int position)
    {
        return xxHash64(block.getLong(position, 0), block.getLong(position, SIZE_OF_LONG));
    }

    private static long xxHash64(long low, long high)
    {
        return XxHash64.hash(low) ^ XxHash64.hash(high);
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(Slice left, Slice right)
    {
        return compare(left, right);
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        long leftLow = leftBlock.getLong(leftPosition, 0);
        long leftHigh = leftBlock.getLong(leftPosition, SIZE_OF_LONG);
        long rightLow = rightBlock.getLong(rightPosition, 0);
        long rightHigh = rightBlock.getLong(rightPosition, SIZE_OF_LONG);
        return compare(leftLow, leftHigh, rightLow, rightHigh);
    }
}
