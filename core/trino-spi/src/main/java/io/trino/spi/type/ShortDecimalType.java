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
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.invoke.MethodHandles.lookup;

final class ShortDecimalType
        extends DecimalType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(ShortDecimalType.class, lookup(), long.class);
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private static final ShortDecimalType[][] INSTANCES;

    static {
        INSTANCES = new ShortDecimalType[MAX_SHORT_PRECISION][MAX_SHORT_PRECISION + 1];
        for (int precision = 1; precision <= MAX_SHORT_PRECISION; precision++) {
            for (int scale = 0; scale <= precision; scale++) {
                INSTANCES[precision - 1][scale] = new ShortDecimalType(precision, scale);
            }
        }
    }

    static ShortDecimalType getInstance(int precision, int scale)
    {
        return INSTANCES[precision - 1][scale];
    }

    private final Range range;

    private ShortDecimalType(int precision, int scale)
    {
        super(precision, scale, long.class, LongArrayBlock.class);
        checkArgument(0 < precision && precision <= MAX_SHORT_PRECISION, "Invalid precision: %s", precision);
        checkArgument(0 <= scale && scale <= precision, "Invalid scale for precision %s: %s", precision, scale);

        // ShortDecimalType instances are created eagerly and shared, so it's OK to precompute some things.
        long max = longTenToNth(precision) - 1;
        range = new Range(-max, max);
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @Override
    public int getFixedSize()
    {
        return SIZE_OF_LONG;
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
                Math.min(expectedEntries, maxBlockSizeInBytes / getFixedSize()));
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
        return new SqlDecimal(BigInteger.valueOf(getLong(block, position)), getPrecision(), getScale());
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
    public int getFlatFixedSize()
    {
        return Long.BYTES;
    }

    @Override
    public Optional<Range> getRange()
    {
        return Optional.of(range);
    }

    @Override
    public Optional<Stream<?>> getDiscreteValues(Range range)
    {
        return Optional.of(LongStream.rangeClosed((long) range.getMin(), (long) range.getMax()).boxed());
    }

    @Override
    public Optional<Object> getPreviousValue(Object object)
    {
        long value = (long) object;
        if ((long) range.getMin() == value) {
            return Optional.empty();
        }
        return Optional.of(value - 1);
    }

    @Override
    public Optional<Object> getNextValue(Object object)
    {
        long value = (long) object;
        if ((long) range.getMax() == value) {
            return Optional.empty();
        }
        return Optional.of(value + 1);
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
    private static boolean equalOperator(long left, long right)
    {
        return left == right;
    }

    @ScalarOperator(HASH_CODE)
    private static long hashCodeOperator(long value)
    {
        return AbstractLongType.hash(value);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(long value)
    {
        return XxHash64.hash(value);
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(long left, long right)
    {
        return Long.compare(left, right);
    }

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(long left, long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(long left, long right)
    {
        return left <= right;
    }
}
