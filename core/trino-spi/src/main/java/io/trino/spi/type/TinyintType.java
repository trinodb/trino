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
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.ByteArrayBlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.ScalarOperator;

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

public final class TinyintType
        extends AbstractType
        implements FixedWidthType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(TinyintType.class, lookup(), long.class);

    public static final TinyintType TINYINT = new TinyintType();

    private TinyintType()
    {
        super(new TypeSignature(StandardTypes.TINYINT), long.class, ByteArrayBlock.class);
    }

    @Override
    public int getFixedSize()
    {
        return Byte.BYTES;
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
        return new ByteArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / Byte.BYTES));
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new ByteArrayBlockBuilder(null, positionCount);
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return getByte(block, position);
    }

    @Override
    public Optional<Range> getRange()
    {
        return Optional.of(new Range((long) Byte.MIN_VALUE, (long) Byte.MAX_VALUE));
    }

    @Override
    public Optional<Object> getPreviousValue(Object object)
    {
        long value = (long) object;
        checkValueValid(value);
        if (value == Byte.MIN_VALUE) {
            return Optional.empty();
        }
        return Optional.of(value - 1);
    }

    @Override
    public Optional<Object> getNextValue(Object object)
    {
        long value = (long) object;
        checkValueValid(value);
        if (value == Byte.MAX_VALUE) {
            return Optional.empty();
        }
        return Optional.of(value + 1);
    }

    @Override
    public Optional<Stream<?>> getDiscreteValues(Range range)
    {
        return Optional.of(LongStream.rangeClosed((long) range.getMin(), (long) range.getMax()).boxed());
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            writeByte(blockBuilder, getByte(block, position));
        }
    }

    @Override
    public long getLong(Block block, int position)
    {
        return getByte(block, position);
    }

    public byte getByte(Block block, int position)
    {
        return readByte((ByteArrayBlock) block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        checkValueValid(value);
        writeByte(blockBuilder, (byte) value);
    }

    public void writeByte(BlockBuilder blockBuilder, byte value)
    {
        ((ByteArrayBlockBuilder) blockBuilder).writeByte(value);
    }

    private static void checkValueValid(long value)
    {
        if (value > Byte.MAX_VALUE) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value %d exceeds MAX_BYTE", value));
        }
        if (value < Byte.MIN_VALUE) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value %d is less than MIN_BYTE", value));
        }
    }

    @Override
    public int getFlatFixedSize()
    {
        return Byte.BYTES;
    }

    @Override
    public boolean equals(Object other)
    {
        return other == TINYINT;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @ScalarOperator(READ_VALUE)
    private static long read(@BlockPosition ByteArrayBlock block, @BlockIndex int position)
    {
        return readByte(block, position);
    }

    private static byte readByte(ByteArrayBlock block, int position)
    {
        return block.getByte(position);
    }

    @ScalarOperator(READ_VALUE)
    private static long readFlat(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            @FlatVariableOffset int unusedVariableSizeOffset)
    {
        return fixedSizeSlice[fixedSizeOffset];
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlat(
            long value,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] unusedVariableSizeSlice,
            int unusedVariableSizeOffset)
    {
        fixedSizeSlice[fixedSizeOffset] = (byte) value;
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(long left, long right)
    {
        return left == right;
    }

    @ScalarOperator(HASH_CODE)
    private static long hashCodeOperator(long value)
    {
        return AbstractLongType.hash((byte) value);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(long value)
    {
        return XxHash64.hash((byte) value);
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(long left, long right)
    {
        return Byte.compare((byte) left, (byte) right);
    }

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(long left, long right)
    {
        return ((byte) left) < ((byte) right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(long left, long right)
    {
        return ((byte) left) <= ((byte) right);
    }
}
