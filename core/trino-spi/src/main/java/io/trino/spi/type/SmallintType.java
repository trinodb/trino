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
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.ShortArrayBlockBuilder;
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

public final class SmallintType
        extends AbstractType
        implements FixedWidthType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(SmallintType.class, lookup(), long.class);
    private static final VarHandle SHORT_HANDLE = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);

    public static final SmallintType SMALLINT = new SmallintType();

    private SmallintType()
    {
        super(new TypeSignature(StandardTypes.SMALLINT), long.class, ShortArrayBlock.class);
    }

    @Override
    public int getFixedSize()
    {
        return Short.BYTES;
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
        return new ShortArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / Short.BYTES));
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new ShortArrayBlockBuilder(null, positionCount);
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

        return getShort(block, position);
    }

    @Override
    public Optional<Range> getRange()
    {
        return Optional.of(new Range((long) Short.MIN_VALUE, (long) Short.MAX_VALUE));
    }

    @Override
    public Optional<Object> getPreviousValue(Object object)
    {
        long value = (long) object;
        checkValueValid(value);
        if (value == Short.MIN_VALUE) {
            return Optional.empty();
        }
        return Optional.of(value - 1);
    }

    @Override
    public Optional<Object> getNextValue(Object object)
    {
        long value = (long) object;
        checkValueValid(value);
        if (value == Short.MAX_VALUE) {
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
            ((ShortArrayBlockBuilder) blockBuilder).writeShort(getShort(block, position));
        }
    }

    @Override
    public long getLong(Block block, int position)
    {
        return getShort(block, position);
    }

    public short getShort(Block block, int position)
    {
        return readShort((ShortArrayBlock) block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        checkValueValid(value);
        writeShort(blockBuilder, (short) value);
    }

    public void writeShort(BlockBuilder blockBuilder, short value)
    {
        ((ShortArrayBlockBuilder) blockBuilder).writeShort(value);
    }

    private void checkValueValid(long value)
    {
        if (value > Short.MAX_VALUE) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value %d exceeds MAX_SHORT for type %s", value, getTypeSignature()));
        }
        if (value < Short.MIN_VALUE) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Value %d is less than MIN_SHORT for type %s", value, getTypeSignature()));
        }
    }

    @Override
    public int getFlatFixedSize()
    {
        return Short.BYTES;
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == SMALLINT;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @ScalarOperator(READ_VALUE)
    private static long read(@BlockPosition ShortArrayBlock block, @BlockIndex int position)
    {
        return readShort(block, position);
    }

    private static short readShort(ShortArrayBlock block, int position)
    {
        return block.getShort(position);
    }

    @ScalarOperator(READ_VALUE)
    private static long readFlat(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            @FlatVariableOffset int unusedVariableSizeOffset)
    {
        return (short) SHORT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlat(
            long value,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] unusedVariableSizeSlice,
            int unusedVariableSizeOffset)
    {
        SHORT_HANDLE.set(fixedSizeSlice, fixedSizeOffset, (short) value);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(long left, long right)
    {
        return left == right;
    }

    @ScalarOperator(HASH_CODE)
    private static long hashCodeOperator(long value)
    {
        return AbstractLongType.hash((short) value);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(long value)
    {
        return XxHash64.hash((short) value);
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(long left, long right)
    {
        return Short.compare((short) left, (short) right);
    }

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(long left, long right)
    {
        return ((short) left) < ((short) right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(long left, long right)
    {
        return ((short) left) <= ((short) right);
    }
}
