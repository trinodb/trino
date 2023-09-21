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
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.ByteArrayBlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.ScalarOperator;

import java.util.Optional;

import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.invoke.MethodHandles.lookup;

public final class BooleanType
        extends AbstractType
        implements FixedWidthType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(BooleanType.class, lookup(), boolean.class);

    private static final long TRUE_XX_HASH = XxHash64.hash(1);
    private static final long FALSE_XX_HASH = XxHash64.hash(0);

    public static final BooleanType BOOLEAN = new BooleanType();

    /**
     * This method signifies a contract to callers that as an optimization, they can encode BooleanType blocks as a byte[] directly
     * and potentially bypass the BlockBuilder / BooleanType abstraction in the name of efficiency. If in the future BooleanType
     * encoding changes such that {@link ByteArrayBlock} is not always a valid or efficient representation, then this method must be
     * removed and any usages changed
     */
    public static Block wrapByteArrayAsBooleanBlockWithoutNulls(byte[] booleansAsBytes)
    {
        return new ByteArrayBlock(booleansAsBytes.length, Optional.empty(), booleansAsBytes);
    }

    public static Block createBlockForSingleNonNullValue(boolean value)
    {
        byte byteValue = value ? (byte) 1 : 0;
        return new ByteArrayBlock(1, Optional.empty(), new byte[]{byteValue});
    }

    private BooleanType()
    {
        super(new TypeSignature(StandardTypes.BOOLEAN), boolean.class);
    }

    @Override
    public int getFixedSize()
    {
        return Byte.BYTES;
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
        return new ByteArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / Byte.BYTES));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, Byte.BYTES);
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

        return block.getByte(position, 0) != 0;
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            ((ByteArrayBlockBuilder) blockBuilder).writeByte(block.getByte(position, 0));
        }
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        return block.getByte(position, 0) != 0;
    }

    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        ((ByteArrayBlockBuilder) blockBuilder).writeByte((byte) (value ? 1 : 0));
    }

    @Override
    public int getFlatFixedSize()
    {
        return 1;
    }

    @Override
    public boolean equals(Object other)
    {
        return other == BOOLEAN;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @ScalarOperator(READ_VALUE)
    private static boolean readFlat(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice)
    {
        return fixedSizeSlice[fixedSizeOffset] != 0;
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlat(
            boolean value,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] unusedVariableSizeSlice,
            int unusedVariableSizeOffset)
    {
        fixedSizeSlice[fixedSizeOffset] = (byte) (value ? 1 : 0);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(boolean left, boolean right)
    {
        return left == right;
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(boolean value)
    {
        return value ? TRUE_XX_HASH : FALSE_XX_HASH;
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(boolean left, boolean right)
    {
        return Boolean.compare(left, right);
    }

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(boolean left, boolean right)
    {
        return !left && right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(boolean left, boolean right)
    {
        return !left || right;
    }
}
