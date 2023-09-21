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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.ByteArrayBlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.type.AbstractType;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.TypeOperatorDeclaration;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.invoke.MethodHandles.lookup;

public final class UnknownType
        extends AbstractType
        implements FixedWidthType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(UnknownType.class, lookup(), boolean.class);

    public static final UnknownType UNKNOWN = new UnknownType();
    public static final String NAME = "unknown";

    private UnknownType()
    {
        // We never access the native container for UNKNOWN because its null check is always true.
        // The actual native container type does not matter here.
        // We choose boolean to represent UNKNOWN because it's the smallest primitive type.
        super(new TypeSignature(NAME), boolean.class);
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
        // call is null in case position is out of bounds
        checkArgument(block.isNull(position), "Expected NULL value for UnknownType");
        return null;
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        blockBuilder.appendNull();
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        // Ideally, this function should never be invoked for unknown type.
        // However, some logic rely on having a default value before the null check.
        checkArgument(block.isNull(position));
        return false;
    }

    @Deprecated
    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        // Ideally, this function should never be invoked for unknown type.
        // However, some logic (e.g. AbstractMinMaxBy) rely on writing a default value before the null check.
        checkArgument(!value);
        blockBuilder.appendNull();
    }

    @Override
    public int getFlatFixedSize()
    {
        return 0;
    }

    @ScalarOperator(READ_VALUE)
    private static boolean readFlat(
            @FlatFixed byte[] unusedFixedSizeSlice,
            @FlatFixedOffset int unusedFixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice)
    {
        throw new AssertionError("value of unknown type should all be NULL");
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlat(
            boolean unusedValue,
            byte[] unusedFixedSizeSlice,
            int unusedFixedSizeOffset,
            byte[] unusedVariableSizeSlice,
            int unusedVariableSizeOffset)
    {
        throw new AssertionError("value of unknown type should all be NULL");
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(boolean unusedLeft, boolean unusedRight)
    {
        throw new AssertionError("value of unknown type should all be NULL");
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(boolean unusedValue)
    {
        throw new AssertionError("value of unknown type should all be NULL");
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(boolean unusedLeft, boolean unusedRight)
    {
        throw new AssertionError("value of unknown type should all be NULL");
    }
}
