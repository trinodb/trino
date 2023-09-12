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
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.ScalarOperator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.UUID;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.block.Int128ArrayBlock.INT128_BYTES;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.Long.reverseBytes;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

/**
 * UUIDs are encoded in big-endian representation (the bytes are stored in
 * the same order as they appear when a UUID is printed in hexadecimal).
 */
public class UuidType
        extends AbstractType
        implements FixedWidthType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(UuidType.class, lookup(), Slice.class);
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    public static final UuidType UUID = new UuidType();

    private UuidType()
    {
        super(new TypeSignature(StandardTypes.UUID), Slice.class);
    }

    @Override
    public int getFixedSize()
    {
        return INT128_BYTES;
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
        long high = reverseBytes(block.getLong(position, 0));
        long low = reverseBytes(block.getLong(position, SIZE_OF_LONG));
        return new UUID(high, low).toString();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            ((Int128ArrayBlockBuilder) blockBuilder).writeInt128(
                    block.getLong(position, 0),
                    block.getLong(position, SIZE_OF_LONG));
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
        ((Int128ArrayBlockBuilder) blockBuilder).writeInt128(
                value.getLong(offset),
                value.getLong(offset + SIZE_OF_LONG));
    }

    @Override
    public final Slice getSlice(Block block, int position)
    {
        Slice value = Slices.allocate(INT128_BYTES);
        value.setLong(0, block.getLong(position, 0));
        value.setLong(SIZE_OF_LONG, block.getLong(position, SIZE_OF_LONG));
        return value;
    }

    @Override
    public int getFlatFixedSize()
    {
        return INT128_BYTES;
    }

    public static Slice javaUuidToTrinoUuid(UUID uuid)
    {
        Slice value = Slices.allocate(INT128_BYTES);
        value.setLong(0, reverseBytes(uuid.getMostSignificantBits()));
        value.setLong(SIZE_OF_LONG, reverseBytes(uuid.getLeastSignificantBits()));
        return value;
    }

    public static UUID trinoUuidToJavaUuid(Slice uuid)
    {
        if (uuid.length() != INT128_BYTES) {
            throw new IllegalStateException(format("Expected value to be exactly %d bytes but was %d", INT128_BYTES, uuid.length()));
        }
        return new UUID(
                reverseBytes(uuid.getLong(0)),
                reverseBytes(uuid.getLong(SIZE_OF_LONG)));
    }

    @ScalarOperator(READ_VALUE)
    private static Slice readFlat(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice)
    {
        return wrappedBuffer(fixedSizeSlice, fixedSizeOffset, INT128_BYTES);
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlat(
            Slice sourceSlice,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] unusedVariableSizeSlice,
            int unusedVariableSizeOffset)
    {
        sourceSlice.getBytes(0, fixedSizeSlice, fixedSizeOffset, INT128_BYTES);
    }

    @ScalarOperator(READ_VALUE)
    private static void readFlatToBlock(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] unusedVariableSizeSlice,
            BlockBuilder blockBuilder)
    {
        ((Int128ArrayBlockBuilder) blockBuilder).writeInt128(
                (long) LONG_HANDLE.get(fixedSizeSlice, fixedSizeOffset),
                (long) LONG_HANDLE.get(fixedSizeSlice, fixedSizeOffset + SIZE_OF_LONG));
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
        return compareLittleEndian(
                left.getLong(0),
                left.getLong(SIZE_OF_LONG),
                right.getLong(0),
                right.getLong(SIZE_OF_LONG));
    }

    @ScalarOperator(COMPARISON_UNORDERED_LAST)
    private static long comparisonOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        return compareLittleEndian(
                leftBlock.getLong(leftPosition, 0),
                leftBlock.getLong(leftPosition, SIZE_OF_LONG),
                rightBlock.getLong(rightPosition, 0),
                rightBlock.getLong(rightPosition, SIZE_OF_LONG));
    }

    private static int compareLittleEndian(long leftLow64le, long leftHigh64le, long rightLow64le, long rightHigh64le)
    {
        int compare = Long.compareUnsigned(reverseBytes(leftLow64le), reverseBytes(rightLow64le));
        if (compare != 0) {
            return compare;
        }
        return Long.compareUnsigned(reverseBytes(leftHigh64le), reverseBytes(rightHigh64le));
    }
}
