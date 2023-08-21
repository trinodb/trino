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
import io.airlift.slice.XxHash64;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.ScalarOperator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.Math.min;
import static java.lang.invoke.MethodHandles.lookup;

public abstract class AbstractVariableWidthType
        extends AbstractType
        implements VariableWidthType
{
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    protected static final int EXPECTED_BYTES_PER_ENTRY = 32;
    protected static final TypeOperatorDeclaration DEFAULT_READ_OPERATORS = extractOperatorDeclaration(DefaultReadOperators.class, lookup(), Slice.class);
    protected static final TypeOperatorDeclaration DEFAULT_COMPARABLE_OPERATORS = extractOperatorDeclaration(DefaultComparableOperators.class, lookup(), Slice.class);
    protected static final TypeOperatorDeclaration DEFAULT_ORDERING_OPERATORS = extractOperatorDeclaration(DefaultOrderingOperators.class, lookup(), Slice.class);

    protected AbstractVariableWidthType(TypeSignature signature, Class<?> javaType)
    {
        super(signature, javaType);
    }

    @Override
    public VariableWidthBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }

        // it is guaranteed Math.min will not overflow; safe to cast
        int expectedBytes = (int) min((long) expectedEntries * expectedBytesPerEntry, maxBlockSizeInBytes);
        return new VariableWidthBlockBuilder(
                blockBuilderStatus,
                expectedBytesPerEntry == 0 ? expectedEntries : Math.min(expectedEntries, maxBlockSizeInBytes / expectedBytesPerEntry),
                expectedBytes);
    }

    @Override
    public VariableWidthBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, EXPECTED_BYTES_PER_ENTRY);
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return DEFAULT_READ_OPERATORS;
    }

    @Override
    public int getFlatFixedSize()
    {
        return 16;
    }

    @Override
    public boolean isFlatVariableWidth()
    {
        return true;
    }

    @Override
    public int getFlatVariableWidthSize(Block block, int position)
    {
        int length = block.getSliceLength(position);
        if (length <= 12) {
            return 0;
        }
        return length;
    }

    @Override
    public int relocateFlatVariableWidthOffsets(byte[] fixedSizeSlice, int fixedSizeOffset, byte[] variableSizeSlice, int variableSizeOffset)
    {
        int length = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
        if (length <= 12) {
            return 0;
        }

        if (variableSizeSlice.length < variableSizeOffset + length) {
            throw new IllegalArgumentException("Variable size slice does not have enough space");
        }
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset + Long.BYTES + Integer.BYTES, variableSizeOffset);
        return length;
    }

    private static class DefaultReadOperators
    {
        @ScalarOperator(READ_VALUE)
        private static Slice readFlatToStack(
                @FlatFixed byte[] fixedSizeSlice,
                @FlatFixedOffset int fixedSizeOffset,
                @FlatVariableWidth byte[] variableSizeSlice)
        {
            int length = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
            if (length <= 12) {
                return wrappedBuffer(fixedSizeSlice, fixedSizeOffset + Integer.BYTES, length);
            }
            int variableSizeOffset = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset + Integer.BYTES + Long.BYTES);
            return wrappedBuffer(variableSizeSlice, variableSizeOffset, length);
        }

        @ScalarOperator(READ_VALUE)
        private static void readFlatToBlock(
                @FlatFixed byte[] fixedSizeSlice,
                @FlatFixedOffset int fixedSizeOffset,
                @FlatVariableWidth byte[] variableSizeSlice,
                BlockBuilder blockBuilder)
        {
            int length = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
            if (length <= 12) {
                ((VariableWidthBlockBuilder) blockBuilder).writeEntry(fixedSizeSlice, fixedSizeOffset + Integer.BYTES, length);
            }
            else {
                int variableSizeOffset = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset + Integer.BYTES + Long.BYTES);
                ((VariableWidthBlockBuilder) blockBuilder).writeEntry(variableSizeSlice, variableSizeOffset, length);
            }
        }

        @ScalarOperator(READ_VALUE)
        private static void writeFlatFromStack(
                Slice value,
                byte[] fixedSizeSlice,
                int fixedSizeOffset,
                byte[] variableSizeSlice,
                int variableSizeOffset)
        {
            int length = value.length();
            INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset, length);
            if (length <= 12) {
                value.getBytes(0, fixedSizeSlice, fixedSizeOffset + Integer.BYTES, length);
            }
            else {
                INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset + Integer.BYTES + Long.BYTES, variableSizeOffset);
                value.getBytes(0, variableSizeSlice, variableSizeOffset, length);
            }
        }

        @ScalarOperator(READ_VALUE)
        private static void writeFlatFromBlock(
                @BlockPosition Block block,
                @BlockIndex int position,
                byte[] fixedSizeSlice,
                int fixedSizeOffset,
                byte[] variableSizeSlice,
                int variableSizeOffset)
        {
            int length = block.getSliceLength(position);
            INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset, length);
            if (length <= 12) {
                block.writeSliceTo(position, 0, length, wrappedBuffer(fixedSizeSlice, fixedSizeOffset + Integer.BYTES, length).getOutput());
            }
            else {
                INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset + Integer.BYTES + Long.BYTES, variableSizeOffset);
                block.writeSliceTo(position, 0, length, wrappedBuffer(variableSizeSlice, variableSizeOffset, length).getOutput());
            }
        }
    }

    private static class DefaultComparableOperators
    {
        @ScalarOperator(EQUAL)
        private static boolean equalOperator(Slice left, Slice right)
        {
            return left.equals(right);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
        {
            int leftLength = leftBlock.getSliceLength(leftPosition);
            int rightLength = rightBlock.getSliceLength(rightPosition);
            if (leftLength != rightLength) {
                return false;
            }
            return leftBlock.equals(leftPosition, 0, rightBlock, rightPosition, 0, leftLength);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(Slice left, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
        {
            return equalOperator(rightBlock, rightPosition, left);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, Slice right)
        {
            int leftLength = leftBlock.getSliceLength(leftPosition);
            int rightLength = right.length();
            if (leftLength != rightLength) {
                return false;
            }
            return leftBlock.bytesEqual(leftPosition, 0, right, 0, leftLength);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(
                @FlatFixed byte[] leftFixedSizeSlice,
                @FlatFixedOffset int leftFixedSizeOffset,
                @FlatVariableWidth byte[] leftVariableSizeSlice,
                @FlatFixed byte[] rightFixedSizeSlice,
                @FlatFixedOffset int rightFixedSizeOffset,
                @FlatVariableWidth byte[] rightVariableSizeSlice)
        {
            int leftLength = (int) INT_HANDLE.get(leftFixedSizeSlice, leftFixedSizeOffset);
            int rightLength = (int) INT_HANDLE.get(rightFixedSizeSlice, rightFixedSizeOffset);
            if (leftLength != rightLength) {
                return false;
            }
            if (leftLength <= 12) {
                return Arrays.equals(
                        leftFixedSizeSlice,
                        leftFixedSizeOffset + Integer.BYTES,
                        leftFixedSizeOffset + Integer.BYTES + leftLength,
                        rightFixedSizeSlice,
                        rightFixedSizeOffset + Integer.BYTES,
                        rightFixedSizeOffset + Integer.BYTES + leftLength);
            }
            else {
                int leftVariableSizeOffset = (int) INT_HANDLE.get(leftFixedSizeSlice, leftFixedSizeOffset + Integer.BYTES + Long.BYTES);
                int rightVariableSizeOffset = (int) INT_HANDLE.get(rightFixedSizeSlice, rightFixedSizeOffset + Integer.BYTES + Long.BYTES);
                return Arrays.equals(
                        leftVariableSizeSlice,
                        leftVariableSizeOffset,
                        leftVariableSizeOffset + leftLength,
                        rightVariableSizeSlice,
                        rightVariableSizeOffset,
                        rightVariableSizeOffset + rightLength);
            }
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(
                @BlockPosition Block leftBlock,
                @BlockIndex int leftPosition,
                @FlatFixed byte[] rightFixedSizeSlice,
                @FlatFixedOffset int rightFixedSizeOffset,
                @FlatVariableWidth byte[] rightVariableSizeSlice)
        {
            return equalOperator(
                    rightFixedSizeSlice,
                    rightFixedSizeOffset,
                    rightVariableSizeSlice,
                    leftBlock,
                    leftPosition);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(
                @FlatFixed byte[] leftFixedSizeSlice,
                @FlatFixedOffset int leftFixedSizeOffset,
                @FlatVariableWidth byte[] leftVariableSizeSlice,
                @BlockPosition Block rightBlock,
                @BlockIndex int rightPosition)
        {
            int leftLength = (int) INT_HANDLE.get(leftFixedSizeSlice, leftFixedSizeOffset);
            if (rightBlock.isNull(rightPosition) || leftLength != rightBlock.getSliceLength(rightPosition)) {
                return false;
            }
            if (leftLength <= 12) {
                return rightBlock.bytesEqual(rightPosition, 0, wrappedBuffer(leftFixedSizeSlice, leftFixedSizeOffset + Integer.BYTES, leftLength), 0, leftLength);
            }
            else {
                int leftVariableSizeOffset = (int) INT_HANDLE.get(leftFixedSizeSlice, leftFixedSizeOffset + Integer.BYTES + Long.BYTES);
                return rightBlock.bytesEqual(rightPosition, 0, wrappedBuffer(leftVariableSizeSlice, leftVariableSizeOffset, leftLength), 0, leftLength);
            }
        }

        @ScalarOperator(XX_HASH_64)
        private static long xxHash64Operator(Slice value)
        {
            return XxHash64.hash(value);
        }

        @ScalarOperator(XX_HASH_64)
        private static long xxHash64Operator(@BlockPosition Block block, @BlockIndex int position)
        {
            return block.hash(position, 0, block.getSliceLength(position));
        }

        @ScalarOperator(XX_HASH_64)
        private static long xxHash64Operator(
                @FlatFixed byte[] fixedSizeSlice,
                @FlatFixedOffset int fixedSizeOffset,
                @FlatVariableWidth byte[] variableSizeSlice)
        {
            int length = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
            if (length <= 12) {
                return XxHash64.hash(wrappedBuffer(fixedSizeSlice, fixedSizeOffset + Integer.BYTES, length));
            }
            int variableSizeOffset = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset + Integer.BYTES + Long.BYTES);
            return XxHash64.hash(wrappedBuffer(variableSizeSlice, variableSizeOffset, length));
        }
    }

    private static class DefaultOrderingOperators
    {
        @ScalarOperator(COMPARISON_UNORDERED_LAST)
        private static long comparisonOperator(Slice left, Slice right)
        {
            return left.compareTo(right);
        }

        @ScalarOperator(COMPARISON_UNORDERED_LAST)
        private static long comparisonOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
        {
            int leftLength = leftBlock.getSliceLength(leftPosition);
            int rightLength = rightBlock.getSliceLength(rightPosition);
            return leftBlock.compareTo(leftPosition, 0, leftLength, rightBlock, rightPosition, 0, rightLength);
        }

        @ScalarOperator(COMPARISON_UNORDERED_LAST)
        private static long comparisonOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, Slice right)
        {
            int leftLength = leftBlock.getSliceLength(leftPosition);
            return leftBlock.bytesCompare(leftPosition, 0, leftLength, right, 0, right.length());
        }

        @ScalarOperator(COMPARISON_UNORDERED_LAST)
        private static long comparisonOperator(Slice left, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
        {
            int rightLength = rightBlock.getSliceLength(rightPosition);
            return -rightBlock.bytesCompare(rightPosition, 0, rightLength, left, 0, left.length());
        }
    }
}
