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
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
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
    // Short strings are encoded with a negative length, so we have to encode the length in big-endian format
    // to shift the sign bit to the beginning
    private static final VarHandle INT_BE_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

    private static final int MAX_SHORT_FLAT_LENGTH = 3;

    protected static final int EXPECTED_BYTES_PER_ENTRY = 32;
    protected static final TypeOperatorDeclaration DEFAULT_READ_OPERATORS = extractOperatorDeclaration(DefaultReadOperators.class, lookup(), Slice.class);
    protected static final TypeOperatorDeclaration DEFAULT_COMPARABLE_OPERATORS = extractOperatorDeclaration(DefaultComparableOperators.class, lookup(), Slice.class);
    protected static final TypeOperatorDeclaration DEFAULT_ORDERING_OPERATORS = extractOperatorDeclaration(DefaultOrderingOperators.class, lookup(), Slice.class);

    protected AbstractVariableWidthType(TypeSignature signature, Class<?> javaType)
    {
        super(signature, javaType, VariableWidthBlock.class);
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
                expectedBytesPerEntry == 0 ? expectedEntries : min(expectedEntries, maxBlockSizeInBytes / expectedBytesPerEntry),
                expectedBytes);
    }

    @Override
    public VariableWidthBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, EXPECTED_BYTES_PER_ENTRY);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
            position = block.getUnderlyingValuePosition(position);
            Slice slice = variableWidthBlock.getRawSlice();
            int offset = variableWidthBlock.getRawSliceOffset(position);
            int length = variableWidthBlock.getSliceLength(position);
            ((VariableWidthBlockBuilder) blockBuilder).writeEntry(slice, offset, length);
        }
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return DEFAULT_READ_OPERATORS;
    }

    @Override
    public int getFlatFixedSize()
    {
        return Integer.BYTES;
    }

    @Override
    public boolean isFlatVariableWidth()
    {
        return true;
    }

    @Override
    public int getFlatVariableWidthSize(Block block, int position)
    {
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int length = variableWidthBlock.getSliceLength(block.getUnderlyingValuePosition(position));
        if (length <= MAX_SHORT_FLAT_LENGTH) {
            return 0;
        }
        return length;
    }

    @Override
    public int getFlatVariableWidthLength(byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        int length = readVariableWidthLength(fixedSizeSlice, fixedSizeOffset);
        if (length <= MAX_SHORT_FLAT_LENGTH) {
            return 0;
        }
        return length;
    }

    static int readVariableWidthLength(byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        int length = (int) INT_BE_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
        if (length < 0) {
            int shortLength = fixedSizeSlice[fixedSizeOffset] & 0x7F;
            if (shortLength > MAX_SHORT_FLAT_LENGTH) {
                throw new IllegalArgumentException("Invalid short variable width length: " + shortLength);
            }
            return shortLength;
        }
        return length;
    }

    static void writeFlatVariableLength(int length, byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        if (length < 0) {
            throw new IllegalArgumentException("Invalid variable width length: " + length);
        }
        if (length <= MAX_SHORT_FLAT_LENGTH) {
            fixedSizeSlice[fixedSizeOffset] = (byte) (length | 0x80);
        }
        else {
            INT_BE_HANDLE.set(fixedSizeSlice, fixedSizeOffset, length);
        }
    }

    private static class DefaultReadOperators
    {
        @ScalarOperator(READ_VALUE)
        private static Slice readFlatToStack(
                @FlatFixed byte[] fixedSizeSlice,
                @FlatFixedOffset int fixedSizeOffset,
                @FlatVariableWidth byte[] variableSizeSlice,
                @FlatVariableOffset int variableSizeOffset)
        {
            int length = readVariableWidthLength(fixedSizeSlice, fixedSizeOffset);
            byte[] bytes;
            int offset;
            if (length <= MAX_SHORT_FLAT_LENGTH) {
                bytes = fixedSizeSlice;
                offset = fixedSizeOffset + 1;
            }
            else {
                bytes = variableSizeSlice;
                offset = variableSizeOffset;
            }
            return wrappedBuffer(bytes, offset, length);
        }

        @ScalarOperator(READ_VALUE)
        private static void readFlatToBlock(
                @FlatFixed byte[] fixedSizeSlice,
                @FlatFixedOffset int fixedSizeOffset,
                @FlatVariableWidth byte[] variableSizeSlice,
                @FlatVariableOffset int variableSizeOffset,
                BlockBuilder blockBuilder)
        {
            int length = readVariableWidthLength(fixedSizeSlice, fixedSizeOffset);
            byte[] bytes;
            int offset;
            if (length <= MAX_SHORT_FLAT_LENGTH) {
                bytes = fixedSizeSlice;
                offset = fixedSizeOffset + 1;
            }
            else {
                bytes = variableSizeSlice;
                offset = variableSizeOffset;
            }
            ((VariableWidthBlockBuilder) blockBuilder).writeEntry(bytes, offset, length);
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
            writeFlatVariableLength(length, fixedSizeSlice, fixedSizeOffset);
            byte[] bytes;
            int offset;
            if (length <= MAX_SHORT_FLAT_LENGTH) {
                bytes = fixedSizeSlice;
                offset = fixedSizeOffset + 1;
            }
            else {
                bytes = variableSizeSlice;
                offset = variableSizeOffset;
            }
            value.getBytes(0, bytes, offset, length);
        }

        @ScalarOperator(READ_VALUE)
        private static void writeFlatFromBlock(
                @BlockPosition VariableWidthBlock block,
                @BlockIndex int position,
                byte[] fixedSizeSlice,
                int fixedSizeOffset,
                byte[] variableSizeSlice,
                int variableSizeOffset)
        {
            Slice rawSlice = block.getRawSlice();
            int rawSliceOffset = block.getRawSliceOffset(position);
            int length = block.getSliceLength(position);

            writeFlatVariableLength(length, fixedSizeSlice, fixedSizeOffset);
            byte[] bytes;
            int offset;
            if (length <= MAX_SHORT_FLAT_LENGTH) {
                bytes = fixedSizeSlice;
                offset = fixedSizeOffset + 1;
            }
            else {
                bytes = variableSizeSlice;
                offset = variableSizeOffset;
            }
            rawSlice.getBytes(rawSliceOffset, bytes, offset, length);
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
        private static boolean equalOperator(@BlockPosition VariableWidthBlock leftBlock, @BlockIndex int leftPosition, @BlockPosition VariableWidthBlock rightBlock, @BlockIndex int rightPosition)
        {
            Slice leftRawSlice = leftBlock.getRawSlice();
            int leftRawSliceOffset = leftBlock.getRawSliceOffset(leftPosition);
            int leftLength = leftBlock.getSliceLength(leftPosition);

            Slice rightRawSlice = rightBlock.getRawSlice();
            int rightRawSliceOffset = rightBlock.getRawSliceOffset(rightPosition);
            int rightLength = rightBlock.getSliceLength(rightPosition);

            return leftRawSlice.equals(leftRawSliceOffset, leftLength, rightRawSlice, rightRawSliceOffset, rightLength);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(Slice left, @BlockPosition VariableWidthBlock rightBlock, @BlockIndex int rightPosition)
        {
            return equalOperator(rightBlock, rightPosition, left);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(@BlockPosition VariableWidthBlock leftBlock, @BlockIndex int leftPosition, Slice right)
        {
            Slice leftRawSlice = leftBlock.getRawSlice();
            int leftRawSliceOffset = leftBlock.getRawSliceOffset(leftPosition);
            int leftLength = leftBlock.getSliceLength(leftPosition);

            return leftRawSlice.equals(leftRawSliceOffset, leftLength, right, 0, right.length());
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(
                @FlatFixed byte[] leftFixedSizeSlice,
                @FlatFixedOffset int leftFixedSizeOffset,
                @FlatVariableWidth byte[] leftVariableSizeSlice,
                @FlatVariableOffset int leftVariableSizeOffset,
                @FlatFixed byte[] rightFixedSizeSlice,
                @FlatFixedOffset int rightFixedSizeOffset,
                @FlatVariableWidth byte[] rightVariableSizeSlice,
                @FlatVariableOffset int rightVariableSizeOffset)
        {
            int leftLength = readVariableWidthLength(leftFixedSizeSlice, leftFixedSizeOffset);
            int rightLength = readVariableWidthLength(rightFixedSizeSlice, rightFixedSizeOffset);
            if (leftLength != rightLength) {
                return false;
            }
            if (leftLength <= MAX_SHORT_FLAT_LENGTH) {
                // Direct equality check on length + bytes for short strings
                return ((int) INT_BE_HANDLE.get(leftFixedSizeSlice, leftFixedSizeOffset)) == ((int) INT_BE_HANDLE.get(rightFixedSizeSlice, rightFixedSizeOffset));
            }
            return Arrays.equals(
                    leftVariableSizeSlice,
                    leftVariableSizeOffset,
                    leftVariableSizeOffset + leftLength,
                    rightVariableSizeSlice,
                    rightVariableSizeOffset,
                    rightVariableSizeOffset + rightLength);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(
                @BlockPosition VariableWidthBlock leftBlock,
                @BlockIndex int leftPosition,
                @FlatFixed byte[] rightFixedSizeSlice,
                @FlatFixedOffset int rightFixedSizeOffset,
                @FlatVariableWidth byte[] rightVariableSizeSlice,
                @FlatVariableOffset int rightVariableSizeOffset)
        {
            return equalOperator(
                    rightFixedSizeSlice,
                    rightFixedSizeOffset,
                    rightVariableSizeSlice,
                    rightVariableSizeOffset,
                    leftBlock,
                    leftPosition);
        }

        @ScalarOperator(EQUAL)
        private static boolean equalOperator(
                @FlatFixed byte[] leftFixedSizeSlice,
                @FlatFixedOffset int leftFixedSizeOffset,
                @FlatVariableWidth byte[] leftVariableSizeSlice,
                @FlatVariableOffset int leftVariableSizeOffset,
                @BlockPosition VariableWidthBlock rightBlock,
                @BlockIndex int rightPosition)
        {
            int leftLength = readVariableWidthLength(leftFixedSizeSlice, leftFixedSizeOffset);

            Slice rightRawSlice = rightBlock.getRawSlice();
            int rightRawSliceOffset = rightBlock.getRawSliceOffset(rightPosition);
            int rightLength = rightBlock.getSliceLength(rightPosition);

            if (leftLength != rightLength) {
                return false;
            }

            byte[] leftBytes;
            int leftOffset;
            if (leftLength <= MAX_SHORT_FLAT_LENGTH) {
                leftBytes = leftFixedSizeSlice;
                leftOffset = leftFixedSizeOffset + 1;
            }
            else {
                leftBytes = leftVariableSizeSlice;
                leftOffset = leftVariableSizeOffset;
            }
            return rightRawSlice.equals(rightRawSliceOffset, rightLength, wrappedBuffer(leftBytes, leftOffset, leftLength), 0, leftLength);
        }

        @ScalarOperator(XX_HASH_64)
        private static long xxHash64Operator(Slice value)
        {
            return XxHash64.hash(value);
        }

        @ScalarOperator(XX_HASH_64)
        private static long xxHash64Operator(@BlockPosition VariableWidthBlock block, @BlockIndex int position)
        {
            return XxHash64.hash(block.getRawSlice(), block.getRawSliceOffset(position), block.getSliceLength(position));
        }

        @ScalarOperator(XX_HASH_64)
        private static long xxHash64Operator(
                @FlatFixed byte[] fixedSizeSlice,
                @FlatFixedOffset int fixedSizeOffset,
                @FlatVariableWidth byte[] variableSizeSlice,
                @FlatVariableOffset int variableSizeOffset)
        {
            int length = readVariableWidthLength(fixedSizeSlice, fixedSizeOffset);
            byte[] bytes;
            int offset;
            if (length <= MAX_SHORT_FLAT_LENGTH) {
                bytes = fixedSizeSlice;
                offset = fixedSizeOffset + 1;
            }
            else {
                bytes = variableSizeSlice;
                offset = variableSizeOffset;
            }
            return XxHash64.hash(wrappedBuffer(bytes, offset, length));
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
        private static long comparisonOperator(@BlockPosition VariableWidthBlock leftBlock, @BlockIndex int leftPosition, @BlockPosition VariableWidthBlock rightBlock, @BlockIndex int rightPosition)
        {
            Slice leftRawSlice = leftBlock.getRawSlice();
            int leftRawSliceOffset = leftBlock.getRawSliceOffset(leftPosition);
            int leftLength = leftBlock.getSliceLength(leftPosition);

            Slice rightRawSlice = rightBlock.getRawSlice();
            int rightRawSliceOffset = rightBlock.getRawSliceOffset(rightPosition);
            int rightLength = rightBlock.getSliceLength(rightPosition);

            return leftRawSlice.compareTo(leftRawSliceOffset, leftLength, rightRawSlice, rightRawSliceOffset, rightLength);
        }

        @ScalarOperator(COMPARISON_UNORDERED_LAST)
        private static long comparisonOperator(@BlockPosition VariableWidthBlock leftBlock, @BlockIndex int leftPosition, Slice right)
        {
            Slice leftRawSlice = leftBlock.getRawSlice();
            int leftRawSliceOffset = leftBlock.getRawSliceOffset(leftPosition);
            int leftLength = leftBlock.getSliceLength(leftPosition);

            return leftRawSlice.compareTo(leftRawSliceOffset, leftLength, right, 0, right.length());
        }

        @ScalarOperator(COMPARISON_UNORDERED_LAST)
        private static long comparisonOperator(Slice left, @BlockPosition VariableWidthBlock rightBlock, @BlockIndex int rightPosition)
        {
            Slice rightRawSlice = rightBlock.getRawSlice();
            int rightRawSliceOffset = rightBlock.getRawSliceOffset(rightPosition);
            int rightLength = rightBlock.getSliceLength(rightPosition);

            return left.compareTo(0, left.length(), rightRawSlice, rightRawSliceOffset, rightLength);
        }
    }
}
