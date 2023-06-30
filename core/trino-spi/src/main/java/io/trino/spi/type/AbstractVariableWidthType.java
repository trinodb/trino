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
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.ScalarOperator;

import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.Math.min;
import static java.lang.invoke.MethodHandles.lookup;

public abstract class AbstractVariableWidthType
        extends AbstractType
        implements VariableWidthType
{
    protected static final int EXPECTED_BYTES_PER_ENTRY = 32;
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
