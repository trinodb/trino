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
package io.trino.operator.scalar;

import io.trino.spi.block.Block;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.function.TypeParameterSpecialization;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionComparison;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.LongArrays;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;

@ScalarFunction("arrays_overlap")
@Description("Returns true if arrays have common elements")
public final class ArraysOverlapFunction
{
    private int[] leftPositions = new int[0];
    private int[] rightPositions = new int[0];

    private long[] leftLongArray = new long[0];
    private long[] rightLongArray = new long[0];

    @TypeParameter("E")
    public ArraysOverlapFunction(@TypeParameter("E") Type elementType) {}

    @SqlNullable
    @TypeParameter("E")
    @TypeParameterSpecialization(name = "E", nativeContainerType = long.class)
    @SqlType(StandardTypes.BOOLEAN)
    public Boolean arraysOverlapInt(
            @OperatorDependency(
                    operator = COMPARISON_UNORDERED_LAST,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {NEVER_NULL, NEVER_NULL}, result = FAIL_ON_NULL)) LongComparison comparisonOperator,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        int leftSize = leftArray.getPositionCount();
        int rightSize = rightArray.getPositionCount();

        if (leftSize == 0 || rightSize == 0) {
            return false;
        }

        if (leftLongArray.length < leftSize) {
            leftLongArray = new long[leftSize * 2];
        }
        if (rightLongArray.length < rightSize) {
            rightLongArray = new long[rightSize * 2];
        }

        int leftNonNullSize = sortLongArray(leftArray, leftLongArray, type, comparisonOperator);
        int rightNonNullSize = sortLongArray(rightArray, rightLongArray, type, comparisonOperator);

        int leftPosition = 0;
        int rightPosition = 0;
        while (leftPosition < leftNonNullSize && rightPosition < rightNonNullSize) {
            long compareValue = comparisonOperator.compare(leftLongArray[leftPosition], rightLongArray[rightPosition]);
            if (compareValue > 0) {
                rightPosition++;
            }
            else if (compareValue < 0) {
                leftPosition++;
            }
            else {
                return true;
            }
        }
        return (leftNonNullSize < leftSize) || (rightNonNullSize < rightSize) ? null : false;
    }

    // Assumes buffer is long enough, returns count of non-null elements.
    private static int sortLongArray(Block array, long[] buffer, Type type, LongComparison comparisonOperator)
    {
        int arraySize = array.getPositionCount();
        int nonNullSize = 0;
        for (int i = 0; i < arraySize; i++) {
            if (!array.isNull(i)) {
                buffer[nonNullSize++] = type.getLong(array, i);
            }
        }

        LongArrays.unstableSort(buffer, 0, nonNullSize, (left, right) -> (int) comparisonOperator.compare(left, right));

        return nonNullSize;
    }

    @SqlNullable
    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    public Boolean arraysOverlap(
            @OperatorDependency(
                    operator = COMPARISON_UNORDERED_LAST,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL)) BlockPositionComparison comparisonOperator,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        int leftPositionCount = leftArray.getPositionCount();
        int rightPositionCount = rightArray.getPositionCount();

        if (leftPositionCount == 0 || rightPositionCount == 0) {
            return false;
        }

        if (leftPositions.length < leftPositionCount) {
            leftPositions = new int[leftPositionCount * 2];
        }

        if (rightPositions.length < rightPositionCount) {
            rightPositions = new int[rightPositionCount * 2];
        }

        for (int i = 0; i < leftPositionCount; i++) {
            leftPositions[i] = i;
        }
        for (int i = 0; i < rightPositionCount; i++) {
            rightPositions[i] = i;
        }
        IntArrays.quickSort(leftPositions, 0, leftPositionCount, intBlockCompare(comparisonOperator, leftArray));
        IntArrays.quickSort(rightPositions, 0, rightPositionCount, intBlockCompare(comparisonOperator, rightArray));

        int leftPosition = 0;
        int rightPosition = 0;
        while (leftPosition < leftPositionCount && rightPosition < rightPositionCount) {
            if (leftArray.isNull(leftPositions[leftPosition]) || rightArray.isNull(rightPositions[rightPosition])) {
                // Nulls are in the end of the array. Non-null elements do not overlap.
                return null;
            }
            long compareValue = comparisonOperator.compare(leftArray, leftPositions[leftPosition], rightArray, rightPositions[rightPosition]);
            if (compareValue > 0) {
                rightPosition++;
            }
            else if (compareValue < 0) {
                leftPosition++;
            }
            else {
                return true;
            }
        }
        return leftArray.isNull(leftPositions[leftPositionCount - 1]) || rightArray.isNull(rightPositions[rightPositionCount - 1]) ? null : false;
    }

    private static IntComparator intBlockCompare(BlockPositionComparison comparisonOperator, Block block)
    {
        return (left, right) -> {
            if (block.isNull(left) && block.isNull(right)) {
                return 0;
            }
            if (block.isNull(left)) {
                return 1;
            }
            if (block.isNull(right)) {
                return -1;
            }
            return (int) comparisonOperator.compare(block, left, block, right);
        };
    }

    public interface LongComparison
    {
        long compare(long left, long right);
    }
}
