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
package io.prestosql.operator.scalar;

import com.google.common.annotations.VisibleForTesting;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.ArrayBlock;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.util.Failures.checkCondition;
import static java.lang.Math.multiplyExact;
import static java.lang.StrictMath.toIntExact;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static java.util.Arrays.setAll;

@ScalarFunction("combinations")
@Description("Return n-element subsets from array")
public final class ArrayCombinationsFunction
{
    private ArrayCombinationsFunction() {}

    private static final int MAX_COMBINATION_LENGTH = 5;
    private static final int MAX_RESULT_ELEMENTS = 100_000;

    @TypeParameter("T")
    @SqlType("array(array(T))")
    public static Block combinations(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block array,
            @SqlType(INTEGER) long n)
    {
        int arrayLength = array.getPositionCount();
        int combinationLength = toIntExact(n);
        checkCondition(combinationLength >= 0, INVALID_FUNCTION_ARGUMENT, "combination size must not be negative: %s", combinationLength);
        checkCondition(combinationLength <= MAX_COMBINATION_LENGTH, INVALID_FUNCTION_ARGUMENT, "combination size must not exceed %s: %s", MAX_COMBINATION_LENGTH, combinationLength);

        ArrayType arrayType = new ArrayType(elementType);
        if (combinationLength > arrayLength) {
            return arrayType.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), 0).build();
        }

        int combinationCount = combinationCount(arrayLength, combinationLength);
        checkCondition(combinationCount * (long) combinationLength <= MAX_RESULT_ELEMENTS, INVALID_FUNCTION_ARGUMENT, "combinations exceed max size");

        int[] ids = new int[combinationCount * combinationLength];
        int idsPosition = 0;

        int[] combination = firstCombination(arrayLength, combinationLength);
        do {
            arraycopy(combination, 0, ids, idsPosition, combinationLength);
            idsPosition += combinationLength;
        }
        while (nextCombination(combination, combinationLength));
        verify(idsPosition == ids.length, "idsPosition != ids.length, %s and %s respectively", idsPosition, ids.length);

        int[] offsets = new int[combinationCount + 1];
        setAll(offsets, i -> i * combinationLength);

        return ArrayBlock.fromElementBlock(combinationCount, Optional.empty(), offsets, new DictionaryBlock(array, ids));
    }

    @VisibleForTesting
    static int combinationCount(int arrayLength, int combinationLength)
    {
        try {
            /*
             * Then combinationCount(n, k) = combinationCount(n-1, k-1) * n/k (https://en.wikipedia.org/wiki/Combination#Number_of_k-combinations)
             * The formula is recursive. Here, instead of starting with k=combinationCount, n=arrayLength and recursing,
             * we start with k=0 n=(arrayLength-combinationLength) and proceed "bottom up".
             */
            int combinations = 1;
            for (int i = 1; i <= combinationLength; i++) {
                combinations = multiplyExact(combinations, arrayLength - combinationLength + i) / i;
            }
            return combinations;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Number of combinations too large for array of size %s and combination length %s", arrayLength, combinationLength));
        }
    }

    private static int[] firstCombination(int arrayLength, int combinationLength)
    {
        int[] combination = new int[combinationLength + 1];
        setAll(combination, i -> i);
        combination[combinationLength] = arrayLength; // sentinel
        return combination;
    }

    private static boolean nextCombination(int[] combination, int combinationLength)
    {
        for (int i = 0; i < combinationLength; i++) {
            if (combination[i] + 1 < combination[i + 1]) {
                combination[i]++;
                resetCombination(combination, i);
                return true;
            }
        }
        return false;
    }

    private static void resetCombination(int[] combination, int to)
    {
        for (int i = 0; i < to; i++) {
            combination[i] = i;
        }
    }
}
