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

import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Math.min;
import static java.lang.StrictMath.toIntExact;

@ScalarFunction("ngrams")
@Description("Return N-grams for the input")
public final class ArrayNgramsFunction
{
    // the result holds one position id per element in an int array, so this bounds that array at about 4 MB,
    // in line with the 4 MB result budget the repeat function applies
    private static final int MAX_RESULT_ELEMENTS = 1_000_000;

    private ArrayNgramsFunction() {}

    @TypeParameter("T")
    @SqlType("array(array(T))")
    public static Block ngrams(@SqlType("array(T)") Block array, @SqlType(INTEGER) long n)
    {
        checkCondition(n > 0, INVALID_FUNCTION_ARGUMENT, "N must be positive");

        // n should not be larger than the array length
        int elementsPerRecord = toIntExact(min(array.getPositionCount(), n));
        int totalRecords = array.getPositionCount() - elementsPerRecord + 1;
        // the element count peaks near arrayLength^2 / 4, so it is computed as a long: for arrays of about
        // 92000 entries and upwards it overflows an int, which would leave the allocation with a negative size
        long totalElements = (long) totalRecords * elementsPerRecord;
        checkCondition(
                totalElements <= MAX_RESULT_ELEMENTS,
                INVALID_FUNCTION_ARGUMENT,
                "ngrams result would have %s elements, which exceeds the maximum of %s",
                totalElements,
                MAX_RESULT_ELEMENTS);
        int[] ids = new int[(int) totalElements];
        int[] offset = new int[totalRecords + 1];
        for (int recordIndex = 0; recordIndex < totalRecords; recordIndex++) {
            for (int elementIndex = 0; elementIndex < elementsPerRecord; elementIndex++) {
                ids[recordIndex * elementsPerRecord + elementIndex] = recordIndex + elementIndex;
            }
            offset[recordIndex + 1] = (recordIndex + 1) * elementsPerRecord;
        }

        return ArrayBlock.fromElementBlock(totalRecords, Optional.empty(), offset, array.getPositions(ids, 0, ids.length));
    }
}
