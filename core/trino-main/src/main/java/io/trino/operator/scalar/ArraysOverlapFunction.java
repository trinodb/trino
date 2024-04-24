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
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;

@ScalarFunction("arrays_overlap")
@Description("Returns true if arrays have common elements")
public final class ArraysOverlapFunction
{
    private ArraysOverlapFunction() {}

    @SqlNullable
    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean arraysOverlap(
            @TypeParameter("E") Type type,
            @OperatorDependency(
                    operator = IS_DISTINCT_FROM,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {BLOCK_POSITION,
                            BLOCK_POSITION}, result = FAIL_ON_NULL)) BlockTypeOperators.BlockPositionIsDistinctFrom elementIsDistinctFrom,
            @OperatorDependency(
                    operator = HASH_CODE,
                    argumentTypes = "E",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL)) BlockTypeOperators.BlockPositionHashCode elementHashCode,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        Block smaller = rightArray;
        Block larger = leftArray;
        if (leftArray.getPositionCount() < rightArray.getPositionCount()) {
            smaller = leftArray;
            larger = rightArray;
        }

        int largerPositionCount = larger.getPositionCount();
        int smallerPositionCount = smaller.getPositionCount();

        if (largerPositionCount == 0 || smallerPositionCount == 0) {
            return false;
        }

        BlockSet smallerSet = new BlockSet(type, elementIsDistinctFrom, elementHashCode, smallerPositionCount);
        for (int position = 0; position < smallerPositionCount; position++) {
            smallerSet.add(smaller, position);
        }

        boolean largerContainsNull = false;
        for (int position = 0; position < largerPositionCount; position++) {
            if (larger.isNull(position)) {
                largerContainsNull = true;
                continue;
            }
            if (smallerSet.contains(larger, position)) {
                return true;
            }
        }
        return largerContainsNull || smallerSet.containsNullElement() ? null : false;
    }
}
