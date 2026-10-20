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
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.type.BlockTypeOperators.BlockPositionComparison;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;

@ScalarFunction("array_is_sorted")
@Description("Returns whether the array is sorted in ascending order, with nulls last")
public final class ArrayIsSortedFunction
{
    private ArrayIsSortedFunction() {}

    @TypeParameter("E")
    @SqlType("boolean")
    public static boolean isSorted(
            @OperatorDependency(
                    operator = COMPARISON_UNORDERED_LAST,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL}, result = FAIL_ON_NULL))
            BlockPositionComparison comparisonOperator,
            @SqlType("array(E)") Block block)
    {
        for (int position = 1; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                continue;
            }
            if (block.isNull(position - 1) || comparisonOperator.compare(block, position - 1, block, position) > 0) {
                return false;
            }
        }
        return true;
    }
}
