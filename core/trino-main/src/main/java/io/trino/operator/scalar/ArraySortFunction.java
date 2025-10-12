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
import io.trino.spi.block.BufferedArrayValueBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionComparison;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;

@ScalarFunction("array_sort")
@Description("Sorts the given array in ascending order according to the natural ordering of its elements.")
public final class ArraySortFunction
{
    public static final String NAME = "array_sort";
    private final BufferedArrayValueBuilder arrayValueBuilder;
    private static final int INITIAL_LENGTH = 128;
    private final IntArrayList positions = new IntArrayList(INITIAL_LENGTH);

    @TypeParameter("E")
    public ArraySortFunction(@TypeParameter("E") Type elementType)
    {
        arrayValueBuilder = BufferedArrayValueBuilder.createBuffered(new ArrayType(elementType));
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block sort(
            @OperatorDependency(
                    operator = COMPARISON_UNORDERED_LAST,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL}, result = FAIL_ON_NULL)) BlockPositionComparison comparisonOperator,
            @SqlType("array(E)") Block block)
    {
        int arrayLength = block.getPositionCount();
        positions.clear();
        for (int i = 0; i < arrayLength; i++) {
            positions.add(i);
        }

        positions.subList(0, arrayLength).sort((left, right) -> {
            boolean nullLeft = block.isNull(left);
            boolean nullRight = block.isNull(right);
            if (nullLeft && nullRight) {
                return 0;
            }
            if (nullLeft) {
                return 1;
            }
            if (nullRight) {
                return -1;
            }

            return (int) comparisonOperator.compare(block, left, block, right);
        });

        return arrayValueBuilder.build(arrayLength, elementBuilder -> {
            ValueBlock valueBlock = block.getUnderlyingValueBlock();
            for (int i = 0; i < arrayLength; i++) {
                elementBuilder.append(valueBlock, block.getUnderlyingValuePosition(positions.getInt(i)));
            }
        });
    }
}
