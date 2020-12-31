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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Convention;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.OperatorDependency;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.Type;
import io.prestosql.type.BlockTypeOperators.BlockPositionComparison;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.function.OperatorType.COMPARISON;

@ScalarFunction("array_sort")
@Description("Sorts the given array in ascending order according to the natural ordering of its elements.")
public final class ArraySortFunction
{
    private final PageBuilder pageBuilder;
    private static final int INITIAL_LENGTH = 128;
    private final IntArrayList positions = new IntArrayList(INITIAL_LENGTH);

    @TypeParameter("E")
    public ArraySortFunction(@TypeParameter("E") Type elementType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(elementType));
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block sort(
            @OperatorDependency(
                    operator = COMPARISON,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL)) BlockPositionComparison comparisonOperator,
            @TypeParameter("E") Type type,
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

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);

        for (int i = 0; i < arrayLength; i++) {
            type.appendTo(block, positions.getInt(i), blockBuilder);
        }
        pageBuilder.declarePositions(arrayLength);

        return blockBuilder.getRegion(blockBuilder.getPositionCount() - arrayLength, arrayLength);
    }
}
