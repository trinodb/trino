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
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import static io.trino.operator.scalar.BlockSet.MAX_FUNCTION_MEMORY;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.type.BigintType.BIGINT;

@ScalarFunction("array_distinct")
@Description("Remove duplicate values from the given array")
public final class ArrayDistinctFunction
{
    public static final String NAME = "array_distinct";
    private final BufferedArrayValueBuilder arrayValueBuilder;

    @TypeParameter("E")
    public ArrayDistinctFunction(@TypeParameter("E") Type elementType)
    {
        arrayValueBuilder = BufferedArrayValueBuilder.createBuffered(new ArrayType(elementType));
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block distinct(
            @TypeParameter("E") Type type,
            @OperatorDependency(
                    operator = IS_DISTINCT_FROM,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL)) BlockPositionIsDistinctFrom elementIsDistinctFrom,
            @OperatorDependency(
                    operator = HASH_CODE,
                    argumentTypes = "E",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL)) BlockPositionHashCode elementHashCode,
            @SqlType("array(E)") Block array)
    {
        if (array.getPositionCount() < 2) {
            return array;
        }

        if (array.getPositionCount() == 2) {
            boolean distinct = elementIsDistinctFrom.isDistinctFrom(array, 0, array, 1);
            if (distinct) {
                return array;
            }
            return array.getSingleValueBlock(0);
        }

        BlockSet distinctElements = new BlockSet(
                type,
                elementIsDistinctFrom,
                elementHashCode,
                array.getPositionCount());

        for (int i = 0; i < array.getPositionCount(); i++) {
            distinctElements.add(array, i);
        }

        return arrayValueBuilder.build(
                distinctElements.size(),
                blockBuilder -> distinctElements.getAllWithSizeLimit(blockBuilder, "array_distinct", MAX_FUNCTION_MEMORY));
    }

    @SqlType("array(bigint)")
    public Block bigintDistinct(@SqlType("array(bigint)") Block array)
    {
        if (array.getPositionCount() == 0) {
            return array;
        }

        return arrayValueBuilder.build(array.getPositionCount(), distinctElementBlockBuilder -> {
            boolean containsNull = false;
            LongSet set = new LongOpenHashSet(array.getPositionCount());

            for (int i = 0; i < array.getPositionCount(); i++) {
                if (array.isNull(i)) {
                    if (!containsNull) {
                        containsNull = true;
                        distinctElementBlockBuilder.appendNull();
                    }
                    continue;
                }
                long value = BIGINT.getLong(array, i);
                if (set.add(value)) {
                    BIGINT.writeLong(distinctElementBlockBuilder, value);
                }
            }
        });
    }
}
