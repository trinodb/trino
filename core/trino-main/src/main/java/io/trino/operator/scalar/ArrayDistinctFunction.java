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

import com.google.common.collect.ImmutableList;
import io.trino.operator.aggregation.TypedSet;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import static io.trino.operator.aggregation.TypedSet.createDistinctTypedSet;
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
    private final PageBuilder pageBuilder;

    @TypeParameter("E")
    public ArrayDistinctFunction(@TypeParameter("E") Type elementType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(elementType));
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

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        BlockBuilder distinctElementsBlockBuilder = pageBuilder.getBlockBuilder(0);
        TypedSet distinctElements = createDistinctTypedSet(
                type,
                elementIsDistinctFrom,
                elementHashCode,
                distinctElementsBlockBuilder,
                array.getPositionCount(),
                "array_distinct");

        for (int i = 0; i < array.getPositionCount(); i++) {
            distinctElements.add(array, i);
        }

        pageBuilder.declarePositions(distinctElements.size());
        return distinctElementsBlockBuilder.getRegion(
                distinctElementsBlockBuilder.getPositionCount() - distinctElements.size(),
                distinctElements.size());
    }

    @SqlType("array(bigint)")
    public Block bigintDistinct(@SqlType("array(bigint)") Block array)
    {
        if (array.getPositionCount() == 0) {
            return array;
        }

        boolean containsNull = false;
        LongSet set = new LongOpenHashSet(array.getPositionCount());
        int distinctCount = 0;

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        BlockBuilder distinctElementBlockBuilder = pageBuilder.getBlockBuilder(0);
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (array.isNull(i)) {
                if (!containsNull) {
                    containsNull = true;
                    distinctElementBlockBuilder.appendNull();
                    distinctCount++;
                }
                continue;
            }
            long value = BIGINT.getLong(array, i);
            if (!set.contains(value)) {
                set.add(value);
                distinctCount++;
                BIGINT.appendTo(array, i, distinctElementBlockBuilder);
            }
        }

        pageBuilder.declarePositions(distinctCount);

        return distinctElementBlockBuilder.getRegion(
                distinctElementBlockBuilder.getPositionCount() - distinctCount,
                distinctCount);
    }
}
