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

import static io.trino.operator.aggregation.TypedSet.createDistinctTypedSet;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;

@ScalarFunction("array_intersect")
@Description("Intersects elements of the two given arrays")
public final class ArrayIntersectFunction
{
    private final PageBuilder pageBuilder;

    @TypeParameter("E")
    public ArrayIntersectFunction(@TypeParameter("E") Type elementType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(elementType));
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block intersect(
            @TypeParameter("E") Type type,
            @OperatorDependency(
                    operator = IS_DISTINCT_FROM,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL)) BlockPositionIsDistinctFrom elementIsDistinctFrom,
            @OperatorDependency(
                    operator = HASH_CODE,
                    argumentTypes = "E",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL)) BlockPositionHashCode elementHashCode,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        if (leftArray.getPositionCount() < rightArray.getPositionCount()) {
            Block tempArray = leftArray;
            leftArray = rightArray;
            rightArray = tempArray;
        }

        int leftPositionCount = leftArray.getPositionCount();
        int rightPositionCount = rightArray.getPositionCount();

        if (rightPositionCount == 0) {
            return rightArray;
        }

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        TypedSet rightTypedSet = createDistinctTypedSet(type, elementIsDistinctFrom, elementHashCode, rightPositionCount, "array_intersect");
        for (int i = 0; i < rightPositionCount; i++) {
            rightTypedSet.add(rightArray, i);
        }

        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);

        // The intersected set can have at most rightPositionCount elements
        TypedSet intersectTypedSet = createDistinctTypedSet(type, elementIsDistinctFrom, elementHashCode, blockBuilder, rightPositionCount, "array_intersect");
        for (int i = 0; i < leftPositionCount; i++) {
            if (rightTypedSet.contains(leftArray, i)) {
                intersectTypedSet.add(leftArray, i);
            }
        }

        pageBuilder.declarePositions(intersectTypedSet.size());

        return blockBuilder.getRegion(blockBuilder.getPositionCount() - intersectTypedSet.size(), intersectTypedSet.size());
    }
}
