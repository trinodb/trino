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

import io.trino.operator.aggregation.histogram.TypedHistogram;
import io.trino.spi.block.Block;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;

@Description("Return a map containing the counts of the elements in the array")
@ScalarFunction(value = "array_histogram")
public final class ArrayHistogramFunction
{
    private ArrayHistogramFunction() {}

    @TypeParameter("T")
    @SqlType("map(T, bigint)")
    public static Block arrayHistogram(
            @TypeParameter("T") Type elementType,
            @OperatorDependency(
                    operator = OperatorType.READ_VALUE,
                    argumentTypes = "T",
                    convention = @Convention(arguments = FLAT, result = BLOCK_BUILDER)) MethodHandle readFlat,
            @OperatorDependency(
                    operator = OperatorType.READ_VALUE,
                    argumentTypes = "T",
                    convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = FLAT_RETURN)) MethodHandle writeFlat,
            @OperatorDependency(
                    operator = OperatorType.HASH_CODE,
                    argumentTypes = "T",
                    convention = @Convention(arguments = FLAT, result = FAIL_ON_NULL)) MethodHandle hashFlat,
            @OperatorDependency(
                    operator = OperatorType.IS_DISTINCT_FROM,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {FLAT, BLOCK_POSITION_NOT_NULL}, result = FAIL_ON_NULL)) MethodHandle distinctFlatBlock,
            @OperatorDependency(
                    operator = OperatorType.HASH_CODE,
                    argumentTypes = "T",
                    convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = FAIL_ON_NULL)) MethodHandle hashBlock,
            @TypeParameter("map(T, bigint)") MapType mapType,
            @SqlType("array(T)") Block arrayBlock)
    {
        TypedHistogram histogram = new TypedHistogram(elementType, readFlat, writeFlat, hashFlat, distinctFlatBlock, hashBlock, false);
        int positionCount = arrayBlock.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            if (!arrayBlock.isNull(position)) {
                histogram.add(0, arrayBlock, position, 1L);
            }
        }
        MapBlockBuilder blockBuilder = mapType.createBlockBuilder(null, histogram.size());
        histogram.serialize(0, blockBuilder);
        MapBlock mapBlock = (MapBlock) blockBuilder.build();
        return mapBlock.getObject(0, Block.class);
    }
}
