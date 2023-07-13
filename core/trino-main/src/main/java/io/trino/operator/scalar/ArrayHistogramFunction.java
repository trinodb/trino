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

import io.trino.operator.aggregation.histogram.SingleTypedHistogram;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlock;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;

@Description("Return a map containing the counts of the elements in the array")
@ScalarFunction(value = "array_histogram")
public final class ArrayHistogramFunction
{
    private static final int EXPECTED_HISTOGRAM_SIZE = 10;

    private ArrayHistogramFunction() {}

    @TypeParameter("T")
    @SqlType("map(T, bigint)")
    public static Block arrayHistogram(
            @TypeParameter("T") Type elementType,
            @OperatorDependency(
                    operator = OperatorType.EQUAL,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = NULLABLE_RETURN))
            BlockPositionEqual equalOperator,
            @OperatorDependency(
                    operator = OperatorType.HASH_CODE,
                    argumentTypes = "T",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL))
            BlockPositionHashCode hashCodeOperator,
            @TypeParameter("map(T, bigint)") MapType mapType,
            @SqlType("array(T)") Block arrayBlock)
    {
        SingleTypedHistogram histogram = new SingleTypedHistogram(
                elementType,
                equalOperator,
                hashCodeOperator,
                EXPECTED_HISTOGRAM_SIZE);
        int positionCount = arrayBlock.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            if (!arrayBlock.isNull(position)) {
                histogram.add(position, arrayBlock, 1L);
            }
        }
        BlockBuilder blockBuilder = mapType.createBlockBuilder(null, histogram.getPositionCount());
        histogram.serialize(blockBuilder);
        MapBlock mapBlock = (MapBlock) blockBuilder.build();
        return mapBlock.getObject(0, Block.class);
    }
}
