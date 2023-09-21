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
package io.trino.operator.aggregation.arrayagg;

import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

@AggregationFunction(value = "array_agg", isOrderSensitive = true)
@Description("return an array of values")
public final class ArrayAggregationFunction
{
    private ArrayAggregationFunction() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @AggregationState("T") ArrayAggregationState state,
            @SqlNullable @BlockPosition @SqlType("T") Block value,
            @BlockIndex int position)
    {
        state.add(value, position);
    }

    @CombineFunction
    public static void combine(
            @AggregationState("T") ArrayAggregationState state,
            @AggregationState("T") ArrayAggregationState otherState)
    {
        state.merge(otherState);
    }

    @OutputFunction("array(T)")
    public static void output(
            @TypeParameter("T") Type elementType,
            @AggregationState("T") ArrayAggregationState state,
            BlockBuilder out)
    {
        if (state.isEmpty()) {
            out.appendNull();
        }
        else {
            ((ArrayBlockBuilder) out).buildEntry(elementBuilder -> state.forEach((block, position) -> elementType.appendTo(block, position, elementBuilder)));
        }
    }
}
