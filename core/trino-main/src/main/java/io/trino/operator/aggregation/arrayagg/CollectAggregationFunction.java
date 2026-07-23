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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
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

/// Collects the input values into a multiset. A multiset shares the array block representation, so
/// this reuses the array_agg accumulator; it differs only in that the result is a multiset and the
/// aggregation is not order sensitive.
@AggregationFunction("collect")
@Description("Collects the input values into a multiset")
public final class CollectAggregationFunction
{
    private CollectAggregationFunction() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @AggregationState("T") ArrayAggregationState state,
            @SqlNullable @BlockPosition @SqlType("T") ValueBlock value,
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

    @OutputFunction("multiset(T)")
    public static void output(
            @AggregationState("T") ArrayAggregationState state,
            BlockBuilder out)
    {
        if (state.isEmpty()) {
            out.appendNull();
        }
        else {
            ((ArrayBlockBuilder) out).buildEntry(state::writeAll);
        }
    }
}
