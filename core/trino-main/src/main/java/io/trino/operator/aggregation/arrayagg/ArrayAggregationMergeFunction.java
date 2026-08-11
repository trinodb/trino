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
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

// merges array_agg intermediates by concatenating the collected values
@AggregationFunction(value = "array_agg$merge", isOrderSensitive = true, hidden = true)
public final class ArrayAggregationMergeFunction
{
    private ArrayAggregationMergeFunction() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @AggregationState("T") ArrayAggregationState state,
            @SqlType("array(T)") Block value)
    {
        ValueBlock elements = value.getUnderlyingValueBlock();
        for (int i = 0; i < value.getPositionCount(); i++) {
            state.add(elements, value.getUnderlyingValuePosition(i));
        }
    }

    @SqlNullable
    @OutputFunction(value = "array(T)", decomposition = @Decomposition(partial = "array_agg$merge"))
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
