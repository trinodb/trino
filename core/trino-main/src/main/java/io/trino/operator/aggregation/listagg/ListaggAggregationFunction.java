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
package io.trino.operator.aggregation.listagg;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;

@AggregationFunction(value = "listagg", isOrderSensitive = true)
@Description("concatenates the input values with the specified separator")
public final class ListaggAggregationFunction
{
    private ListaggAggregationFunction() {}

    @InputFunction
    public static void input(
            @AggregationState ListaggAggregationState state,
            @BlockPosition @SqlType("VARCHAR") Block value,
            @BlockIndex int position,
            @SqlType("VARCHAR") Slice separator,
            @SqlType("BOOLEAN") boolean overflowError,
            @SqlType("VARCHAR") Slice overflowFiller,
            @SqlType("BOOLEAN") boolean showOverflowEntryCount)
    {
        state.initialize(separator, overflowError, overflowFiller, showOverflowEntryCount);
        state.add(value, position);
    }

    @CombineFunction
    public static void combine(@AggregationState ListaggAggregationState state, @AggregationState ListaggAggregationState otherState)
    {
        state.merge(otherState);
    }

    @OutputFunction("VARCHAR")
    public static void output(ListaggAggregationState state, BlockBuilder blockBuilder)
    {
        state.write((VariableWidthBlockBuilder) blockBuilder);
    }
}
