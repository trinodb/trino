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
package io.trino.operator.aggregation.multimapagg;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
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

@AggregationFunction(value = "multimap_agg", isOrderSensitive = true)
@Description("Aggregates all the rows (key/value pairs) into a single multimap")
public final class MultimapAggregationFunction
{
    private MultimapAggregationFunction() {}

    @InputFunction
    @TypeParameter("K")
    @TypeParameter("V")
    public static void input(
            @AggregationState({"K", "V"}) MultimapAggregationState state,
            @BlockPosition @SqlType("K") Block key,
            @BlockIndex int keyPosition,
            @SqlNullable @BlockPosition @SqlType("V") Block value,
            @BlockIndex int valuePosition)
    {
        state.add(key, keyPosition, value, valuePosition);
    }

    @CombineFunction
    public static void combine(
            @AggregationState({"K", "V"}) MultimapAggregationState state,
            @AggregationState({"K", "V"}) MultimapAggregationState otherState)
    {
        state.merge(otherState);
    }

    @OutputFunction("map(K, array(V))")
    public static void output(@AggregationState({"K", "V"}) MultimapAggregationState state, BlockBuilder out)
    {
        state.writeAll((MapBlockBuilder) out);
    }
}
