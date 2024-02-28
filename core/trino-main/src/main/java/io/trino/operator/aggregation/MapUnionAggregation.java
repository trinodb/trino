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
package io.trino.operator.aggregation;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

@AggregationFunction("map_union")
@Description("Aggregate all the maps into a single map")
public final class MapUnionAggregation
{
    private MapUnionAggregation() {}

    @InputFunction
    @TypeParameter("K")
    @TypeParameter("V")
    public static void input(
            @TypeParameter("V") Type valueType,
            @AggregationState({"K", "V"}) MapAggregationState state,
            @SqlType("map(K,V)") SqlMap value)
    {
        int rawOffset = value.getRawOffset();
        Block rawKeyBlock = value.getRawKeyBlock();
        Block rawValueBlock = value.getRawValueBlock();

        ValueBlock rawKeyValues = rawKeyBlock.getUnderlyingValueBlock();
        ValueBlock rawValueValues = rawValueBlock.getUnderlyingValueBlock();
        for (int i = 0; i < value.getSize(); i++) {
            state.add(rawKeyValues, rawKeyBlock.getUnderlyingValuePosition(rawOffset + i), rawValueValues, rawValueBlock.getUnderlyingValuePosition(rawOffset + i));
        }
    }

    @CombineFunction
    public static void combine(
            @AggregationState({"K", "V"}) MapAggregationState state,
            @AggregationState({"K", "V"}) MapAggregationState otherState)
    {
        state.merge(otherState);
    }

    @OutputFunction("map(K, V)")
    public static void output(@AggregationState({"K", "V"}) MapAggregationState state, BlockBuilder out)
    {
        state.writeAll((MapBlockBuilder) out);
    }
}
