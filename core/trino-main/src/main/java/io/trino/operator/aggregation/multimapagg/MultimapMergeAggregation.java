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

import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

// merges multimap_agg intermediates, which are maps from key to the collected values
@AggregationFunction(value = "multimap_agg$merge", isOrderSensitive = true, hidden = true)
public final class MultimapMergeAggregation
{
    private MultimapMergeAggregation() {}

    @InputFunction
    @TypeParameter("K")
    @TypeParameter("V")
    public static void input(
            @AggregationState({"K", "V"}) MultimapAggregationState state,
            @SqlType("map(K, array(V))") SqlMap value)
    {
        int rawOffset = value.getRawOffset();
        Block rawKeyBlock = value.getRawKeyBlock();
        Block rawValueBlock = value.getRawValueBlock();

        ValueBlock rawKeyValues = rawKeyBlock.getUnderlyingValueBlock();
        ArrayBlock rawValueValues = (ArrayBlock) rawValueBlock.getUnderlyingValueBlock();
        for (int i = 0; i < value.getSize(); i++) {
            int keyPosition = rawKeyBlock.getUnderlyingValuePosition(rawOffset + i);
            Block values = rawValueValues.getArray(rawValueBlock.getUnderlyingValuePosition(rawOffset + i));
            ValueBlock valueElements = values.getUnderlyingValueBlock();
            for (int j = 0; j < values.getPositionCount(); j++) {
                state.add(rawKeyValues, keyPosition, valueElements, values.getUnderlyingValuePosition(j));
            }
        }
    }

    @SqlNullable
    @OutputFunction(value = "map(K, array(V))", decomposition = @Decomposition(partial = "multimap_agg$merge", output = "multimap_agg$merge"))
    public static void output(@AggregationState({"K", "V"}) MultimapAggregationState state, BlockBuilder out)
    {
        state.writeAll((MapBlockBuilder) out);
    }
}
