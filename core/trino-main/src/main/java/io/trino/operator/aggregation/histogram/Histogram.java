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
package io.trino.operator.aggregation.histogram;

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
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

@AggregationFunction("histogram")
@Description("Count the number of times each value occurs")
public final class Histogram
{
    private Histogram() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @TypeParameter("T") Type type,
            @AggregationState("T") HistogramState state,
            @BlockPosition @SqlType("T") Block key,
            @BlockIndex int position)
    {
        state.add(key, position, 1L);
    }

    @CombineFunction
    public static void combine(@AggregationState("T") HistogramState state, @AggregationState("T") HistogramState otherState)
    {
        state.merge(otherState);
    }

    @OutputFunction("map(T, BIGINT)")
    public static void output(@TypeParameter("T") Type type, @AggregationState("T") HistogramState state, BlockBuilder out)
    {
        state.writeAll((MapBlockBuilder) out);
    }
}
