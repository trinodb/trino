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

import static io.trino.spi.type.BigintType.BIGINT;

// merges histogram intermediates, which are maps from value to count
@AggregationFunction(value = "histogram$merge", hidden = true)
public final class HistogramMergeAggregation
{
    private HistogramMergeAggregation() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @AggregationState("T") HistogramState state,
            @SqlType("map(T, bigint)") SqlMap value)
    {
        int rawOffset = value.getRawOffset();
        Block rawKeyBlock = value.getRawKeyBlock();
        Block rawValueBlock = value.getRawValueBlock();

        ValueBlock rawKeyValues = rawKeyBlock.getUnderlyingValueBlock();
        for (int i = 0; i < value.getSize(); i++) {
            long count = BIGINT.getLong(rawValueBlock, rawOffset + i);
            state.add(rawKeyValues, rawKeyBlock.getUnderlyingValuePosition(rawOffset + i), count);
        }
    }

    @SqlNullable
    @OutputFunction(value = "map(T, BIGINT)", decomposition = @Decomposition(partial = "histogram$merge", output = "histogram$merge"))
    public static void output(@AggregationState("T") HistogramState state, BlockBuilder out)
    {
        state.writeAll((MapBlockBuilder) out);
    }
}
