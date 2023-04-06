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

import io.trino.operator.aggregation.state.LongState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;

import static java.lang.Math.max;

@AggregationFunction(value = MaxDataSizeForStats.NAME, hidden = true)
public final class MaxDataSizeForStats
{
    public static final String NAME = "$internal$max_data_size_for_stats";

    private MaxDataSizeForStats() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(@AggregationState LongState state, @NullablePosition @BlockPosition @SqlType("T") Block block, @BlockIndex int index)
    {
        update(state, block.getEstimatedDataSizeForStats(index));
    }

    @CombineFunction
    public static void combine(@AggregationState LongState state, @AggregationState LongState otherState)
    {
        update(state, otherState.getValue());
    }

    private static void update(LongState state, long size)
    {
        state.setValue(max(state.getValue(), size));
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState LongState state, BlockBuilder out)
    {
        BigintType.BIGINT.writeLong(out, state.getValue());
    }
}
