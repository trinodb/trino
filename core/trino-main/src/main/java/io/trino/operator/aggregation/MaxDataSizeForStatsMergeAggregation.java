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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;

import static java.lang.Math.max;

// merges $internal$max_data_size_for_stats partial results, preserving the 0 result on empty input
@AggregationFunction(value = "$internal$max_data_size_for_stats$merge", hidden = true)
public final class MaxDataSizeForStatsMergeAggregation
{
    private MaxDataSizeForStatsMergeAggregation() {}

    @InputFunction
    public static void input(@AggregationState LongState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        state.setValue(max(state.getValue(), value));
    }

    @OutputFunction(value = StandardTypes.BIGINT, decomposition = @Decomposition(partial = "$internal$max_data_size_for_stats$merge", output = "$internal$max_data_size_for_stats$merge"))
    public static void output(@AggregationState LongState state, BlockBuilder out)
    {
        BigintType.BIGINT.writeLong(out, state.getValue());
    }
}
