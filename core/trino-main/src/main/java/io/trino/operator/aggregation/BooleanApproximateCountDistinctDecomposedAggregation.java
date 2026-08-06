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

import io.trino.operator.aggregation.state.BooleanDistinctState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TinyintType.TINYINT;

// merges the boolean approx_distinct bit-set intermediate and computes the final count
@AggregationFunction
public final class BooleanApproximateCountDistinctDecomposedAggregation
{
    private BooleanApproximateCountDistinctDecomposedAggregation() {}

    @InputFunction
    public static void input(@AggregationState BooleanDistinctState state, @SqlType(StandardTypes.TINYINT) long value)
    {
        state.setByte((byte) (state.getByte() | value));
    }

    @AggregationFunction(value = "approx_distinct_boolean$merge", hidden = true)
    @OutputFunction(value = StandardTypes.TINYINT, decomposition = @Decomposition(partial = "approx_distinct_boolean$merge", output = "approx_distinct_boolean$merge"))
    public static void intermediateOutput(@AggregationState BooleanDistinctState state, BlockBuilder out)
    {
        TINYINT.writeByte(out, state.getByte());
    }

    @AggregationFunction(value = "approx_distinct_boolean$final", hidden = true)
    @OutputFunction(value = StandardTypes.BIGINT, decomposition = @Decomposition(partial = "approx_distinct_boolean$merge", output = "approx_distinct_boolean$final"))
    public static void output(@AggregationState BooleanDistinctState state, BlockBuilder out)
    {
        BIGINT.writeLong(out, Integer.bitCount(state.getByte()));
    }
}
