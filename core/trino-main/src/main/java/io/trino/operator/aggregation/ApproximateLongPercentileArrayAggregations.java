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

import io.airlift.stats.TDigest;
import io.trino.operator.aggregation.state.TDigestAndPercentileArrayState;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.List;

import static io.trino.operator.aggregation.ApproximateDoublePercentileArrayAggregations.valuesAtPercentiles;
import static io.trino.operator.aggregation.ApproximateLongPercentileAggregations.toDoubleExact;
import static io.trino.spi.type.BigintType.BIGINT;

@AggregationFunction("approx_percentile")
public final class ApproximateLongPercentileArrayAggregations
{
    private ApproximateLongPercentileArrayAggregations() {}

    @InputFunction
    public static void input(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateDoublePercentileArrayAggregations.input(state, toDoubleExact(value), percentilesArrayBlock);
    }

    @InputFunction
    public static void weightedInput(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateDoublePercentileArrayAggregations.weightedInput(state, toDoubleExact(value), weight, percentilesArrayBlock);
    }

    @CombineFunction
    public static void combine(@AggregationState TDigestAndPercentileArrayState state, TDigestAndPercentileArrayState otherState)
    {
        ApproximateDoublePercentileArrayAggregations.combine(state, otherState);
    }

    @OutputFunction("array(bigint)")
    public static void output(@AggregationState TDigestAndPercentileArrayState state, BlockBuilder out)
    {
        TDigest digest = state.getDigest();
        List<Double> percentiles = state.getPercentiles();

        if (percentiles == null || digest == null) {
            out.appendNull();
            return;
        }

        ((ArrayBlockBuilder) out).buildEntry(elementBuilder -> {
            List<Double> valuesAtPercentiles = valuesAtPercentiles(digest, percentiles);
            for (double value : valuesAtPercentiles) {
                BIGINT.writeLong(elementBuilder, Math.round(value));
            }
        });
    }
}
