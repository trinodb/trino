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
package io.prestosql.operator.aggregation;

import io.airlift.stats.TDigest;
import io.prestosql.operator.aggregation.state.TDigestAndPercentileState;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.util.Failures.checkCondition;

@AggregationFunction("approx_percentile")
public final class ApproximateLongPercentileAggregations
{
    private ApproximateLongPercentileAggregations() {}

    @InputFunction
    public static void input(@AggregationState TDigestAndPercentileState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        ApproximateDoublePercentileAggregations.input(state, toDoubleExact(value), percentile);
    }

    @InputFunction
    public static void weightedInput(@AggregationState TDigestAndPercentileState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        ApproximateDoublePercentileAggregations.weightedInput(state, toDoubleExact(value), weight, percentile);
    }

    @CombineFunction
    public static void combine(@AggregationState TDigestAndPercentileState state, TDigestAndPercentileState otherState)
    {
        ApproximateDoublePercentileAggregations.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState TDigestAndPercentileState state, BlockBuilder out)
    {
        TDigest digest = state.getDigest();
        double percentile = state.getPercentile();
        if (digest == null || digest.getCount() == 0.0) {
            out.appendNull();
        }
        else {
            checkState(percentile != -1.0, "Percentile is missing");
            checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
            BIGINT.writeLong(out, Math.round(digest.valueAt(percentile)));
        }
    }

    public static double toDoubleExact(long value)
    {
        double doubleValue = (double) value;
        checkCondition((long) doubleValue == value, INVALID_FUNCTION_ARGUMENT, "no exact double representation for long: %s", value);
        return doubleValue;
    }
}
