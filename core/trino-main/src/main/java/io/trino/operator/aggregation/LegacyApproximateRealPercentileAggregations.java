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

import io.airlift.stats.QuantileDigest;
import io.trino.operator.aggregation.state.QuantileDigestAndPercentileState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.aggregation.FloatingPointBitsConverterUtil.floatToSortableInt;
import static io.trino.operator.aggregation.FloatingPointBitsConverterUtil.sortableIntToFloat;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("approx_percentile")
public final class LegacyApproximateRealPercentileAggregations
{
    private LegacyApproximateRealPercentileAggregations() {}

    // This function is deprecated. It uses QuantileDigest while other 'approx_percentile' functions use TDigest. TDigest does not accept the accuracy parameter.
    @Deprecated
    @Description("(DEPRECATED) Use approx_percentile(x, weight, percentile) instead")
    @InputFunction
    public static void weightedInput(@AggregationState QuantileDigestAndPercentileState state, @SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType(StandardTypes.DOUBLE) double percentile, @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        LegacyApproximateLongPercentileAggregations.weightedInput(state, floatToSortableInt(intBitsToFloat((int) value)), weight, percentile, accuracy);
    }

    @CombineFunction
    public static void combine(@AggregationState QuantileDigestAndPercentileState state, @AggregationState QuantileDigestAndPercentileState otherState)
    {
        LegacyApproximateLongPercentileAggregations.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.REAL)
    public static void output(@AggregationState QuantileDigestAndPercentileState state, BlockBuilder out)
    {
        QuantileDigest digest = state.getDigest();
        double percentile = state.getPercentile();
        if (digest == null || digest.getCount() == 0.0) {
            out.appendNull();
        }
        else {
            checkState(percentile != -1.0, "Percentile is missing");
            checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
            REAL.writeLong(out, floatToRawIntBits(sortableIntToFloat((int) digest.getQuantile(percentile))));
        }
    }
}
