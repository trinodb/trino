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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.stats.QuantileDigest;
import io.trino.operator.aggregation.state.QuantileDigestAndPercentileState;
import io.trino.operator.aggregation.state.QuantileDigestAndPercentileStateSerializer;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.aggregation.FloatingPointBitsConverterUtil.sortableIntToFloat;
import static io.trino.operator.aggregation.FloatingPointBitsConverterUtil.sortableLongToDouble;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Float.floatToRawIntBits;

// the deprecated qdigest-based approx_percentile variants share a (qdigest, percentile)
// intermediate serialized as varbinary
@AggregationFunction
public final class LegacyApproximatePercentileDecomposedAggregation
{
    private static final QuantileDigestAndPercentileStateSerializer SERIALIZER = new QuantileDigestAndPercentileStateSerializer();

    private LegacyApproximatePercentileDecomposedAggregation() {}

    @InputFunction
    public static void bigintWeightedInput(@AggregationState QuantileDigestAndPercentileState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType(StandardTypes.DOUBLE) double percentile, @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        LegacyApproximateLongPercentileAggregations.weightedInput(state, value, weight, percentile, accuracy);
    }

    @InputFunction
    public static void doubleWeightedInput(@AggregationState QuantileDigestAndPercentileState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType(StandardTypes.DOUBLE) double percentile, @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        LegacyApproximateDoublePercentileAggregations.weightedInput(state, value, weight, percentile, accuracy);
    }

    @InputFunction
    public static void realWeightedInput(@AggregationState QuantileDigestAndPercentileState state, @SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType(StandardTypes.DOUBLE) double percentile, @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        LegacyApproximateRealPercentileAggregations.weightedInput(state, value, weight, percentile, accuracy);
    }

    // the intermediate layout must match QuantileDigestAndPercentileStateSerializer
    @InputFunction(hidden = true)
    public static void intermediateInput(@AggregationState QuantileDigestAndPercentileState state, @SqlType(StandardTypes.VARBINARY) Slice value)
    {
        SliceInput input = value.getInput();
        double percentile = input.readDouble();
        int length = input.readInt();
        QuantileDigest incoming = new QuantileDigest(input.readSlice(length));

        QuantileDigest previous = state.getDigest();
        if (previous == null) {
            state.setDigest(incoming);
            state.addMemoryUsage(incoming.estimatedInMemorySizeInBytes());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySizeInBytes());
            previous.merge(incoming);
            state.addMemoryUsage(previous.estimatedInMemorySizeInBytes());
        }
        state.setPercentile(percentile);
    }

    @AggregationFunction(value = "approx_percentile_legacy$intermediate", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.VARBINARY, decomposition = @Decomposition(partial = "approx_percentile_legacy$intermediate"))
    public static void intermediateOutput(@AggregationState QuantileDigestAndPercentileState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }

    @AggregationFunction(value = "approx_percentile_legacy_bigint$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.BIGINT, decomposition = @Decomposition(partial = "approx_percentile_legacy$intermediate", output = "approx_percentile_legacy_bigint$final"))
    public static void bigintOutput(@AggregationState QuantileDigestAndPercentileState state, BlockBuilder out)
    {
        QuantileDigest digest = state.getDigest();
        double percentile = state.getPercentile();
        if (digest == null || digest.getCount() == 0.0) {
            out.appendNull();
        }
        else {
            checkPercentile(percentile);
            BIGINT.writeLong(out, digest.getQuantile(percentile));
        }
    }

    @AggregationFunction(value = "approx_percentile_legacy_double$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.DOUBLE, decomposition = @Decomposition(partial = "approx_percentile_legacy$intermediate", output = "approx_percentile_legacy_double$final"))
    public static void doubleOutput(@AggregationState QuantileDigestAndPercentileState state, BlockBuilder out)
    {
        QuantileDigest digest = state.getDigest();
        double percentile = state.getPercentile();
        if (digest == null || digest.getCount() == 0.0) {
            out.appendNull();
        }
        else {
            checkPercentile(percentile);
            DOUBLE.writeDouble(out, sortableLongToDouble(digest.getQuantile(percentile)));
        }
    }

    @AggregationFunction(value = "approx_percentile_legacy_real$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.REAL, decomposition = @Decomposition(partial = "approx_percentile_legacy$intermediate", output = "approx_percentile_legacy_real$final"))
    public static void realOutput(@AggregationState QuantileDigestAndPercentileState state, BlockBuilder out)
    {
        QuantileDigest digest = state.getDigest();
        double percentile = state.getPercentile();
        if (digest == null || digest.getCount() == 0.0) {
            out.appendNull();
        }
        else {
            checkPercentile(percentile);
            REAL.writeLong(out, floatToRawIntBits(sortableIntToFloat((int) digest.getQuantile(percentile))));
        }
    }

    private static void checkPercentile(double percentile)
    {
        checkState(percentile != -1.0, "Percentile is missing");
        checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
    }
}
