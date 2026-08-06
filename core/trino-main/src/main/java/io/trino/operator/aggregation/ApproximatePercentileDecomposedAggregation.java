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
import io.airlift.stats.TDigest;
import io.trino.operator.aggregation.state.TDigestAndPercentileState;
import io.trino.operator.aggregation.state.TDigestAndPercentileStateSerializer;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.operator.aggregation.ApproximateLongPercentileAggregations.toDoubleExact;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

// all scalar approx_percentile variants share a (tdigest, percentile) intermediate serialized as
// varbinary, so the partials and finals for every input type live in this single class
@AggregationFunction
public final class ApproximatePercentileDecomposedAggregation
{
    private static final TDigestAndPercentileStateSerializer SERIALIZER = new TDigestAndPercentileStateSerializer();

    private ApproximatePercentileDecomposedAggregation() {}

    @InputFunction
    public static void doubleInput(@AggregationState TDigestAndPercentileState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        ApproximateDoublePercentileAggregations.input(state, value, percentile);
    }

    @InputFunction
    public static void doubleWeightedInput(@AggregationState TDigestAndPercentileState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        ApproximateDoublePercentileAggregations.weightedInput(state, value, weight, percentile);
    }

    @InputFunction
    public static void bigintInput(@AggregationState TDigestAndPercentileState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        ApproximateDoublePercentileAggregations.input(state, toDoubleExact(value), percentile);
    }

    @InputFunction
    public static void bigintWeightedInput(@AggregationState TDigestAndPercentileState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        ApproximateDoublePercentileAggregations.weightedInput(state, toDoubleExact(value), weight, percentile);
    }

    @InputFunction
    public static void realInput(@AggregationState TDigestAndPercentileState state, @SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        ApproximateDoublePercentileAggregations.input(state, intBitsToFloat((int) value), percentile);
    }

    @InputFunction
    public static void realWeightedInput(@AggregationState TDigestAndPercentileState state, @SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        ApproximateDoublePercentileAggregations.weightedInput(state, intBitsToFloat((int) value), weight, percentile);
    }

    // the intermediate layout must match TDigestAndPercentileStateSerializer
    @InputFunction(hidden = true)
    public static void intermediateInput(@AggregationState TDigestAndPercentileState state, @SqlType(StandardTypes.VARBINARY) Slice value)
    {
        SliceInput input = value.getInput();
        double percentile = input.readDouble();
        int length = input.readInt();
        TDigest incoming = TDigest.deserialize(input.readSlice(length));

        TDigest previous = state.getDigest();
        if (previous == null) {
            state.setDigest(incoming);
            state.addMemoryUsage(incoming.estimatedInMemorySizeInBytes());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySizeInBytes());
            previous.mergeWith(incoming);
            state.addMemoryUsage(previous.estimatedInMemorySizeInBytes());
        }
        state.setPercentile(percentile);
    }

    @AggregationFunction(value = "approx_percentile$intermediate", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.VARBINARY, decomposition = @Decomposition(partial = "approx_percentile$intermediate"))
    public static void intermediateOutput(@AggregationState TDigestAndPercentileState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }

    @AggregationFunction(value = "approx_percentile_bigint$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.BIGINT, decomposition = @Decomposition(partial = "approx_percentile$intermediate", output = "approx_percentile_bigint$final"))
    public static void bigintOutput(@AggregationState TDigestAndPercentileState state, BlockBuilder out)
    {
        TDigest digest = state.getDigest();
        double percentile = state.getPercentile();
        if (digest == null || digest.getCount() == 0.0) {
            out.appendNull();
        }
        else {
            checkPercentile(percentile);
            BIGINT.writeLong(out, Math.round(digest.valueAt(percentile)));
        }
    }

    @AggregationFunction(value = "approx_percentile_double$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.DOUBLE, decomposition = @Decomposition(partial = "approx_percentile$intermediate", output = "approx_percentile_double$final"))
    public static void doubleOutput(@AggregationState TDigestAndPercentileState state, BlockBuilder out)
    {
        TDigest digest = state.getDigest();
        double percentile = state.getPercentile();
        if (digest == null || digest.getCount() == 0.0) {
            out.appendNull();
        }
        else {
            checkPercentile(percentile);
            DOUBLE.writeDouble(out, digest.valueAt(percentile));
        }
    }

    @AggregationFunction(value = "approx_percentile_real$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.REAL, decomposition = @Decomposition(partial = "approx_percentile$intermediate", output = "approx_percentile_real$final"))
    public static void realOutput(@AggregationState TDigestAndPercentileState state, BlockBuilder out)
    {
        TDigest digest = state.getDigest();
        double percentile = state.getPercentile();
        if (digest == null || digest.getCount() == 0.0) {
            out.appendNull();
        }
        else {
            checkPercentile(percentile);
            REAL.writeLong(out, floatToRawIntBits((float) digest.valueAt(percentile)));
        }
    }

    private static void checkPercentile(double percentile)
    {
        checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
    }
}
