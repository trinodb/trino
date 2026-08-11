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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.stats.TDigest;
import io.trino.operator.aggregation.state.TDigestAndPercentileArrayState;
import io.trino.operator.aggregation.state.TDigestAndPercentileArrayStateSerializer;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.List;

import static io.trino.operator.aggregation.ApproximateDoublePercentileArrayAggregations.valuesAtPercentiles;
import static io.trino.operator.aggregation.ApproximateLongPercentileAggregations.toDoubleExact;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

// all array approx_percentile variants share a (tdigest, percentiles) intermediate serialized as
// varbinary, so the partials and finals for every input type live in this single class
@AggregationFunction
public final class ApproximatePercentileArrayDecomposedAggregation
{
    private static final TDigestAndPercentileArrayStateSerializer SERIALIZER = new TDigestAndPercentileArrayStateSerializer();

    private ApproximatePercentileArrayDecomposedAggregation() {}

    @InputFunction
    public static void doubleInput(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateDoublePercentileArrayAggregations.input(state, value, percentilesArrayBlock);
    }

    @InputFunction
    public static void doubleWeightedInput(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateDoublePercentileArrayAggregations.weightedInput(state, value, weight, percentilesArrayBlock);
    }

    @InputFunction
    public static void bigintInput(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateDoublePercentileArrayAggregations.input(state, toDoubleExact(value), percentilesArrayBlock);
    }

    @InputFunction
    public static void bigintWeightedInput(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateDoublePercentileArrayAggregations.weightedInput(state, toDoubleExact(value), weight, percentilesArrayBlock);
    }

    @InputFunction
    public static void realInput(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.REAL) long value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateDoublePercentileArrayAggregations.input(state, intBitsToFloat((int) value), percentilesArrayBlock);
    }

    @InputFunction
    public static void realWeightedInput(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateDoublePercentileArrayAggregations.weightedInput(state, intBitsToFloat((int) value), weight, percentilesArrayBlock);
    }

    // the intermediate layout must match TDigestAndPercentileArrayStateSerializer
    @InputFunction(hidden = true)
    public static void intermediateInput(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.VARBINARY) Slice value)
    {
        SliceInput input = value.getInput();
        int percentileCount = input.readInt();
        ImmutableList.Builder<Double> percentiles = ImmutableList.builderWithExpectedSize(percentileCount);
        for (int i = 0; i < percentileCount; i++) {
            percentiles.add(input.readDouble());
        }
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
        state.setPercentiles(percentiles.build());
    }

    @AggregationFunction(value = "approx_percentile_array$intermediate", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.VARBINARY, decomposition = @Decomposition(partial = "approx_percentile_array$intermediate"))
    public static void intermediateOutput(@AggregationState TDigestAndPercentileArrayState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }

    @AggregationFunction(value = "approx_percentile_array_bigint$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = "array(bigint)", decomposition = @Decomposition(partial = "approx_percentile_array$intermediate", output = "approx_percentile_array_bigint$final"))
    public static void bigintOutput(@AggregationState TDigestAndPercentileArrayState state, BlockBuilder out)
    {
        TDigest digest = state.getDigest();
        List<Double> percentiles = state.getPercentiles();
        if (percentiles == null || digest == null) {
            out.appendNull();
            return;
        }
        List<Double> valuesAtPercentiles = valuesAtPercentiles(digest, percentiles);
        ((ArrayBlockBuilder) out).buildEntry(elementBuilder -> {
            for (double value : valuesAtPercentiles) {
                BIGINT.writeLong(elementBuilder, Math.round(value));
            }
        });
    }

    @AggregationFunction(value = "approx_percentile_array_double$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = "array(double)", decomposition = @Decomposition(partial = "approx_percentile_array$intermediate", output = "approx_percentile_array_double$final"))
    public static void doubleOutput(@AggregationState TDigestAndPercentileArrayState state, BlockBuilder out)
    {
        TDigest digest = state.getDigest();
        List<Double> percentiles = state.getPercentiles();
        if (percentiles == null || digest == null) {
            out.appendNull();
            return;
        }
        List<Double> valuesAtPercentiles = valuesAtPercentiles(digest, percentiles);
        ((ArrayBlockBuilder) out).buildEntry(elementBuilder -> {
            for (double value : valuesAtPercentiles) {
                DOUBLE.writeDouble(elementBuilder, value);
            }
        });
    }

    @AggregationFunction(value = "approx_percentile_array_real$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = "array(real)", decomposition = @Decomposition(partial = "approx_percentile_array$intermediate", output = "approx_percentile_array_real$final"))
    public static void realOutput(@AggregationState TDigestAndPercentileArrayState state, BlockBuilder out)
    {
        TDigest digest = state.getDigest();
        List<Double> percentiles = state.getPercentiles();
        if (percentiles == null || digest == null) {
            out.appendNull();
            return;
        }
        List<Double> valuesAtPercentiles = valuesAtPercentiles(digest, percentiles);
        ((ArrayBlockBuilder) out).buildEntry(elementBuilder -> {
            for (double value : valuesAtPercentiles) {
                REAL.writeLong(elementBuilder, floatToRawIntBits((float) value));
            }
        });
    }
}
