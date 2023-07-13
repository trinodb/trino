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
import com.google.common.primitives.Doubles;
import io.airlift.stats.TDigest;
import io.trino.operator.aggregation.state.TDigestAndPercentileArrayState;
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

import static io.trino.operator.scalar.TDigestFunctions.verifyWeight;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.util.Failures.checkCondition;

@AggregationFunction("approx_percentile")
public final class ApproximateDoublePercentileArrayAggregations
{
    private ApproximateDoublePercentileArrayAggregations() {}

    @InputFunction
    public static void input(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        initializePercentilesArray(state, percentilesArrayBlock);
        initializeDigest(state);

        TDigest digest = state.getDigest();
        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
    }

    @InputFunction
    public static void weightedInput(@AggregationState TDigestAndPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        verifyWeight(weight);

        initializePercentilesArray(state, percentilesArrayBlock);
        initializeDigest(state);

        TDigest digest = state.getDigest();
        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value, weight);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
    }

    @CombineFunction
    public static void combine(@AggregationState TDigestAndPercentileArrayState state, TDigestAndPercentileArrayState otherState)
    {
        TDigest otherDigest = otherState.getDigest();
        TDigest digest = state.getDigest();

        if (digest == null) {
            state.setDigest(otherDigest);
            state.addMemoryUsage(otherDigest.estimatedInMemorySizeInBytes());
        }
        else {
            state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
            digest.mergeWith(otherDigest);
            state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
        }

        state.setPercentiles(otherState.getPercentiles());
    }

    @OutputFunction("array(double)")
    public static void output(@AggregationState TDigestAndPercentileArrayState state, BlockBuilder out)
    {
        TDigest digest = state.getDigest();
        List<Double> percentiles = state.getPercentiles();

        if (percentiles == null || digest == null) {
            out.appendNull();
            return;
        }

        BlockBuilder blockBuilder = out.beginBlockEntry();

        List<Double> valuesAtPercentiles = valuesAtPercentiles(digest, percentiles);
        for (double value : valuesAtPercentiles) {
            DOUBLE.writeDouble(blockBuilder, value);
        }

        out.closeEntry();
    }

    public static List<Double> valuesAtPercentiles(TDigest digest, List<Double> percentiles)
    {
        int[] indexes = new int[percentiles.size()];
        double[] sortedPercentiles = new double[percentiles.size()];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = i;
            sortedPercentiles[i] = percentiles.get(i);
        }

        it.unimi.dsi.fastutil.Arrays.quickSort(0, percentiles.size(), (a, b) -> Doubles.compare(sortedPercentiles[a], sortedPercentiles[b]), (a, b) -> {
            double tempPercentile = sortedPercentiles[a];
            sortedPercentiles[a] = sortedPercentiles[b];
            sortedPercentiles[b] = tempPercentile;

            int tempIndex = indexes[a];
            indexes[a] = indexes[b];
            indexes[b] = tempIndex;
        });

        double[] valuesAtPercentiles = digest.valuesAt(sortedPercentiles);
        double[] result = new double[valuesAtPercentiles.length];
        for (int i = 0; i < valuesAtPercentiles.length; i++) {
            result[indexes[i]] = valuesAtPercentiles[i];
        }

        return Doubles.asList(result);
    }

    private static void initializePercentilesArray(@AggregationState TDigestAndPercentileArrayState state, Block percentilesArrayBlock)
    {
        if (state.getPercentiles() == null) {
            ImmutableList.Builder<Double> percentilesListBuilder = ImmutableList.builder();

            for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
                checkCondition(!percentilesArrayBlock.isNull(i), INVALID_FUNCTION_ARGUMENT, "Percentile cannot be null");
                double percentile = DOUBLE.getDouble(percentilesArrayBlock, i);
                checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
                percentilesListBuilder.add(percentile);
            }

            state.setPercentiles(percentilesListBuilder.build());
        }
    }

    private static void initializeDigest(@AggregationState TDigestAndPercentileArrayState state)
    {
        TDigest digest = state.getDigest();
        if (digest == null) {
            digest = new TDigest();
            state.setDigest(digest);
            state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
        }
    }
}
