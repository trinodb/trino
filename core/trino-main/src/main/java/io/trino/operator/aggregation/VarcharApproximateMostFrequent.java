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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

/**
 * <p>
 * Aggregation function that approximates the frequency of the top-K elements.
 * This function keeps counts for a "frequent" subset of elements and assumes all other elements
 * once fewer than the least-frequent "frequent" element.
 * </p>
 *
 * <p>
 * The algorithm is based loosely on:
 * <a href="https://dl.acm.org/doi/10.1007/978-3-540-30570-5_27">Efficient Computation of Frequent and Top-*k* Elements in Data Streams</a>
 * by Ahmed Metwally, Divyakant Agrawal, and Amr El Abbadi
 * </p>
 */
@AggregationFunction("approx_most_frequent")
public final class VarcharApproximateMostFrequent
{
    private VarcharApproximateMostFrequent() {}

    @AccumulatorStateMetadata(stateSerializerClass = StringApproximateMostFrequentStateSerializer.class, stateFactoryClass = StringApproximateMostFrequentStateFactory.class)
    public interface State
            extends AccumulatorState
    {
        ApproximateMostFrequentHistogram<Slice> get();

        void set(ApproximateMostFrequentHistogram<Slice> value);
    }

    @InputFunction
    public static void input(@AggregationState State state, @SqlType(BIGINT) long buckets, @SqlType(VARCHAR) Slice value, @SqlType(BIGINT) long capacity)
    {
        ApproximateMostFrequentHistogram<Slice> histogram = state.get();
        if (histogram == null) {
            checkCondition(buckets >= 2, INVALID_FUNCTION_ARGUMENT, "approx_most_frequent bucket count must be greater than one");
            histogram = new ApproximateMostFrequentHistogram<>(
                    toIntExact(buckets),
                    toIntExact(capacity),
                    StringApproximateMostFrequentStateSerializer::serializeBucket,
                    StringApproximateMostFrequentStateSerializer::deserializeBucket);
            state.set(histogram);
        }

        histogram.add(value);
    }

    @CombineFunction
    public static void combine(@AggregationState State state, @AggregationState State otherState)
    {
        ApproximateMostFrequentHistogram<Slice> otherHistogram = otherState.get();

        ApproximateMostFrequentHistogram<Slice> histogram = state.get();
        if (histogram == null) {
            state.set(otherHistogram);
        }
        else {
            histogram.merge(otherHistogram);
        }
    }

    @OutputFunction("map(varchar,bigint)")
    public static void output(@AggregationState State state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            ((MapBlockBuilder) out).buildEntry((keyBuilder, valueBuilder) -> state.get().forEachBucket((key, value) -> {
                VarcharType.VARCHAR.writeSlice(keyBuilder, key);
                BigintType.BIGINT.writeLong(valueBuilder, value);
            }));
        }
    }
}
