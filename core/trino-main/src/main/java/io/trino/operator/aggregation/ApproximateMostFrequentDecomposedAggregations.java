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
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

// merges approx_most_frequent intermediates, and computes the final maps
public final class ApproximateMostFrequentDecomposedAggregations
{
    private ApproximateMostFrequentDecomposedAggregations() {}

    @AggregationFunction
    public static final class BigintDecomposed
    {
        private BigintDecomposed() {}

        // the intermediate layout must match LongApproximateMostFrequentStateSerializer
        @InputFunction
        public static void input(@AggregationState BigintApproximateMostFrequent.State state, @SqlType(StandardTypes.VARBINARY) Slice value)
        {
            ApproximateMostFrequentHistogram<Long> incoming = new ApproximateMostFrequentHistogram<>(
                    value,
                    LongApproximateMostFrequentStateSerializer::serializeBucket,
                    LongApproximateMostFrequentStateSerializer::deserializeBucket);
            ApproximateMostFrequentHistogram<Long> previous = state.get();
            if (previous == null) {
                state.set(incoming);
            }
            else {
                previous.merge(incoming);
            }
        }

        @AggregationFunction(value = "approx_most_frequent_bigint$merge", hidden = true)
        @SqlNullable
        @OutputFunction(value = StandardTypes.VARBINARY, decomposition = @Decomposition(partial = "approx_most_frequent_bigint$merge"))
        public static void intermediateOutput(@AggregationState BigintApproximateMostFrequent.State state, BlockBuilder out)
        {
            if (state.get() == null) {
                out.appendNull();
            }
            else {
                VarbinaryType.VARBINARY.writeSlice(out, state.get().serialize());
            }
        }

        @AggregationFunction(value = "approx_most_frequent_bigint$final", hidden = true)
        @SqlNullable
        @OutputFunction(value = "map(bigint,bigint)", decomposition = @Decomposition(partial = "approx_most_frequent_bigint$merge", output = "approx_most_frequent_bigint$final"))
        public static void output(@AggregationState BigintApproximateMostFrequent.State state, BlockBuilder out)
        {
            if (state.get() == null) {
                out.appendNull();
            }
            else {
                ((MapBlockBuilder) out).buildEntry((keyBuilder, valueBuilder) -> state.get().forEachBucket((key, value) -> {
                    BigintType.BIGINT.writeLong(keyBuilder, key);
                    BigintType.BIGINT.writeLong(valueBuilder, value);
                }));
            }
        }
    }

    @AggregationFunction
    public static final class VarcharDecomposed
    {
        private VarcharDecomposed() {}

        // the intermediate layout must match StringApproximateMostFrequentStateSerializer
        @InputFunction
        public static void input(@AggregationState VarcharApproximateMostFrequent.State state, @SqlType(StandardTypes.VARBINARY) Slice value)
        {
            ApproximateMostFrequentHistogram<Slice> incoming = new ApproximateMostFrequentHistogram<>(
                    value,
                    StringApproximateMostFrequentStateSerializer::serializeBucket,
                    StringApproximateMostFrequentStateSerializer::deserializeBucket);
            ApproximateMostFrequentHistogram<Slice> previous = state.get();
            if (previous == null) {
                state.set(incoming);
            }
            else {
                previous.merge(incoming);
            }
        }

        @AggregationFunction(value = "approx_most_frequent_varchar$merge", hidden = true)
        @SqlNullable
        @OutputFunction(value = StandardTypes.VARBINARY, decomposition = @Decomposition(partial = "approx_most_frequent_varchar$merge"))
        public static void intermediateOutput(@AggregationState VarcharApproximateMostFrequent.State state, BlockBuilder out)
        {
            if (state.get() == null) {
                out.appendNull();
            }
            else {
                VarbinaryType.VARBINARY.writeSlice(out, state.get().serialize());
            }
        }

        @AggregationFunction(value = "approx_most_frequent_varchar$final", hidden = true)
        @SqlNullable
        @OutputFunction(value = "map(varchar,bigint)", decomposition = @Decomposition(partial = "approx_most_frequent_varchar$merge", output = "approx_most_frequent_varchar$final"))
        public static void output(@AggregationState VarcharApproximateMostFrequent.State state, BlockBuilder out)
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
}
