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
import io.trino.spi.type.StandardTypes;

import java.util.Map;

import static io.trino.operator.aggregation.DoubleHistogramAggregation.ENTRY_BUFFER_SIZE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

// both numeric_histogram variants share a NumericHistogram intermediate serialized as varbinary
@AggregationFunction
public final class NumericHistogramDecomposedAggregation
{
    private NumericHistogramDecomposedAggregation() {}

    @InputFunction
    public static void doubleInput(@AggregationState DoubleHistogramAggregation.State state, @SqlType(StandardTypes.BIGINT) long buckets, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double weight)
    {
        DoubleHistogramAggregation.add(state, buckets, value, weight);
    }

    @InputFunction
    public static void doubleUnweightedInput(@AggregationState DoubleHistogramAggregation.State state, @SqlType(StandardTypes.BIGINT) long buckets, @SqlType(StandardTypes.DOUBLE) double value)
    {
        DoubleHistogramAggregation.add(state, buckets, value, 1);
    }

    @InputFunction
    public static void realInput(@AggregationState DoubleHistogramAggregation.State state, @SqlType(StandardTypes.BIGINT) long buckets, @SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.DOUBLE) double weight)
    {
        DoubleHistogramAggregation.add(state, buckets, intBitsToFloat((int) value), weight);
    }

    @InputFunction
    public static void realUnweightedInput(@AggregationState DoubleHistogramAggregation.State state, @SqlType(StandardTypes.BIGINT) long buckets, @SqlType(StandardTypes.REAL) long value)
    {
        DoubleHistogramAggregation.add(state, buckets, intBitsToFloat((int) value), 1);
    }

    // the intermediate layout must match DoubleHistogramStateSerializer
    @InputFunction(hidden = true)
    public static void intermediateInput(@AggregationState DoubleHistogramAggregation.State state, @SqlType(StandardTypes.VARBINARY) Slice value)
    {
        NumericHistogram incoming = new NumericHistogram(value, ENTRY_BUFFER_SIZE);
        NumericHistogram previous = state.get();
        if (previous == null) {
            state.set(incoming);
        }
        else {
            previous.mergeWith(incoming);
        }
    }

    @AggregationFunction(value = "numeric_histogram$intermediate", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.VARBINARY, decomposition = @Decomposition(partial = "numeric_histogram$intermediate", output = "numeric_histogram$intermediate"))
    public static void intermediateOutput(@AggregationState DoubleHistogramAggregation.State state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            VARBINARY.writeSlice(out, state.get().serialize());
        }
    }

    @AggregationFunction(value = "numeric_histogram$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = "map(double,double)", decomposition = @Decomposition(partial = "numeric_histogram$intermediate", output = "numeric_histogram$final"))
    public static void doubleOutput(@AggregationState DoubleHistogramAggregation.State state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            Map<Double, Double> value = state.get().getBuckets();
            ((MapBlockBuilder) out).buildEntry((keyBuilder, valueBuilder) -> {
                for (Map.Entry<Double, Double> entry : value.entrySet()) {
                    DOUBLE.writeDouble(keyBuilder, entry.getKey());
                    DOUBLE.writeDouble(valueBuilder, entry.getValue());
                }
            });
        }
    }

    @AggregationFunction(value = "numeric_histogram_real$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = "map(real,real)", decomposition = @Decomposition(partial = "numeric_histogram$intermediate", output = "numeric_histogram_real$final"))
    public static void realOutput(@AggregationState DoubleHistogramAggregation.State state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            Map<Double, Double> value = state.get().getBuckets();
            ((MapBlockBuilder) out).buildEntry((keyBuilder, valueBuilder) -> {
                for (Map.Entry<Double, Double> entry : value.entrySet()) {
                    REAL.writeLong(keyBuilder, floatToRawIntBits(entry.getKey().floatValue()));
                    REAL.writeLong(valueBuilder, floatToRawIntBits(entry.getValue().floatValue()));
                }
            });
        }
    }
}
