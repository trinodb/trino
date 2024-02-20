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

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.Map;

import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("numeric_histogram")
public final class RealHistogramAggregation
{
    private RealHistogramAggregation() {}

    @InputFunction
    public static void add(@AggregationState DoubleHistogramAggregation.State state, @SqlType(StandardTypes.BIGINT) long buckets, @SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.DOUBLE) double weight)
    {
        DoubleHistogramAggregation.add(state, buckets, intBitsToFloat((int) value), weight);
    }

    @InputFunction
    public static void add(@AggregationState DoubleHistogramAggregation.State state, @SqlType(StandardTypes.BIGINT) long buckets, @SqlType(StandardTypes.REAL) long value)
    {
        add(state, buckets, value, 1);
    }

    @CombineFunction
    public static void merge(@AggregationState DoubleHistogramAggregation.State state, @AggregationState DoubleHistogramAggregation.State other)
    {
        DoubleHistogramAggregation.merge(state, other);
    }

    @OutputFunction("map(real,real)")
    public static void output(@AggregationState DoubleHistogramAggregation.State state, BlockBuilder out)
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
