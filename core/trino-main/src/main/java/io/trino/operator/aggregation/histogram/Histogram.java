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
package io.trino.operator.aggregation.histogram;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

@AggregationFunction("histogram")
@Description("Count the number of times each value occurs")
public final class Histogram
{
    private Histogram() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @TypeParameter("T") Type type,
            @AggregationState("T") HistogramState state,
            @BlockPosition @SqlType("T") Block key,
            @BlockIndex int position)
    {
        TypedHistogram typedHistogram = state.get();
        long startSize = typedHistogram.getEstimatedSize();
        typedHistogram.add(position, key, 1L);
        state.addMemoryUsage(typedHistogram.getEstimatedSize() - startSize);
    }

    @CombineFunction
    public static void combine(@AggregationState("T") HistogramState state, @AggregationState("T") HistogramState otherState)
    {
        // NOTE: state = current merged state; otherState = scratchState (new data to be added)
        // for grouped histograms and single histograms, we have a single histogram object. In neither case, can otherState.get() return null.
        // Semantically, a histogram object will be returned even if the group is empty.
        // In that case, the histogram object will represent an empty histogram until we call add() on
        // it.
        requireNonNull(otherState.get(), "scratch state should always be non-null");
        TypedHistogram typedHistogram = state.get();
        long startSize = typedHistogram.getEstimatedSize();
        typedHistogram.addAll(otherState.get());
        state.addMemoryUsage(typedHistogram.getEstimatedSize() - startSize);
    }

    @OutputFunction("map(T, BIGINT)")
    public static void output(@TypeParameter("T") Type type, @AggregationState("T") HistogramState state, BlockBuilder out)
    {
        TypedHistogram typedHistogram = state.get();
        typedHistogram.serialize(out);
    }
}
