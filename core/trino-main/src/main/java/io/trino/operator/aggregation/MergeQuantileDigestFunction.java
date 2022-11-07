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
import io.trino.operator.aggregation.state.QuantileDigestState;
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

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.util.MoreMath.nearlyEqual;

@AggregationFunction(value = "merge", isOrderSensitive = true)
@Description("Merges the input quantile digests into a single quantile digest")
public final class MergeQuantileDigestFunction
{
    private MergeQuantileDigestFunction() {}

    private static final double COMPARISON_EPSILON = 1.0E-6;

    @InputFunction
    @TypeParameter("V")
    public static void input(
            @TypeParameter("qdigest(V)") Type type,
            @AggregationState QuantileDigestState state,
            @BlockPosition @SqlType("qdigest(V)") Block value,
            @BlockIndex int index)
    {
        merge(state, new QuantileDigest(type.getSlice(value, index)));
    }

    @CombineFunction
    public static void combine(@AggregationState QuantileDigestState state, @AggregationState QuantileDigestState otherState)
    {
        merge(state, otherState.getQuantileDigest());
    }

    private static void merge(QuantileDigestState state, QuantileDigest input)
    {
        if (input == null) {
            return;
        }
        QuantileDigest previous = state.getQuantileDigest();
        if (previous == null) {
            state.setQuantileDigest(input);
            state.addMemoryUsage(input.estimatedInMemorySizeInBytes());
        }
        else {
            checkArgument(nearlyEqual(previous.getMaxError(), input.getMaxError(), COMPARISON_EPSILON),
                    "Cannot merge qdigests with different accuracies (%s vs. %s)", state.getQuantileDigest().getMaxError(), input.getMaxError());
            checkArgument(nearlyEqual(previous.getAlpha(), input.getAlpha(), COMPARISON_EPSILON),
                    "Cannot merge qdigests with different alpha values (%s vs. %s)", state.getQuantileDigest().getAlpha(), input.getAlpha());
            state.addMemoryUsage(-previous.estimatedInMemorySizeInBytes());
            previous.merge(input);
            state.addMemoryUsage(previous.estimatedInMemorySizeInBytes());
        }
    }

    @OutputFunction("qdigest(V)")
    public static void output(
            @TypeParameter("qdigest(V)") Type type,
            @AggregationState QuantileDigestState state,
            BlockBuilder out)
    {
        if (state.getQuantileDigest() == null) {
            out.appendNull();
        }
        else {
            type.writeSlice(out, state.getQuantileDigest().serialize());
        }
    }
}
