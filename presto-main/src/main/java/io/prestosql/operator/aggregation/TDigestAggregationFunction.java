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
package io.prestosql.operator.aggregation;

import io.airlift.stats.TDigest;
import io.prestosql.operator.aggregation.state.StateCompiler;
import io.prestosql.operator.aggregation.state.TDigestState;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.operator.scalar.TDigestFunctions.verifyWeight;

@AggregationFunction("tdigest_agg")
public final class TDigestAggregationFunction
{
    private static final AccumulatorStateSerializer<TDigestState> serializer = StateCompiler.generateStateSerializer(TDigestState.class);

    private TDigestAggregationFunction() {}

    @InputFunction
    public static void input(@AggregationState TDigestState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        TDigest tdigest = state.getTDigest();
        if (tdigest == null) {
            tdigest = new TDigest();
            state.setTDigest(tdigest);
            state.addMemoryUsage(tdigest.estimatedInMemorySizeInBytes());
        }
        state.addMemoryUsage(-tdigest.estimatedInMemorySizeInBytes());
        tdigest.add(value);
        state.addMemoryUsage(tdigest.estimatedInMemorySizeInBytes());
    }

    @InputFunction
    public static void weightedInput(@AggregationState TDigestState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double weight)
    {
        TDigest tdigest = state.getTDigest();
        if (tdigest == null) {
            tdigest = new TDigest();
            state.setTDigest(tdigest);
            state.addMemoryUsage(tdigest.estimatedInMemorySizeInBytes());
        }
        state.addMemoryUsage(-tdigest.estimatedInMemorySizeInBytes());
        tdigest.add(value, verifyWeight(weight));
        state.addMemoryUsage(tdigest.estimatedInMemorySizeInBytes());
    }

    @CombineFunction
    public static void combine(@AggregationState TDigestState state, @AggregationState TDigestState otherState)
    {
        TDigest input = otherState.getTDigest();
        if (input == null) {
            return;
        }
        TDigest previous = state.getTDigest();
        if (previous == null) {
            state.setTDigest(input);
            state.addMemoryUsage(input.estimatedInMemorySizeInBytes());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySizeInBytes());
            previous.mergeWith(input);
            state.addMemoryUsage(previous.estimatedInMemorySizeInBytes());
        }
    }

    @OutputFunction(StandardTypes.TDIGEST)
    public static void output(@AggregationState TDigestState state, BlockBuilder out)
    {
        serializer.serialize(state, out);
    }
}
