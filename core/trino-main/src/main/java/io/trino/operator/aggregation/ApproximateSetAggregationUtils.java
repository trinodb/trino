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

import io.airlift.stats.cardinality.HyperLogLog;
import io.trino.operator.aggregation.state.HyperLogLogState;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.AggregationState;

public final class ApproximateSetAggregationUtils
{
    private static final int NUMBER_OF_BUCKETS = 4096;
    private static final AccumulatorStateSerializer<HyperLogLogState> SERIALIZER = StateCompiler.generateStateSerializer(HyperLogLogState.class);

    private ApproximateSetAggregationUtils() {}

    public static HyperLogLog newHyperLogLog()
    {
        return HyperLogLog.newInstance(NUMBER_OF_BUCKETS);
    }

    public static HyperLogLog getOrCreateHyperLogLog(@AggregationState HyperLogLogState state)
    {
        HyperLogLog hll = state.getHyperLogLog();
        if (hll == null) {
            hll = newHyperLogLog();
            state.setHyperLogLog(hll);
            state.addMemoryUsage(hll.estimatedInMemorySize());
        }
        return hll;
    }

    public static void combineState(@AggregationState HyperLogLogState state, @AggregationState HyperLogLogState otherState)
    {
        HyperLogLog input = otherState.getHyperLogLog();

        HyperLogLog previous = state.getHyperLogLog();
        if (previous == null) {
            state.setHyperLogLog(input);
            state.addMemoryUsage(input.estimatedInMemorySize());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySize());
            previous.mergeWith(input);
            state.addMemoryUsage(previous.estimatedInMemorySize());
        }
    }

    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }
}
