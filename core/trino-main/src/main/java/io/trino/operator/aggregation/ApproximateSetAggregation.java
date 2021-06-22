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
import io.airlift.stats.cardinality.HyperLogLog;
import io.trino.operator.aggregation.state.HyperLogLogState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.operator.aggregation.ApproximateSetAggregationUtils.getOrCreateHyperLogLog;

@AggregationFunction("approx_set")
public final class ApproximateSetAggregation
{
    private ApproximateSetAggregation() {}

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(Double.doubleToLongBits(value));
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState HyperLogLogState state, @SqlType("varchar(x)") Slice value)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @CombineFunction
    public static void combineState(@AggregationState HyperLogLogState state, @AggregationState HyperLogLogState otherState)
    {
        ApproximateSetAggregationUtils.combineState(state, otherState);
    }

    @OutputFunction(StandardTypes.HYPER_LOG_LOG)
    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out)
    {
        ApproximateSetAggregationUtils.evaluateFinal(state, out);
    }
}
