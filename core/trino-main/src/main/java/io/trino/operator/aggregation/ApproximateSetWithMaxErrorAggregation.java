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
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.operator.aggregation.ApproximateCountDistinctAggregation.standardErrorToBuckets;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 * Variant of {@code approx_set} that accepts an explicit {@code maxStandardError}, controlling the
 * precision (the number of buckets) of the produced {@link HyperLogLog} sketch. A smaller
 * {@code maxStandardError} yields a more accurate sketch at the cost of more memory during
 * aggregation and a larger serialized size. The permitted range is enforced by
 * {@link ApproximateCountDistinctAggregation#standardErrorToBuckets(double)}, matching
 * {@code approx_distinct}.
 * <p>
 * Caveats, which are surfaced in the user-facing documentation as well:
 * <ul>
 *     <li>The single-argument {@code approx_set(x)} uses a fixed precision that differs from most
 *     explicit {@code maxStandardError} values, so sketches produced by the two forms are generally
 *     not mergeable with each other.</li>
 *     <li>{@code merge()} and combining partial sketches require all inputs to share the same
 *     precision; merging sketches created with different {@code maxStandardError} values fails at
 *     runtime (the underlying HyperLogLog rejects merges across different bucket counts). Every
 *     sketch intended to be merged together must be produced with the same {@code maxStandardError}.</li>
 * </ul>
 */
@AggregationFunction("approx_set")
public final class ApproximateSetWithMaxErrorAggregation
{
    private static final AccumulatorStateSerializer<HyperLogLogState> SERIALIZER = StateCompiler.generateStateSerializer(HyperLogLogState.class);

    private ApproximateSetWithMaxErrorAggregation() {}

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state, maxStandardError);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(Double.doubleToLongBits(value));
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState HyperLogLogState state, @SqlType("varchar(x)") Slice value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state, maxStandardError);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state, maxStandardError);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    private static HyperLogLog getOrCreateHyperLogLog(HyperLogLogState state, double maxStandardError)
    {
        HyperLogLog hll = state.getHyperLogLog();
        if (hll == null) {
            hll = HyperLogLog.newInstance(standardErrorToBuckets(maxStandardError));
            state.setHyperLogLog(hll);
            state.addMemoryUsage(hll.estimatedInMemorySize());
        }
        return hll;
    }

    @CombineFunction
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
            try {
                previous.mergeWith(input);
            }
            catch (IllegalArgumentException e) {
                // Partial sketches can only be combined when they share the same precision (bucket count).
                // This happens when maxStandardError is a non-constant expression that yields different
                // precisions across splits, which is a user error rather than an internal failure.
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
            }
            state.addMemoryUsage(previous.estimatedInMemorySize());
        }
    }

    @SqlNullable
    @OutputFunction(StandardTypes.HYPER_LOG_LOG)
    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }
}
