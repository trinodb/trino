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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Convention;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.util.Failures.internalError;

@AggregationFunction(value = "$approx_set", hidden = true)
// $approx_set is hidden as used only for statistics collection in ANALYZE so far; eventually it may be used for implementation of user visible approx_set
public final class ApproximateSetGenericAggregation
{
    private static final int NUMBER_OF_BUCKETS = 4096;
    private static final AccumulatorStateSerializer<HyperLogLogState> SERIALIZER = StateCompiler.generateStateSerializer(HyperLogLogState.class);

    private ApproximateSetGenericAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState HyperLogLogState state,
            @BlockPosition @SqlType("unknown") Block block,
            @BlockIndex int index)
    {
        // do nothing -- unknown type is always NULL
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(
                    operator = XX_HASH_64,
                    argumentTypes = "T",
                    convention = @Convention(arguments = NEVER_NULL, result = FAIL_ON_NULL))
                    MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") double value)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        long hash;
        try {
            hash = (long) methodHandle.invoke(value);
        }
        catch (Throwable t) {
            throw internalError(t);
        }
        hll.addHash(hash);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(
                    operator = XX_HASH_64,
                    argumentTypes = "T",
                    convention = @Convention(arguments = NEVER_NULL, result = FAIL_ON_NULL))
                    MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") long value)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        long hash;
        try {
            hash = (long) methodHandle.invoke(value);
        }
        catch (Throwable t) {
            throw internalError(t);
        }
        hll.addHash(hash);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType("BOOLEAN") boolean value)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.addHash(value ? 19144387141682250L : -2447670524089286488L);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(
                    operator = XX_HASH_64,
                    argumentTypes = "T",
                    convention = @Convention(arguments = NEVER_NULL, result = FAIL_ON_NULL))
                    MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") Object value)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        long hash;
        try {
            hash = (long) methodHandle.invoke(value);
        }
        catch (Throwable t) {
            throw internalError(t);
        }
        hll.addHash(hash);
        state.addMemoryUsage(hll.estimatedInMemorySize());
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
            previous.mergeWith(input);
            state.addMemoryUsage(previous.estimatedInMemorySize());
        }
    }

    @OutputFunction(StandardTypes.HYPER_LOG_LOG)
    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
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

    public static HyperLogLog newHyperLogLog()
    {
        return HyperLogLog.newInstance(NUMBER_OF_BUCKETS);
    }
}
