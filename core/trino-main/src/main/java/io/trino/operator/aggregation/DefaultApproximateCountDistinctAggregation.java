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

import io.trino.operator.aggregation.state.HyperLogLogState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
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

@AggregationFunction("approx_distinct")
public final class DefaultApproximateCountDistinctAggregation
{
    private static final double DEFAULT_STANDARD_ERROR = 0.023;

    private DefaultApproximateCountDistinctAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState HyperLogLogState state,
            @BlockPosition @SqlType("unknown") Block block,
            @BlockIndex int index)
    {
        // do nothing
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
        ApproximateCountDistinctAggregation.input(methodHandle, state, value, DEFAULT_STANDARD_ERROR);
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
        ApproximateCountDistinctAggregation.input(methodHandle, state, value, DEFAULT_STANDARD_ERROR);
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
        ApproximateCountDistinctAggregation.input(methodHandle, state, value, DEFAULT_STANDARD_ERROR);
    }

    @CombineFunction
    public static void combineState(@AggregationState HyperLogLogState state, @AggregationState HyperLogLogState otherState)
    {
        ApproximateCountDistinctAggregation.combineState(state, otherState);
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out)
    {
        ApproximateCountDistinctAggregation.evaluateFinal(state, out);
    }
}
