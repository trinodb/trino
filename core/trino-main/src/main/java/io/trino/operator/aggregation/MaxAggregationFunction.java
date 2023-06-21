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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.InOut;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.IN_OUT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;

@AggregationFunction("max")
@Description("Returns the maximum value of the argument")
public final class MaxAggregationFunction
{
    private MaxAggregationFunction() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(
                    operator = OperatorType.COMPARISON_UNORDERED_FIRST,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {BLOCK_POSITION_NOT_NULL, IN_OUT}, result = FAIL_ON_NULL))
                    MethodHandle compare,
            @AggregationState("T") InOut state,
            @BlockPosition @SqlType("T") Block block,
            @BlockIndex int position)
            throws Throwable
    {
        if (state.isNull() || ((long) compare.invokeExact(block, position, state)) > 0) {
            state.set(block, position);
        }
    }

    @CombineFunction
    public static void combine(
            @OperatorDependency(
                    operator = OperatorType.COMPARISON_UNORDERED_FIRST,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {IN_OUT, IN_OUT}, result = FAIL_ON_NULL))
                    MethodHandle compare,
            @AggregationState("T") InOut state,
            @AggregationState("T") InOut otherState)
            throws Throwable
    {
        if (state.isNull() || ((long) compare.invokeExact(otherState, state)) > 0) {
            state.set(otherState);
        }
    }

    @OutputFunction("T")
    public static void output(@AggregationState("T") InOut state, BlockBuilder out)
    {
        state.get(out);
    }
}
