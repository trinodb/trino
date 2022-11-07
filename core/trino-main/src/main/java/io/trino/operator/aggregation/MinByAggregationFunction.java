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

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.IN_OUT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;

@AggregationFunction("min_by")
@Description("Returns the value of the first argument, associated with the minimum value of the second argument")
public final class MinByAggregationFunction
{
    private MinByAggregationFunction() {}

    @InputFunction
    @TypeParameter("V")
    @TypeParameter("K")
    public static void input(
            @OperatorDependency(
                    operator = OperatorType.COMPARISON_UNORDERED_LAST,
                    argumentTypes = {"K", "K"},
                    convention = @Convention(arguments = {BLOCK_POSITION, IN_OUT}, result = FAIL_ON_NULL))
                    MethodHandle compare,
            @AggregationState("K") InOut keyState,
            @AggregationState("V") InOut valueState,
            @NullablePosition @BlockPosition @SqlType("V") Block valueBlock,
            @BlockPosition @SqlType("K") Block keyBlock,
            @BlockIndex int position)
            throws Throwable
    {
        if (keyState.isNull() || ((long) compare.invokeExact(keyBlock, position, keyState)) < 0) {
            keyState.set(keyBlock, position);
            valueState.set(valueBlock, position);
        }
    }

    @CombineFunction
    public static void combine(
            @OperatorDependency(
                    operator = OperatorType.COMPARISON_UNORDERED_LAST,
                    argumentTypes = {"K", "K"},
                    convention = @Convention(arguments = {IN_OUT, IN_OUT}, result = FAIL_ON_NULL))
                    MethodHandle compare,
            @AggregationState("K") InOut keyState,
            @AggregationState("V") InOut valueState,
            @AggregationState("K") InOut otherKeyState,
            @AggregationState("V") InOut otherValueState)
            throws Throwable
    {
        if (otherKeyState.isNull()) {
            return;
        }
        if (keyState.isNull() || ((long) compare.invokeExact(otherKeyState, keyState)) < 0) {
            keyState.set(otherKeyState);
            valueState.set(otherValueState);
        }
    }

    @OutputFunction("V")
    public static void output(
            @AggregationState("K") InOut keyState,
            @AggregationState("V") InOut valueState,
            BlockBuilder out)
    {
        valueState.get(out);
    }
}
