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

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InOut;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.IN_OUT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;

// merges min_by intermediates, which carry the current best key and its associated value
@AggregationFunction
public final class MinByDecomposedAggregation
{
    private MinByDecomposedAggregation() {}

    @InputFunction
    @TypeParameter("V")
    @TypeParameter("K")
    public static void intermediateInput(
            @OperatorDependency(
                    operator = OperatorType.COMPARISON_UNORDERED_LAST,
                    argumentTypes = {"K", "K"},
                    convention = @Convention(arguments = {VALUE_BLOCK_POSITION_NOT_NULL, IN_OUT}, result = FAIL_ON_NULL))
            MethodHandle compare,
            @AggregationState("K") InOut keyState,
            @AggregationState("V") InOut valueState,
            @SqlType("row(K, V)") SqlRow value)
            throws Throwable
    {
        int rawIndex = value.getRawIndex();
        ValueBlock keyBlock = value.getRawFieldBlock(0).getUnderlyingValueBlock();
        int keyPosition = value.getRawFieldBlock(0).getUnderlyingValuePosition(rawIndex);
        if (keyBlock.isNull(keyPosition)) {
            return;
        }
        if (keyState.isNull() || ((long) compare.invokeExact(keyBlock, keyPosition, keyState)) < 0) {
            keyState.set(keyBlock, keyPosition);
            valueState.set(value.getRawFieldBlock(1).getUnderlyingValueBlock(), value.getRawFieldBlock(1).getUnderlyingValuePosition(rawIndex));
        }
    }

    @AggregationFunction(value = "min_by$merge", hidden = true)
    @SqlNullable
    @OutputFunction(value = "row(K, V)", decomposition = @Decomposition(partial = "min_by$merge", output = "min_by$merge"))
    public static void intermediateOutput(
            @AggregationState("K") InOut keyState,
            @AggregationState("V") InOut valueState,
            BlockBuilder out)
    {
        MinByDecomposedAggregation.writeIntermediate(keyState, valueState, out);
    }

    static void writeIntermediate(InOut keyState, InOut valueState, BlockBuilder out)
    {
        ((RowBlockBuilder) out).buildEntry(fieldBuilders -> {
            keyState.get(fieldBuilders.get(0));
            valueState.get(fieldBuilders.get(1));
        });
    }

    @AggregationFunction(value = "min_by$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = "V", decomposition = @Decomposition(partial = "min_by$merge", output = "min_by$final"))
    public static void output(
            @AggregationState("K") InOut keyState,
            @AggregationState("V") InOut valueState,
            BlockBuilder out)
    {
        valueState.get(out);
    }
}
