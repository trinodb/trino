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

import io.trino.operator.aggregation.state.LongDecimalWithOverflowState;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowStateSerializer;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Int128Math.addWithOverflow;

// merges and finishes sum(decimal) intermediates: the row carries the raw 128-bit sum in a
// decimal(38, s) field (values may lie outside the valid decimal range) plus the count of wraps
// around 2^128; the raw-input partial lives in DecimalSumAggregation
@AggregationFunction
public final class DecimalSumDecomposedAggregation
{
    private DecimalSumDecomposedAggregation() {}

    @InputFunction
    @LiteralParameters("s")
    public static void intermediateInput(
            @AggregationState LongDecimalWithOverflowState state,
            @BlockPosition @SqlType("row(sum decimal(38, s), overflow bigint)") ValueBlock block,
            @BlockIndex int position)
    {
        RowBlock rowBlock = (RowBlock) block;
        Block sumField = rowBlock.getFieldBlock(0);
        Int128ArrayBlock sumBlock = (Int128ArrayBlock) sumField.getUnderlyingValueBlock();
        int sumPosition = sumField.getUnderlyingValuePosition(position);
        long high = sumBlock.getInt128High(sumPosition);
        long low = sumBlock.getInt128Low(sumPosition);
        long overflow = BIGINT.getLong(rowBlock.getFieldBlock(1), position);

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();
        if (state.isNotNull()) {
            long carry = addWithOverflow(
                    decimal[offset],
                    decimal[offset + 1],
                    high,
                    low,
                    decimal,
                    offset);
            state.addOverflow(Math.addExact(carry, overflow));
        }
        else {
            state.setNotNull();
            decimal[offset] = high;
            decimal[offset + 1] = low;
            state.setOverflow(overflow);
        }
    }

    // must write exactly the layout of LongDecimalWithOverflowStateSerializer
    @AggregationFunction(value = "sum_decimal$merge", hidden = true)
    @SqlNullable
    @OutputFunction(value = "row(sum decimal(38, s), overflow bigint)", decomposition = @Decomposition(partial = "sum_decimal$merge"))
    public static void intermediateOutput(@AggregationState LongDecimalWithOverflowState state, BlockBuilder out)
    {
        if (!state.isNotNull()) {
            out.appendNull();
            return;
        }
        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();
        LongDecimalWithOverflowStateSerializer.write(decimal[offset], decimal[offset + 1], state.getOverflow(), (RowBlockBuilder) out);
    }

    @AggregationFunction(value = "sum_decimal$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = "decimal(38,s)", decomposition = @Decomposition(partial = "sum_decimal$merge", output = "sum_decimal$final"))
    public static void output(@AggregationState LongDecimalWithOverflowState state, BlockBuilder out)
    {
        DecimalSumAggregation.outputDecimal(state, out);
    }
}
