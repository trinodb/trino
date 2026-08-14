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

import io.trino.operator.aggregation.state.LongDecimalWithOverflowAndLongState;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowAndLongStateSerializer;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.Int128ArrayBlockBuilder;
import io.trino.spi.block.LongArrayBlock;
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
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Int128Math.addWithOverflow;

// merges and finishes avg(decimal) intermediates. avg(decimal(p, s)) returns decimal(p, s), and
// the final step is resolved from the intermediate type alone, so the intermediate row carries the
// input type: the sum field holds the input-width low bits of the raw 128-bit running sum, high
// the remaining bits (zero for long decimals), overflow the wraps around 2^128, and count the
// input rows. The row layout must match LongDecimalWithOverflowAndLongStateSerializer; the
// raw-input partial lives in DecimalAverageAggregation.
@AggregationFunction
public final class DecimalAverageDecomposedAggregation
{
    private DecimalAverageDecomposedAggregation() {}

    @InputFunction
    @LiteralParameters({"p", "s"})
    public static void intermediateInput(
            @AggregationState LongDecimalWithOverflowAndLongState state,
            @BlockPosition @SqlType("row(sum decimal(p, s), high bigint, overflow bigint, count bigint)") ValueBlock block,
            @BlockIndex int position)
    {
        RowBlock rowBlock = (RowBlock) block;
        Block sumField = rowBlock.getFieldBlock(0);
        int sumPosition = sumField.getUnderlyingValuePosition(position);
        long high;
        long low;
        if (sumField.getUnderlyingValueBlock() instanceof Int128ArrayBlock sumBlock) {
            high = sumBlock.getInt128High(sumPosition);
            low = sumBlock.getInt128Low(sumPosition);
        }
        else {
            low = ((LongArrayBlock) sumField.getUnderlyingValueBlock()).getLong(sumPosition);
            high = BIGINT.getLong(rowBlock.getFieldBlock(1), position);
        }
        long overflow = BIGINT.getLong(rowBlock.getFieldBlock(2), position);
        long count = BIGINT.getLong(rowBlock.getFieldBlock(3), position);

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();
        if (state.getLong() > 0) {
            long carry = addWithOverflow(
                    decimal[offset],
                    decimal[offset + 1],
                    high,
                    low,
                    decimal,
                    offset);
            state.addOverflow(carry + overflow);
        }
        else {
            decimal[offset] = high;
            decimal[offset + 1] = low;
            state.setOverflow(overflow);
        }
        state.addLong(count);
    }

    @AggregationFunction(value = "avg_decimal$merge", hidden = true)
    @SqlNullable
    @OutputFunction(value = "row(sum decimal(p, s), high bigint, overflow bigint, count bigint)", decomposition = @Decomposition(partial = "avg_decimal$merge"))
    public static void intermediateOutput(
            @TypeParameter("decimal(p, s)") Type type,
            @AggregationState LongDecimalWithOverflowAndLongState state,
            BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
            return;
        }
        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();
        LongDecimalWithOverflowAndLongStateSerializer.write(
                ((DecimalType) type).isShort(),
                decimal[offset],
                decimal[offset + 1],
                state.getOverflow(),
                count,
                (RowBlockBuilder) out);
    }

    @AggregationFunction(value = "avg_decimal$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = "decimal(p,s)", decomposition = @Decomposition(partial = "avg_decimal$merge", output = "avg_decimal$final"))
    public static void output(
            @TypeParameter("decimal(p,s)") Type type,
            @AggregationState LongDecimalWithOverflowAndLongState state,
            BlockBuilder out)
    {
        DecimalAverageAggregation.outputDecimal(type, state, out);
    }

    @AggregationFunction(value = "avg_decimal_sum$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = "decimal(38,s)", decomposition = @Decomposition(partial = "avg_decimal$merge", output = "avg_decimal_sum$final"))
    public static void sumOutput(@AggregationState LongDecimalWithOverflowAndLongState state, BlockBuilder out)
    {
        if (state.getLong() == 0) {
            out.appendNull();
            return;
        }
        if (state.getOverflow() != 0) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
        }

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();
        long rawHigh = decimal[offset];
        long rawLow = decimal[offset + 1];
        if (Decimals.overflows(rawHigh, rawLow)) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
        }
        ((Int128ArrayBlockBuilder) out).writeInt128(rawHigh, rawLow);
    }

    @AggregationFunction(value = "avg_decimal_count$final", hidden = true)
    @OutputFunction(value = StandardTypes.BIGINT, decomposition = @Decomposition(partial = "avg_decimal$merge", output = "avg_decimal_count$final"))
    public static void countOutput(@AggregationState LongDecimalWithOverflowAndLongState state, BlockBuilder out)
    {
        BIGINT.writeLong(out, state.getLong());
    }
}
