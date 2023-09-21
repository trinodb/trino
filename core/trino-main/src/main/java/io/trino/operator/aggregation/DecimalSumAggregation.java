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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.Int128ArrayBlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.type.Int128Math.addWithOverflow;

@AggregationFunction("sum")
@Description("Calculates the sum over the input values")
public final class DecimalSumAggregation
{
    private DecimalSumAggregation() {}

    @InputFunction
    @LiteralParameters({"p", "s"})
    public static void inputShortDecimal(
            @AggregationState LongDecimalWithOverflowState state,
            @SqlType("decimal(p,s)") long rightLow)
    {
        state.setNotNull();

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();

        long rightHigh = rightLow >> 63;

        long overflow = addWithOverflow(
                decimal[offset],
                decimal[offset + 1],
                rightHigh,
                rightLow,
                decimal,
                offset);
        state.setOverflow(Math.addExact(overflow, state.getOverflow()));
    }

    @InputFunction
    @LiteralParameters({"p", "s"})
    public static void inputLongDecimal(
            @AggregationState LongDecimalWithOverflowState state,
            @BlockPosition @SqlType(value = "decimal(p,s)", nativeContainerType = Int128.class) Block block,
            @BlockIndex int position)
    {
        state.setNotNull();

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();

        long rightHigh = block.getLong(position, 0);
        long rightLow = block.getLong(position, SIZE_OF_LONG);

        long overflow = addWithOverflow(
                decimal[offset],
                decimal[offset + 1],
                rightHigh,
                rightLow,
                decimal,
                offset);

        state.addOverflow(overflow);
    }

    @CombineFunction
    public static void combine(@AggregationState LongDecimalWithOverflowState state, @AggregationState LongDecimalWithOverflowState otherState)
    {
        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();

        long[] otherDecimal = otherState.getDecimalArray();
        int otherOffset = otherState.getDecimalArrayOffset();

        if (state.isNotNull()) {
            long overflow = addWithOverflow(
                    decimal[offset],
                    decimal[offset + 1],
                    otherDecimal[otherOffset],
                    otherDecimal[otherOffset + 1],
                    decimal,
                    offset);
            state.addOverflow(Math.addExact(overflow, otherState.getOverflow()));
        }
        else {
            state.setNotNull();
            decimal[offset] = otherDecimal[otherOffset];
            decimal[offset + 1] = otherDecimal[otherOffset + 1];
            state.setOverflow(otherState.getOverflow());
        }
    }

    @OutputFunction("decimal(38,s)")
    public static void outputDecimal(@AggregationState LongDecimalWithOverflowState state, BlockBuilder out)
    {
        if (state.isNotNull()) {
            if (state.getOverflow() != 0) {
                throw new ArithmeticException("Decimal overflow");
            }

            long[] decimal = state.getDecimalArray();
            int offset = state.getDecimalArrayOffset();

            long rawHigh = decimal[offset];
            long rawLow = decimal[offset + 1];

            Decimals.throwIfOverflows(rawHigh, rawLow);
            ((Int128ArrayBlockBuilder) out).writeInt128(rawHigh, rawLow);
        }
        else {
            out.appendNull();
        }
    }
}
