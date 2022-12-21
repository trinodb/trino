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

import com.google.common.annotations.VisibleForTesting;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowAndLongState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
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
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.Decimals.writeShortDecimal;
import static io.trino.spi.type.Int128Math.addWithOverflow;
import static io.trino.spi.type.Int128Math.divideRoundUp;
import static java.math.RoundingMode.HALF_UP;

@AggregationFunction("avg")
@Description("Calculates the average value")
public final class DecimalAverageAggregation
{
    private static final BigInteger TWO = new BigInteger("2");
    private static final BigInteger OVERFLOW_MULTIPLIER = TWO.pow(128);

    private DecimalAverageAggregation() {}

    @InputFunction
    @LiteralParameters({"p", "s"})
    public static void inputShortDecimal(
            @AggregationState LongDecimalWithOverflowAndLongState state,
            @BlockPosition @SqlType(value = "decimal(p, s)", nativeContainerType = long.class) Block block,
            @BlockIndex int position)
    {
        state.addLong(1); // row counter

        state.setNotNull();

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();

        long rightLow = block.getLong(position, 0);
        long rightHigh = rightLow >> 63;

        long overflow = addWithOverflow(
                decimal[offset],
                decimal[offset + 1],
                rightHigh,
                rightLow,
                decimal,
                offset);

        state.addOverflow(overflow);
    }

    @InputFunction
    @LiteralParameters({"p", "s"})
    public static void inputLongDecimal(
            @AggregationState LongDecimalWithOverflowAndLongState state,
            @BlockPosition @SqlType(value = "decimal(p, s)", nativeContainerType = Int128.class) Block block,
            @BlockIndex int position)
    {
        state.addLong(1); // row counter

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
    public static void combine(@AggregationState LongDecimalWithOverflowAndLongState state, @AggregationState LongDecimalWithOverflowAndLongState otherState)
    {
        state.addLong(otherState.getLong()); // row counter

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
            state.addOverflow(overflow + otherState.getOverflow());
        }
        else {
            state.setNotNull();
            decimal[offset] = otherDecimal[otherOffset];
            decimal[offset + 1] = otherDecimal[otherOffset + 1];
            state.setOverflow(otherState.getOverflow());
        }
    }

    @OutputFunction("decimal(p,s)")
    public static void outputShortDecimal(
            @TypeParameter("decimal(p,s)") Type type,
            @AggregationState LongDecimalWithOverflowAndLongState state,
            BlockBuilder out)
    {
        DecimalType decimalType = (DecimalType) type;
        if (state.getLong() == 0) {
            out.appendNull();
            return;
        }
        Int128 average = average(state, decimalType);
        if (decimalType.isShort()) {
            writeShortDecimal(out, average.toLongExact());
        }
        else {
            type.writeObject(out, average);
        }
    }

    @VisibleForTesting
    public static Int128 average(LongDecimalWithOverflowAndLongState state, DecimalType type)
    {
        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();

        long overflow = state.getOverflow();
        if (overflow != 0) {
            BigDecimal sum = new BigDecimal(Int128.valueOf(decimal[offset], decimal[offset + 1]).toBigInteger(), type.getScale());
            sum = sum.add(new BigDecimal(OVERFLOW_MULTIPLIER.multiply(BigInteger.valueOf(overflow))));

            BigDecimal count = BigDecimal.valueOf(state.getLong());
            return Decimals.encodeScaledValue(sum.divide(count, type.getScale(), HALF_UP), type.getScale());
        }

        Int128 result = divideRoundUp(decimal[offset], decimal[offset + 1], 0, 0, state.getLong(), 0);
        if (overflows(result)) {
            throw new ArithmeticException("Decimal overflow");
        }
        return result;
    }
}
