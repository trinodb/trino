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

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowState;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.Int128ArrayBlock;
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
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.Int128Math.addWithOverflow;

@AggregationFunction(value = "sum", windowAccumulator = DecimalSumAggregation.DecimalSumWindowAccumulator.class)
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
            @BlockPosition @SqlType(value = "decimal(p,s)", nativeContainerType = Int128.class) Int128ArrayBlock block,
            @BlockIndex int position)
    {
        state.setNotNull();

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();

        long rightHigh = block.getInt128High(position);
        long rightLow = block.getInt128Low(position);

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

    @SqlNullable
    @OutputFunction("decimal(38,s)")
    public static void outputDecimal(@AggregationState LongDecimalWithOverflowState state, BlockBuilder out)
    {
        if (state.isNotNull()) {
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
        else {
            out.appendNull();
        }
    }

    public static class DecimalSumWindowAccumulator
            implements WindowAccumulator
    {
        // sum[0] is the high 64 bits, sum[1] the low 64 bits of the running 128-bit sum
        private final long[] sum;
        private long overflow;
        private long count;

        // The window accumulator is shared by the short- and long-decimal signatures and is not told which one
        // it serves, so the backing type is detected once from the argument block.
        private boolean typeResolved;
        private boolean longDecimal;

        @UsedByGeneratedCode
        public DecimalSumWindowAccumulator()
        {
            this(new long[2], 0, 0);
        }

        private DecimalSumWindowAccumulator(long[] sum, long overflow, long count)
        {
            this.sum = sum;
            this.overflow = overflow;
            this.count = count;
        }

        @Override
        public long getEstimatedSize()
        {
            return Long.BYTES // count
                    + Long.BYTES // overflow
                    + Long.BYTES * 2L; // sum
        }

        @Override
        public WindowAccumulator copy()
        {
            return new DecimalSumWindowAccumulator(sum.clone(), overflow, count);
        }

        @Override
        public void addInput(WindowIndex index, int startPosition, int endPosition)
        {
            accumulate(index, startPosition, endPosition, false);
        }

        @Override
        public boolean removeInput(WindowIndex index, int startPosition, int endPosition)
        {
            // Removal is addition of the negated value: because the accumulator is plain modular 128-bit
            // arithmetic, this exactly reverses a prior addInput, including the overflow count.
            accumulate(index, startPosition, endPosition, true);
            return true;
        }

        private void accumulate(WindowIndex index, int startPosition, int endPosition, boolean subtract)
        {
            for (int i = startPosition; i <= endPosition; i++) {
                if (index.isNull(0, i)) {
                    continue;
                }

                long high;
                long low;
                if (longDecimal(index, i)) {
                    Int128 value = (Int128) index.getObject(0, i);
                    high = value.getHigh();
                    low = value.getLow();
                }
                else {
                    long value = index.getLong(0, i);
                    high = value >> 63;
                    low = value;
                }

                if (subtract) {
                    // Two's-complement negation of the 128-bit addend; decimal magnitudes never reach
                    // Int128.MIN, so negation cannot overflow.
                    long negatedHigh = ~high;
                    if (low == 0) {
                        negatedHigh++;
                    }
                    high = negatedHigh;
                    low = -low;
                    count--;
                }
                else {
                    count++;
                }

                overflow += addWithOverflow(sum[0], sum[1], high, low, sum, 0);
            }
        }

        @Override
        public void output(BlockBuilder blockBuilder)
        {
            if (count == 0) {
                blockBuilder.appendNull();
                return;
            }
            if (overflow != 0 || Decimals.overflows(sum[0], sum[1])) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }
            ((Int128ArrayBlockBuilder) blockBuilder).writeInt128(sum[0], sum[1]);
        }

        private boolean longDecimal(WindowIndex index, int position)
        {
            if (!typeResolved) {
                longDecimal = index.getSingleValueBlock(0, position).getUnderlyingValueBlock() instanceof Int128ArrayBlock;
                typeResolved = true;
            }
            return longDecimal;
        }
    }
}
