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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.Int128;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.LongInterval;

import java.math.BigInteger;

import static io.trino.spi.type.Int128Math.addWithOverflow;
import static io.trino.spi.type.IntervalField.DAY;
import static io.trino.spi.type.IntervalField.SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.type.IntervalDayTimeType.MAX_FRACTIONAL_PRECISION;
import static io.trino.type.IntervalDayTimeType.createIntervalDayTimeType;

// The average is a derived interval that keeps the input's fractional-seconds precision. The sum is
// accumulated as a 128-bit count of picoseconds so it neither overflows nor loses the sub-microsecond
// fraction, then divided by the row count at output.
@AggregationFunction("avg")
public final class IntervalDayToSecondAverageAggregation
{
    private static final BigInteger PICOSECONDS_PER_MICROSECOND_BIG = BigInteger.valueOf(PICOSECONDS_PER_MICROSECOND);
    private static final BigInteger OVERFLOW_MULTIPLIER = BigInteger.TWO.pow(128);

    private IntervalDayToSecondAverageAggregation() {}

    @InputFunction
    @LiteralParameters("p")
    public static void averageShort(@AggregationState LongDecimalWithOverflowAndLongState state, @SqlType("interval day(9) to second(p)") long value)
    {
        addPicoseconds(state, value, 0);
    }

    @InputFunction
    @LiteralParameters("p")
    public static void averageLong(@AggregationState LongDecimalWithOverflowAndLongState state, @SqlType("interval day(9) to second(p)") LongInterval value)
    {
        addPicoseconds(state, value.getMicros(), value.getPicosOfMicro());
    }

    /// Adds an interval's `micros * 1_000_000 + picosOfMicro` total picoseconds to the 128-bit accumulator.
    private static void addPicoseconds(LongDecimalWithOverflowAndLongState state, long micros, long picosOfMicro)
    {
        long productLow = micros * PICOSECONDS_PER_MICROSECOND;
        long productHigh = Math.multiplyHigh(micros, PICOSECONDS_PER_MICROSECOND);
        long low = productLow + picosOfMicro;
        if (Long.compareUnsigned(low, productLow) < 0) {
            productHigh++;
        }

        long[] sum = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();
        long overflow = addWithOverflow(sum[offset], sum[offset + 1], productHigh, low, sum, offset);
        state.addOverflow(overflow);
        state.addLong(1);
    }

    @CombineFunction
    public static void combine(@AggregationState LongDecimalWithOverflowAndLongState state, @AggregationState LongDecimalWithOverflowAndLongState otherState)
    {
        long[] sum = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();
        long[] otherSum = otherState.getDecimalArray();
        int otherOffset = otherState.getDecimalArrayOffset();

        if (state.getLong() > 0) {
            long overflow = addWithOverflow(sum[offset], sum[offset + 1], otherSum[otherOffset], otherSum[otherOffset + 1], sum, offset);
            state.addOverflow(Math.addExact(overflow, otherState.getOverflow()));
        }
        else {
            sum[offset] = otherSum[otherOffset];
            sum[offset + 1] = otherSum[otherOffset + 1];
            state.setOverflow(otherState.getOverflow());
        }
        state.addLong(otherState.getLong());
    }

    @SqlNullable
    @OutputFunction("interval day(9) to second(p)")
    public static void output(@LiteralParameter("p") long precision, @AggregationState LongDecimalWithOverflowAndLongState state, BlockBuilder out)
    {
        if (state.getLong() == 0) {
            out.appendNull();
            return;
        }

        long[] sum = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();
        BigInteger total = Int128.valueOf(sum[offset], sum[offset + 1]).toBigInteger();
        if (state.getOverflow() != 0) {
            total = total.add(OVERFLOW_MULTIPLIER.multiply(BigInteger.valueOf(state.getOverflow())));
        }
        // Round the exact average once to the output precision, with ties toward positive infinity.
        // Rounding to picoseconds first can move a value below the final midpoint onto that midpoint.
        BigInteger quantum = BigInteger.TEN.pow(MAX_FRACTIONAL_PRECISION - (int) precision);
        BigInteger divisor = BigInteger.valueOf(state.getLong()).multiply(quantum);
        BigInteger[] quotientAndRemainder = total.divideAndRemainder(divisor);
        BigInteger units = quotientAndRemainder[0];
        int comparison = quotientAndRemainder[1].abs().shiftLeft(1).compareTo(divisor);
        if (comparison > 0 || (comparison == 0 && total.signum() > 0)) {
            units = units.add(BigInteger.valueOf(total.signum()));
        }
        BigInteger averagePicoseconds = units.multiply(quantum);

        // split the average into a microsecond count plus the picoseconds within the last microsecond
        BigInteger[] microsAndPicos = averagePicoseconds.divideAndRemainder(PICOSECONDS_PER_MICROSECOND_BIG);
        long micros = microsAndPicos[0].longValueExact();
        long picosOfMicro = microsAndPicos[1].longValue();
        if (picosOfMicro < 0) {
            micros--;
            picosOfMicro += PICOSECONDS_PER_MICROSECOND;
        }

        IntervalDayTimeType type = createIntervalDayTimeType(DAY, SECOND, IntervalDayTimeType.maxLeadingPrecision(DAY), (int) precision);
        if (type.isShort()) {
            type.writeLong(out, micros);
        }
        else {
            type.writeObject(out, new LongInterval(micros, (int) picosOfMicro));
        }
    }
}
