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

import io.trino.operator.aggregation.state.LongIntervalState;
import io.trino.spi.TrinoException;
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
import io.trino.type.IntervalDayTimeType;
import io.trino.type.LongInterval;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.IntervalField.DAY;
import static io.trino.spi.type.IntervalField.SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.type.IntervalDayTimeType.createIntervalDayTimeType;
import static java.lang.Math.addExact;
import static java.lang.String.format;

// The sum is a derived interval, so it carries the field-maximum leading precision and keeps the input's
// fractional-seconds precision (the canonical day-time interval that every operand widens to, and that
// the sum cannot spuriously overflow). A precision above 6 carries picoseconds, so the accumulator keeps
// a microsecond count plus the sub-microsecond fraction.
@AggregationFunction("sum")
public final class IntervalDayToSecondSumAggregation
{
    private IntervalDayToSecondSumAggregation() {}

    @InputFunction
    @LiteralParameters("p")
    public static void sumShort(@AggregationState LongIntervalState state, @SqlType("interval day(9) to second(p)") long value)
    {
        state.setNull(false);
        state.setMicros(addMicros(state.getMicros(), value));
    }

    @InputFunction
    @LiteralParameters("p")
    public static void sumLong(@AggregationState LongIntervalState state, @SqlType("interval day(9) to second(p)") LongInterval value)
    {
        add(state, value.getMicros(), value.getPicosOfMicro());
    }

    @CombineFunction
    public static void combine(@AggregationState LongIntervalState state, @AggregationState LongIntervalState otherState)
    {
        if (otherState.isNull()) {
            return;
        }
        if (state.isNull()) {
            state.setNull(false);
            state.setMicros(otherState.getMicros());
            state.setPicosOfMicro(otherState.getPicosOfMicro());
            return;
        }
        add(state, otherState.getMicros(), otherState.getPicosOfMicro());
    }

    private static void add(LongIntervalState state, long micros, long picosOfMicro)
    {
        state.setNull(false);
        long resultMicros = addMicros(state.getMicros(), micros);
        long resultPicos = state.getPicosOfMicro() + picosOfMicro;
        if (resultPicos >= PICOSECONDS_PER_MICROSECOND) {
            resultMicros = addMicros(resultMicros, 1);
            resultPicos -= PICOSECONDS_PER_MICROSECOND;
        }
        state.setMicros(resultMicros);
        state.setPicosOfMicro(resultPicos);
    }

    private static long addMicros(long left, long right)
    {
        try {
            return addExact(left, right);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("interval day to second sum overflow: %s + %s", left, right), e);
        }
    }

    @SqlNullable
    @OutputFunction("interval day(9) to second(p)")
    public static void output(@LiteralParameter("p") long precision, @AggregationState LongIntervalState state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
            return;
        }
        IntervalDayTimeType type = createIntervalDayTimeType(DAY, SECOND, IntervalDayTimeType.maxLeadingPrecision(DAY), (int) precision);
        if (type.isShort()) {
            type.writeLong(out, state.getMicros());
        }
        else {
            type.writeObject(out, new LongInterval(state.getMicros(), (int) state.getPicosOfMicro()));
        }
    }
}
