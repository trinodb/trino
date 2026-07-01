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
package io.trino.type;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.function.LongSupplier;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.IntervalField.SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.IntervalCasts.dayTimeToDayTimeLongToShort;
import static io.trino.type.IntervalCasts.dayTimeToDayTimeShortToShort;
import static io.trino.type.IntervalCasts.negatePicos;
import static io.trino.type.IntervalCasts.roundToLongInterval;
import static io.trino.type.IntervalCasts.varcharToDayTime;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIntervalCasts
{
    private static final long SECOND_MICROS = 1_000_000L;

    @Test
    public void testNegatePicosAtLongBoundaries()
    {
        // with a positive sub-microsecond increment the negated micros is ~micros == -micros - 1, which is
        // representable for every micros — including both long extrema — with no overflowing intermediate
        assertThat(negatePicos(Long.MAX_VALUE, 5)).containsExactly(Long.MIN_VALUE, PICOSECONDS_PER_MICROSECOND - 5);
        assertThat(negatePicos(Long.MIN_VALUE, 7)).containsExactly(Long.MAX_VALUE, PICOSECONDS_PER_MICROSECOND - 7);

        // a whole-microsecond value negates directly; only Long.MIN_VALUE has no representable negation
        assertThat(negatePicos(Long.MAX_VALUE, 0)).containsExactly(-Long.MAX_VALUE, 0);
        assertTrinoExceptionThrownBy(() -> negatePicos(Long.MIN_VALUE, 0))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testRoundToLongIntervalCarryOverflow()
    {
        // a sub-microsecond fraction that rounds up to a full microsecond carries into the microseconds; at
        // Long.MAX_VALUE that carry overflows the long range and is rejected rather than wrapping. Precision
        // 7 is the first long form (roundToLongInterval only runs for 7–12), where 950_000 ps rounds up to a
        // whole microsecond.
        assertTrinoExceptionThrownBy(() -> roundToLongInterval(Long.MAX_VALUE, 950_000, 7))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        // one microsecond below the boundary the same carry lands on Long.MAX_VALUE cleanly
        assertThat(roundToLongInterval(Long.MAX_VALUE - 1, 950_000, 7).getMicros()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void testLongToShortCarryOverflow()
    {
        // narrowing a long value to microsecond resolution rounds the microseconds half-up by the
        // sub-microsecond fraction; at Long.MAX_VALUE that carry overflows and is rejected
        assertTrinoExceptionThrownBy(() -> dayTimeToDayTimeLongToShort(new LongInterval(Long.MAX_VALUE, 900_000), SECOND_MICROS, 9, SECOND, 6, SECOND))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testRoundingAtMicrosecondBoundaries()
    {
        for (long micros : new long[] {Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MIN_VALUE + 1000, -15, -6, -5, -4, 0, 4, 5, 6, 15, Long.MAX_VALUE - 1000, Long.MAX_VALUE - 1, Long.MAX_VALUE}) {
            for (int precision = 0; precision <= 6; precision++) {
                int fractionalPrecision = precision;
                BigInteger unit = BigInteger.TEN.pow(6 - precision);
                BigInteger shifted = BigInteger.valueOf(micros).add(unit.divide(BigInteger.TWO));
                BigInteger[] quotient = shifted.divideAndRemainder(unit);
                BigInteger rounded = quotient[0].subtract(quotient[1].signum() < 0 ? BigInteger.ONE : BigInteger.ZERO).multiply(unit);
                assertRounded(() -> dayTimeToDayTimeShortToShort(micros, SECOND_MICROS, 13, SECOND, fractionalPrecision, SECOND), rounded);
                if (precision < 6) {
                    assertRounded(() -> dayTimeToDayTimeLongToShort(new LongInterval(micros, 999_999), SECOND_MICROS, 13, SECOND, fractionalPrecision, SECOND), rounded);
                }
            }
        }
    }

    @Test
    public void testVarcharRoundingOverflow()
    {
        for (String value : new String[] {"9223372036854.775807", "-9223372036854.775808"}) {
            assertTrinoExceptionThrownBy(() -> varcharToDayTime(utf8Slice(value), SECOND, SECOND, 13, 5))
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        }
    }

    private static void assertRounded(LongSupplier actual, BigInteger expected)
    {
        if (expected.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0 || expected.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
            assertTrinoExceptionThrownBy(actual::getAsLong).hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            return;
        }
        assertThat(actual.getAsLong()).isEqualTo(expected.longValueExact());
    }
}
