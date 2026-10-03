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

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.IntervalField.SECOND;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIntervalDayTimeArithmetic
{
    private static final BigInteger PICOS_PER_MICRO = BigInteger.valueOf(1_000_000);
    private static final BigInteger MIN_PICOS = BigInteger.valueOf(Long.MIN_VALUE).multiply(PICOS_PER_MICRO);
    private static final BigInteger MAX_PICOS = BigInteger.valueOf(Long.MAX_VALUE).multiply(PICOS_PER_MICRO).add(PICOS_PER_MICRO.subtract(BigInteger.ONE));
    private static final long[] MICROSECOND_BOUNDARIES = {Long.MIN_VALUE, Long.MIN_VALUE + 1, -1, 0, 1, Long.MAX_VALUE - 1, Long.MAX_VALUE};
    private static final int[] PICOSECOND_BOUNDARIES = {0, 1, 499_999, 500_000, 999_999};

    @Test
    public void testAdditionAndSubtractionBoundaries()
    {
        for (long leftMicros : MICROSECOND_BOUNDARIES) {
            for (long rightMicros : MICROSECOND_BOUNDARIES) {
                for (int leftPicos : PICOSECOND_BOUNDARIES) {
                    LongInterval left = new LongInterval(leftMicros, leftPicos);
                    for (int rightPicos : PICOSECOND_BOUNDARIES) {
                        LongInterval right = new LongInterval(rightMicros, rightPicos);
                        assertResult(() -> IntervalDayTimeOperators.addLongLong(left, right), picoseconds(left).add(picoseconds(right)));
                        assertResult(() -> IntervalDayTimeOperators.subtractLongLong(left, right), picoseconds(left).subtract(picoseconds(right)));
                        if (leftPicos == 0) {
                            assertResult(() -> IntervalDayTimeOperators.addShortLong(leftMicros, right), picoseconds(left).add(picoseconds(right)));
                            assertResult(() -> IntervalDayTimeOperators.subtractShortLong(leftMicros, right), picoseconds(left).subtract(picoseconds(right)));
                        }
                        if (rightPicos == 0) {
                            assertResult(() -> IntervalDayTimeOperators.addLongShort(left, rightMicros), picoseconds(left).add(picoseconds(right)));
                            assertResult(() -> IntervalDayTimeOperators.subtractLongShort(left, rightMicros), picoseconds(left).subtract(picoseconds(right)));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testIntegerMultiplicationBoundaries()
    {
        for (long micros : MICROSECOND_BOUNDARIES) {
            for (int picos : PICOSECOND_BOUNDARIES) {
                LongInterval value = new LongInterval(micros, picos);
                for (long factor : new long[] {Long.MIN_VALUE, -2, -1, 0, 1, 2, 10_000_000_000_000L, Long.MAX_VALUE}) {
                    BigInteger expected = picoseconds(value).multiply(BigInteger.valueOf(factor));
                    assertResult(() -> IntervalDayTimeOperators.multiplyByBigintLong(value, factor, 12), expected);
                    assertResult(() -> IntervalDayTimeOperators.bigintMultiplyLong(factor, value, 12), expected);
                }
            }
        }
    }

    @Test
    public void testDoubleScalingOverflow()
    {
        for (long micros : new long[] {Long.MIN_VALUE, Long.MAX_VALUE}) {
            LongInterval value = new LongInterval(micros, 0);
            for (int precision = 7; precision <= 12; precision++) {
                int fractionalPrecision = precision;
                assertTrinoExceptionThrownBy(() -> IntervalDayTimeOperators.multiplyByDoubleLong(value, 2, fractionalPrecision))
                        .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
                assertTrinoExceptionThrownBy(() -> IntervalDayTimeOperators.doubleMultiplyLong(2, value, fractionalPrecision))
                        .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
                assertTrinoExceptionThrownBy(() -> IntervalDayTimeOperators.divideByDoubleLong(value, 0.5, fractionalPrecision))
                        .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            }
            assertTrinoExceptionThrownBy(() -> IntervalDayTimeOperators.multiplyByDoubleShort(micros, 2, 6, SECOND))
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            assertTrinoExceptionThrownBy(() -> IntervalDayTimeOperators.divideByDoubleShort(micros, 0.5, 6, SECOND))
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        }
        for (double factor : new double[] {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            assertTrinoExceptionThrownBy(() -> IntervalDayTimeOperators.multiplyByDoubleLong(new LongInterval(0, 0), factor, 12))
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            assertTrinoExceptionThrownBy(() -> IntervalDayTimeOperators.multiplyByDoubleShort(0, factor, 6, SECOND))
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        }
        for (long micros : new long[] {Long.MIN_VALUE, (long) Math.nextDown(0x1.0p63)}) {
            LongInterval value = new LongInterval(micros, 0);
            assertThat(IntervalDayTimeOperators.multiplyByDoubleLong(value, 1, 12)).isEqualTo(value);
            assertThat(IntervalDayTimeOperators.multiplyByDoubleShort(micros, 1, 6, SECOND)).isEqualTo(micros);
        }
    }

    @Test
    public void testSqlArithmeticBoundaries()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertThat(assertions.query("SELECT INTERVAL '0.000000000002' SECOND(1,12) * BIGINT '9223372036854775807'"))
                    .matches("VALUES INTERVAL '18446744.073709551614' SECOND(13,12)");
            assertThat(assertions.query("SELECT INTERVAL '-0.000000000001' SECOND(1,12) * BIGINT '10000000000000'"))
                    .matches("VALUES INTERVAL '-10' SECOND(13,12)");
            assertThat(assertions.query("SELECT INTERVAL '-9223372036854.7758075' SECOND(13,7) + INTERVAL '-0.0000005' SECOND(1,7)"))
                    .matches("VALUES INTERVAL '-9223372036854.7758080' SECOND(13,7)");
            assertThat(assertions.query("SELECT INTERVAL '9223372036854.775807' SECOND(13,6) - INTERVAL '-0.0000005' SECOND(1,7)"))
                    .matches("VALUES INTERVAL '9223372036854.7758075' SECOND(13,7)");
            assertThat(assertions.query("SELECT CAST(sum(x) AS interval second(13,7)) FROM (VALUES INTERVAL '-9223372036854.7758075' SECOND(13,7), INTERVAL '-0.0000005' SECOND(1,7)) t(x)"))
                    .matches("VALUES INTERVAL '-9223372036854.7758080' SECOND(13,7)");
            for (String operator : new String[] {"* DOUBLE '2'", "/ DOUBLE '0.5'"}) {
                assertTrinoExceptionThrownBy(assertions.expression("INTERVAL '9223372036854.775807' SECOND(13,12) " + operator)::evaluate)
                        .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            }
        }
    }

    private static void assertResult(Supplier<LongInterval> actual, BigInteger expected)
    {
        if (expected.compareTo(MIN_PICOS) < 0 || expected.compareTo(MAX_PICOS) > 0) {
            assertTrinoExceptionThrownBy(actual::get).hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            return;
        }
        assertThat(picoseconds(actual.get())).isEqualTo(expected);
    }

    private static BigInteger picoseconds(LongInterval value)
    {
        return BigInteger.valueOf(value.getMicros()).multiply(PICOS_PER_MICRO).add(BigInteger.valueOf(value.getPicosOfMicro()));
    }
}
