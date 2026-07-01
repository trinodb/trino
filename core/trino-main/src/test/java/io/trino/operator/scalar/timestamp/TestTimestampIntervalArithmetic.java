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
package io.trino.operator.scalar.timestamp;

import io.trino.spi.type.LongTimestamp;
import io.trino.type.LongInterval;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

class TestTimestampIntervalArithmetic
{
    private static final BigInteger PICOS_PER_MICRO = BigInteger.valueOf(1_000_000);
    private static final BigInteger MIN_PICOS = BigInteger.valueOf(Long.MIN_VALUE).multiply(PICOS_PER_MICRO);
    private static final BigInteger MAX_PICOS = BigInteger.valueOf(Long.MAX_VALUE).multiply(PICOS_PER_MICRO).add(PICOS_PER_MICRO.subtract(BigInteger.ONE));

    @Test
    void testNormalizedBoundaries()
    {
        long[] micros = {Long.MIN_VALUE, Long.MIN_VALUE + 1, -1, 0, 1, Long.MAX_VALUE - 1, Long.MAX_VALUE};
        int[] fractions = {0, 1, 500_000, 999_999};
        for (long leftMicros : micros) {
            for (long rightMicros : micros) {
                for (int leftPicos : fractions) {
                    for (int rightPicos : fractions) {
                        LongTimestamp left = new LongTimestamp(leftMicros, leftPicos);
                        LongTimestamp right = new LongTimestamp(rightMicros, rightPicos);
                        LongInterval interval = new LongInterval(rightMicros, rightPicos);
                        BigInteger sum = picoseconds(left).add(picoseconds(right));
                        BigInteger difference = picoseconds(left).subtract(picoseconds(right));
                        assertResult(() -> TimestampOperators.TimestampPlusIntervalDayToSecond.add(left, interval), sum);
                        assertResult(() -> TimestampOperators.TimestampMinusIntervalDayToSecond.subtract(left, interval), difference);
                        assertResult(() -> {
                            LongInterval result = TimestampOperators.subtractTimestampsLong(left, right);
                            return new LongTimestamp(result.getMicros(), result.getPicosOfMicro());
                        }, difference);
                        if (leftPicos == 0) {
                            assertResult(() -> TimestampOperators.TimestampPlusIntervalDayToSecond.add(leftMicros, interval), sum);
                            assertResult(() -> TimestampOperators.TimestampMinusIntervalDayToSecond.subtract(leftMicros, interval), difference);
                        }
                        if (rightPicos == 0) {
                            assertResult(() -> TimestampOperators.TimestampMinusIntervalDayToSecond.subtract(left, rightMicros), difference);
                            if (leftPicos == 0) {
                                assertResult(() -> new LongTimestamp(TimestampOperators.TimestampMinusIntervalDayToSecond.subtract(leftMicros, rightMicros), 0), difference);
                                assertResult(() -> new LongTimestamp(TimestampOperators.subtractTimestampsShort(leftMicros, rightMicros), 0), difference);
                            }
                        }
                    }
                }
            }
        }
    }

    private static BigInteger picoseconds(LongTimestamp timestamp)
    {
        return BigInteger.valueOf(timestamp.getEpochMicros()).multiply(PICOS_PER_MICRO).add(BigInteger.valueOf(timestamp.getPicosOfMicro()));
    }

    private static void assertResult(Supplier<LongTimestamp> operation, BigInteger expected)
    {
        if (expected.compareTo(MIN_PICOS) < 0 || expected.compareTo(MAX_PICOS) > 0) {
            assertTrinoExceptionThrownBy(operation::get).hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            return;
        }
        assertThat(picoseconds(operation.get())).isEqualTo(expected);
    }
}
