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
package io.trino.operator.scalar.timetz;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimeWithTimeZone;

import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.TimeType.MAX_PRECISION;
import static io.trino.type.DateTimes.MINUTES_PER_HOUR;
import static io.trino.type.DateTimes.PICOSECONDS_PER_HOUR;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MINUTE;
import static io.trino.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_SECOND;
import static io.trino.type.DateTimes.SECONDS_PER_MINUTE;
import static io.trino.type.DateTimes.scaleFactor;
import static java.lang.Math.abs;

@ScalarOperator(CAST)
public final class TimeWithTimeZoneToVarcharCast
{
    private TimeWithTimeZoneToVarcharCast() {}

    @LiteralParameters({"x", "p"})
    @SqlType("varchar(x)")
    public static Slice cast(@LiteralParameter("p") long precision, @SqlType("time(p) with time zone") long packedTime)
    {
        long nanos = unpackTimeNanos(packedTime);
        int offsetMinutes = unpackOffsetMinutes(packedTime);

        return format((int) precision, nanos * PICOSECONDS_PER_NANOSECOND, offsetMinutes);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("varchar(x)")
    public static Slice cast(@LiteralParameter("p") long precision, @SqlType("time(p) with time zone") LongTimeWithTimeZone time)
    {
        return format((int) precision, time.getPicoseconds(), time.getOffsetMinutes());
    }

    private static Slice format(int precision, long picos, int offsetMinutes)
    {
        int size = 8 + // hour:minute:second
                (precision > 0 ? 1 : 0) + // period
                precision + // fraction
                6; // +hh:mm offset

        int hours = (int) (picos / PICOSECONDS_PER_HOUR);
        int minutes = (int) ((picos / PICOSECONDS_PER_MINUTE) % MINUTES_PER_HOUR);
        int seconds = (int) ((picos / PICOSECONDS_PER_SECOND) % SECONDS_PER_MINUTE);
        int zoneOffsetHours = abs(offsetMinutes / 60);
        int zoneOffsetMinutes = abs(offsetMinutes % 60);

        byte[] bytes = new byte[size];
        appendTwoDecimalDigits(0, bytes, hours);
        bytes[2] = ':';
        appendTwoDecimalDigits(3, bytes, minutes);
        bytes[5] = ':';
        appendTwoDecimalDigits(6, bytes, seconds);

        int offsetStartAt = 8;
        if (precision > 0) {
            bytes[8] = '.';
            long scaledFraction = (picos % PICOSECONDS_PER_SECOND) / scaleFactor(precision, MAX_PRECISION);
            for (int index = 8 + precision; index > 8; index--) {
                long temp = scaledFraction / 10;
                int digit = (int) (scaledFraction - (temp * 10));
                scaledFraction = temp;
                bytes[index] = (byte) ('0' + digit);
            }
            offsetStartAt += (precision + 1);
        }
        bytes[offsetStartAt] = offsetMinutes >= 0 ? (byte) '+' : (byte) '-';
        appendTwoDecimalDigits(offsetStartAt + 1, bytes, zoneOffsetHours);
        bytes[offsetStartAt + 3] = ':';
        appendTwoDecimalDigits(offsetStartAt + 4, bytes, zoneOffsetMinutes);

        return Slices.wrappedBuffer(bytes);
    }

    private static void appendTwoDecimalDigits(int index, byte[] bytes, int value)
    {
        int tens = value / 10;
        int ones = value - (tens * 10);
        bytes[index] = (byte) ('0' + tens);
        bytes[index + 1] = (byte) ('0' + ones);
    }
}
