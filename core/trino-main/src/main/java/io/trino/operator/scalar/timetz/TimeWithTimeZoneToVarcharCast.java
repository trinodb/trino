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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
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
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

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

        return formatAsString((int) precision, nanos * PICOSECONDS_PER_NANOSECOND, offsetMinutes);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("varchar(x)")
    public static Slice cast(@LiteralParameter("p") long precision, @SqlType("time(p) with time zone") LongTimeWithTimeZone time)
    {
        return formatAsString((int) precision, time.getPicoseconds(), time.getOffsetMinutes());
    }

    // Can't name this format() because we can't have a qualified reference to String.format() below
    private static Slice formatAsString(int precision, long picos, int offsetMinutes)
    {
        int size = 8 + // hour:minute:second
                (precision > 0 ? 1 : 0) + // period
                precision + // fraction
                6; // +hh:mm offset

        DynamicSliceOutput output = new DynamicSliceOutput(size);

        String formatted = format(
                "%02d:%02d:%02d",
                picos / PICOSECONDS_PER_HOUR,
                (picos / PICOSECONDS_PER_MINUTE) % MINUTES_PER_HOUR,
                (picos / PICOSECONDS_PER_SECOND) % SECONDS_PER_MINUTE);
        output.appendBytes(formatted.getBytes(UTF_8));

        if (precision > 0) {
            long scaledFraction = (picos % PICOSECONDS_PER_SECOND) / scaleFactor(precision, MAX_PRECISION);
            output.appendByte('.');
            output.appendBytes(format("%0" + precision + "d", scaledFraction).getBytes(UTF_8));
        }
        output.appendBytes(format("%s%02d:%02d", offsetMinutes >= 0 ? '+' : '-', abs(offsetMinutes / 60), abs(offsetMinutes % 60)).getBytes(UTF_8));

        return output.slice();
    }
}
