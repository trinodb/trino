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
package io.prestosql.operator.scalar.timetz;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimeWithTimeZone;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.prestosql.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.prestosql.spi.type.TimeType.MAX_PRECISION;
import static io.prestosql.type.DateTimes.MINUTES_PER_HOUR;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_HOUR;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_MINUTE;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_SECOND;
import static io.prestosql.type.DateTimes.SECONDS_PER_MINUTE;
import static io.prestosql.type.DateTimes.scaleFactor;
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
        return formatAsString((int) precision, time.getPicoSeconds(), time.getOffsetMinutes());
    }

    // Can't name this format() because we can't have a qualified reference to String.format() below
    private static Slice formatAsString(int precision, long picos, int offsetMinutes)
    {
        int size = (int) (8 + // hour:minute:second
                (precision > 0 ? 1 : 0) + // period
                precision + // fraction
                6); // +hh:mm offset

        DynamicSliceOutput output = new DynamicSliceOutput(size);

        String formatted = format(
                "%02d:%02d:%02d",
                picos / PICOSECONDS_PER_HOUR,
                (picos / PICOSECONDS_PER_MINUTE) % MINUTES_PER_HOUR,
                (picos / PICOSECONDS_PER_SECOND) % SECONDS_PER_MINUTE);
        output.appendBytes(formatted.getBytes(UTF_8));

        if (precision > 0) {
            long scaledFraction = (picos % PICOSECONDS_PER_SECOND) / scaleFactor((int) precision, MAX_PRECISION);
            output.appendByte('.');
            output.appendBytes(format("%0" + precision + "d", scaledFraction).getBytes(UTF_8));
        }
        output.appendBytes(format("%+03d:%02d", offsetMinutes / 60, offsetMinutes % 60).getBytes(UTF_8));

        return output.slice();
    }
}
