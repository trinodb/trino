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

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimeWithTimeZone;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static io.prestosql.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.prestosql.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static io.prestosql.type.DateTimes.round;
import static java.lang.Math.floorMod;

@ScalarFunction(value = "$current_time", hidden = true)
public final class CurrentTime
{
    private CurrentTime() {}

    @LiteralParameters("p")
    @SqlType("time(p) with time zone")
    public static long shortTime(
            @LiteralParameter("p") long precision,
            ConnectorSession session,
            @SqlNullable @SqlType("time(p) with time zone") Long dummy) // need a dummy value since the type inferencer can't bind type arguments exclusively from return type
    {
        Instant start = session.getStart();
        ZoneId zoneId = session.getTimeZoneKey().getZoneId();

        long nanos = ZonedDateTime.ofInstant(start, zoneId)
                .toLocalTime()
                .toNanoOfDay();

        int offsetSeconds = zoneId.getRules()
                .getOffset(start)
                .getTotalSeconds();

        nanos = round(nanos, (int) (9 - precision)) % NANOSECONDS_PER_DAY;
        return packTimeWithTimeZone(nanos, offsetSeconds / 60);
    }

    @LiteralParameters("p")
    @SqlType("time(p) with time zone")
    public static LongTimeWithTimeZone longTime(
            @LiteralParameter("p") long precision,
            ConnectorSession session,
            @SqlNullable @SqlType("time(p) with time zone") LongTimeWithTimeZone dummy) // need a dummy value since the type inferencer can't bind type arguments exclusively from return type
    {
        Instant start = session.getStart();
        ZoneId zoneId = session.getTimeZoneKey().getZoneId();

        long nanos = ZonedDateTime.ofInstant(start, zoneId)
                .toLocalTime()
                .toNanoOfDay();

        int offsetSeconds = zoneId.getRules()
                .getOffset(start)
                .getTotalSeconds();

        long picos = floorMod(round(nanos * PICOSECONDS_PER_NANOSECOND, (int) (12 - precision)), PICOSECONDS_PER_DAY);
        return new LongTimeWithTimeZone(picos, offsetSeconds / 60);
    }
}
