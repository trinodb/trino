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
package io.prestosql.operator.scalar.timestamp;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.type.DateTimes;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static io.prestosql.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static io.prestosql.type.DateTimes.epochSecondToMicrosWithRounding;
import static io.prestosql.type.DateTimes.round;

@ScalarFunction(value = "$localtimestamp", hidden = true)
public final class LocalTimestamp
{
    private LocalTimestamp() {}

    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long localTimestamp(
            @LiteralParameter("p") long precision,
            ConnectorSession session,
            @SqlNullable @SqlType("timestamp(p)") Long dummy) // need a dummy value since the type inferencer can't bind type arguments exclusively from return type
    {
        Instant start = LocalDateTime.ofInstant(session.getStart(), session.getTimeZoneKey().getZoneId())
                .toInstant(ZoneOffset.UTC);

        long epochMicros = epochSecondToMicrosWithRounding(start.getEpochSecond(), ((long) start.getNano()) * PICOSECONDS_PER_NANOSECOND);
        return round(epochMicros, (int) (MAX_SHORT_PRECISION - precision));
    }

    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static LongTimestamp localTimestamp(
            @LiteralParameter("p") long precision,
            ConnectorSession session,
            @SqlNullable @SqlType("timestamp(p)") LongTimestamp dummy) // need a dummy value since the type inferencer can't bind type arguments exclusively from return type
    {
        Instant start = LocalDateTime.ofInstant(session.getStart(), session.getTimeZoneKey().getZoneId())
                .toInstant(ZoneOffset.UTC);

        return DateTimes.longTimestamp(precision, start);
    }
}
