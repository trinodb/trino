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

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.type.DateTimes;

import java.time.LocalDateTime;

@ScalarFunction(value = "$localtimestamp", hidden = true, neverFails = true)
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
        LongTimestamp withoutRounding = localTimestamp(TimestampType.MAX_PRECISION, session, (LongTimestamp) null);
        return TimestampToTimestampCast.longToShort(precision, withoutRounding);
    }

    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static LongTimestamp localTimestamp(
            @LiteralParameter("p") long precision,
            ConnectorSession session,
            @SqlNullable @SqlType("timestamp(p)") LongTimestamp dummy) // need a dummy value since the type inferencer can't bind type arguments exclusively from return type
    {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(session.getStart(), session.getTimeZoneKey().getZoneId());
        return TimestampToTimestampCast.longToLong(precision, DateTimes.longTimestamp(localDateTime));
    }
}
