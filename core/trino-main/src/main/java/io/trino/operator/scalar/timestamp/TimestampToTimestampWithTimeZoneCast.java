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

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.type.DateTimes.getMicrosOfMilli;
import static io.trino.type.DateTimes.rescale;
import static io.trino.type.DateTimes.roundToNearest;
import static io.trino.type.DateTimes.scaleEpochMicrosToMillis;
import static io.trino.type.DateTimes.scaleFactor;
import static io.trino.util.DateTimeZoneIndex.getChronology;
import static java.lang.Math.incrementExact;

@ScalarOperator(CAST)
public final class TimestampToTimestampWithTimeZoneCast
{
    private TimestampToTimestampWithTimeZoneCast() {}

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision) with time zone")
    public static long shortToShort(
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("timestamp(sourcePrecision)") long timestamp)
    {
        long epochMillis = rescale(roundDiv(timestamp, scaleFactor((int) targetPrecision, 6)), (int) targetPrecision, 3);
        return toShort(session, epochMillis);
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision) with time zone")
    public static long longToShort(
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("timestamp(sourcePrecision)") LongTimestamp timestamp)
    {
        long epochMillis = rescale(roundDiv(timestamp.getEpochMicros(), scaleFactor((int) targetPrecision, 6)), (int) targetPrecision, 3);
        return toShort(session, epochMillis);
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision) with time zone")
    public static LongTimestampWithTimeZone shortToLong(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("timestamp(sourcePrecision)") long epochMicros)
    {
        if (sourcePrecision > targetPrecision) {
            try {
                epochMicros = round(epochMicros, (int) (6 - targetPrecision));
            }
            catch (ArithmeticException e) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Timestamp out of range", e);
            }
        }

        return toLong(session, epochMicros, 0);
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision) with time zone")
    public static LongTimestampWithTimeZone longToLong(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            @LiteralParameter("targetPrecision") long targetPrecision,
            ConnectorSession session,
            @SqlType("timestamp(sourcePrecision)") LongTimestamp timestamp)
    {
        if (sourcePrecision <= targetPrecision) {
            return toLong(session, timestamp.getEpochMicros(), timestamp.getPicosOfMicro());
        }

        long epochMicros = timestamp.getEpochMicros();
        int picosOfMicro = timestamp.getPicosOfMicro();

        try {
            if (targetPrecision < 6) {
                epochMicros = round(epochMicros, (int) (6 - targetPrecision));
                picosOfMicro = 0;
            }
            else if (targetPrecision == 6) {
                if (roundToNearest(picosOfMicro, PICOSECONDS_PER_MICROSECOND) == PICOSECONDS_PER_MICROSECOND) {
                    epochMicros = incrementExact(epochMicros);
                }
                picosOfMicro = 0;
            }
            else {
                picosOfMicro = (int) round(picosOfMicro, (int) (12 - targetPrecision));
                if (picosOfMicro == PICOSECONDS_PER_MICROSECOND) {
                    epochMicros = incrementExact(epochMicros);
                    picosOfMicro = 0;
                }
            }
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Timestamp out of range", e);
        }

        return toLong(session, epochMicros, picosOfMicro);
    }

    private static long toShort(ConnectorSession session, long epochMillis)
    {
        // This cast does treat TIMESTAMP as wall time in session TZ. This means that in order to get
        // its UTC representation we need to shift the value by the offset of TZ.
        epochMillis = getChronology(session.getTimeZoneKey())
                .getZone()
                .convertLocalToUTC(epochMillis, false);

        try {
            return packDateTimeWithZone(epochMillis, session.getTimeZoneKey());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Out of range for timestamp with time zone: " + epochMillis, e);
        }
    }

    private static LongTimestampWithTimeZone toLong(ConnectorSession session, long epochMicros, int picoOfMicroFraction)
    {
        // This cast does treat TIMESTAMP as wall time in session TZ. This means that in order to get
        // its UTC representation we need to shift the value by the offset of TZ.
        long epochMillis = getChronology(session.getTimeZoneKey())
                .getZone()
                .convertLocalToUTC(scaleEpochMicrosToMillis(epochMicros), false);

        int picoOfMilliFraction = getMicrosOfMilli(epochMicros) * PICOSECONDS_PER_MICROSECOND + picoOfMicroFraction;
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picoOfMilliFraction, session.getTimeZoneKey());
    }
}
