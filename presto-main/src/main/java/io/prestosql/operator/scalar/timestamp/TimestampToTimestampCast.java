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

import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.TimestampType.MAX_PRECISION;
import static io.prestosql.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_MICROSECOND;
import static io.prestosql.type.DateTimes.round;
import static io.prestosql.type.DateTimes.roundToNearest;

@ScalarOperator(CAST)
public final class TimestampToTimestampCast
{
    private TimestampToTimestampCast() {}

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision)")
    public static long shortToShort(
            @LiteralParameter("sourcePrecision") long sourcePrecision,
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("timestamp(sourcePrecision)") long value)
    {
        long epochMicros = value;

        if (sourcePrecision <= targetPrecision) {
            return epochMicros;
        }

        return round(epochMicros, (int) (MAX_SHORT_PRECISION - targetPrecision));
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision)")
    public static LongTimestamp shortToLong(@SqlType("timestamp(sourcePrecision)") long value)
    {
        return new LongTimestamp(value, 0);
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision)")
    public static long longToShort(
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("timestamp(sourcePrecision)") LongTimestamp value)
    {
        long epochMicros = value.getEpochMicros();
        if (targetPrecision < MAX_SHORT_PRECISION) {
            return round(epochMicros, (int) (MAX_SHORT_PRECISION - targetPrecision));
        }

        if (roundToNearest(value.getPicosOfMicro(), PICOSECONDS_PER_MICROSECOND) == PICOSECONDS_PER_MICROSECOND) {
            epochMicros++;
        }

        return epochMicros;
    }

    @LiteralParameters({"sourcePrecision", "targetPrecision"})
    @SqlType("timestamp(targetPrecision)")
    public static LongTimestamp longToLong(
            @LiteralParameter("targetPrecision") long targetPrecision,
            @SqlType("timestamp(sourcePrecision)") LongTimestamp value)
    {
        return new LongTimestamp(value.getEpochMicros(), (int) round(value.getPicosOfMicro(), (int) (MAX_PRECISION - targetPrecision)));
    }
}
