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
package io.trino.operator.scalar.timestamptz;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.type.DateTimes;

import java.io.IOException;
import java.time.ZoneId;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.type.DateTimes.formatTimestampWithTimeZone;
import static io.trino.util.JsonUtil.createJsonFactory;
import static io.trino.util.JsonUtil.createJsonGenerator;
import static java.lang.String.format;

@ScalarOperator(CAST)
public final class TimestampWithTimeZoneToJsonCast
{
    private static final JsonFactory JSON_FACTORY = createJsonFactory();

    private TimestampWithTimeZoneToJsonCast() {}

    @LiteralParameters({"x", "p"})
    @SqlType(JSON)
    public static Slice cast(@LiteralParameter("p") long precision, @SqlType("timestamp(p) with time zone") long packedEpochMillis)
    {
        long epochMillis = unpackMillisUtc(packedEpochMillis);
        ZoneId zoneId = unpackZoneKey(packedEpochMillis).getZoneId();

        return toJson(formatTimestampWithTimeZone((int) precision, epochMillis, 0, zoneId));
    }

    @LiteralParameters({"x", "p"})
    @SqlType(JSON)
    public static Slice cast(@LiteralParameter("p") long precision, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
    {
        return toJson(DateTimes.formatTimestampWithTimeZone(
                (int) precision,
                timestamp.getEpochMillis(),
                timestamp.getPicosOfMilli(),
                getTimeZoneKey(timestamp.getTimeZoneKey()).getZoneId()));
    }

    private static Slice toJson(String formatted)
    {
        try {
            SliceOutput output = new DynamicSliceOutput(formatted.length() + 2); // 2 for the quotes
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeString(formatted);
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", formatted, JSON));
        }
    }
}
