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

import com.fasterxml.jackson.core.JsonGenerator;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.StandardTypes.JSON;
import static io.prestosql.type.Timestamps.formatTimestamp;
import static io.prestosql.type.Timestamps.scaleEpochMillisToMicros;
import static io.prestosql.util.JsonUtil.JSON_FACTORY;
import static io.prestosql.util.JsonUtil.createJsonGenerator;
import static java.lang.String.format;

@ScalarOperator(CAST)
public final class TimestampToJsonCast
{
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");

    private TimestampToJsonCast() {}

    @LiteralParameters("p")
    @SqlType(JSON)
    public static Slice cast(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("timestamp(p)") long timestamp)
    {
        long epochMicros = timestamp;
        if (precision <= 3) {
            epochMicros = scaleEpochMillisToMicros(timestamp);
        }

        ZoneId zoneId = ZoneOffset.UTC;
        if (session.isLegacyTimestamp()) {
            zoneId = session.getTimeZoneKey().getZoneId();
        }

        return toJson(formatTimestamp((int) precision, epochMicros, 0, zoneId, TIMESTAMP_FORMATTER));
    }

    @LiteralParameters({"x", "p"})
    @SqlType(JSON)
    public static Slice cast(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        ZoneId zoneId = ZoneOffset.UTC;
        if (session.isLegacyTimestamp()) {
            zoneId = session.getTimeZoneKey().getZoneId();
        }

        return toJson(formatTimestamp((int) precision, timestamp.getEpochMicros(), timestamp.getPicosOfMicro(), zoneId, TIMESTAMP_FORMATTER));
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
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", formatted, JSON));
        }
    }
}
