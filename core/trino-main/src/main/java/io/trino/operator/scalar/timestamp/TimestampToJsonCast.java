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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;

import java.io.IOException;
import java.time.format.DateTimeFormatter;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.type.DateTimes.formatTimestamp;
import static io.trino.util.JsonUtil.createJsonFactory;
import static io.trino.util.JsonUtil.createJsonGenerator;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

@ScalarOperator(CAST)
public final class TimestampToJsonCast
{
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
    private static final JsonFactory JSON_FACTORY = createJsonFactory();

    private TimestampToJsonCast() {}

    @LiteralParameters("p")
    @SqlType(JSON)
    public static Slice cast(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("timestamp(p)") long timestamp)
    {
        return toJson(formatTimestamp((int) precision, timestamp, 0, UTC, TIMESTAMP_FORMATTER));
    }

    @LiteralParameters({"x", "p"})
    @SqlType(JSON)
    public static Slice cast(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        return toJson(formatTimestamp((int) precision, timestamp.getEpochMicros(), timestamp.getPicosOfMicro(), UTC, TIMESTAMP_FORMATTER));
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
