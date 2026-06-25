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

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;

import java.time.ZoneId;

import static io.trino.operator.scalar.StringFunctions.trim;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.CAST;

// fallible
@ScalarOperator(CAST)
public final class CharToTimestampWithTimeZoneCast
{
    private CharToTimestampWithTimeZoneCast() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static long castToShort(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("char(x)") Slice value)
    {
        try {
            return VarcharToTimestampWithTimeZoneCast.toShort((int) precision, trim(value).toStringUtf8(), timezone -> {
                if (timezone == null) {
                    return session.getTimeZoneKey().getZoneId();
                }
                return ZoneId.of(timezone);
            });
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static LongTimestampWithTimeZone castToLong(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("char(x)") Slice value)
    {
        try {
            return VarcharToTimestampWithTimeZoneCast.toLong((int) precision, trim(value).toStringUtf8(), timezone -> {
                if (timezone == null) {
                    return session.getTimeZoneKey().getZoneId();
                }
                return ZoneId.of(timezone);
            });
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }
}
