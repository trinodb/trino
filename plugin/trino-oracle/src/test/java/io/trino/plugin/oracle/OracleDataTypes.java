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
package io.trino.plugin.oracle;

import io.trino.spi.type.Type;
import io.trino.testing.datatype.DataType;
import io.trino.testing.datatype.SqlDataTypeTest;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;

import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.lang.String.format;

public final class OracleDataTypes
{
    private OracleDataTypes() {}

    @SuppressWarnings("MisusedWeekYear")
    public static DataType<ZonedDateTime> oracleTimestamp3TimeZoneDataType()
    {
        return dataType(
                "TIMESTAMP(3) WITH TIME ZONE",
                TIMESTAMP_TZ_MILLIS,
                zonedDateTime -> {
                    String zoneId = zonedDateTime.getZone().getId();
                    if (zoneId.equals("Z")) {
                        zoneId = "UTC";
                    }
                    return format(
                            "from_tz(TIMESTAMP '%s', '%s')",
                            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSSSSS").format(zonedDateTime.toLocalDateTime()),
                            zoneId);
                },
                OracleDataTypes::normalizeForOracleStorage);
    }

    private static ZonedDateTime normalizeForOracleStorage(ZonedDateTime zonedDateTime)
    {
        String zoneId = zonedDateTime.getZone().getId();
        if (zoneId.equals("Z")) {
            // Oracle conflates UTC-equivalent zones to UTC.
            return zonedDateTime.withZoneSameInstant(ZoneId.of("UTC"));
        }
        return zonedDateTime;
    }

    /**
     * @deprecated {@code toTrinoQueryResult} concept is deprecated. Use {@link SqlDataTypeTest} instead.
     */
    @Deprecated
    private static <T> DataType<T> dataType(
            String insertType,
            Type trinoResultType,
            Function<T, String> toLiteral,
            Function<T, ?> toTrinoQueryResult)
    {
        return DataType.dataType(insertType, trinoResultType, toLiteral, toTrinoQueryResult);
    }
}
