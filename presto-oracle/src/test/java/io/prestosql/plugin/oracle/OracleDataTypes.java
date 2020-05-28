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
package io.prestosql.plugin.oracle;

import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.datatype.DataType;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;

import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.lang.String.format;

public final class OracleDataTypes
{
    private OracleDataTypes() {}

    /* Datetime types */

    public static DataType<LocalDate> dateDataType()
    {
        return dataType("DATE", TimestampType.TIMESTAMP,
                DateTimeFormatter.ofPattern("'DATE '''yyyy-MM-dd''")::format,
                LocalDate::atStartOfDay);
    }

    public static DataType<ZonedDateTime> prestoTimestampWithTimeZoneDataType()
    {
        return dataType(
                "timestamp with time zone",
                TIMESTAMP_WITH_TIME_ZONE,
                DateTimeFormatter.ofPattern("'TIMESTAMP '''yyyy-MM-dd HH:mm:ss.SSS VV''")::format,
                OracleDataTypes::normalizeForOracleStorage);
    }

    @SuppressWarnings("MisusedWeekYear")
    public static DataType<ZonedDateTime> oracleTimestamp3TimeZoneDataType()
    {
        return dataType(
                "TIMESTAMP(3) WITH TIME ZONE",
                TIMESTAMP_WITH_TIME_ZONE,
                zonedDateTime -> {
                    String zoneId = zonedDateTime.getZone().getId();
                    if (zoneId.equals("Z")) {
                        zoneId = "UTC";
                    }
                    return format(
                            "from_tz(TIMESTAMP '%s', '%s')",
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").format(zonedDateTime.toLocalDateTime()),
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

    /* Utility */

    private static <T> DataType<T> dataType(
            String insertType,
            Type prestoResultType,
            Function<T, String> toLiteral,
            Function<T, ?> toPrestoQueryResult)
    {
        return DataType.dataType(insertType, prestoResultType, toLiteral, toPrestoQueryResult);
    }
}
