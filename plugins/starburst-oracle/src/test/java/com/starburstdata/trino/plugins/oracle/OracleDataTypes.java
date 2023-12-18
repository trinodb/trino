/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

import io.trino.spi.type.Type;
import io.trino.testing.datatype.DataType;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;

import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.lang.String.format;

public final class OracleDataTypes
{
    private OracleDataTypes() {}

    /* Datetime types */

    public static DataType<ZonedDateTime> prestoTimestampWithTimeZoneDataType()
    {
        return dataType(
                "timestamp with time zone",
                TIMESTAMP_TZ_MILLIS,
                DateTimeFormatter.ofPattern("'TIMESTAMP '''yyyy-MM-dd HH:mm:ss.SSS VV''")::format,
                OracleDataTypes::normalizeForOracleStorage);
    }

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
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").format(zonedDateTime.toLocalDateTime()),
                            zoneId);
                },
                OracleDataTypes::normalizeForOracleStorage);
    }

    private static ZonedDateTime normalizeForOracleStorage(ZonedDateTime zonedDateTime)
    {
        // Oracle conflates UTC-equivalent zones to UTC.
        String zoneId = zonedDateTime.getZone().getId();
        if (zoneId.equals("Z")) {
            // Oracle conflates UTC-equivalent zones to UTC.
            return zonedDateTime.withZoneSameInstant(ZoneId.of("UTC"));
        }
        return zonedDateTime;
    }

    /* Utility */

    private static <T> DataType<T> dataType(String insertType, Type prestoResultType,
            Function<T, String> toLiteral, Function<T, ?> toTrinoQueryResult)
    {
        return DataType.dataType(insertType, prestoResultType, toLiteral, toTrinoQueryResult);
    }
}
