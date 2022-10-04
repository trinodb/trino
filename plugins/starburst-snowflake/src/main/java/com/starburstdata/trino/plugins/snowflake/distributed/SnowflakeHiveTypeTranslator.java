/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.distributed;

import io.trino.plugin.hive.HiveType;
import io.trino.spi.type.Type;

import static io.trino.plugin.hive.HiveType.HIVE_TIMESTAMP;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;

/**
 * The distributed Snowflake connector is relying on the Hive connector, but
 * Hive does not have a TIME type.  To be able to create HiveColumns for TIME,
 * we're using the TIMESTAMP type info; that works because in the Snowflake connector
 * we're getting the column metadata through JDBC, so the type info isn't really used.
 */
public class SnowflakeHiveTypeTranslator
{
    private SnowflakeHiveTypeTranslator() {}

    public static HiveType toHiveType(Type type)
    {
        if (TIME_MILLIS.equals(type)) {
            return HIVE_TIMESTAMP;
        }
        if (TIMESTAMP_TZ_MILLIS.equals(type)) {
            return HIVE_TIMESTAMP;
        }

        return HiveType.toHiveType(type);
    }
}
