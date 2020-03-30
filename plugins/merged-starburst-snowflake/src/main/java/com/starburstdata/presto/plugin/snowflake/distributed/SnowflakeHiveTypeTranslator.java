/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import static io.prestosql.plugin.hive.HiveType.HIVE_TIMESTAMP;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;

/**
 * The distributed Snowflake connector is relying on the Hive connector, but
 * Hive does not have a TIME type.  To be able to create HiveColumns for TIME,
 * we're using the TIMESTAMP type info; that works because in the Snowflake connector
 * we're getting the column metadata through JDBC, so the type info isn't really used.
 */
public class SnowflakeHiveTypeTranslator
        extends HiveTypeTranslator
{
    @Override
    public TypeInfo translate(Type type)
    {
        if (TIME.equals(type)) {
            return HIVE_TIMESTAMP.getTypeInfo();
        }
        if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            return HIVE_TIMESTAMP.getTypeInfo();
        }
        return super.translate(type);
    }
}
