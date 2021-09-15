/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarcharType;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.DataTypeTest;
import io.trino.testing.datatype.SqlDataTypeTest;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.ZoneId;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.datatype.DataType.stringDataType;
import static io.trino.testing.datatype.DataType.timestampDataType;
import static io.trino.testing.datatype.DataType.varcharDataType;
import static java.lang.String.format;

public class TestDistributedSnowflakeTypeMapping
        extends BaseSnowflakeTypeMappingTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return distributedBuilder()
                .withServer(server)
                .withConnectorProperties(impersonationDisabled())
                .build();
    }

    @Test
    @Override
    public void varcharMapping()
    {
        testTypeMapping(
                DataTypeTest.create()
                        .addRoundTrip(varcharDataType(10), "string 010")
                        .addRoundTrip(varcharDataType(20), "string 020")
                        .addRoundTrip(stringDataType("varchar(" + MAX_VARCHAR + ")", createVarcharType(HiveVarchar.MAX_VARCHAR_LENGTH)), "string max size")
                        .addRoundTrip(varcharDataType(5), null)
                        .addRoundTrip(varcharDataType(213), "攻殻機動隊")
                        .addRoundTrip(varcharDataType(42), null));
    }

    @Test
    @Override
    public void varcharReadMapping()
    {
        testTypeReadMapping(
                DataTypeTest.create()
                        .addRoundTrip(stringDataType("varchar(10)", VarcharType.createVarcharType(10)), "string 010")
                        .addRoundTrip(stringDataType("varchar(20)", VarcharType.createVarcharType(20)), "string 020")
                        .addRoundTrip(stringDataType(format("varchar(%s)", MAX_VARCHAR), VarcharType.createVarcharType(HiveVarchar.MAX_VARCHAR_LENGTH)), "string max size")
                        .addRoundTrip(stringDataType("character(10)", VarcharType.createVarcharType(10)), null)
                        .addRoundTrip(stringDataType("char(100)", VarcharType.createVarcharType(100)), "攻殻機動隊")
                        .addRoundTrip(stringDataType("text", VarcharType.createVarcharType(HiveVarchar.MAX_VARCHAR_LENGTH)), "攻殻機動隊")
                        .addRoundTrip(stringDataType("string", VarcharType.createVarcharType(HiveVarchar.MAX_VARCHAR_LENGTH)), "攻殻機動隊"));
    }

    @DataProvider
    @Override
    public Object[][] testTimestampDataProvider()
    {
        return new Object[][] {
                {true, timestampDataType()},
                {false, timestampDataType(3)},
        };
    }

    @Test
    @Override
    public void testTimestampMapping()
    {
        SqlDataTypeTest.create()
                // precision 0 ends up as precision 0
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'")

                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.1'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.100'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.9'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.900'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.123'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.123000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.123000000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.999'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.999'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.999999999'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:01.000'")

                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2020-09-27 12:34:56.1'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.100'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2020-09-27 12:34:56.9'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.900'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2020-09-27 12:34:56.123'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2020-09-27 12:34:56.123000'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2020-09-27 12:34:56.123000000'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2020-09-27 12:34:56.999'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:56.999'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2020-09-27 12:34:56.999999'", createTimestampType(3), "TIMESTAMP '2020-09-27 12:34:57.000'")

                // before epoch with second fraction
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1969-12-31 23:59:59.123'", createTimestampType(3), "TIMESTAMP '1969-12-31 23:59:59.123'")

                // round up to next second
                .addRoundTrip("TIMESTAMP", "CAST('1970-01-01 00:00:00.9999999995' AS TIMESTAMP(9))", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:01.000'")

                // round up to next day
                .addRoundTrip("TIMESTAMP", "CAST('1970-01-01 23:59:59.9999999995' AS TIMESTAMP(9))", createTimestampType(3), "TIMESTAMP '1970-01-02 00:00:00.000'")

                // negative epoch
                .addRoundTrip("TIMESTAMP", "CAST('1969-12-31 23:59:59.9999999995' AS TIMESTAMP(9))", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'")

                .execute(getQueryRunner(), prestoCreateAsSelect())
                .execute(getQueryRunner(), prestoCreateAndInsert());
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    @Override
    public void testTimestampWithTimeZoneMapping(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // time doubled in JVM zone
                .addRoundTrip("TIMESTAMP '2018-10-28 01:33:17.456 UTC'", "TIMESTAMP '2018-10-28 01:33:17.456 UTC'")
                // time double in Vilnius
                .addRoundTrip("TIMESTAMP '2018-10-28 03:33:33.333 UTC'", "TIMESTAMP '2018-10-28 03:33:33.333 UTC'")
                // time gap in Vilnius
                .addRoundTrip("TIMESTAMP '2018-03-25 03:17:17.123 UTC'", "TIMESTAMP '2018-03-25 03:17:17.123 UTC'")
                // time gap in Kathmandu
                .addRoundTrip("TIMESTAMP '1986-01-01 00:13:07.123 UTC'", "TIMESTAMP '1986-01-01 00:13:07.123 UTC'")

                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00 UTC'", "TIMESTAMP '1970-01-01 00:00:00.000 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1 UTC'", "TIMESTAMP '1970-01-01 00:00:00.100 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.9 UTC'", "TIMESTAMP '1970-01-01 00:00:00.900 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123 UTC'", "TIMESTAMP '1970-01-01 00:00:00.123 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123000 UTC'", "TIMESTAMP '1970-01-01 00:00:00.123 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.999 UTC'", "TIMESTAMP '1970-01-01 00:00:00.999 UTC'")
                // max supported precision
                .addRoundTrip("TIMESTAMP WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.123456 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:00.123 UTC'")

                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.1 UTC'", "TIMESTAMP '2020-09-27 12:34:56.100 UTC'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.9 UTC'", "TIMESTAMP '2020-09-27 12:34:56.900 UTC'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123 UTC'", "TIMESTAMP '2020-09-27 12:34:56.123 UTC'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123000 UTC'", "TIMESTAMP '2020-09-27 12:34:56.123 UTC'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.999 UTC'", "TIMESTAMP '2020-09-27 12:34:56.999 UTC'")
                // max supported precision
                .addRoundTrip("TIMESTAMP WITH TIME ZONE", "TIMESTAMP '2020-09-27 12:34:56.123456 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2020-09-27 12:34:56.123 UTC'")

                // round down
                .addRoundTrip("TIMESTAMP WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.12341 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:00.123 UTC'")

                // micro round up, end result rounds down
                .addRoundTrip("TIMESTAMP WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.123499 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:00.123 UTC'")

                // round up
                .addRoundTrip("TIMESTAMP WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.1235 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:00.124 UTC'")

                // max precision
                .addRoundTrip("TIMESTAMP WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.111 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:00.111 UTC'")

                // round up to next second
                .addRoundTrip("TIMESTAMP WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.9995 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:01.000 UTC'")

                // round up to next day
                .addRoundTrip("TIMESTAMP WITH TIME ZONE", "TIMESTAMP '1970-01-01 23:59:59.9995 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-02 00:00:00.000 UTC'")

                // negative epoch
                .addRoundTrip("TIMESTAMP WITH TIME ZONE", "TIMESTAMP '1969-12-31 23:59:59.999 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1969-12-31 23:59:59.999 UTC'")

                .execute(getQueryRunner(), session, prestoCreateAsSelect())
                .execute(getQueryRunner(), session, prestoCreateAndInsert());
    }

    @Override
    protected TimestampWithTimeZoneType defaultTimestampWithTimeZoneType()
    {
        return TIMESTAMP_TZ_MILLIS;
    }
}
