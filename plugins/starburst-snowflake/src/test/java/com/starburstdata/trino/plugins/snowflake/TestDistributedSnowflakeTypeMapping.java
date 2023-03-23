/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TestTable;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.util.Optional;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDistributedSnowflakeTypeMapping
        extends BaseSnowflakeTypeMappingTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return distributedBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .build();
    }

    @Test
    @Override
    public void varcharMapping()
    {
        // Override because the max varchar length is different from JDBC client
        assertThatThrownBy(super::varcharMapping)
                .hasMessageContaining("""
                                expected: varchar(16777216)
                                 but was: varchar(65535)
                                 """);

        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'string 010'", createVarcharType(10), "'string 010'")
                .addRoundTrip("varchar(20)", "'string 020'", createVarcharType(20), "CAST('string 020' AS VARCHAR(20))")
                .addRoundTrip("varchar(65535)", "'string max size'", createVarcharType(HiveVarchar.MAX_VARCHAR_LENGTH), "CAST('string max size' AS VARCHAR(65535))")
                .addRoundTrip("varchar(5)", "null", createVarcharType(5), "CAST(null AS VARCHAR(5))")
                .addRoundTrip("varchar(213)", "'攻殻機動隊'", createVarcharType(213), "CAST('攻殻機動隊' AS VARCHAR(213))")
                .addRoundTrip("varchar(42)", "null", createVarcharType(42), "CAST(null AS VARCHAR(42))")
                .execute(getQueryRunner(), trinoCreateAsSelect());
    }

    @Test
    @Override
    public void varcharReadMapping()
    {
        // Override because the max varchar length is different from JDBC client
        assertThatThrownBy(super::varcharReadMapping)
                .hasMessageContaining("""
                                expected: varchar(16777216)
                                 but was: varchar(65535)
                                 """);

        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'string 010'", createVarcharType(10), "'string 010'")
                .addRoundTrip("varchar(20)", "'string 020'", createVarcharType(20), "CAST('string 020' AS VARCHAR(20))")
                .addRoundTrip("varchar(65535)", "'string max size'", createVarcharType(HiveVarchar.MAX_VARCHAR_LENGTH), "CAST('string max size' AS VARCHAR(65535))")
                .addRoundTrip("character(10)", "null", createVarcharType(10), "CAST(null AS VARCHAR(10))")
                .addRoundTrip("char(100)", "'攻殻機動隊'", createVarcharType(100), "CAST('攻殻機動隊' AS VARCHAR(100))")
                .addRoundTrip("text", "'攻殻機動隊'", createVarcharType(HiveVarchar.MAX_VARCHAR_LENGTH), "CAST('攻殻機動隊' AS VARCHAR(65535))")
                .addRoundTrip("string", "'攻殻機動隊'", createVarcharType(HiveVarchar.MAX_VARCHAR_LENGTH), "CAST('攻殻機動隊' AS VARCHAR(65535))")
                .execute(getQueryRunner(), snowflakeCreateAsSelect());
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    @Override
    public void testTimestamp(ZoneId sessionZone)
    {
        // Override because the timestamp precision of result literal is different from JDBC client
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("timestamp(9)", "TIMESTAMP '1958-01-01 13:18:03.123000000'", createTimestampType(3), "TIMESTAMP '1958-01-01 13:18:03.123'") // dateTimeBeforeEpoch
                .addRoundTrip("timestamp(9)", "TIMESTAMP '2019-03-18 10:01:17.987000000'", createTimestampType(3), "TIMESTAMP '2019-03-18 10:01:17.987'") // dateTimeAfterEpoch
                .addRoundTrip("timestamp(9)", "TIMESTAMP '2018-10-28 01:33:17.456000000'", createTimestampType(3), "TIMESTAMP '2018-10-28 01:33:17.456'") // dateTimeDoubledInJvmZone
                .addRoundTrip("timestamp(9)", "TIMESTAMP '2018-10-28 03:33:33.333000000'", createTimestampType(3), "TIMESTAMP '2018-10-28 03:33:33.333'") // dateTimeDoubledInVilnius
                .addRoundTrip("timestamp(9)", "TIMESTAMP '1970-01-01 00:00:00.000000000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'") // dateTimeEpoch, epoch also is a gap in JVM zone
                .addRoundTrip("timestamp(9)", "TIMESTAMP '1970-01-01 00:13:42.000000000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:13:42.000'") // dateTimeGapInJvmZone1
                .addRoundTrip("timestamp(9)", "TIMESTAMP '2018-04-01 02:13:55.123000000'", createTimestampType(3), "TIMESTAMP '2018-04-01 02:13:55.123'") // dateTimeGapInJvmZone2
                .addRoundTrip("timestamp(9)", "TIMESTAMP '2018-03-25 03:17:17.000000000'", createTimestampType(3), "TIMESTAMP '2018-03-25 03:17:17.000'") // dateTimeGapInVilnius
                .addRoundTrip("timestamp(9)", "TIMESTAMP '1986-01-01 00:13:07.000000000'", createTimestampType(3), "TIMESTAMP '1986-01-01 00:13:07.000'") // dateTimeGapInKathmandu
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session))
                .execute(getQueryRunner(), session, snowflakeCreateAndInsert());
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

                // around Julian/Gregorian switch
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1582-10-04 00:00:00.000000000'", createTimestampType(3), "TIMESTAMP '1582-10-04 00:00:00.000'") // before julian->gregorian switch
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1582-10-05 00:00:00.000000000'", createTimestampType(3), "TIMESTAMP '1582-10-05 00:00:00.000'") // begin julian->gregorian switch
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1582-10-14 00:00:00.000000000'", createTimestampType(3), "TIMESTAMP '1582-10-14 00:00:00.000'") // end julian->gregorian switch

                // smallest value
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '0000-01-01 00:00:00.000000'", createTimestampType(3), "TIMESTAMP '0000-01-01 00:00:00.000'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '0000-01-01 00:00:00.000000000'", createTimestampType(3), "TIMESTAMP '0000-01-01 00:00:00.000'")

                // almost smallest value (which is smallest in some other connectors, e.g. teradata)
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '0001-01-01 00:00:00.000000'", createTimestampType(3), "TIMESTAMP '0001-01-01 00:00:00.000'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '0001-01-01 00:00:00.000000000'", createTimestampType(3), "TIMESTAMP '0001-01-01 00:00:00.000'")

                .execute(getQueryRunner(), trinoCreateAsSelect())
                .execute(getQueryRunner(), trinoCreateAndInsert());
    }

    @Override
    @Deprecated // TODO https://starburstdata.atlassian.net/browse/SEP-9994
    @Test
    public void testTimestampMappingNegative()
    {
        super.testTimestampMappingNegative();
        try (TestTable table = new TestTable(sqls -> getQueryRunner().execute(sqls), "test_schema_2.test_timestamp", "(c1 timestamp(9))")) {
            assertQueryFails(format("INSERT INTO %s (c1) VALUES (TIMESTAMP '-0001-01-01 00:00:00.000000000')", table.getName()),
                    "Failed to insert data: Timestamp '-0001-01-01T00:00' is not recognized");
        }
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

                .execute(getQueryRunner(), session, trinoCreateAsSelect())
                .execute(getQueryRunner(), session, trinoCreateAndInsert());
    }

    @Override
    protected TimestampWithTimeZoneType defaultTimestampWithTimeZoneType()
    {
        return TIMESTAMP_TZ_MILLIS;
    }
}
