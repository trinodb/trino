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

import com.google.common.io.Closer;
import io.trino.Session;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.DataType;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import io.trino.testng.services.ManageTestResources;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.BiFunction;

import static com.google.common.base.Verify.verify;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

public abstract class BaseSnowflakeTypeMappingTest
        extends AbstractTestQueryFramework
{
    protected static final int MAX_VARCHAR = 16777216;
    @ManageTestResources.Suppress(because = "Mock to remote server")
    protected final SnowflakeServer server = new SnowflakeServer();
    @ManageTestResources.Suppress(because = "Used by mocks")
    protected final Closer closer = Closer.create();
    @ManageTestResources.Suppress(because = "Mock to remote database")
    protected final TestDatabase testDatabase = closer.register(server.createTestDatabase());

    private LocalDateTime dateTimeBeforeEpoch;
    private LocalDateTime dateTimeEpoch;
    private LocalDateTime dateTimeAfterEpoch;
    protected ZoneId jvmZone;
    private LocalDateTime dateTimeGapInJvmZone1;
    private LocalDateTime dateTimeGapInJvmZone2;
    private LocalDateTime dateTimeDoubledInJvmZone;

    // no DST in 1970, but has DST in later years (e.g. 2018)
    protected ZoneId vilnius;
    private LocalDateTime dateTimeGapInVilnius;
    private LocalDateTime dateTimeDoubledInVilnius;

    // minutes offset change since 1970-01-01, no DST
    protected ZoneId kathmandu;
    private LocalDateTime dateTimeGapInKathmandu;

    @BeforeClass
    public void setUp()
    {
        dateTimeBeforeEpoch = LocalDateTime.of(1958, 1, 1, 13, 18, 3, 123_000_000);
        dateTimeEpoch = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
        dateTimeAfterEpoch = LocalDateTime.of(2019, 3, 18, 10, 1, 17, 987_000_000);

        jvmZone = ZoneId.systemDefault();

        dateTimeGapInJvmZone1 = LocalDateTime.of(1970, 1, 1, 0, 13, 42);
        checkIsGap(jvmZone, dateTimeGapInJvmZone1);
        dateTimeGapInJvmZone2 = LocalDateTime.of(2018, 4, 1, 2, 13, 55, 123_000_000);
        checkIsGap(jvmZone, dateTimeGapInJvmZone2);
        dateTimeDoubledInJvmZone = LocalDateTime.of(2018, 10, 28, 1, 33, 17, 456_000_000);
        checkIsDoubled(jvmZone, dateTimeDoubledInJvmZone);

        vilnius = ZoneId.of("Europe/Vilnius");

        dateTimeGapInVilnius = LocalDateTime.of(2018, 3, 25, 3, 17, 17);
        checkIsGap(vilnius, dateTimeGapInVilnius);
        dateTimeDoubledInVilnius = LocalDateTime.of(2018, 10, 28, 3, 33, 33, 333_000_000);
        checkIsDoubled(vilnius, dateTimeDoubledInVilnius);

        kathmandu = ZoneId.of("Asia/Kathmandu");

        dateTimeGapInKathmandu = LocalDateTime.of(1986, 1, 1, 0, 13, 7);
        checkIsGap(kathmandu, dateTimeGapInKathmandu);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        closer.close();
    }

    @Test
    public void booleanMappings()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN)
                .addRoundTrip("boolean", "false", BOOLEAN)
                .execute(getQueryRunner(), trinoCreateAsSelect());
    }

    @Test
    public void floatingPointMappings()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double", "1.0E100", DOUBLE)
                .addRoundTrip("double", "123.456E10", DOUBLE)
                .addRoundTrip("double", "nan()", DOUBLE)
                .addRoundTrip("double", "+infinity()", DOUBLE)
                .addRoundTrip("double", "-infinity()", DOUBLE)
                .addRoundTrip("double", "CAST(NULL AS double)", DOUBLE)
                .execute(getQueryRunner(), trinoCreateAsSelect());
    }

    @Test
    public void snowflakeFloatingPointMappings()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double precision", "1.0E100", DOUBLE, "double '1.0E100'")
                .addRoundTrip("double", "1.0", DOUBLE, "double '1.0'")
                .addRoundTrip("real", "123456.123456", DOUBLE, "double '123456.123456'")
                .addRoundTrip("float", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("float8", "1.0E15", DOUBLE, "double '1.0E15'")
                .addRoundTrip("float4", "1.0", DOUBLE, "double '1.0'")
                .addRoundTrip("float8", "1.23456789001234E9", DOUBLE, "double '1.23456789001234E9'")
                .addRoundTrip("real", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double", "100000.0", DOUBLE, "double '100000.0'")
                .addRoundTrip("double precision", "123000.0", DOUBLE, "double '123000.0'")
                .execute(getQueryRunner(), snowflakeCreateAsSelect());
    }

    @Test
    public void varcharMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'string 010'", createVarcharType(10), "'string 010'")
                .addRoundTrip("varchar(20)", "'string 020'", createVarcharType(20), "CAST('string 020' AS VARCHAR(20))")
                .addRoundTrip("varchar(30)", "'Upper Case'", createVarcharType(30), "CAST('Upper Case' AS VARCHAR(30))")
                .addRoundTrip("varchar(16777216)", "'string max size'", createVarcharType(MAX_VARCHAR), "CAST('string max size' AS VARCHAR(16777216))")
                .addRoundTrip("varchar(5)", "null", createVarcharType(5), "CAST(null AS VARCHAR(5))")
                .addRoundTrip("varchar(213)", "'攻殻機動隊'", createVarcharType(213), "CAST('攻殻機動隊' AS VARCHAR(213))")
                .addRoundTrip("varchar(42)", "null", createVarcharType(42), "CAST(null AS VARCHAR(42))")
                .execute(getQueryRunner(), trinoCreateAsSelect());
    }

    @Test
    public void varcharReadMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'string 010'", createVarcharType(10), "'string 010'")
                .addRoundTrip("varchar(20)", "'string 020'", createVarcharType(20), "CAST('string 020' AS VARCHAR(20))")
                .addRoundTrip("varchar(30)", "'Upper Case'", createVarcharType(30), "CAST('Upper Case' AS VARCHAR(30))")
                .addRoundTrip("varchar(16777216)", "'string max size'", createVarcharType(MAX_VARCHAR), "CAST('string max size' AS VARCHAR(16777216))")
                .addRoundTrip("character(10)", "null", createVarcharType(10), "CAST(null AS VARCHAR(10))")
                .addRoundTrip("char(100)", "'攻殻機動隊'", createVarcharType(100), "CAST('攻殻機動隊' AS VARCHAR(100))")
                .addRoundTrip("text", "'攻殻機動隊'", createVarcharType(MAX_VARCHAR), "CAST('攻殻機動隊' AS VARCHAR(16777216))")
                .addRoundTrip("string", "'攻殻機動隊'", createVarcharType(MAX_VARCHAR), "CAST('攻殻機動隊' AS VARCHAR(16777216))")
                .execute(getQueryRunner(), snowflakeCreateAsSelect());
    }

    @Test
    public void charMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "'string 010'", createVarcharType(10), "'string 010'")
                .addRoundTrip("char(20)", "'string 020          '", createVarcharType(20), "'string 020          '")
                .addRoundTrip("char(10)", "'Upper Case'", createVarcharType(10), "'Upper Case'")
                .addRoundTrip("char(10)", "null", createVarcharType(10), "CAST(null AS VARCHAR(10))")
                .execute(getQueryRunner(), trinoCreateAsSelect());
    }

    @Test
    public void charReadMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "'string 010'", createVarcharType(10), "'string 010'")
                .addRoundTrip("char(20)", "'string 020'", createVarcharType(20), "CAST('string 020' AS VARCHAR(20))")
                .addRoundTrip("char(10)", "'Upper Case'", createVarcharType(10), "CAST('Upper Case' AS VARCHAR(10))")
                .addRoundTrip("char(5)", "null", createVarcharType(5), "CAST(null AS VARCHAR(5))")
                .execute(getQueryRunner(), snowflakeCreateAsSelect());
    }

    @Test
    public void testVarbinary()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varbinary", "NULL", VARBINARY, "CAST(NULL AS VARBINARY)")
                .addRoundTrip("varbinary", "X''", VARBINARY, "X''")
                .addRoundTrip("varbinary", "X'000000000000'", VARBINARY, "X'000000000000'")
                .addRoundTrip("varbinary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("varbinary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varbinary", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbinary", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'")
                .execute(getQueryRunner(), snowflakeCreateAsSelect())
                .execute(getQueryRunner(), trinoCreateAsSelect());
    }

    @Test
    public void testBinary()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("binary", "NULL", VARBINARY, "CAST(NULL AS VARBINARY)")
                .addRoundTrip("binary", "X''", VARBINARY, "X''")
                .addRoundTrip("binary", "X'000000000000'", VARBINARY, "X'000000000000'")
                .addRoundTrip("binary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("binary(100)", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("binary(8388608)", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("binary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("binary", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("binary", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'")
                .execute(getQueryRunner(), snowflakeCreateAsSelect());
    }

    @Test
    public void decimalMapping()
    {
        numericTests("decimal", DecimalType::createDecimalType)
                .execute(getQueryRunner(), trinoCreateAsSelect());
    }

    @Test
    public void decimalReadMapping()
    {
        numericTests("decimal", DecimalType::createDecimalType).execute(getQueryRunner(), snowflakeCreateAsSelect());
        numericTests("numeric", DecimalType::createDecimalType).execute(getQueryRunner(), snowflakeCreateAsSelect());
        numericTests("number", DecimalType::createDecimalType).execute(getQueryRunner(), snowflakeCreateAsSelect());
    }

    @Test
    public void integerMappings()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("TINYINT", "0", createDecimalType(3), "CAST(0 AS DECIMAL(3))")
                .addRoundTrip("TINYINT", "null", createDecimalType(3), "CAST(null AS DECIMAL(3))")
                .addRoundTrip("SMALLINT", "0", createDecimalType(5), "CAST(0 AS DECIMAL(5))")
                .addRoundTrip("SMALLINT", "-32768", createDecimalType(5), "CAST(-32768 AS DECIMAL(5))")
                .addRoundTrip("SMALLINT", "32767", createDecimalType(5), "CAST(32767 AS DECIMAL(5))")
                .addRoundTrip("SMALLINT", "null", createDecimalType(5), "CAST(null AS DECIMAL(5))")
                .addRoundTrip("INTEGER", "0", createDecimalType(10), "CAST(0 AS DECIMAL(10))")
                .addRoundTrip("INTEGER", "-2147483648", createDecimalType(10), "CAST(-2147483648 AS DECIMAL(10))")
                .addRoundTrip("INTEGER", "2147483647", createDecimalType(10), "CAST(2147483647 AS DECIMAL(10))")
                .addRoundTrip("INTEGER", "null", createDecimalType(10), "CAST(null AS DECIMAL(10))")
                .addRoundTrip("BIGINT", "0", createDecimalType(19), "CAST(0 AS DECIMAL(19))")
                .addRoundTrip("BIGINT", "-9223372036854775807", createDecimalType(19), "CAST(-9223372036854775807 AS DECIMAL(19))")
                .addRoundTrip("BIGINT", "9223372036854775807", createDecimalType(19), "CAST(9223372036854775807 AS DECIMAL(19))")
                .addRoundTrip("BIGINT", "null", createDecimalType(19), "CAST(null AS DECIMAL(19))")
                .execute(getQueryRunner(), trinoCreateAsSelect());
    }

    private static SqlDataTypeTest numericTests(String typeName, BiFunction<Integer, Integer, Type> decimalType)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip(typeName + "(3, 0)", "193", decimalType.apply(3, 0), "CAST(193 AS DECIMAL(3, 0))") // full p
                .addRoundTrip(typeName + "(3, 0)", "19", decimalType.apply(3, 0), "CAST(19 AS DECIMAL(3, 0))") // partial p
                .addRoundTrip(typeName + "(3, 0)", "-193", decimalType.apply(3, 0), "CAST(-193 AS DECIMAL(3, 0))") // negative full p
                .addRoundTrip(typeName + "(3, 1)", "10.0", decimalType.apply(3, 1), "CAST(10.0 AS DECIMAL(3, 1))") // 0 decimal
                .addRoundTrip(typeName + "(3, 1)", "10.1", decimalType.apply(3, 1), "CAST(10.1 AS DECIMAL(3, 1))") // full ps
                .addRoundTrip(typeName + "(3, 1)", "-10.1", decimalType.apply(3, 1), "CAST(-10.1 AS DECIMAL(3, 1))") // negative ps
                .addRoundTrip(typeName + "(4, 2)", "2", decimalType.apply(4, 2), "CAST(2 AS DECIMAL(4, 2))")
                .addRoundTrip(typeName + "(4, 2)", "2.3", decimalType.apply(4, 2), "CAST(2.3 AS DECIMAL(4, 2))")
                .addRoundTrip(typeName + "(24, 2)", "2", decimalType.apply(24, 2), "CAST(2 AS DECIMAL(24, 2))")
                .addRoundTrip(typeName + "(24, 2)", "2.3", decimalType.apply(24, 2), "CAST(2.3 AS DECIMAL(24, 2))")
                .addRoundTrip(typeName + "(24, 2)", "123456789.3", decimalType.apply(24, 2), "CAST(123456789.3 AS DECIMAL(24, 2))")
                .addRoundTrip(typeName + "(24, 4)", "12345678901234567890.31", decimalType.apply(24, 4), "CAST(12345678901234567890.31 AS DECIMAL(24, 4))")
                .addRoundTrip(typeName + "(30, 5)", "3141592653589793238462643.38327", decimalType.apply(30, 5), "CAST(3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip(typeName + "(30, 5)", "-3141592653589793238462643.38327", decimalType.apply(30, 5), "CAST(-3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip(typeName + "(38, 0)", "CAST('27182818284590452353602874713526624977' AS DECIMAL(38, 0))", decimalType.apply(38, 0), "CAST('27182818284590452353602874713526624977' AS DECIMAL(38, 0))")
                .addRoundTrip(typeName + "(38, 0)", "CAST('-27182818284590452353602874713526624977' AS DECIMAL(38, 0))", decimalType.apply(38, 0), "CAST('-27182818284590452353602874713526624977' AS DECIMAL(38, 0))")
                .addRoundTrip(typeName + "(38, 37)", ".1000020000300004000050000600007000088", decimalType.apply(38, 37), "CAST(.1000020000300004000050000600007000088 AS DECIMAL(38, 37))")
                .addRoundTrip(typeName + "(38, 37)", "-.2718281828459045235360287471352662497", decimalType.apply(38, 37), "CAST(-.2718281828459045235360287471352662497 AS DECIMAL(38, 37))")
                .addRoundTrip(typeName + "(10, 3)", "null", decimalType.apply(10, 3), "CAST(null AS DECIMAL(10, 3))")
                .addRoundTrip(typeName + "(30, 5)", "null", decimalType.apply(30, 5), "CAST(null AS DECIMAL(30, 5))");
    }

    @Test
    public void testDateMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("date", "'-5877641-06-23'", DATE, "date '-5877641-06-23'") // The epoch is integer min
                .addRoundTrip("date", "'-9999-12-31'", DATE, "date '-9999-12-31'")
                .addRoundTrip("date", "'-0999-12-31'", DATE, "date '-0999-12-31'")
                .addRoundTrip("date", "'-0099-12-31'", DATE, "date '-0099-12-31'")
                .addRoundTrip("date", "'-0009-12-31'", DATE, "date '-0009-12-31'")
                .addRoundTrip("date", "'0000-01-01'", DATE, "date '0000-01-01'")
                .addRoundTrip("date", "'0001-01-01'", DATE, "date '0001-01-01'")
                .addRoundTrip("date", "'1582-10-04'", DATE, "date '1582-10-04'")
                .addRoundTrip("date", "'1582-10-05'", DATE, "date '1582-10-05'") // begin julian->gregorian switch
                .addRoundTrip("date", "'1582-10-14'", DATE, "date '1582-10-14'") // end julian->gregorian switch
                .addRoundTrip("date", "'1582-10-15'", DATE, "date '1582-10-15'")
                .addRoundTrip("date", "'1952-04-03'", DATE, "date '1952-04-03'")
                .addRoundTrip("date", "'1970-01-01'", DATE, "date '1970-01-01'")
                .addRoundTrip("date", "'1970-02-03'", DATE, "date '1970-02-03'")
                .addRoundTrip("date", "'1983-04-01'", DATE, "date '1983-04-01'")
                .addRoundTrip("date", "'1983-10-01'", DATE, "date '1983-10-01'")
                .addRoundTrip("date", "'2017-01-01'", DATE, "date '2017-01-01'")
                .addRoundTrip("date", "'2017-07-01'", DATE, "date '2017-07-01'")
                .addRoundTrip("date", "'9999-12-31'", DATE, "date '9999-12-31'")
                .addRoundTrip("date", "'5881580-07-11'", DATE, "date '5881580-07-11'") // The epoch is integer max
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL as date)")
                .execute(getQueryRunner(), trinoCreateAsSelect())
                .execute(getQueryRunner(), snowflakeCreateAsSelect());
    }

    @Test
    public void testDateOverflow()
    {
        // Snowflake causes overflow when the date value is out of integer range
        SqlDataTypeTest.create()
                .addRoundTrip("date", "'-5877641-06-22'", DATE, "date '5881580-07-11'") // '-5877641-06-22' is integer min - 1
                .addRoundTrip("date", "'5881580-07-12'", DATE, "date '-5877641-06-23'") // '5881580-07-12' is integer max + 1
                .execute(getQueryRunner(), snowflakeCreateAsSelect());
    }

    @Test
    public void testVariantReadMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("VARIANT", "to_variant('hello world')", VarcharType.createUnboundedVarcharType(), "VARCHAR 'hello world'")
                .addRoundTrip("VARIANT", "to_variant(42)", VarcharType.createUnboundedVarcharType(), "VARCHAR '42'")
                .addRoundTrip("VARIANT", "to_variant(OBJECT_CONSTRUCT('key1', 42, 'key2', 54))", VarcharType.createUnboundedVarcharType(), "VARCHAR '{\"key1\":42,\"key2\":54}'")
                .execute(getQueryRunner(), snowflakeCreateAsSelect());
    }

    @Test
    public void testObjectReadMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("VARIANT", "OBJECT_CONSTRUCT('key1',42,'key2',54)", VarcharType.createUnboundedVarcharType(), "VARCHAR '{\"key1\":42,\"key2\":54}'")
                .addRoundTrip("VARIANT", "OBJECT_CONSTRUCT('key1','foo','key2','bar')", VarcharType.createUnboundedVarcharType(), "VARCHAR '{\"key1\":\"foo\",\"key2\":\"bar\"}'")
                .execute(getQueryRunner(), snowflakeCreateAsSelect());
    }

    @Test
    public void testArrayReadMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("ARRAY", "'hello world'", VarcharType.createUnboundedVarcharType(), "VARCHAR '[\"hello world\"]'")
                .addRoundTrip("ARRAY", "42", VarcharType.createUnboundedVarcharType(), "VARCHAR '[42]'")
                .addRoundTrip("ARRAY", "OBJECT_CONSTRUCT('key1', 42, 'key2', 54)", VarcharType.createUnboundedVarcharType(), "VARCHAR '[{\"key1\":42,\"key2\":54}]'")
                .execute(getQueryRunner(), snowflakeCreateAsSelect());
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTime(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("TIME", "'00:00:00'", TIME_MILLIS, "TIME '00:00:00.000'") // gap in JVM zone on Epoch day
                .addRoundTrip("TIME", "'00:13:42'", TIME_MILLIS, "TIME '00:13:42.000'") // gap in JVM
                .addRoundTrip("TIME", "'13:18:03.123'", TIME_MILLIS, "TIME '13:18:03.123'")
                .addRoundTrip("TIME", "'14:18:03.423'", TIME_MILLIS, "TIME '14:18:03.423'")
                .addRoundTrip("TIME", "'15:18:03.523'", TIME_MILLIS, "TIME '15:18:03.523'")
                .addRoundTrip("TIME", "'16:18:03.623'", TIME_MILLIS, "TIME '16:18:03.623'")
                .addRoundTrip("TIME", "'10:01:17.987'", TIME_MILLIS, "TIME '10:01:17.987'")
                .addRoundTrip("TIME", "'19:01:17.987'", TIME_MILLIS, "TIME '19:01:17.987'")
                .addRoundTrip("TIME", "'20:01:17.987'", TIME_MILLIS, "TIME '20:01:17.987'")
                .addRoundTrip("TIME", "'21:01:17.987'", TIME_MILLIS, "TIME '21:01:17.987'")
                .addRoundTrip("TIME", "'01:33:17.456'", TIME_MILLIS, "TIME '01:33:17.456'")
                .addRoundTrip("TIME", "'03:17:17.000'", TIME_MILLIS, "TIME '03:17:17.000'")
                .addRoundTrip("TIME", "'22:59:59.000'", TIME_MILLIS, "TIME '22:59:59.000'")
                .addRoundTrip("TIME", "'22:59:59.999'", TIME_MILLIS, "TIME '22:59:59.999'")
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session))
                .execute(getQueryRunner(), session, snowflakeCreateAndInsert());
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimeArray(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip(
                        "ARRAY",
                        "ARRAY_CONSTRUCT(" +
                                "TIME '13:18:03.123'," +
                                "TIME '14:18:03.423'," +
                                "TIME '15:18:03.523'," +
                                "TIME '16:18:03.623'," +
                                "TIME '10:01:17.987'," +
                                "TIME '19:01:17.987'," +
                                "TIME '20:01:17.987'," +
                                "TIME '21:01:17.987'," +
                                "TIME '01:33:17.456'," +
                                "TIME '03:17:17.000'," +
                                "TIME '22:59:59.999')",
                        VarcharType.createUnboundedVarcharType(),
                        "VARCHAR '[" +
                                "\"13:18:03.123000000\"," +
                                "\"14:18:03.423000000\"," +
                                "\"15:18:03.523000000\"," +
                                "\"16:18:03.623000000\"," +
                                "\"10:01:17.987000000\"," +
                                "\"19:01:17.987000000\"," +
                                "\"20:01:17.987000000\"," +
                                "\"21:01:17.987000000\"," +
                                "\"01:33:17.456000000\"," +
                                "\"03:17:17.000000000\"," +
                                "\"22:59:59.999000000\"]'")
                .execute(getQueryRunner(), session, snowflakeCreateAsSelect());
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("timestamp(9)", "TIMESTAMP '1958-01-01 13:18:03.123000000'", createTimestampType(9), "TIMESTAMP '1958-01-01 13:18:03.123000000'") // dateTimeBeforeEpoch
                .addRoundTrip("timestamp(9)", "TIMESTAMP '2019-03-18 10:01:17.987000000'", createTimestampType(9), "TIMESTAMP '2019-03-18 10:01:17.987000000'") // dateTimeAfterEpoch
                .addRoundTrip("timestamp(9)", "TIMESTAMP '2018-10-28 01:33:17.456000000'", createTimestampType(9), "TIMESTAMP '2018-10-28 01:33:17.456000000'") // dateTimeDoubledInJvmZone
                .addRoundTrip("timestamp(9)", "TIMESTAMP '2018-10-28 03:33:33.333000000'", createTimestampType(9), "TIMESTAMP '2018-10-28 03:33:33.333000000'") // dateTimeDoubledInVilnius
                .addRoundTrip("timestamp(9)", "TIMESTAMP '1970-01-01 00:00:00.000000000'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.000000000'") // dateTimeEpoch, epoch also is a gap in JVM zone
                .addRoundTrip("timestamp(9)", "TIMESTAMP '1970-01-01 00:13:42.000000000'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:13:42.000000000'") // dateTimeGapInJvmZone1
                .addRoundTrip("timestamp(9)", "TIMESTAMP '2018-04-01 02:13:55.123000000'", createTimestampType(9), "TIMESTAMP '2018-04-01 02:13:55.123000000'") // dateTimeGapInJvmZone2
                .addRoundTrip("timestamp(9)", "TIMESTAMP '2018-03-25 03:17:17.000000000'", createTimestampType(9), "TIMESTAMP '2018-03-25 03:17:17.000000000'") // dateTimeGapInVilnius
                .addRoundTrip("timestamp(9)", "TIMESTAMP '1986-01-01 00:13:07.000000000'", createTimestampType(9), "TIMESTAMP '1986-01-01 00:13:07.000000000'") // dateTimeGapInKathmandu
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session))
                .execute(getQueryRunner(), session, snowflakeCreateAndInsert());
    }

    @Test
    public void testTimestampMapping()
    {
        SqlDataTypeTest.create()
                // precision 0 ends up as precision 0
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00'", "TIMESTAMP '1970-01-01 00:00:00'")

                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1'", "TIMESTAMP '1970-01-01 00:00:00.1'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.9'", "TIMESTAMP '1970-01-01 00:00:00.9'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123'", "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123000'", "TIMESTAMP '1970-01-01 00:00:00.123000'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123000000'", "TIMESTAMP '1970-01-01 00:00:00.123000000'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.999'", "TIMESTAMP '1970-01-01 00:00:00.999'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.999999999'", "TIMESTAMP '1970-01-01 00:00:00.999999999'")

                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.1'", "TIMESTAMP '2020-09-27 12:34:56.1'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.9'", "TIMESTAMP '2020-09-27 12:34:56.9'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123'", "TIMESTAMP '2020-09-27 12:34:56.123'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123000'", "TIMESTAMP '2020-09-27 12:34:56.123000'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123000000'", "TIMESTAMP '2020-09-27 12:34:56.123000000'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.999'", "TIMESTAMP '2020-09-27 12:34:56.999'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.999999999'", "TIMESTAMP '2020-09-27 12:34:56.999999999'")

                // before epoch with second fraction
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.123'", "TIMESTAMP '1969-12-31 23:59:59.123'")

                // round up to next second
                .addRoundTrip("CAST('1970-01-01 00:00:00.9999999995' AS TIMESTAMP(9))", "TIMESTAMP '1970-01-01 00:00:01.000000000'")

                // round up to next day
                .addRoundTrip("CAST('1970-01-01 23:59:59.9999999995' AS TIMESTAMP(9))", "TIMESTAMP '1970-01-02 00:00:00.000000000'")

                // negative epoch
                .addRoundTrip("CAST('1969-12-31 23:59:59.9999999995' AS TIMESTAMP(9))", "TIMESTAMP '1970-01-01 00:00:00.000000000'")

                // around Julian/Gregorian switch
                .addRoundTrip("timestamp(9)", "TIMESTAMP '1582-10-04 00:00:00.000000000'", createTimestampType(9), "TIMESTAMP '1582-10-04 00:00:00.000000000'") // before julian->gregorian switch
                // TODO https://starburstdata.atlassian.net/browse/SEP-10094
                // .addRoundTrip("timestamp(9)", "TIMESTAMP '1582-10-05 00:00:00.000000000'", createTimestampType(9), "TIMESTAMP '1582-10-05 00:00:00.000000000'") // begin julian->gregorian switch
                // .addRoundTrip("timestamp(9)", "TIMESTAMP '1582-10-14 00:00:00.000000000'", createTimestampType(9), "TIMESTAMP '1582-10-14 00:00:00.000000000'") // end julian->gregorian switch

                // smallest value
                // TODO support for jdbc connector see TestJdbcSnowflakeTypeMapping#testTimestampMappingDataCorruption https://starburstdata.atlassian.net/browse/SEP-10002
                // .addRoundTrip("timestamp(6)", "TIMESTAMP '0000-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '0000-01-01 00:00:00.000000'")
                // .addRoundTrip("timestamp(9)", "TIMESTAMP '0000-01-01 00:00:00.000000000'", createTimestampType(9), "TIMESTAMP '0000-01-01 00:00:00.000000000'")

                // almost smallest value (which is smallest in some other connectors, e.g. teradata)
                .addRoundTrip("timestamp(6)", "TIMESTAMP '0001-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '0001-01-01 00:00:00.000000'")
                .addRoundTrip("timestamp(9)", "TIMESTAMP '0001-01-01 00:00:00.000000000'", createTimestampType(9), "TIMESTAMP '0001-01-01 00:00:00.000000000'")

                .execute(getQueryRunner(), trinoCreateAsSelect())
                .execute(getQueryRunner(), trinoCreateAndInsert());
    }

    @Deprecated // TODO https://starburstdata.atlassian.net/browse/SEP-9994
    @Test
    public void testTimestampMappingNegative()
    {
        //timestamp(9) -0001-01-01 00:00:00.000000000 has different results in jdbc and distributed connectors
        try (TestTable table = new TestTable(sqls -> getQueryRunner().execute(sqls), "test_schema_2.test_timestamp", "(c1 timestamp(6))")) {
            assertQueryFails(format("INSERT INTO %s (c1) VALUES (TIMESTAMP '-0001-01-01 00:00:00.000000')", table.getName()),
                    "Failed to insert data: Timestamp '-0001-01-01T00:00' is not recognized");
        }
        try (TestTable table = new TestTable(sqls -> getQueryRunner().execute(sqls), "test_schema_2.test_timestamp", "(c1 timestamp(3))")) {
            assertQueryFails(format("INSERT INTO %s (c1) VALUES (TIMESTAMP '-0001-01-01 00:00:00.000')", table.getName()),
                    "Failed to insert data: Timestamp '-0001-01-01T00:00' is not recognized");
        }
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestampArray(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("ARRAY",
                        "ARRAY_CONSTRUCT(" +
                                "TIMESTAMP '1958-01-01 13:18:03.123'," +
                                "TIMESTAMP '1970-01-01 00:00:00.000'," +
                                "TIMESTAMP '2019-03-18 10:01:17.987'," +
                                "TIMESTAMP '1970-01-01 00:13:42.000'," +
                                "TIMESTAMP '2018-04-01 02:13:55.123'," +
                                "TIMESTAMP '2018-10-28 01:33:17.456'," +
                                "TIMESTAMP '2018-03-25 03:17:17.000'," +
                                "TIMESTAMP '1986-01-01 00:13:07.000'," +
                                "TIMESTAMP '2018-10-28 03:33:33.333')",
                        VarcharType.createUnboundedVarcharType(),
                        "VARCHAR '[" +
                                "\"1958-01-01T13:18:03.123000000Z\"," +
                                "\"1970-01-01T00:00:00.000000000Z\"," +
                                "\"2019-03-18T10:01:17.987000000Z\"," +
                                "\"1970-01-01T00:13:42.000000000Z\"," +
                                "\"2018-04-01T02:13:55.123000000Z\"," +
                                "\"2018-10-28T01:33:17.456000000Z\"," +
                                "\"2018-03-25T03:17:17.000000000Z\"," +
                                "\"1986-01-01T00:13:07.000000000Z\"," +
                                "\"2018-10-28T03:33:33.333000000Z\"]'")
                .execute(getQueryRunner(), session, snowflakeCreateAsSelect());
    }

    @Test(dataProvider = "testTimestampWithTimeZoneDataProvider")
    public void testTimestampWithTimeZone(boolean insertWithTrino, String timestampType, ZoneId resultZone)
    {
        DataType<ZonedDateTime> dataType;
        DataSetup dataSetup;

        LocalDateTime minSnowflakeDate = LocalDateTime.of(1, 1, 1, 0, 0, 0);
        LocalDateTime maxSnowflakeDate = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999000000);
        if (insertWithTrino) {
            dataType = trinoTimestampWithTimeZoneDataType(resultZone);
            dataSetup = trinoCreateAsSelect();
        }
        else {
            dataType = snowflakeSqlTimestampWithTimeZoneDataType(timestampType, resultZone);
            dataSetup = snowflakeCreateAsSelect();
        }

        if (timestampType.equals("TIMESTAMP_LTZ") && !insertWithTrino) {
            // TODO: improve tests for TIMESTAMP_LTZ
            SqlDataTypeTest.create()
                    .addRoundTrip("TIMESTAMP_LTZ", dataType.toLiteral(dateTimeEpoch.atZone(UTC)), dataType.getTrinoResultType(), dataType.toTrinoLiteral(toZone(dateTimeEpoch.atZone(UTC), resultZone)))
                    .addRoundTrip("TIMESTAMP_LTZ", dataType.toLiteral(dateTimeEpoch.atZone(kathmandu)), dataType.getTrinoResultType(), dataType.toTrinoLiteral(toZone(dateTimeEpoch.atZone(kathmandu), resultZone)))
                    .addRoundTrip("TIMESTAMP_LTZ", dataType.toLiteral(dateTimeBeforeEpoch.atZone(UTC)), dataType.getTrinoResultType(), dataType.toTrinoLiteral(toZone(dateTimeBeforeEpoch.atZone(UTC), resultZone)))
                    .addRoundTrip("TIMESTAMP_LTZ", dataType.toLiteral(dateTimeBeforeEpoch.atZone(kathmandu)), dataType.getTrinoResultType(), dataType.toTrinoLiteral(toZone(dateTimeBeforeEpoch.atZone(kathmandu), resultZone)))
                    .execute(getQueryRunner(), dataSetup);
        }
        else {
            String inputType = dataType.getInsertType();
            Type resultType = dataType.getTrinoResultType();

            SqlDataTypeTest.create()
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeEpoch.atZone(UTC)), resultType, dataType.toTrinoLiteral(toZone(dateTimeEpoch.atZone(UTC), UTC)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeEpoch.atZone(kathmandu)), resultType, dataType.toTrinoLiteral(toZone(dateTimeEpoch.atZone(kathmandu), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeBeforeEpoch.atZone(UTC)), resultType, dataType.toTrinoLiteral(toZone(dateTimeBeforeEpoch.atZone(UTC), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeBeforeEpoch.atZone(kathmandu)), resultType, dataType.toTrinoLiteral(toZone(dateTimeBeforeEpoch.atZone(kathmandu), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeAfterEpoch.atZone(UTC)), resultType, dataType.toTrinoLiteral(toZone(dateTimeAfterEpoch.atZone(UTC), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeAfterEpoch.atZone(kathmandu)), resultType, dataType.toTrinoLiteral(toZone(dateTimeAfterEpoch.atZone(kathmandu), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeDoubledInJvmZone.atZone(UTC)), resultType, dataType.toTrinoLiteral(toZone(dateTimeDoubledInJvmZone.atZone(UTC), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeDoubledInJvmZone.atZone(jvmZone)), resultType, dataType.toTrinoLiteral(toZone(dateTimeDoubledInJvmZone.atZone(jvmZone), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeDoubledInJvmZone.atZone(kathmandu)), resultType, dataType.toTrinoLiteral(toZone(dateTimeDoubledInJvmZone.atZone(kathmandu), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeDoubledInVilnius.atZone(UTC)), resultType, dataType.toTrinoLiteral(toZone(dateTimeDoubledInVilnius.atZone(UTC), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeDoubledInVilnius.atZone(vilnius)), resultType, dataType.toTrinoLiteral(toZone(dateTimeDoubledInVilnius.atZone(vilnius), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeDoubledInVilnius.atZone(kathmandu)), resultType, dataType.toTrinoLiteral(toZone(dateTimeDoubledInVilnius.atZone(kathmandu), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeGapInJvmZone1.atZone(UTC)), resultType, dataType.toTrinoLiteral(toZone(dateTimeGapInJvmZone1.atZone(UTC), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeGapInJvmZone1.atZone(kathmandu)), resultType, dataType.toTrinoLiteral(toZone(dateTimeGapInJvmZone1.atZone(kathmandu), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeGapInJvmZone2.atZone(UTC)), resultType, dataType.toTrinoLiteral(toZone(dateTimeGapInJvmZone2.atZone(UTC), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeGapInJvmZone2.atZone(kathmandu)), resultType, dataType.toTrinoLiteral(toZone(dateTimeGapInJvmZone2.atZone(kathmandu), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeGapInVilnius.atZone(kathmandu)), resultType, dataType.toTrinoLiteral(toZone(dateTimeGapInVilnius.atZone(kathmandu), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(dateTimeGapInKathmandu.atZone(vilnius)), resultType, dataType.toTrinoLiteral(toZone(dateTimeGapInKathmandu.atZone(vilnius), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(maxSnowflakeDate.atZone(UTC)), resultType, dataType.toTrinoLiteral(toZone(maxSnowflakeDate.atZone(UTC), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(maxSnowflakeDate.atZone(kathmandu)), resultType, dataType.toTrinoLiteral(toZone(maxSnowflakeDate.atZone(kathmandu), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(maxSnowflakeDate.atZone(vilnius)), resultType, dataType.toTrinoLiteral(toZone(maxSnowflakeDate.atZone(vilnius), resultZone)))
                    .addRoundTrip(inputType, dataType.toLiteral(minSnowflakeDate.atZone(UTC)), resultType, dataType.toTrinoLiteral(toZone(minSnowflakeDate.atZone(UTC), resultZone)))
                    .execute(getQueryRunner(), dataSetup);
        }
    }

    private static ZonedDateTime toZone(ZonedDateTime zonedDateTime, ZoneId resultZone)
    {
        if (!resultZone.getId().equals("UTC")) {
            return zonedDateTime.withZoneSameInstant(resultZone).withFixedOffsetZone();
        }

        if (zonedDateTime.getOffset().getTotalSeconds() == 0) {
            // convert to UTC for testing purposes
            return zonedDateTime.withZoneSameInstant(ZoneId.of("UTC"));
        }

        return zonedDateTime.withFixedOffsetZone();
    }

    @DataProvider
    public Object[][] testTimestampWithTimeZoneDataProvider()
    {
        return new Object[][] {
                {true, "TIMESTAMP_TZ", ZoneId.of("UTC")},
                {false, "TIMESTAMP_TZ", ZoneId.of("UTC")},
                {true, "TIMESTAMP_LTZ", ZoneId.of("UTC")},
                {false, "TIMESTAMP_LTZ", ZoneId.of("America/Bahia_Banderas")},
        };
    }

    @Test(dataProvider = "sessionZonesDataProvider")
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

                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00 UTC'", "TIMESTAMP '1970-01-01 00:00:00 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1 UTC'", "TIMESTAMP '1970-01-01 00:00:00.1 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.9 UTC'", "TIMESTAMP '1970-01-01 00:00:00.9 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123 UTC'", "TIMESTAMP '1970-01-01 00:00:00.123 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123000 UTC'", "TIMESTAMP '1970-01-01 00:00:00.123000 UTC'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.999 UTC'", "TIMESTAMP '1970-01-01 00:00:00.999 UTC'")
                // max supported precision
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456 UTC'", "TIMESTAMP '1970-01-01 00:00:00.123456 UTC'")

                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.1 UTC'", "TIMESTAMP '2020-09-27 12:34:56.1 UTC'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.9 UTC'", "TIMESTAMP '2020-09-27 12:34:56.9 UTC'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123 UTC'", "TIMESTAMP '2020-09-27 12:34:56.123 UTC'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123000 UTC'", "TIMESTAMP '2020-09-27 12:34:56.123000 UTC'")
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.999 UTC'", "TIMESTAMP '2020-09-27 12:34:56.999 UTC'")
                // max supported precision
                .addRoundTrip("TIMESTAMP '2020-09-27 12:34:56.123456 UTC'", "TIMESTAMP '2020-09-27 12:34:56.123456 UTC'")

                // round down
                .addRoundTrip("CAST('1970-01-01 00:00:00.1234561 UTC' AS TIMESTAMP(6) WITH TIME ZONE)", "TIMESTAMP '1970-01-01 00:00:00.123456 UTC'")

                // nano round up, end result rounds down
                .addRoundTrip("CAST('1970-01-01 00:00:00.123456499 UTC' AS TIMESTAMP(6) WITH TIME ZONE)", "TIMESTAMP '1970-01-01 00:00:00.123456 UTC'")

                // round up
                .addRoundTrip("CAST('1970-01-01 00:00:00.1234565 UTC' AS TIMESTAMP(6) WITH TIME ZONE)", "TIMESTAMP '1970-01-01 00:00:00.123457 UTC'")

                // max precision
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.111222333 UTC'", "TIMESTAMP '1970-01-01 00:00:00.111222333 UTC'")

                // round up to next second
                .addRoundTrip("CAST('1970-01-01 00:00:00.9999995 UTC' AS TIMESTAMP(6) WITH TIME ZONE)", "TIMESTAMP '1970-01-01 00:00:01.000000 UTC'")

                // round up to next day
                .addRoundTrip("CAST('1970-01-01 23:59:59.9999995 UTC' AS TIMESTAMP(6) WITH TIME ZONE)", "TIMESTAMP '1970-01-02 00:00:00.000000 UTC'")

                // negative epoch
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.999999999 UTC'", "TIMESTAMP '1969-12-31 23:59:59.999999999 UTC'")

                .execute(getQueryRunner(), session, trinoCreateAsSelect())
                .execute(getQueryRunner(), session, trinoCreateAndInsert());
    }

    @DataProvider
    public Object[][] sessionZonesDataProvider()
    {
        return new Object[][] {
                {UTC},
                {jvmZone},
                // using two non-JVM zones so that we don't need to worry what Snowflake system zone is
                // no DST in 1970, but has DST in later years (e.g. 2018)
                {ZoneId.of("Europe/Vilnius")},
                // minutes offset change since 1970-01-01, no DST
                {ZoneId.of("Asia/Kathmandu")},
                {ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
        };
    }

    private static DataType<ZonedDateTime> trinoTimestampWithTimeZoneDataType(ZoneId resultZone)
    {
        return DataType.dataType(
                "timestamp with time zone",
                TIMESTAMP_TZ_MILLIS,
                DateTimeFormatter.ofPattern("'TIMESTAMP '''yyyy-MM-dd HH:mm:ss.SSS VV''")::format,
                zonedDateTime -> {
                    if (!resultZone.getId().equals("UTC")) {
                        return zonedDateTime.withZoneSameInstant(resultZone).withFixedOffsetZone();
                    }

                    if (zonedDateTime.getOffset().getTotalSeconds() == 0) {
                        // convert to UTC for testing purposes
                        return zonedDateTime.withZoneSameInstant(ZoneId.of("UTC"));
                    }

                    return zonedDateTime.withFixedOffsetZone();
                });
    }

    private DataType<ZonedDateTime> snowflakeSqlTimestampWithTimeZoneDataType(String timestampType, ZoneId resultZone)
    {
        return DataType.dataType(
                timestampType,
                defaultTimestampWithTimeZoneType(),
                zonedDateTime -> DateTimeFormatter.ofPattern(format("'TO_%s('''yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX''')'", timestampType)).format(zonedDateTime),
                zonedDateTime -> DateTimeFormatter.ofPattern(format("'TIMESTAMP '''yyyy-MM-dd HH:mm:ss.%s VV''", "S".repeat(defaultTimestampWithTimeZoneType().getPrecision()))).format(zonedDateTime),
                zonedDateTime -> {
                    if (!resultZone.getId().equals("UTC")) {
                        return zonedDateTime.withZoneSameInstant(resultZone).withFixedOffsetZone();
                    }

                    if (zonedDateTime.getOffset().getTotalSeconds() == 0) {
                        // convert to UTC for testing purposes
                        return zonedDateTime.withZoneSameInstant(ZoneId.of("UTC"));
                    }

                    return zonedDateTime.withFixedOffsetZone();
                });
    }

    protected TimestampWithTimeZoneType defaultTimestampWithTimeZoneType()
    {
        return TIMESTAMP_TZ_NANOS;
    }

    private static void checkIsGap(ZoneId zone, LocalDateTime dateTime)
    {
        verify(isGap(zone, dateTime), "Expected %s to be a gap in %s", dateTime, zone);
    }

    private static boolean isGap(ZoneId zone, LocalDateTime dateTime)
    {
        return zone.getRules().getValidOffsets(dateTime).isEmpty();
    }

    private static void checkIsDoubled(ZoneId zone, LocalDateTime dateTime)
    {
        verify(zone.getRules().getValidOffsets(dateTime).size() == 2, "Expected %s to be doubled in %s", dateTime, zone);
    }

    protected DataSetup trinoCreateAsSelect()
    {
        return new CreateAsSelectDataSetup(
                new TrinoSqlExecutor(getQueryRunner()),
                "test_table_" + randomTableSuffix());
    }

    protected DataSetup trinoCreateAsSelect(Session session)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), "test_table_" + randomTableSuffix());
    }

    protected DataSetup trinoCreateAndInsert()
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner()), "test_insert_table_" + randomTableSuffix());
    }

    protected DataSetup snowflakeCreateAsSelect()
    {
        return new CreateAsSelectDataSetup(
                getSqlExecutor(),
                "test_table_" + randomTableSuffix());
    }

    protected DataSetup snowflakeCreateAndInsert()
    {
        return new CreateAndInsertDataSetup(
                getSqlExecutor(),
                "test_table_" + randomTableSuffix());
    }

    protected SqlExecutor getSqlExecutor()
    {
        return sql -> {
            try {
                server.executeOnDatabase(testDatabase.getName(), String.format("USE SCHEMA %s", TEST_SCHEMA), sql);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
