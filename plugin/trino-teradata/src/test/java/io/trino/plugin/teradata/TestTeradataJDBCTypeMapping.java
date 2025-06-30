/**
 * Unpublished work.
 * Copyright 2025 by Teradata Corporation. All rights reserved
 * TERADATA CORPORATION CONFIDENTIAL AND TRADE SECRET
 */

package io.trino.plugin.teradata;

import io.trino.testing.datatype.SqlDataTypeTest;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createVarcharType;

public class TestTeradataJDBCTypeMapping
        extends AbstractTeradataJDBCTest
{
    public TestTeradataJDBCTypeMapping()
    {
        super("trino_test_type");
    }

    static String padBinaryString(String prefix, int length)
    {
        // simple function to help with byte test cases
        StringBuilder result = new StringBuilder(prefix);
        while (result.length() < length * 2) {
            result.append("0");
        }
        return "X'" + result + "'";
    }

    @Override
    protected void initTables()
    {
    }

    @Test
    public void testByteint()
    {
        SqlDataTypeTest.create().addRoundTrip("byteint", "0", TINYINT, "CAST(0 AS TINYINT)").addRoundTrip("byteint", "127", TINYINT, "CAST(127 AS TINYINT)").addRoundTrip("byteint", "-128", TINYINT, "CAST(-128 AS TINYINT)").addRoundTrip("byteint", "null", TINYINT, "CAST(null AS TINYINT)").execute(getQueryRunner(), teradataJDBCCreateAndInsert("byteint"));
    }

    @Test
    public void testSmallint()
    {
        SqlDataTypeTest.create().addRoundTrip("smallint", "0", SMALLINT, "CAST(0 AS SMALLINT)").addRoundTrip("smallint", "32767", SMALLINT, "CAST(32767 AS SMALLINT)").addRoundTrip("smallint", "-32768", SMALLINT, "CAST(-32768 AS SMALLINT)").addRoundTrip("smallint", "null", SMALLINT, "CAST(null AS SMALLINT)").execute(getQueryRunner(), teradataJDBCCreateAndInsert("smallint"));
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create().addRoundTrip("integer", "0", INTEGER, "0").addRoundTrip("integer", "2147483647", INTEGER, "2147483647").addRoundTrip("integer", "-2147483648", INTEGER, "-2147483648").addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)").execute(getQueryRunner(), teradataJDBCCreateAndInsert("integer"));
    }

    @Test
    public void testBigint()
    {
        SqlDataTypeTest.create().addRoundTrip("bigint", "0", BIGINT, "CAST(0 AS BIGINT)").addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807").addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808").addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)").execute(getQueryRunner(), teradataJDBCCreateAndInsert("bigint"));
    }

    @Test
    public void testFloat()
    {
        // float / real / double are the same in teradata (64 bit)
        // trino has real and double (32 vs 64 bit)
        // we map teradata float to trino double
        SqlDataTypeTest.create().addRoundTrip("float", "0", DOUBLE, "CAST(0 AS DOUBLE)").addRoundTrip("real", "0", DOUBLE, "CAST(0 AS DOUBLE)").addRoundTrip("double precision", "0", DOUBLE, "CAST(0 AS DOUBLE)").addRoundTrip("float", "1.797e308", DOUBLE, "1.797e308").addRoundTrip("real", "1.797e308", DOUBLE, "1.797e308").addRoundTrip("double precision", "1.797e308", DOUBLE, "1.797e308").addRoundTrip("float", "2.226e-308", DOUBLE, "2.226e-308").addRoundTrip("real", "2.226e-308", DOUBLE, "2.226e-308").addRoundTrip("double precision", "2.226e-308", DOUBLE, "2.226e-308").addRoundTrip("float", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)").addRoundTrip("real", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)").addRoundTrip("double precision", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)").execute(getQueryRunner(), teradataJDBCCreateAndInsert("float"));
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create().addRoundTrip("decimal(3, 0)", "0", createDecimalType(3, 0), "CAST('0' AS decimal(3, 0))").addRoundTrip("numeric(3, 0)", "0", createDecimalType(3, 0), "CAST('0' AS decimal(3, 0))").addRoundTrip("decimal(3, 1)", "0.0", createDecimalType(3, 1), "CAST('0.0' AS decimal(3, 1))").addRoundTrip("numeric(3, 1)", "0.0", createDecimalType(3, 1), "CAST('0.0' AS decimal(3, 1))").addRoundTrip("decimal(1, 0)", "1", createDecimalType(1, 0), "CAST('1' AS decimal(1, 0))").addRoundTrip("numeric(1, 0)", "1", createDecimalType(1, 0), "CAST('1' AS decimal(1, 0))").addRoundTrip("decimal(1, 0)", "-1", createDecimalType(1, 0), "CAST('-1' AS decimal(1, 0))").addRoundTrip("numeric(1, 0)", "-1", createDecimalType(1, 0), "CAST('-1' AS decimal(1, 0))").addRoundTrip("decimal(3, 0)", "1", createDecimalType(3, 0), "CAST('1' AS decimal(3, 0))").addRoundTrip("numeric(3, 0)", "1", createDecimalType(3, 0), "CAST('1' AS decimal(3, 0))").addRoundTrip("decimal(3, 0)", "-1", createDecimalType(3, 0), "CAST('-1' AS decimal(3, 0))").addRoundTrip("numeric(3, 0)", "-1", createDecimalType(3, 0), "CAST('-1' AS decimal(3, 0))").addRoundTrip("decimal(3, 0)", "123", createDecimalType(3, 0), "CAST('123' AS decimal(3, 0))").addRoundTrip("numeric(3, 0)", "123", createDecimalType(3, 0), "CAST('123' AS decimal(3, 0))").addRoundTrip("decimal(3, 0)", "-123", createDecimalType(3, 0), "CAST('-123' AS decimal(3, 0))").addRoundTrip("numeric(3, 0)", "-123", createDecimalType(3, 0), "CAST('-123' AS decimal(3, 0))").addRoundTrip("decimal(3, 1)", "10.0", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))").addRoundTrip("numeric(3, 1)", "10.0", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))").addRoundTrip("decimal(3, 1)", "12.3", createDecimalType(3, 1), "CAST('12.3' AS decimal(3, 1))").addRoundTrip("numeric(3, 1)", "12.3", createDecimalType(3, 1), "CAST('12.3' AS decimal(3, 1))").addRoundTrip("decimal(3, 1)", "-12.3", createDecimalType(3, 1), "CAST('-12.3' AS decimal(3, 1))").addRoundTrip("numeric(3, 1)", "-12.3", createDecimalType(3, 1), "CAST('-12.3' AS decimal(3, 1))").addRoundTrip("decimal(38, 0)", "12345678901234567890123456789012345678", createDecimalType(38, 0), "CAST('12345678901234567890123456789012345678' AS decimal(38, 0))").addRoundTrip("numeric(38, 0)", "12345678901234567890123456789012345678", createDecimalType(38, 0), "CAST('12345678901234567890123456789012345678' AS decimal(38, 0))").addRoundTrip("decimal(38, 0)", "-12345678901234567890123456789012345678", createDecimalType(38, 0), "CAST('-12345678901234567890123456789012345678' AS decimal(38, 0))").addRoundTrip("numeric(38, 0)", "-12345678901234567890123456789012345678", createDecimalType(38, 0), "CAST('-12345678901234567890123456789012345678' AS decimal(38, 0))").addRoundTrip("decimal(1, 0)", "null", createDecimalType(1, 0), "CAST(null AS decimal(1, 0))").execute(getQueryRunner(), teradataJDBCCreateAndInsert("decimal"));
    }

    @Test
    public void testNumber()
    {
        SqlDataTypeTest.create().addRoundTrip("numeric(3)", "0", createDecimalType(3, 0), "CAST('0' AS decimal(3, 0))").addRoundTrip("number(5,2)", "0", createDecimalType(5, 2), "CAST('0' AS decimal(5, 2))").addRoundTrip("number(38)", "0", createDecimalType(38, 0), "CAST('0' AS decimal(38, 0))").addRoundTrip("number(38,2)", "123456789012345678901234567890123456.78", createDecimalType(38, 2), "CAST('123456789012345678901234567890123456.78' AS decimal(38, 2))").addRoundTrip("numeric(38)", "12345678901234567890123456789012345678", createDecimalType(38, 0), "CAST('12345678901234567890123456789012345678' AS decimal(38, 0))").addRoundTrip("numeric(3)", "null", createDecimalType(3, 0), "CAST(null AS decimal(3, 0))").execute(getQueryRunner(), teradataJDBCCreateAndInsert("number"));

        // TODO - these should all fail (default precision is 40 for number type)
        //        .addRoundTrip("number", "0", createDecimalType(38, 0), "CAST('0' AS decimal(38, 0))")
        //        .addRoundTrip("number(*)", "0", createDecimalType(38, 0), "CAST('0' AS decimal(38, 0))")
        //        .addRoundTrip("number(*,2)", "0", createDecimalType(38, 2), "CAST('0' AS decimal(38, 2))")
    }

    @Test
    public void testChar()
    {
        SqlDataTypeTest.create().addRoundTrip("char(3)", "''", createCharType(3), "CAST('' AS char(3))").addRoundTrip("char(3)", "' '", createCharType(3), "CAST(' ' AS char(3))").addRoundTrip("char(3)", "'  '", createCharType(3), "CAST('  ' AS char(3))").addRoundTrip("char(3)", "'   '", createCharType(3), "CAST('   ' AS char(3))").addRoundTrip("char(3)", "'A'", createCharType(3), "CAST('A' AS char(3))").addRoundTrip("char(3)", "'A  '", createCharType(3), "CAST('A  ' AS char(3))").addRoundTrip("char(3)", "' B '", createCharType(3), "CAST(' B ' AS char(3))").addRoundTrip("char(3)", "'  C'", createCharType(3), "CAST('  C' AS char(3))").addRoundTrip("char(3)", "'AB'", createCharType(3), "CAST('AB' AS char(3))").addRoundTrip("char(3)", "'ABC'", createCharType(3), "CAST('ABC' AS char(3))").addRoundTrip("char(3)", "'A C'", createCharType(3), "CAST('A C' AS char(3))").addRoundTrip("char(3)", "' BC'", createCharType(3), "CAST(' BC' AS char(3))").addRoundTrip("char(3)", "null", createCharType(3), "CAST(null AS char(3))").execute(getQueryRunner(), teradataJDBCCreateAndInsert("char"));

        // TODO - test non-latin
        // TODO - case specific / uppercase

        // truncation
        SqlDataTypeTest.create().addRoundTrip("char(3)", "'ABCD'", createCharType(3), "CAST('ABCD' AS char(3))").execute(getQueryRunner(), teradataJDBCCreateAndInsert("chart"));

        // max-size
        SqlDataTypeTest.create().addRoundTrip("char(64000)", "'max'", createCharType(64000), "CAST('max' AS char(64000))").execute(getQueryRunner(), teradataJDBCCreateAndInsert("charl"));
    }

    @Test
    public void testVarchar()
    {
        SqlDataTypeTest.create().addRoundTrip("varchar(32)", "''", createVarcharType(32), "CAST('' AS varchar(32))").addRoundTrip("varchar(32)", "' '", createVarcharType(32), "CAST(' ' AS varchar(32))").addRoundTrip("varchar(32)", "' '", createVarcharType(32), "CAST(' ' AS varchar(32))").addRoundTrip("varchar(32)", "'  '", createVarcharType(32), "CAST('  ' AS varchar(32))").addRoundTrip("varchar(32)", "'   '", createVarcharType(32), "CAST('   ' AS varchar(32))").addRoundTrip("varchar(32)", "'A'", createVarcharType(32), "CAST('A' AS varchar(32))").addRoundTrip("varchar(32)", "'A  '", createVarcharType(32), "CAST('A  ' AS varchar(32))").addRoundTrip("varchar(32)", "' B '", createVarcharType(32), "CAST(' B ' AS varchar(32))").addRoundTrip("varchar(32)", "'  C'", createVarcharType(32), "CAST('  C' AS varchar(32))").addRoundTrip("varchar(32)", "'AB'", createVarcharType(32), "CAST('AB' AS varchar(32))").addRoundTrip("varchar(32)", "'ABC'", createVarcharType(32), "CAST('ABC' AS varchar(32))").addRoundTrip("varchar(32)", "'A C'", createVarcharType(32), "CAST('A C' AS varchar(32))").addRoundTrip("varchar(32)", "' BC'", createVarcharType(32), "CAST(' BC' AS varchar(32))").addRoundTrip("varchar(32)", "null", createVarcharType(32), "CAST(null AS varchar(32))").execute(getQueryRunner(), teradataJDBCCreateAndInsert("varchar"));

        // TODO - test non-latin
        // TODO - case specific / uppercase

        // truncation
        SqlDataTypeTest.create().addRoundTrip("varchar(3)", "'ABCD'", createVarcharType(3), "CAST('ABCD' AS varchar(3))").execute(getQueryRunner(), teradataJDBCCreateAndInsert("varchart"));

        // max-size
        SqlDataTypeTest.create().addRoundTrip("long varchar", "'max'", createVarcharType(64000), "CAST('max' AS varchar(64000))").execute(getQueryRunner(), teradataJDBCCreateAndInsert("varcharl"));
    }

    @Test
    public void testByte()
    {
        SqlDataTypeTest.create().addRoundTrip("byte(3)", "'000000'XB", VARBINARY, "X'000000'").addRoundTrip("byte(3)", "'012345'XB", VARBINARY, "X'012345'").addRoundTrip("byte(3)", "'FEDCBA'XB", VARBINARY, "X'FEDCBA'").addRoundTrip("byte(3)", "'AA'XB", VARBINARY, padBinaryString("AA", 3)).addRoundTrip("byte(3)", "'00AA'XB", VARBINARY, padBinaryString("00AA", 3)).addRoundTrip("byte(3)", "'AA00'XB", VARBINARY, padBinaryString("AA00", 3)).addRoundTrip("byte(3)", "null", VARBINARY, "CAST(null AS varbinary)").execute(getQueryRunner(), teradataJDBCCreateAndInsert("byte"));

        // truncation
        SqlDataTypeTest.create().addRoundTrip("byte(3)", "'01234567'XB", VARBINARY, "X'012345'").execute(getQueryRunner(), teradataJDBCCreateAndInsert("bytet"));

        // max-size
        SqlDataTypeTest.create().addRoundTrip("byte(64000)", "'FF'XB", VARBINARY, padBinaryString("FF", 64000)).execute(getQueryRunner(), teradataJDBCCreateAndInsert("bytel"));
    }

    @Test
    public void testVarbyte()
    {
        SqlDataTypeTest.create().addRoundTrip("varbyte(32)", "'000000'XB", VARBINARY, "X'000000'").addRoundTrip("varbyte(32)", "'012345'XB", VARBINARY, "X'012345'").addRoundTrip("varbyte(32)", "'FEDCBA'XB", VARBINARY, "X'FEDCBA'").addRoundTrip("varbyte(32)", "'AA'XB", VARBINARY, "X'AA'").addRoundTrip("varbyte(32)", "'00AA'XB", VARBINARY, "X'00AA'").addRoundTrip("varbyte(32)", "'AA00'XB", VARBINARY, "X'AA00'").addRoundTrip("varbyte(32)", "null", VARBINARY, "CAST(null AS varbinary)").execute(getQueryRunner(), teradataJDBCCreateAndInsert("varbyte"));

        // truncation
        SqlDataTypeTest.create().addRoundTrip("varbyte(3)", "'01234567'XB", VARBINARY, "X'012345'").execute(getQueryRunner(), teradataJDBCCreateAndInsert("varbytet"));

        // max-size
        SqlDataTypeTest.create().addRoundTrip("varbyte(64000)", "'FF'XB", VARBINARY, "X'FF'").execute(getQueryRunner(), teradataJDBCCreateAndInsert("varbytel"));
    }

    @Test
    public void testDate()
    {
        SqlDataTypeTest.create().addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'").addRoundTrip("date", "DATE '0012-12-12'", DATE, "DATE '0012-12-12'").addRoundTrip("date", "DATE '1500-01-01'", DATE, "DATE '1500-01-01'").addRoundTrip("date", "DATE '1582-10-04'", DATE, "DATE '1582-10-04'").addRoundTrip("date", "DATE '1582-10-15'", DATE, "DATE '1582-10-15'").addRoundTrip("date", "DATE '1952-04-03'", DATE, "DATE '1952-04-03'").addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'").addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'").addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'").addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'").addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'").addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'").addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'").addRoundTrip("date", "DATE '2024-02-29'", DATE, "DATE '2024-02-29'").addRoundTrip("date", "DATE '9999-12-31'", DATE, "DATE '9999-12-31'").addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)").execute(getQueryRunner(), teradataJDBCCreateAndInsert("date"));

        // TODO - research dates around gregorian calendar switch
    }

    @Test
    public void testTime()
    {
        // teradata time = time(6)
        // trino time = time(3)
        SqlDataTypeTest.create().addRoundTrip("time", "time '00:00:00'", createTimeType(6), "CAST('00:00:00' AS TIME(6))").addRoundTrip("time(0)", "time '00:00:00'", createTimeType(0), "CAST('00:00:00' AS TIME(0))").addRoundTrip("time(2)", "time '00:00:00.00'", createTimeType(2), "CAST('00:00:00.00' AS TIME(2))").addRoundTrip("time(3)", "time '00:00:00.000'", createTimeType(3), "TIME '00:00:00.000'").addRoundTrip("time(6)", "time '00:00:00.000000'", createTimeType(6), "CAST('00:00:00.000000' AS TIME(6))").addRoundTrip("time", "time '23:59:59'", createTimeType(6), "CAST('23:59:59' AS TIME(6))").addRoundTrip("time(0)", "time '23:59:59'", createTimeType(0), "CAST('23:59:59' AS TIME(0))").addRoundTrip("time(2)", "time '23:59:59.99'", createTimeType(2), "CAST('23:59:59.99' AS TIME(2))").addRoundTrip("time(3)", "time '23:59:59.999'", createTimeType(3), "TIME '23:59:59.999'").addRoundTrip("time(6)", "time '23:59:59.999999'", createTimeType(6), "CAST('23:59:59.999999' AS TIME(6))").execute(getQueryRunner(), teradataJDBCCreateAndInsert("time"));
    }

    @Test
    public void testTimeWithTimeZone()
    {
        // teradata time = time(6)
        // trino time = time(3)
        SqlDataTypeTest.create().addRoundTrip("time(0) with time zone", "time '00:00:00-00:00'", createTimeWithTimeZoneType(0), "CAST('00:00:00-00:00' AS TIME(0) WITH TIME ZONE)").addRoundTrip("time(0) with time zone", "time '23:59:59-00:00'", createTimeWithTimeZoneType(0), "CAST('23:59:59-00:00' AS TIME(0) WITH TIME ZONE)").addRoundTrip("time(3) with time zone", "time '01:23:45.678-08:00'", createTimeWithTimeZoneType(3), "CAST('01:23:45.678-08:00' AS TIME WITH TIME ZONE)").addRoundTrip("time(6) with time zone", "time '00:00:00.000000-00:00'", createTimeWithTimeZoneType(6), "CAST('00:00:00.000000-00:00' AS TIME(6) WITH TIME ZONE)").addRoundTrip("time(6) with time zone", "time '23:59:59.999999-00:00'", createTimeWithTimeZoneType(6), "CAST('23:59:59.999999-00:00' AS TIME(6) WITH TIME ZONE)").execute(getQueryRunner(), teradataJDBCCreateAndInsert("time_tz1"));
    }

    @Test
    public void testTimestamp()
    {
        // teradata timestamp = timestamp(6)
        // trino timestamp = time(3)
        SqlDataTypeTest.create().addRoundTrip("timestamp", "timestamp '0001-01-01 00:00:00'", createTimestampType(6), "CAST('0001-01-01 00:00:00' AS TIMESTAMP(6))").addRoundTrip("timestamp", "timestamp '0001-01-01 23:59:59.999999'", createTimestampType(6), "CAST('0001-01-01 23:59:59.999999' AS TIMESTAMP(6))").addRoundTrip("timestamp(2)", "timestamp '0001-01-01 23:59:59.99'", createTimestampType(2), "CAST('0001-01-01 23:59:59.99' AS TIMESTAMP(2))").addRoundTrip("timestamp(3)", "timestamp '0001-01-01 23:59:59.999'", createTimestampType(3), "TIMESTAMP '0001-01-01 23:59:59.999'").addRoundTrip("timestamp", "timestamp '9999-12-31 23:59:59'", createTimestampType(6), "CAST('9999-12-31 23:59:59' AS TIMESTAMP(6))").addRoundTrip("timestamp", "timestamp '9999-12-31 23:59:59.999999'", createTimestampType(6), "CAST('9999-12-31 23:59:59.999999' AS TIMESTAMP(6))").addRoundTrip("timestamp(2)", "timestamp '9999-12-31 23:59:59.99'", createTimestampType(2), "CAST('9999-12-31 23:59:59.99' AS TIMESTAMP(2))").addRoundTrip("timestamp(3)", "timestamp '9999-12-31 23:59:59.999'", createTimestampType(3), "TIMESTAMP '9999-12-31 23:59:59.999'").execute(getQueryRunner(), teradataJDBCCreateAndInsert("timestamp"));
    }

    @Test
    public void testTimestampWithTimeZone()
    {
        SqlDataTypeTest.create().addRoundTrip("TIMESTAMP(0) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00-00:00'", createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 00:00:00 UTC'").addRoundTrip("TIMESTAMP(0) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00' AT 'GMT'", createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 00:00:00 UTC'").addRoundTrip("TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.000000-00:00'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-01 00:00:00.000000 UTC'").addRoundTrip("TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '1970-01-01 00:00:00.000000' AT 'GMT'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-01 00:00:00.000000 UTC'").addRoundTrip("TIMESTAMP(0) WITH TIME ZONE", "TIMESTAMP '2000-01-01 00:00:00-07:00'", createTimestampWithTimeZoneType(0), "TIMESTAMP '2000-01-01 00:00:00 -07:00'").addRoundTrip("TIMESTAMP(0) WITH TIME ZONE", "TIMESTAMP '9999-12-31 23:59:59-00:00'", createTimestampWithTimeZoneType(0), "TIMESTAMP '9999-12-31 23:59:59 UTC'").addRoundTrip("TIMESTAMP(0) WITH TIME ZONE", "TIMESTAMP '9999-12-31 23:59:59' AT 'GMT'", createTimestampWithTimeZoneType(0), "TIMESTAMP '9999-12-31 23:59:59 UTC'").addRoundTrip("TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '9999-12-31 23:59:59.999999-00:00'", createTimestampWithTimeZoneType(6), "TIMESTAMP '9999-12-31 23:59:59.999999 UTC'").addRoundTrip("TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '9999-12-31 23:59:59.999999' AT 'GMT'", createTimestampWithTimeZoneType(6), "TIMESTAMP '9999-12-31 23:59:59.999999 UTC'").execute(getQueryRunner(), teradataJDBCCreateAndInsert("timestamp_tz"));
    }

    @Test
    public void testClob()
    {
        // TODO - need to implement mapping first
    }

    @Test
    public void testBlob()
    {
        // TODO - need to implement mapping first
    }

    @Test
    public void testInterval()
    {
        // TODO - need to implement mapping first
    }

    @Test
    public void testPeriod()
    {
        // TODO - need to implement mapping first
    }

    @Test
    public void testJson()
    {
        // TODO - need to implement mapping first (presently goes to varchar)
//        SqlDataTypeTest.create()
//                // we add the byteint column so teradata will have a valid default primary index column
//                .addRoundTrip("byteint", "0", TINYINT, "CAST(0 AS TINYINT)")
//                .addRoundTrip("JSON", "NEW JSON('{\"name\": \"Alice\", \"age\": 30}')", JSON, "JSON '{\"name\": \"Alice\", \"age\": 30}'")
//                .addRoundTrip("JSON", "CAST(NULL AS JSON)", JSON, "CAST(NULL AS JSON)")
//                .execute(getQueryRunner(), teradataJDBCCreateAndInsert("json"));
    }

    @Test
    public void testArray()
    {
        // TODO - need to implement mapping first
    }

    @Test
    public void testVector()
    {
        // TODO - need to implement mapping first
    }

    @Test
    public void testUdt()
    {
        // TODO - need to implement mapping first
    }
}
