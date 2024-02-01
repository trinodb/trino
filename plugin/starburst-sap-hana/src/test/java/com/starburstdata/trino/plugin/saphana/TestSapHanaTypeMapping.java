/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.type.CharType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.DataType;
import io.trino.testing.datatype.DataTypeTest;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.starburstdata.trino.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.datatype.DataType.doubleDataType;
import static io.trino.testing.datatype.DataType.realDataType;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSapHanaTypeMapping
        extends AbstractTestQueryFramework
{
    private static final LocalDate EPOCH_DAY = LocalDate.ofEpochDay(0);

    private TestingSapHanaServer server;

    private final ZoneId jvmZone = ZoneId.systemDefault();
    private final LocalDateTime timeGapInJvmZone1 = LocalDateTime.of(1970, 1, 1, 0, 13, 42);
    private final LocalDateTime timeGapInJvmZone2 = LocalDateTime.of(2018, 4, 1, 2, 13, 55, 123_000_000);
    private final LocalDateTime timeDoubledInJvmZone = LocalDateTime.of(2018, 10, 28, 1, 33, 17, 456_000_000);

    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");
    private final LocalDateTime timeGapInVilnius = LocalDateTime.of(2018, 3, 25, 3, 17, 17);
    private final LocalDateTime timeDoubledInVilnius = LocalDateTime.of(2018, 10, 28, 3, 33, 33, 333_000_000);

    // minutes offset change since 1970-01-01, no DST
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");
    private final LocalDateTime timeGapInKathmandu = LocalDateTime.of(1986, 1, 1, 0, 13, 7);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(TestingSapHanaServer.create());
        return createSapHanaQueryRunner(
                server,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of());
    }

    @BeforeClass
    public void setUp()
    {
        checkIsGap(jvmZone, timeGapInJvmZone1);
        checkIsGap(jvmZone, timeGapInJvmZone2);
        checkIsDoubled(jvmZone, timeDoubledInJvmZone);

        checkIsGap(vilnius, timeGapInVilnius);
        checkIsDoubled(vilnius, timeDoubledInVilnius);

        checkIsGap(kathmandu, timeGapInKathmandu);
    }

    @Test
    public void testBasicTypes()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN, "true")
                .addRoundTrip("boolean", "false", BOOLEAN, "false")
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("double", "123.45", DOUBLE, "DOUBLE '123.45'")
                .addRoundTrip("real", "123.45", REAL, "REAL '123.45'")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_basic_types"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testReal()
    {
        DataType<Float> dataType = realDataType();
        SqlDataTypeTest.create()
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'")
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS REAL)")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_real"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_real"));

        testSapHanaUnsupportedValue(dataType, Float.NaN, "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: NaN");
        testSapHanaUnsupportedValue(dataType, Float.NEGATIVE_INFINITY, "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: -Infinity");
        testSapHanaUnsupportedValue(dataType, Float.POSITIVE_INFINITY, "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: Infinity");
    }

    @Test
    public void testDouble()
    {
        DataType<Double> dataType = doubleDataType();
        SqlDataTypeTest.create()
                .addRoundTrip("double", "1.0E100", DOUBLE, "DOUBLE '1.0E100'")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_double"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_double"));

        testSapHanaUnsupportedValue(dataType, Double.NaN, "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: NaN");
        testSapHanaUnsupportedValue(dataType, Double.NEGATIVE_INFINITY, "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: -Infinity");
        testSapHanaUnsupportedValue(dataType, Double.POSITIVE_INFINITY, "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: Infinity");
    }

    private <T> void testSapHanaUnsupportedValue(DataType<T> dataType, T value, String expectedMessage)
    {
        assertThatThrownBy(() ->
                trinoCreateAsSelect("test_unsupported")
                        .setupTemporaryRelation(List.of(new DataTypeTest.Input<>(dataType, value, false)))
                        .close())
                .hasStackTraceContaining(expectedMessage);
    }

    @Test
    public void testFloat()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("float(10)", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("float(24)", "3.1415927", REAL, "REAL '3.1415927'")
                .addRoundTrip("float(25)", "3.1415927", DOUBLE, "DOUBLE '3.1415927'")
                .addRoundTrip("float(31)", "1.2345678912", DOUBLE, "DOUBLE '1.2345678912'")
                .addRoundTrip("float(53)", "1.234567891234567", DOUBLE, "DOUBLE '1.234567891234567'")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_float"));
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "NULL", createDecimalType(3, 0), "CAST(NULL AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('19' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('19' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('-193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('-193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.0' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('-10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('-10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(4, 2)", "CAST('2' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2' AS decimal(4, 2))")
                .addRoundTrip("decimal(4, 2)", "CAST('2.3' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2.3' AS decimal(4, 2))")
                .addRoundTrip("decimal(24, 2)", "NULL", createDecimalType(24, 2), "CAST(NULL AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('123456789.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('123456789.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 4)", "CAST('12345678901234567890.31' AS decimal(24, 4))", createDecimalType(24, 4), "CAST('12345678901234567890.31' AS decimal(24, 4))")
                .addRoundTrip("decimal(30, 5)", "CAST('3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(30, 5)", "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(38, 0)", "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_decimal"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"));
    }

    @Test
    public void testSmalldecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smalldecimal", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("smalldecimal", "CAST('193' AS SMALLDECIMAL)", DOUBLE, "CAST('193.0' AS DOUBLE)")
                .addRoundTrip("smalldecimal", "CAST('19' AS SMALLDECIMAL)", DOUBLE, "CAST('19.0' AS DOUBLE)")
                .addRoundTrip("smalldecimal", "CAST('-193' AS SMALLDECIMAL)", DOUBLE, "CAST('-193.0' AS DOUBLE)")
                .addRoundTrip("smalldecimal", "CAST('10.0' AS SMALLDECIMAL)", DOUBLE, "CAST('10.0' AS DOUBLE)")
                .addRoundTrip("smalldecimal", "CAST('10.1' AS SMALLDECIMAL)", DOUBLE, "CAST('10.1' AS DOUBLE)")
                .addRoundTrip("smalldecimal", "CAST('-10.1' AS SMALLDECIMAL)", DOUBLE, "CAST('-10.1' AS DOUBLE)")
                .addRoundTrip("smalldecimal", "CAST('2' AS SMALLDECIMAL)", DOUBLE, "CAST('2.0' AS DOUBLE)")
                .addRoundTrip("smalldecimal", "CAST('2.3' AS SMALLDECIMAL)", DOUBLE, "CAST('2.3' AS DOUBLE)")
                .addRoundTrip("smalldecimal", "CAST('2' AS SMALLDECIMAL)", DOUBLE, "CAST('2.0' AS DOUBLE)")
                .addRoundTrip("smalldecimal", "CAST('2.3' AS SMALLDECIMAL)", DOUBLE, "CAST('2.3' AS DOUBLE)")
                .addRoundTrip("smalldecimal", "CAST('123456789.3' AS SMALLDECIMAL)", DOUBLE, "CAST('123456789.3' AS DOUBLE)")
                // up to 16 decimal digits
                .addRoundTrip("smalldecimal", "CAST('12345678901234.31' AS SMALLDECIMAL)", DOUBLE, "CAST('12345678901234.31' AS DOUBLE)")
                .addRoundTrip("smalldecimal", "CAST('3141592653589793000000000.00000' AS SMALLDECIMAL)", DOUBLE, "CAST('3141592653589793000000000.00000' AS DOUBLE)")
                .addRoundTrip("smalldecimal", "CAST('-3141592653589793000000000.00000' AS SMALLDECIMAL)", DOUBLE, "CAST('-3141592653589793000000000.00000' AS DOUBLE)")
                // large number
                .addRoundTrip("smalldecimal", "CAST('1.234E103' AS SMALLDECIMAL)", DOUBLE, "CAST('1.234E103' AS DOUBLE)")
                .addRoundTrip("smalldecimal", "CAST('-2.34E102' AS SMALLDECIMAL)", DOUBLE, "CAST('-2.34E102' AS DOUBLE)")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_smalldecimal"));

        try (TestTable table = new TestTable(server::execute, "tpch.test_unsupported_smalldecimal", "(col SMALLDECIMAL)")) {
            testSapHanaUnsupportedValue(table.getName(), "nan()", "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: NaN");
            testSapHanaUnsupportedValue(table.getName(), "-infinity()", "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: -Infinity");
            testSapHanaUnsupportedValue(table.getName(), "infinity()", "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: Infinity");
        }
    }

    private void testSapHanaUnsupportedValue(String table, String literal, String expectedMessage)
    {
        assertThatThrownBy(() -> getQueryRunner().execute("INSERT INTO " + table + " VALUES (" + literal + ")"))
                .hasStackTraceContaining(expectedMessage);
    }

    @Test
    public void testDecimalUnbounded()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('193' AS DECIMAL)", DOUBLE, "CAST('193' AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('19' AS DECIMAL)", DOUBLE, "CAST('19' AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('-193' AS DECIMAL)", DOUBLE, "CAST('-193' AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('10.0' AS DECIMAL)", DOUBLE, "CAST('10.0' AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('10.1' AS DECIMAL)", DOUBLE, "CAST('10.1' AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('-10.1' AS DECIMAL)", DOUBLE, "CAST('-10.1' AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('2' AS DECIMAL)", DOUBLE, "CAST('2' AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('2.3' AS DECIMAL)", DOUBLE, "CAST('2.3' AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('2' AS DECIMAL)", DOUBLE, "CAST('2' AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('2.3' AS DECIMAL)", DOUBLE, "CAST('2.3' AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('123456789.3' AS DECIMAL)", DOUBLE, "CAST('123456789.3' AS DOUBLE)")
                // TODO mappings with precision >15 are lossy because of mapping hana SMALLDECIMAL and DECIMAL to java double. see testDecimalUnboundedLossyMapping()
                .addRoundTrip("decimal", "CAST('123456789012345673' AS DECIMAL)", DOUBLE, "CAST('123456789012345673' AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('12345678901234567890.31' AS DECIMAL)", DOUBLE, "CAST('12345678901234567890.31' AS DOUBLE)")
                // up to 34 decimal digits
                .addRoundTrip("decimal", "CAST('3141592653589793238462643.383271234' AS DECIMAL)", DOUBLE, "CAST('3141592653589793238462643.383271234' AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('-3141592653589793238462643.383271234' AS DECIMAL)", DOUBLE, "CAST('-3141592653589793238462643.383271234' AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('27182818284590452353602874713526624977' AS DECIMAL)", DOUBLE, "CAST('27182818284590452353602874713526624977' AS DOUBLE)")
                .addRoundTrip("decimal", "CAST('-27182818284590452353602874713526624977' AS DECIMAL)", DOUBLE, "CAST('-27182818284590452353602874713526624977' AS DOUBLE)")
                // large number
                .addRoundTrip("decimal", format("CAST('1234%s' AS DECIMAL)", "0".repeat(100)), DOUBLE, format("CAST('1234%s' AS DOUBLE)", "0".repeat(100)))
                .addRoundTrip("decimal", format("CAST('-234%s' AS DECIMAL)", "0".repeat(100)), DOUBLE, format("CAST('-234%s' AS DOUBLE)", "0".repeat(100)))
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_decimal_unbounded"));
    }

    @Deprecated // TODO mappings with precision ~ >15 are lossy because of mapping hana SMALLDECIMAL and DECIMAL to java double.
    @Test(dataProvider = "largePrecisionDecimalProvider")
    public void testDecimalUnboundedLossyMapping(String value)
    {
        // these are expected to fail as they are within java double precision
        assertThatThrownBy(() -> SqlDataTypeTest.create()
                .addRoundTrip("decimal", "CAST('123456789012345678' AS DECIMAL)", DOUBLE, "CAST('123456789012345672' AS DOUBLE)")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_decimal_unbounded")))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> SqlDataTypeTest.create()
                .addRoundTrip("decimal", format("CAST('%s' AS DECIMAL)", value), DOUBLE, "CAST('1234567890123456000' AS DOUBLE)")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_decimal_unbounded")))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> SqlDataTypeTest.create()
                .addRoundTrip("decimal", format("CAST('-%s' AS DECIMAL)", value), DOUBLE, "CAST('-1234567890123456000' AS DOUBLE)")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_decimal_unbounded")))
                .isInstanceOf(AssertionError.class);

        // these should not pass because compared values are different (see numbers endings) but they are beyond java double precision
        SqlDataTypeTest.create()
                .addRoundTrip("decimal", "CAST('123456789012345678' AS DECIMAL)", DOUBLE, "CAST('123456789012345673' AS DOUBLE)")
                .addRoundTrip("decimal", format("CAST('%s' AS DECIMAL)", value), DOUBLE, "CAST('1234567890123456700' AS DOUBLE)")
                .addRoundTrip("decimal", format("CAST('-%s' AS DECIMAL)", value), DOUBLE, "CAST('-1234567890123456700' AS DOUBLE)")
                .addRoundTrip("decimal", format("CAST('%s' AS DECIMAL)", value), DOUBLE, "CAST('1234567890123456789' AS DOUBLE)")
                .addRoundTrip("decimal", format("CAST('-%s' AS DECIMAL)", value), DOUBLE, "CAST('-1234567890123456789' AS DOUBLE)")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_decimal_unbounded"));
    }

    @DataProvider
    public Object[][] largePrecisionDecimalProvider()
    {
        return new Object[][] {
                {"1234567890123456789"},
                {"1234567890123456789.012"},
                {"1234567890123456789.012345"},
        };
    }

    @Test
    public void testChar()
    {
        // A single Unicode character that occupies 4 bytes in UTF-8 encoding and uses surrogate pairs in UTF-16 representation
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "'text_a'", createCharType(10), "CAST('text_a' AS char(10))")
                .addRoundTrip("char(255)", "'text_b'", createCharType(255), "CAST('text_b' AS char(255))")
                .addRoundTrip("char(2000)", format("'%s'", "a".repeat(2000)), createCharType(2000), format("CAST('%s' AS char(2000))", "a".repeat(2000))) // max length
                .addRoundTrip("char(25)", "'攻殻機動隊'", createCharType(25), "CAST('攻殻機動隊' AS char(25))") // Connector does not extend char type to accommodate for different counting semantics
                .addRoundTrip("char(100)", "'攻殻機動隊'", createCharType(100), "CAST('攻殻機動隊' AS char(100))")
                .addRoundTrip("char(2000)", "'攻殻機動隊'", createCharType(2000), "CAST('攻殻機動隊' AS char(2000))")
                .addRoundTrip("char(77)", "'Ну, погоди!'", createCharType(77), "CAST('Ну, погоди!' AS char(77))")
                .addRoundTrip("char(16)", "'😂'", createCharType(16), "CAST('😂' AS char(16))") // Connector does not extend char type to accommodate for different counting semantics
                .addRoundTrip("char(610)", format("'%s'", "😂".repeat(100)), createCharType(610), format("CAST('%s' AS char(610))", "😂".repeat(100))) // Connector does not extend char type to accommodate for different counting semantics
                .execute(getQueryRunner(), trinoCreateAsSelect("test_char"))
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_char"));

        // too long for a char in SAP HANA
        int length = 2001;
        //noinspection ConstantConditions
        verify(length <= CharType.MAX_LENGTH);
        SqlDataTypeTest.create()
                .addRoundTrip("char(2001)", "'text_f'", VARCHAR, format("CAST('text_f%s' AS varchar)", " ".repeat(2001 - "text_f".length())))
                .addRoundTrip("char(2001)", format("'%s'", "a".repeat(length)), VARCHAR, format("CAST('%s' AS varchar)", "a".repeat(length)))
                .addRoundTrip("char(2001)", format("'%s'", "😂".repeat(length)), VARCHAR, format("CAST('%s' AS varchar)", "😂".repeat(length)))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_char"));
    }

    @Test
    public void testNchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("nchar(10)", "'text_a'", createCharType(10), "CAST('text_a' AS char(10))")
                .addRoundTrip("nchar(255)", "'text_b'", createCharType(255), "CAST('text_b' AS char(255))")
                .addRoundTrip("nchar(2000)", format("'%s'", "a".repeat(2000)), createCharType(2000), format("CAST('%s' AS char(2000))", "a".repeat(2000)))
                .addRoundTrip("nchar(25)", "'攻殻機動隊'", createCharType(25), "CAST('攻殻機動隊' AS char(25))")
                .addRoundTrip("nchar(100)", "'攻殻機動隊'", createCharType(100), "CAST('攻殻機動隊' AS char(100))")
                .addRoundTrip("nchar(2000)", "'攻殻機動隊'", createCharType(2000), "CAST('攻殻機動隊' AS char(2000))")
                .addRoundTrip("nchar(77)", "'Ну, погоди!'", createCharType(77), "CAST('Ну, погоди!' AS char(77))")
                .addRoundTrip("nchar(16)", "'😂'", createCharType(16), "CAST('😂' AS char(16))")
                .addRoundTrip("nchar(610)", format("'%s'", "😂".repeat(100)), createCharType(610), format("CAST('%s' AS char(610))", "😂".repeat(100)))
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_nchar"));
    }

    @Test
    public void testVarchar()
    {
        // varchar(p)
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("varchar(5000)", format("'%s'", "a".repeat(5000)), createVarcharType(5000), format("CAST('%s' AS varchar(5000))", "a".repeat(5000)))
                .addRoundTrip("varchar(25)", "'攻殻機動隊'", createVarcharType(25), "CAST('攻殻機動隊' AS varchar(25))")
                .addRoundTrip("varchar(100)", "'攻殻機動隊'", createVarcharType(100), "CAST('攻殻機動隊' AS varchar(100))")
                .addRoundTrip("varchar(5000)", "'攻殻機動隊'", createVarcharType(5000), "CAST('攻殻機動隊' AS varchar(5000))")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS varchar(77))")
                .addRoundTrip("varchar(16)", "'😂'", createVarcharType(16), "CAST('😂' AS varchar(16))")
                .addRoundTrip("varchar(610)", format("'%s'", "😂".repeat(100)), createVarcharType(610), format("CAST('%s' AS varchar(610))", "😂".repeat(100)))
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"));

        // varchar
        SqlDataTypeTest.create()
                .addRoundTrip("varchar", "'a'", createVarcharType(1), "'a'")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));

        SqlDataTypeTest.create()
                .addRoundTrip("varchar", "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip("varchar", "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip("varchar", format("'%s'", "a".repeat(5000)), VARCHAR, format("CAST('%s' AS varchar)", "a".repeat(5000)))
                .addRoundTrip("varchar", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                .addRoundTrip("varchar", "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("varchar", format("'%s'", "😂".repeat(100)), VARCHAR, format("CAST('%s' AS varchar)", "😂".repeat(100)))
                .addRoundTrip("varchar", "'text_f'", VARCHAR, "CAST('text_f' AS varchar)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"));
    }

    @Test
    public void testNvarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("nvarchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip("nvarchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("nvarchar(5000)", "'text_d'", createVarcharType(5000), "CAST('text_d' AS varchar(5000))")
                .addRoundTrip("nvarchar(25)", "'攻殻機動隊'", createVarcharType(25), "CAST('攻殻機動隊' AS varchar(25))")
                .addRoundTrip("nvarchar(100)", "'攻殻機動隊'", createVarcharType(100), "CAST('攻殻機動隊' AS varchar(100))")
                .addRoundTrip("nvarchar(5000)", "'攻殻機動隊'", createVarcharType(5000), "CAST('攻殻機動隊' AS varchar(5000))")
                .addRoundTrip("nvarchar(77)", "'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS varchar(77))")
                .addRoundTrip("nvarchar(16)", "'😂'", createVarcharType(16), "CAST('😂' AS varchar(16))")
                .addRoundTrip("nvarchar(610)", format("'%s'", "😂".repeat(100)), createVarcharType(610), format("CAST('%s' AS varchar(610))", "😂".repeat(100)))
                .addRoundTrip("nvarchar", "'a'", createVarcharType(1), "'a'")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testAlphanum()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("alphanum(10)", "NULL", createVarcharType(10), "CAST(NULL AS varchar(10))")
                .addRoundTrip("alphanum(10)", "''", createVarcharType(10), "CAST('' AS varchar(10))")
                .addRoundTrip("alphanum(10)", "'abcdef'", createVarcharType(10), "CAST('abcdef' AS varchar(10))")
                .addRoundTrip("alphanum(10)", "'123456'", createVarcharType(10), "CAST('0000123456' AS varchar(10))") // "purely numeric value" is a distinguished case in documentation
                .addRoundTrip("alphanum(127)", format("'%s'", "a".repeat(127)), createVarcharType(127), format("CAST('%s' AS varchar(127))", "a".repeat(127))) // max length
                .addRoundTrip("alphanum", "''", createVarcharType(1), "CAST('' AS varchar(1))") // default length
                .addRoundTrip("alphanum", "'a'", createVarcharType(1), "CAST('a' AS varchar(1))") // default length
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testShorttext()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("shorttext(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip("shorttext(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("shorttext(5000)", "'text_d'", createVarcharType(5000), "CAST('text_d' AS varchar(5000))")
                .addRoundTrip("shorttext(25)", "'攻殻機動隊'", createVarcharType(25), "CAST('攻殻機動隊' AS varchar(25))")
                .addRoundTrip("shorttext(100)", "'攻殻機動隊'", createVarcharType(100), "CAST('攻殻機動隊' AS varchar(100))")
                .addRoundTrip("shorttext(5000)", "'攻殻機動隊'", createVarcharType(5000), "CAST('攻殻機動隊' AS varchar(5000))")
                .addRoundTrip("shorttext(77)", "'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS varchar(77))")
                .addRoundTrip("shorttext(16)", "'😂'", createVarcharType(16), "CAST('😂' AS varchar(16))")
                .addRoundTrip("shorttext(610)", format("'%s'", "😂".repeat(100)), createVarcharType(610), format("CAST('%s' AS varchar(610))", "😂".repeat(100)))
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testText()
    {
        characterDataTypeTest("text")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testBintext()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bintext", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .addRoundTrip("bintext", "''", VARCHAR, "CAST('' AS varchar)")
                .addRoundTrip("bintext", "'abc'", VARCHAR, "CAST('abc' AS varchar)")
                .addRoundTrip("bintext", format("'%s'", "a".repeat(500)), createUnboundedVarcharType(), format("CAST('%s' AS varchar)", "a".repeat(500)))
                .addRoundTrip("bintext", "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("bintext", "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                // TODO SAMPLE_LENGTHY_CHARACTER comes back garbled
                // TODO SAMPLE_LENGTHY_CHARACTER.repeat(100) comes back garbled
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testClob()
    {
        characterDataTypeTest("clob")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testNclob()
    {
        characterDataTypeTest("nclob")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));
    }

    private SqlDataTypeTest characterDataTypeTest(String inputType)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip(inputType, "'text_a'", VARCHAR, "CAST('text_a' AS varchar)")
                .addRoundTrip(inputType, "'text_b'", VARCHAR, "CAST('text_b' AS varchar)")
                .addRoundTrip(inputType, "'text_d'", VARCHAR, "CAST('text_d' AS varchar)")
                .addRoundTrip(inputType, "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip(inputType, "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip(inputType, "'攻殻機動隊'", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip(inputType, "'Ну, погоди!'", VARCHAR, "CAST('Ну, погоди!' AS varchar)")
                .addRoundTrip(inputType, "'😂'", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip(inputType, format("'%s'", "😂".repeat(100)), VARCHAR, format("CAST('%s' AS varchar)", "😂".repeat(100)))
                .addRoundTrip(inputType, "'text_f'", VARCHAR, "CAST('text_f' AS varchar)");
    }

    @Test
    public void testVarbinary()
    {
        varbinaryTestCases("blob")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("varbinary(29)")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("varbinary(5000)") // longest allowed
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases("varbinary")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varbinary"));
    }

    private SqlDataTypeTest varbinaryTestCases(String inputType)
    {
        return SqlDataTypeTest.create()
                .addRoundTrip(inputType, "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip(inputType, "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip(inputType, "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip(inputType, "NULL", VARBINARY, "CAST(NULL AS VARBINARY)")
                .addRoundTrip(inputType, "X''", VARBINARY, "X''")
                .addRoundTrip(inputType, "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'");
    }

    @Test
    public void testDate()
    {
        ZoneId jvmZone = ZoneId.systemDefault();
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        checkIsGap(jvmZone, dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(someZone, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(someZone, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            SqlDataTypeTest.create()
                    .addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'") // min value in SAP HANA
                    .addRoundTrip("date", "DATE '1952-04-03'", DATE, "DATE '1952-04-03'") // before epoch
                    .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                    .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                    .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                    .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                    .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                    .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                    .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'")
                    .addRoundTrip("date", "DATE '9999-12-31'", DATE, "DATE '9999-12-31'") // max value in SAP HANA
                    .execute(getQueryRunner(), session, sapHanaCreateAndInsert("tpch.test_date"))
                    .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_date"));
        }
    }

    @Test
    public void testDateJulianGregorianSwitch()
    {
        // Merge into 'testDate()' when converting the method to SqlDataTypeTest. Currently, we can't test these values with DataTypeTest.
        SqlDataTypeTest.create()
                // SAP HANA adds 10 days
                .addRoundTrip("DATE", "'1582-10-05'", DATE, "DATE '1582-10-15'")
                .addRoundTrip("DATE", "'1582-10-14'", DATE, "DATE '1582-10-24'")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_julian_gregorian"))
                .execute(getQueryRunner(), trinoCreateAsSelect("tpch.test_julian_gregorian"));
    }

    @Test
    public void testUnsupportedDate()
    {
        // The range of the date value is between 0001-01-01 and 9999-12-31 in SAP HANA
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_unsupported_date", "(dt DATE)")) {
            assertQueryFails(format("INSERT INTO %s VALUES (DATE '0000-12-31')", table.getName()), "SAP DBTech JDBC: Date/time value out of range.*");
            assertQueryFails(format("INSERT INTO %s VALUES (DATE '10000-01-01')", table.getName()), "SAP DBTech JDBC: Date/time value out of range.*");
        }
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testSapHanaTime(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        LocalTime timeGapInJvmZone = LocalTime.of(0, 12, 34, 567_000_000);
        checkIsGap(jvmZone, timeGapInJvmZone.atDate(EPOCH_DAY));

        // SAP HANA's TIME does not support second fraction
        SqlDataTypeTest.create()
                .addRoundTrip("time", "TIME '01:12:34.000000000'", createTimeType(0), "TIME '01:12:34'")
                .addRoundTrip("time", "TIME '02:12:34.000000000'", createTimeType(0), "TIME '02:12:34'")
                .addRoundTrip("time", "TIME '02:12:34.001000000'", createTimeType(0), "TIME '02:12:34'")
                .addRoundTrip("time", "TIME '03:12:34.000000000'", createTimeType(0), "TIME '03:12:34'")
                .addRoundTrip("time", "TIME '04:12:34.000000000'", createTimeType(0), "TIME '04:12:34'")
                .addRoundTrip("time", "TIME '05:12:34.000000000'", createTimeType(0), "TIME '05:12:34'")
                .addRoundTrip("time", "TIME '06:12:34.000000000'", createTimeType(0), "TIME '06:12:34'")
                .addRoundTrip("time", "TIME '09:12:34.000000000'", createTimeType(0), "TIME '09:12:34'")
                .addRoundTrip("time", "TIME '10:12:34.000000000'", createTimeType(0), "TIME '10:12:34'")
                .addRoundTrip("time", "TIME '15:12:34.567000000'", createTimeType(0), "TIME '15:12:34'")
                .addRoundTrip("time", "TIME '23:59:59.999000000'", createTimeType(0), "TIME '23:59:59'")
                // epoch is also a gap in JVM zone
                .addRoundTrip("time", "TIME '00:00:00.000000000'", createTimeType(0), "TIME '00:00:00'")
                .addRoundTrip("time", "TIME '00:12:34.567000000'", createTimeType(0), "TIME '00:12:34'")
                .execute(getQueryRunner(), session, sapHanaCreateAndInsert("tpch.test_time"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testPrestoTime(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        LocalTime timeGapInJvmZone = LocalTime.of(0, 12, 34);
        checkIsGap(jvmZone, timeGapInJvmZone.atDate(EPOCH_DAY));

        // TODO: Need to check round up behavior
        SqlDataTypeTest.create()
                .addRoundTrip("time(0)", "TIME '01:12:34'", createTimeType(0), "TIME '01:12:34'")
                .addRoundTrip("time(1)", "TIME '02:12:34.0'", createTimeType(0), "TIME '02:12:34'")
                .addRoundTrip("time(2)", "TIME '02:12:34.00'", createTimeType(0), "TIME '02:12:34'")
                .addRoundTrip("time(3)", "TIME '03:12:34.000'", createTimeType(0), "TIME '03:12:34'")
                .addRoundTrip("time(4)", "TIME '04:12:34.0000'", createTimeType(0), "TIME '04:12:34'")
                .addRoundTrip("time(5)", "TIME '05:12:34.00000'", createTimeType(0), "TIME '05:12:34'")
                .addRoundTrip("time(6)", "TIME '06:12:34.000000'", createTimeType(0), "TIME '06:12:34'")
                .addRoundTrip("time(7)", "TIME '09:12:34.0000000'", createTimeType(0), "TIME '09:12:34'")
                .addRoundTrip("time(8)", "TIME '10:12:34.00000000'", createTimeType(0), "TIME '10:12:34'")
                .addRoundTrip("time(9)", "TIME '15:12:34.567000000'", createTimeType(0), "TIME '15:12:35'")
                .addRoundTrip("time(10)", "TIME '23:59:59.999000000'", createTimeType(0), "TIME '00:00:00'")
                // highest possible value
                .addRoundTrip("time(9)", "TIME '23:59:59.999999999'", createTimeType(0), "TIME '00:00:00'")
                // epoch is also a gap in JVM zone
                .addRoundTrip("time(0)", "TIME '00:00:00'", createTimeType(0), "TIME '00:00:00'")
                .addRoundTrip("time(3)", "TIME '00:00:00.000'", createTimeType(0), "TIME '00:00:00'")
                .addRoundTrip("time(6)", "TIME '00:00:00.000000'", createTimeType(0), "TIME '00:00:00'")
                .addRoundTrip("time(9)", "TIME '00:00:00.000000000'", createTimeType(0), "TIME '00:00:00'")
                .addRoundTrip("time(12)", "TIME '00:00:00.000000000'", createTimeType(0), "TIME '00:00:00'")
                .addRoundTrip("time(0)", "TIME '00:12:34'", createTimeType(0), "TIME '00:12:34'")
                .addRoundTrip("time(3)", "TIME '00:12:34.567'", createTimeType(0), "TIME '00:12:35'")
                .addRoundTrip("time(6)", "TIME '00:12:34.567123'", createTimeType(0), "TIME '00:12:35'")
                .addRoundTrip("time(9)", "TIME '00:12:34.567123456'", createTimeType(0), "TIME '00:12:35'")
                .addRoundTrip("time(12)", "TIME '00:12:34.567123456'", createTimeType(0), "TIME '00:12:35'")
                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_time"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_time"));
    }

    /**
     * Additional test supplementing {@link #testPrestoTime} with time precision higher than expressible with {@link LocalTime}.
     *
     * @see #testPrestoTime
     */
    @Test
    public void testTimeCoercion()
    {
        testCreateTableAsAndInsertConsistency("TIME '00:00:00'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '12:34:56'", "TIME '12:34:56'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59'", "TIME '23:59:59'");

        // round down
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.000000000001'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.1'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.123456'", "TIME '00:00:00'");

        // round down, maximal value
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.4'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.49'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.449'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.4449'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.44449'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.444449'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.4444449'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.44444449'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.444444449'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.4444444449'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.44444444449'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.444444444449'", "TIME '00:00:00'");

        // round up, minimal value
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.5'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.50'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.500'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.5000'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.50000'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.500000'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.5000000'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.50000000'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.500000000'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.5000000000'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.50000000000'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.500000000000'", "TIME '00:00:01'");

        // round up, maximal value
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.9'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.99'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.999'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.9999'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.99999'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.999999'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.9999999'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.99999999'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.999999999'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.9999999999'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.99999999999'", "TIME '00:00:01'");
        testCreateTableAsAndInsertConsistency("TIME '00:00:00.999999999999'", "TIME '00:00:01'");

        // round up to next day, minimal value
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.5'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.50'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.500'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.5000'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.50000'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.500000'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.5000000'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.50000000'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.500000000'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.5000000000'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.50000000000'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.500000000000'", "TIME '00:00:00'");

        // round up to next day, maximal value
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.9'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.99'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.999'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.9999'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.99999'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.999999'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.9999999'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.99999999'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.999999999'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.9999999999'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.99999999999'", "TIME '00:00:00'");
        testCreateTableAsAndInsertConsistency("TIME '23:59:59.999999999999'", "TIME '00:00:00'");
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testSeconddate(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("seconddate", "TIMESTAMP '1958-01-01 13:18:03'", createTimestampType(0), "TIMESTAMP '1958-01-01 13:18:03'")
                .addRoundTrip("seconddate", "TIMESTAMP '2019-03-18 10:01:17'", createTimestampType(0), "TIMESTAMP '2019-03-18 10:01:17'")
                .addRoundTrip("seconddate", "TIMESTAMP '2018-10-28 01:33:17'", createTimestampType(0), "TIMESTAMP '2018-10-28 01:33:17'")
                .addRoundTrip("seconddate", "TIMESTAMP '2018-10-28 03:33:33'", createTimestampType(0), "TIMESTAMP '2018-10-28 03:33:33'")
                .addRoundTrip("seconddate", "TIMESTAMP '1970-01-01 00:00:00'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:00'") // epoch also is a gap in JVM zone
                .addRoundTrip("seconddate", "TIMESTAMP '1970-01-01 00:13:42'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:13:42'")
                .addRoundTrip("seconddate", "TIMESTAMP '2018-04-01 02:13:55'", createTimestampType(0), "TIMESTAMP '2018-04-01 02:13:55'")
                .addRoundTrip("seconddate", "TIMESTAMP '2018-03-25 03:17:17'", createTimestampType(0), "TIMESTAMP '2018-03-25 03:17:17'")
                .addRoundTrip("seconddate", "TIMESTAMP '1986-01-01 00:13:07'", createTimestampType(0), "TIMESTAMP '1986-01-01 00:13:07'")
                // test arbitrary time for all supported precisions
                .addRoundTrip("seconddate", "TIMESTAMP '1970-01-01 00:00:00'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:00'")
                .execute(getQueryRunner(), session, sapHanaCreateAndInsert("tpch.test_seconddate"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testSapHanaTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("timestamp", "TIMESTAMP '1958-01-01 13:18:03.1230000'", createTimestampType(7), "TIMESTAMP '1958-01-01 13:18:03.1230000'") // beforeEpoch
                .addRoundTrip("timestamp", "TIMESTAMP '2019-03-18 10:01:17.9870000'", createTimestampType(7), "TIMESTAMP '2019-03-18 10:01:17.9870000'") // afterEpoch
                .addRoundTrip("timestamp", "TIMESTAMP '2018-10-28 01:33:17.4560000'", createTimestampType(7), "TIMESTAMP '2018-10-28 01:33:17.4560000'") // timeDoubledInJvmZone
                .addRoundTrip("timestamp", "TIMESTAMP '2018-10-28 03:33:33.3330000'", createTimestampType(7), "TIMESTAMP '2018-10-28 03:33:33.3330000'") // timeDoubledInVilnius
                .addRoundTrip("timestamp", "TIMESTAMP '1970-01-01 00:00:00.0000000'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.0000000'") // epoch also is a gap in JVM zone
                .addRoundTrip("timestamp", "TIMESTAMP '1970-01-01 00:13:42.0000000'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:13:42.0000000'") // timeGapInJvmZone1
                .addRoundTrip("timestamp", "TIMESTAMP '2018-04-01 02:13:55.1230000'", createTimestampType(7), "TIMESTAMP '2018-04-01 02:13:55.1230000'") // timeGapInJvmZone2
                .addRoundTrip("timestamp", "TIMESTAMP '2018-03-25 03:17:17.0000000'", createTimestampType(7), "TIMESTAMP '2018-03-25 03:17:17.0000000'") // timeGapInVilnius
                .addRoundTrip("timestamp", "TIMESTAMP '1986-01-01 00:13:07.0000000'", createTimestampType(7), "TIMESTAMP '1986-01-01 00:13:07.0000000'") // timeGapInKathmandu
                // test arbitrary time for all supported precisions
                .addRoundTrip("timestamp", "TIMESTAMP '1970-01-01 00:00:00.0000000'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.0000000'")
                .addRoundTrip("timestamp", "TIMESTAMP '1970-01-01 00:00:00.1000000'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1000000'")
                .addRoundTrip("timestamp", "TIMESTAMP '1970-01-01 00:00:00.1200000'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1200000'")
                .addRoundTrip("timestamp", "TIMESTAMP '1970-01-01 00:00:00.1230000'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1230000'")
                .addRoundTrip("timestamp", "TIMESTAMP '1970-01-01 00:00:00.1234000'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1234000'")
                .addRoundTrip("timestamp", "TIMESTAMP '1970-01-01 00:00:00.1234500'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1234500'")
                .addRoundTrip("timestamp", "TIMESTAMP '1970-01-01 00:00:00.1234560'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1234560'")
                .addRoundTrip("timestamp", "TIMESTAMP '1970-01-01 00:00:00.1234567'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1234567'")
                // before epoch with nanos
                .addRoundTrip("timestamp", "TIMESTAMP '1969-12-31 23:59:59.1234560'", createTimestampType(7), "TIMESTAMP '1969-12-31 23:59:59.1234560'")
                .addRoundTrip("timestamp", "TIMESTAMP '1969-12-31 23:59:59.1234567'", createTimestampType(7), "TIMESTAMP '1969-12-31 23:59:59.1234567'")
                .execute(getQueryRunner(), session, sapHanaCreateAndInsert("tpch.test_timestamp"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testPrestoTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1958-01-01 13:18:03.123'", createTimestampType(7), "TIMESTAMP '1958-01-01 13:18:03.1230000'") // before epoch
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", createTimestampType(7), "TIMESTAMP '2019-03-18 10:01:17.9870000'") // after epoch
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", createTimestampType(7), "TIMESTAMP '2018-10-28 01:33:17.4560000'") // time doubled in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", createTimestampType(7), "TIMESTAMP '2018-10-28 03:33:33.3330000'") // time doubled in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:00.000'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.0000000'") // epoch is also a gap in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:13:42.0000000'") // time gap in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", createTimestampType(7), "TIMESTAMP '2018-04-01 02:13:55.1230000'") // time gap in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-03-25 03:17:17.000'", createTimestampType(7), "TIMESTAMP '2018-03-25 03:17:17.0000000'") // time gap in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1986-01-01 00:13:07.000'", createTimestampType(7), "TIMESTAMP '1986-01-01 00:13:07.0000000'") // time gap in Kathmandu

                .addRoundTrip("timestamp(7)", "TIMESTAMP '1958-01-01 13:18:03.1230000'", createTimestampType(7), "TIMESTAMP '1958-01-01 13:18:03.1230000'") // before epoch
                .addRoundTrip("timestamp(7)", "TIMESTAMP '2019-03-18 10:01:17.9870000'", createTimestampType(7), "TIMESTAMP '2019-03-18 10:01:17.9870000'") // after epoch
                .addRoundTrip("timestamp(7)", "TIMESTAMP '2018-10-28 01:33:17.4560000'", createTimestampType(7), "TIMESTAMP '2018-10-28 01:33:17.4560000'") // time doubled in JVM zone
                .addRoundTrip("timestamp(7)", "TIMESTAMP '2018-10-28 03:33:33.3330000'", createTimestampType(7), "TIMESTAMP '2018-10-28 03:33:33.3330000'") // time doubled in Vilnius
                .addRoundTrip("timestamp(7)", "TIMESTAMP '1970-01-01 00:00:00.0000000'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.0000000'") // epoch is also a gap in JVM zone
                .addRoundTrip("timestamp(7)", "TIMESTAMP '1970-01-01 00:13:42.0000000'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:13:42.0000000'") // time gap in JVM zone
                .addRoundTrip("timestamp(7)", "TIMESTAMP '2018-04-01 02:13:55.1230000'", createTimestampType(7), "TIMESTAMP '2018-04-01 02:13:55.1230000'") // time gap in JVM zone
                .addRoundTrip("timestamp(7)", "TIMESTAMP '2018-03-25 03:17:17.0000000'", createTimestampType(7), "TIMESTAMP '2018-03-25 03:17:17.0000000'") // time gap in Vilnius
                .addRoundTrip("timestamp(7)", "TIMESTAMP '1986-01-01 00:13:07.0000000'", createTimestampType(7), "TIMESTAMP '1986-01-01 00:13:07.0000000'") // time gap in Kathmandu

                // test some arbitrary time for all supported precisions
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00'", "TIMESTAMP '1970-01-01 00:00:00'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1'", "TIMESTAMP '1970-01-01 00:00:00.1000000'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.12'", "TIMESTAMP '1970-01-01 00:00:00.1200000'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123'", "TIMESTAMP '1970-01-01 00:00:00.1230000'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1234'", "TIMESTAMP '1970-01-01 00:00:00.1234000'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.12345'", "TIMESTAMP '1970-01-01 00:00:00.1234500'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456'", "TIMESTAMP '1970-01-01 00:00:00.1234560'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1234567'", "TIMESTAMP '1970-01-01 00:00:00.1234567'")
                // precision loss causing rounding
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.12345678'", "TIMESTAMP '1970-01-01 00:00:00.1234568'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456789'", "TIMESTAMP '1970-01-01 00:00:00.1234568'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.1123456789'", "TIMESTAMP '1970-01-01 00:00:00.1123457'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.11223456789'", "TIMESTAMP '1970-01-01 00:00:00.1122346'")
                // max supported precision in SAP HANA
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.112233445566'", "TIMESTAMP '1970-01-01 00:00:00.1122334'")

                // before epoch with nanos
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1969-12-31 23:59:59.123456'", createTimestampType(7), "TIMESTAMP '1969-12-31 23:59:59.1234560'")
                .addRoundTrip("timestamp(7)", "TIMESTAMP '1969-12-31 23:59:59.1234567'", createTimestampType(7), "TIMESTAMP '1969-12-31 23:59:59.1234567'")

                // round down before and after epoch
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456712'", "TIMESTAMP '1970-01-01 00:00:00.1234567'")
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.123456712'", "TIMESTAMP '1969-12-31 23:59:59.1234567'")

                // round up before and after epoch
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456789'", "TIMESTAMP '1970-01-01 00:00:00.1234568'")
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.123456789'", "TIMESTAMP '1969-12-31 23:59:59.1234568'")

                // picos round up, end result rounds down
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456749'", "TIMESTAMP '1970-01-01 00:00:00.1234567'")
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.123456749999'", "TIMESTAMP '1970-01-01 00:00:00.1234567'")

                // round up to next second
                .addRoundTrip("TIMESTAMP '1970-01-01 00:00:00.99999995'", "TIMESTAMP '1970-01-01 00:00:01.0000000'")

                // round up to next day
                .addRoundTrip("TIMESTAMP '1970-01-01 23:59:59.99999995'", "TIMESTAMP '1970-01-02 00:00:00.0000000'")

                // negative epoch
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.99999995'", "TIMESTAMP '1970-01-01 00:00:00.0000000'")
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.999999949999'", "TIMESTAMP '1969-12-31 23:59:59.9999999'")
                .addRoundTrip("TIMESTAMP '1969-12-31 23:59:59.99999994'", "TIMESTAMP '1969-12-31 23:59:59.9999999'")

                .execute(getQueryRunner(), session, trinoCreateAsSelect(session, "test_timestamp"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(session, "test_timestamp"));
    }

    @DataProvider
    public Object[][] sessionZonesDataProvider()
    {
        return new Object[][] {
                {UTC},
                {jvmZone},
                // using two non-JVM zones
                {vilnius},
                {kathmandu},
                {ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
        };
    }

    @Test
    public void testTimestampWithTimeZone()
    {
        for (int precision = 0; precision <= 12; precision++) {
            String tableName = "test_create_table_with_timestamp_with_time_zone";
            assertQueryFails(
                    format("CREATE TABLE " + tableName + " (a timestamp(%s) with time zone)", precision),
                    format("Unsupported column type: timestamp\\(%s\\) with time zone", precision));
        }
    }

    private void testCreateTableAsAndInsertConsistency(String inputLiteral, String expectedResult)
    {
        String tableName = "test_ctas_and_insert_" + randomNameSuffix();

        // CTAS
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT " + inputLiteral + " a", 1);
        assertThat(query("SELECT a FROM " + tableName))
                .matches("VALUES " + expectedResult);

        // INSERT as a control query, where the coercion is done by the engine
        server.execute("DELETE FROM tpch." + tableName);
        assertUpdate("INSERT INTO " + tableName + " (a) VALUES (" + inputLiteral + ")", 1);
        assertThat(query("SELECT a FROM " + tableName))
                .matches("VALUES " + expectedResult);

        // opportunistically test predicate pushdown if applies to given type
        assertThat(query("SELECT count(*) FROM " + tableName + " WHERE a = " + expectedResult))
                .matches("VALUES BIGINT '1'")
                .isFullyPushedDown();

        assertUpdate("DROP TABLE " + tableName);
    }

    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    // TODO use in time, timestamp tests
    private DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup sapHanaCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(server::execute, tableNamePrefix);
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
}
