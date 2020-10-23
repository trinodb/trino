/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.datatype.CreateAndInsertDataSetup;
import io.prestosql.testing.datatype.CreateAsSelectDataSetup;
import io.prestosql.testing.datatype.DataSetup;
import io.prestosql.testing.datatype.DataType;
import io.prestosql.testing.datatype.DataTypeTest;
import io.prestosql.testing.sql.PrestoSqlExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.prestoTimeForSapHanaDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.prestoTimestampForSapHanaDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.prestoVarcharForSapHanaDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaAlphanumDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaBintextDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaBlobDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaClobDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaDecimalDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaLongFloatDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaNcharDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaNclobDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaNvarcharDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaSeconddateDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaShortFloatDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaShorttextDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaSmalldecimalDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaTextDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaTimeDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaTimestampDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaDataTypes.sapHanaVarbinaryDataType;
import static com.starburstdata.presto.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.Chars.padSpaces;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.datatype.DataType.bigintDataType;
import static io.prestosql.testing.datatype.DataType.booleanDataType;
import static io.prestosql.testing.datatype.DataType.charDataType;
import static io.prestosql.testing.datatype.DataType.dataType;
import static io.prestosql.testing.datatype.DataType.dateDataType;
import static io.prestosql.testing.datatype.DataType.decimalDataType;
import static io.prestosql.testing.datatype.DataType.doubleDataType;
import static io.prestosql.testing.datatype.DataType.integerDataType;
import static io.prestosql.testing.datatype.DataType.realDataType;
import static io.prestosql.testing.datatype.DataType.smallintDataType;
import static io.prestosql.testing.datatype.DataType.stringDataType;
import static io.prestosql.testing.datatype.DataType.tinyintDataType;
import static io.prestosql.testing.datatype.DataType.varbinaryDataType;
import static io.prestosql.testing.datatype.DataType.varcharDataType;
import static io.prestosql.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSapHanaTypeMapping
        extends AbstractTestQueryFramework
{
    private static final LocalDate EPOCH_DAY = LocalDate.ofEpochDay(0);

    // A single Unicode character that occupies 4 bytes in UTF-8 encoding and uses surrogate pairs in UTF-16 representation
    private static final String SAMPLE_LENGTHY_CHARACTER = "\uD83D\uDE02";
    private static final String SAMPLE_UNICODE_TEXT = "\u653b\u6bbb\u6a5f\u52d5\u968a";
    private static final String SAMPLE_OTHER_UNICODE_TEXT = "\u041d\u0443, \u043f\u043e\u0433\u043e\u0434\u0438!";

    private TestingSapHanaServer server;

    private final LocalDateTime beforeEpoch = LocalDateTime.of(1958, 1, 1, 13, 18, 3, 123_000_000);
    private final LocalDateTime epoch = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
    private final LocalDateTime afterEpoch = LocalDateTime.of(2019, 3, 18, 10, 1, 17, 987_000_000);

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
        server = new TestingSapHanaServer();
        return createSapHanaQueryRunner(
                server,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        server.close();
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
        DataTypeTest.create(true)
                .addRoundTrip(booleanDataType(), true)
                .addRoundTrip(booleanDataType(), false)
                .addRoundTrip(bigintDataType(), 123_456_789_012L)
                .addRoundTrip(integerDataType(), 1_234_567_890)
                .addRoundTrip(smallintDataType(), (short) 32_456)
                .addRoundTrip(tinyintDataType(), (byte) 5)
                .addRoundTrip(doubleDataType(), 123.45d)
                .addRoundTrip(realDataType(), 123.45f)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testReal()
    {
        DataType<Float> dataType = realDataType();
        DataTypeTest dataTypeTest = DataTypeTest.create(true)
                .addRoundTrip(dataType, 3.14f)
                .addRoundTrip(dataType, 3.1415927f)
                .addRoundTrip(dataType, null);

        dataTypeTest.execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_real"));
        dataTypeTest.execute(getQueryRunner(), prestoCreateAsSelect("test_real"));

        testSapHanaUnsupportedValue(dataType, Float.NaN, "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: NaN");
        testSapHanaUnsupportedValue(dataType, Float.NEGATIVE_INFINITY, "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: -Infinity");
        testSapHanaUnsupportedValue(dataType, Float.POSITIVE_INFINITY, "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: Infinity");
    }

    @Test
    public void testDouble()
    {
        DataType<Double> dataType = doubleDataType();
        DataTypeTest dataTypeTest = DataTypeTest.create(true)
                .addRoundTrip(dataType, 1.0e100d)
                .addRoundTrip(dataType, null);

        dataTypeTest.execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_double"));
        dataTypeTest.execute(getQueryRunner(), prestoCreateAsSelect("test_double"));

        testSapHanaUnsupportedValue(dataType, Double.NaN, "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: NaN");
        testSapHanaUnsupportedValue(dataType, Double.NEGATIVE_INFINITY, "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: -Infinity");
        testSapHanaUnsupportedValue(dataType, Double.POSITIVE_INFINITY, "com.sap.db.jdbc.exceptions.SQLDataExceptionSapDB: Invalid number: Infinity");
    }

    private <T> void testSapHanaUnsupportedValue(DataType<T> dataType, T value, String expectedMessage)
    {
        assertThatThrownBy(() ->
                prestoCreateAsSelect("test_unsupported")
                        .setupTestTable(List.of(new DataTypeTest.Input<>(dataType, value, false)))
                        .close())
                .hasStackTraceContaining(expectedMessage);
    }

    @Test
    public void testFloat()
    {
        DataTypeTest.create(true)
                .addRoundTrip(sapHanaShortFloatDataType(10), 3.14f)
                .addRoundTrip(sapHanaShortFloatDataType(24), 3.1415927f)
                .addRoundTrip(sapHanaLongFloatDataType(25), 3.1415927)
                .addRoundTrip(sapHanaLongFloatDataType(31), 1.2345678912)
                .addRoundTrip(sapHanaLongFloatDataType(53), 1.234567891234567)
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_float"));
    }

    @Test
    public void testDecimal()
    {
        DataTypeTest dataTypeTest = DataTypeTest.create()
                .addRoundTrip(decimalDataType(3, 0), null)
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("193"))
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("19"))
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("-193"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("10.0"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("10.1"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("-10.1"))
                .addRoundTrip(decimalDataType(4, 2), new BigDecimal("2"))
                .addRoundTrip(decimalDataType(4, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalDataType(24, 2), null)
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("2"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("123456789.3"))
                .addRoundTrip(decimalDataType(24, 4), new BigDecimal("12345678901234567890.31"))
                .addRoundTrip(decimalDataType(30, 5), new BigDecimal("3141592653589793238462643.38327"))
                .addRoundTrip(decimalDataType(30, 5), new BigDecimal("-3141592653589793238462643.38327"))
                .addRoundTrip(decimalDataType(38, 0), new BigDecimal("27182818284590452353602874713526624977"))
                .addRoundTrip(decimalDataType(38, 0), new BigDecimal("-27182818284590452353602874713526624977"));

        dataTypeTest.execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_decimal"));
        dataTypeTest.execute(getQueryRunner(), prestoCreateAsSelect("test_decimal"));
    }

    @Test
    public void testSmalldecimal()
    {
        DataType<BigDecimal> dataType = sapHanaSmalldecimalDataType();
        DataTypeTest.create()
                .addRoundTrip(dataType, null)
                .addRoundTrip(dataType, new BigDecimal("193"))
                .addRoundTrip(dataType, new BigDecimal("19"))
                .addRoundTrip(dataType, new BigDecimal("-193"))
                .addRoundTrip(dataType, new BigDecimal("10.0"))
                .addRoundTrip(dataType, new BigDecimal("10.1"))
                .addRoundTrip(dataType, new BigDecimal("-10.1"))
                .addRoundTrip(dataType, new BigDecimal("2"))
                .addRoundTrip(dataType, new BigDecimal("2.3"))
                .addRoundTrip(dataType, new BigDecimal("2"))
                .addRoundTrip(dataType, new BigDecimal("2.3"))
                .addRoundTrip(dataType, new BigDecimal("123456789.3"))
                // up to 16 decimal digits
                .addRoundTrip(dataType, new BigDecimal("12345678901234.31"))
                .addRoundTrip(dataType, new BigDecimal("3141592653589793000000000.00000"))
                .addRoundTrip(dataType, new BigDecimal("-3141592653589793000000000.00000"))
                // large number
                .addRoundTrip(dataType, new BigDecimal("1234" + "0".repeat(100)))
                .addRoundTrip(dataType, new BigDecimal("-234" + "0".repeat(100)))
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_smalldecimal"));
    }

    @Test
    public void testDecimalUnbounded()
    {
        DataType<BigDecimal> dataType = sapHanaDecimalDataType();
        DataTypeTest.create()
                .addRoundTrip(dataType, null)
                .addRoundTrip(dataType, new BigDecimal("193"))
                .addRoundTrip(dataType, new BigDecimal("19"))
                .addRoundTrip(dataType, new BigDecimal("-193"))
                .addRoundTrip(dataType, new BigDecimal("10.0"))
                .addRoundTrip(dataType, new BigDecimal("10.1"))
                .addRoundTrip(dataType, new BigDecimal("-10.1"))
                .addRoundTrip(dataType, new BigDecimal("2"))
                .addRoundTrip(dataType, new BigDecimal("2.3"))
                .addRoundTrip(dataType, new BigDecimal("2"))
                .addRoundTrip(dataType, new BigDecimal("2.3"))
                .addRoundTrip(dataType, new BigDecimal("123456789.3"))
                .addRoundTrip(dataType, new BigDecimal("12345678901234567890.31"))
                // up to 34 decimal digits
                .addRoundTrip(dataType, new BigDecimal("3141592653589793238462643.383271234"))
                .addRoundTrip(dataType, new BigDecimal("-3141592653589793238462643.383271234"))
                .addRoundTrip(dataType, new BigDecimal("27182818284590452353602874713526624977"))
                .addRoundTrip(dataType, new BigDecimal("-27182818284590452353602874713526624977"))
                // large number
                .addRoundTrip(dataType, new BigDecimal("1234" + "0".repeat(100)))
                .addRoundTrip(dataType, new BigDecimal("-234" + "0".repeat(100)))
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_decimal_unbounded"));
    }

    @Test
    public void testChar()
    {
        characterDataTypeTest(DataType::charDataType, string -> charDataType(sapHanaTextLength(string)), charDataType(2000))
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_char"));

        int sapHanaMaxCharLength = 2000;
        DataTypeTest.create()
                .addRoundTrip(charDataType(10), "text_a")
                .addRoundTrip(charDataType(255), "text_b")
                .addRoundTrip(charDataType(sapHanaMaxCharLength), "a".repeat(sapHanaMaxCharLength)) // max length
                .addRoundTrip(charDataType(100), SAMPLE_UNICODE_TEXT)
                .addRoundTrip(charDataType(sapHanaTextLength(SAMPLE_UNICODE_TEXT)), SAMPLE_UNICODE_TEXT) // Connector does not extend char type to accommodate for different counting semantics
                .addRoundTrip(charDataType(sapHanaMaxCharLength), SAMPLE_UNICODE_TEXT)
                .addRoundTrip(charDataType(77), SAMPLE_OTHER_UNICODE_TEXT)
                .addRoundTrip(charDataType(sapHanaTextLength(SAMPLE_LENGTHY_CHARACTER)), SAMPLE_LENGTHY_CHARACTER) // Connector does not extend char type to accommodate for different counting semantics
                .addRoundTrip(charDataType(sapHanaTextLength(SAMPLE_LENGTHY_CHARACTER.repeat(100))), SAMPLE_LENGTHY_CHARACTER.repeat(100)) // Connector does not extend char type to accommodate for different counting semantics
                .execute(getQueryRunner(), prestoCreateAsSelect("test_char"));

        // too long for a char in SAP HANA
        int length = 2001;
        //noinspection ConstantConditions
        verify(length <= CharType.MAX_LENGTH);
        DataType<String> longChar = dataType(
                format("char(%s)", length),
                createUnboundedVarcharType(),
                DataType::formatStringLiteral,
                input -> padSpaces(utf8Slice(input), length).toStringUtf8());
        DataTypeTest.create()
                .addRoundTrip(longChar, "text_f")
                .addRoundTrip(longChar, "a".repeat(length))
                .addRoundTrip(longChar, SAMPLE_LENGTHY_CHARACTER.repeat(length))
                .execute(getQueryRunner(), prestoCreateAsSelect("test_char"));
    }

    @Test
    public void testNchar()
    {
        characterDataTypeTest(SapHanaDataTypes::sapHanaNcharDataType, string -> sapHanaNcharDataType(sapHanaTextLength(string)), sapHanaNcharDataType(2000))
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_nchar"));
    }

    @Test
    public void testVarchar()
    {
        // varchar(p)
        characterDataTypeTest(DataType::varcharDataType, string -> varcharDataType(sapHanaTextLength(string)), varcharDataType(5000))
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));

        characterDataTypeTest(SapHanaDataTypes::prestoVarcharForSapHanaDataType, string -> prestoVarcharForSapHanaDataType(sapHanaTextLength(string)), prestoVarcharForSapHanaDataType(5000))
                .execute(getQueryRunner(), prestoCreateAsSelect("test_varchar"));

        // varchar
        DataTypeTest.create()
                .addRoundTrip(stringDataType("varchar", createVarcharType(1)), "a")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));

        DataType<String> varcharDataType = varcharDataType();
        longVarcharDataTypeTest(length -> varcharDataType, string -> varcharDataType, varcharDataType)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_varchar"));
    }

    @Test
    public void testNvarchar()
    {
        characterDataTypeTest(SapHanaDataTypes::sapHanaNvarcharDataType, string -> sapHanaNvarcharDataType(sapHanaTextLength(string)), sapHanaNvarcharDataType(5000))
                .addRoundTrip(stringDataType("nvarchar", createVarcharType(1)), "a")
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testAlphanum()
    {
        DataTypeTest.create()
                .addRoundTrip(sapHanaAlphanumDataType(10), null)
                .addRoundTrip(sapHanaAlphanumDataType(10), "")
                .addRoundTrip(sapHanaAlphanumDataType(10), "abcdef")
                .addRoundTrip(sapHanaAlphanumDataType(10), "123456") // "purely numeric value" is a distinguished case in documentation
                .addRoundTrip(sapHanaAlphanumDataType(127), "a".repeat(127)) // max length
                .addRoundTrip(sapHanaAlphanumDataType(), "") // default length
                .addRoundTrip(sapHanaAlphanumDataType(), "a") // default length
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testShorttext()
    {
        characterDataTypeTest(SapHanaDataTypes::sapHanaShorttextDataType, string -> sapHanaShorttextDataType(sapHanaTextLength(string)), sapHanaShorttextDataType(5000))
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testText()
    {
        DataType<String> dataType = sapHanaTextDataType();
        longVarcharDataTypeTest(length -> dataType, string -> dataType, dataType)
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testBintext()
    {
        DataType<String> dataType = sapHanaBintextDataType();
        DataTypeTest.create()
                .addRoundTrip(dataType, null)
                .addRoundTrip(dataType, "")
                .addRoundTrip(dataType, "abc")
                .addRoundTrip(dataType, "a".repeat(500))
                .addRoundTrip(dataType, SAMPLE_UNICODE_TEXT)
                .addRoundTrip(dataType, SAMPLE_OTHER_UNICODE_TEXT)
                // TODO SAMPLE_LENGTHY_CHARACTER comes back garbled
                // TODO SAMPLE_LENGTHY_CHARACTER.repeat(100) comes back garbled
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testClob()
    {
        DataType<String> dataType = sapHanaClobDataType();
        longVarcharDataTypeTest(length -> dataType, string -> dataType, dataType)
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));
    }

    @Test
    public void testNclob()
    {
        DataType<String> dataType = sapHanaNclobDataType();
        longVarcharDataTypeTest(length -> dataType, string -> dataType, dataType)
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varchar"));
    }

    private DataTypeTest longVarcharDataTypeTest(Function<Integer, DataType<String>> dataTypeForLength, Function<String, DataType<String>> dataTypeForValue, DataType<String> maxLengthType)
    {
        return characterDataTypeTest(dataTypeForLength, dataTypeForValue, maxLengthType)
                .addRoundTrip(dataTypeForLength.apply(10485760), "text_f");
    }

    private DataTypeTest characterDataTypeTest(Function<Integer, DataType<String>> dataTypeForLength, Function<String, DataType<String>> dataTypeForValue, DataType<String> maxLengthType)
    {
        return DataTypeTest.create()
                .addRoundTrip(dataTypeForLength.apply(10), "text_a")
                .addRoundTrip(dataTypeForLength.apply(255), "text_b")
                .addRoundTrip(maxLengthType, "text_d")
                .addRoundTrip(dataTypeForValue.apply(SAMPLE_UNICODE_TEXT), SAMPLE_UNICODE_TEXT)
                .addRoundTrip(dataTypeForLength.apply(100), SAMPLE_UNICODE_TEXT)
                .addRoundTrip(maxLengthType, SAMPLE_UNICODE_TEXT)
                .addRoundTrip(dataTypeForLength.apply(77), SAMPLE_OTHER_UNICODE_TEXT)
                .addRoundTrip(dataTypeForValue.apply(SAMPLE_LENGTHY_CHARACTER), SAMPLE_LENGTHY_CHARACTER)
                .addRoundTrip(dataTypeForValue.apply(SAMPLE_LENGTHY_CHARACTER.repeat(100)), SAMPLE_LENGTHY_CHARACTER.repeat(100));
    }

    @Test
    public void testVarbinary()
    {
        varbinaryTestCases(sapHanaBlobDataType())
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases(sapHanaVarbinaryDataType(29)) // shortest to contain the test cases
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases(sapHanaVarbinaryDataType(5000)) // longest allowed
                .execute(getQueryRunner(), sapHanaCreateAndInsert("tpch.test_varbinary"));

        varbinaryTestCases(varbinaryDataType())
                .execute(getQueryRunner(), prestoCreateAsSelect("test_varbinary"));
    }

    private DataTypeTest varbinaryTestCases(DataType<byte[]> varbinaryDataType)
    {
        return DataTypeTest.create()
                .addRoundTrip(varbinaryDataType, "hello".getBytes(UTF_8))
                .addRoundTrip(varbinaryDataType, "Piękna łąka w 東京都".getBytes(UTF_8))
                .addRoundTrip(varbinaryDataType, "Bag full of 💰".getBytes(UTF_16LE))
                .addRoundTrip(varbinaryDataType, null)
                .addRoundTrip(varbinaryDataType, new byte[] {})
                .addRoundTrip(varbinaryDataType, new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 13, -7, 54, 122, -89, 0, 0, 0});
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

        DataTypeTest testCases = DataTypeTest.create(true)
                .addRoundTrip(dateDataType(), LocalDate.of(1952, 4, 3)) // before epoch
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 1, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 2, 3))
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 7, 1)) // summer on northern hemisphere (possible DST)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 1, 1)) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeForwardAtMidnightInJvmZone)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeForwardAtMidnightInSomeZone)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeBackwardAtMidnightInSomeZone);

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            testCases.execute(getQueryRunner(), session, sapHanaCreateAndInsert("tpch.test_date"));
            testCases.execute(getQueryRunner(), session, prestoCreateAsSelect(session, "test_date"));
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

        DataType<LocalTime> dataType = sapHanaTimeDataType();
        DataTypeTest.create()
                .addRoundTrip(dataType, LocalTime.of(1, 12, 34, 0))
                .addRoundTrip(dataType, LocalTime.of(2, 12, 34, 0))
                .addRoundTrip(dataType, LocalTime.of(2, 12, 34, 1_000_000))
                .addRoundTrip(dataType, LocalTime.of(3, 12, 34, 0))
                .addRoundTrip(dataType, LocalTime.of(4, 12, 34, 0))
                .addRoundTrip(dataType, LocalTime.of(5, 12, 34, 0))
                .addRoundTrip(dataType, LocalTime.of(6, 12, 34, 0))
                .addRoundTrip(dataType, LocalTime.of(9, 12, 34, 0))
                .addRoundTrip(dataType, LocalTime.of(10, 12, 34, 0))
                .addRoundTrip(dataType, LocalTime.of(15, 12, 34, 567_000_000))
                .addRoundTrip(dataType, LocalTime.of(23, 59, 59, 999_000_000))
                // epoch is also a gap in JVM zone
                .addRoundTrip(dataType, epoch.toLocalTime())
                .addRoundTrip(dataType, timeGapInJvmZone)
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

        DataTypeTest dataTypeTest = DataTypeTest.create()
                .addRoundTrip(prestoTimeForSapHanaDataType(0), LocalTime.of(1, 12, 34, 0))
                .addRoundTrip(prestoTimeForSapHanaDataType(1), LocalTime.of(2, 12, 34, 0))
                .addRoundTrip(prestoTimeForSapHanaDataType(2), LocalTime.of(2, 12, 34, 1_000_000))
                .addRoundTrip(prestoTimeForSapHanaDataType(3), LocalTime.of(3, 12, 34, 0))
                .addRoundTrip(prestoTimeForSapHanaDataType(4), LocalTime.of(4, 12, 34, 0))
                .addRoundTrip(prestoTimeForSapHanaDataType(5), LocalTime.of(5, 12, 34, 0))
                .addRoundTrip(prestoTimeForSapHanaDataType(6), LocalTime.of(6, 12, 34, 0))
                .addRoundTrip(prestoTimeForSapHanaDataType(7), LocalTime.of(9, 12, 34, 0))
                .addRoundTrip(prestoTimeForSapHanaDataType(8), LocalTime.of(10, 12, 34, 0))
                .addRoundTrip(prestoTimeForSapHanaDataType(9), LocalTime.of(15, 12, 34, 567_000_000))
                .addRoundTrip(prestoTimeForSapHanaDataType(10), LocalTime.of(23, 59, 59, 999_000_000))
                // highest possible value
                .addRoundTrip(prestoTimeForSapHanaDataType(9), LocalTime.of(23, 59, 59, 999_999_999))
                // epoch is also a gap in JVM zone
                .addRoundTrip(prestoTimeForSapHanaDataType(0), epoch.toLocalTime())
                .addRoundTrip(prestoTimeForSapHanaDataType(3), epoch.toLocalTime())
                .addRoundTrip(prestoTimeForSapHanaDataType(6), epoch.toLocalTime())
                .addRoundTrip(prestoTimeForSapHanaDataType(9), epoch.toLocalTime())
                .addRoundTrip(prestoTimeForSapHanaDataType(12), epoch.toLocalTime())
                .addRoundTrip(prestoTimeForSapHanaDataType(0), timeGapInJvmZone.withNano(0))
                .addRoundTrip(prestoTimeForSapHanaDataType(3), timeGapInJvmZone.withNano(567_000_000))
                .addRoundTrip(prestoTimeForSapHanaDataType(6), timeGapInJvmZone.withNano(567_123_000))
                .addRoundTrip(prestoTimeForSapHanaDataType(9), timeGapInJvmZone.withNano(567_123_456))
                .addRoundTrip(prestoTimeForSapHanaDataType(12), timeGapInJvmZone.withNano(567_123_456));

        dataTypeTest.execute(getQueryRunner(), session, prestoCreateAsSelect(session, "test_time"));
        dataTypeTest.execute(getQueryRunner(), session, prestoCreateAndInsert(session, "test_time"));
    }

    /**
     * Additional test supplementing {@link #testPrestoTime} with timestamp precision higher than expressible with {@code LocalTime}.
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

        DataType<LocalDateTime> dataType = sapHanaSeconddateDataType();

        DataTypeTest.create(true)
                .addRoundTrip(dataType, beforeEpoch.withNano(0))
                .addRoundTrip(dataType, afterEpoch.withNano(0))
                .addRoundTrip(dataType, timeDoubledInJvmZone.withNano(0))
                .addRoundTrip(dataType, timeDoubledInVilnius.withNano(0))
                .addRoundTrip(dataType, epoch.withNano(0)) // epoch also is a gap in JVM zone
                .addRoundTrip(dataType, timeGapInJvmZone1.withNano(0))
                .addRoundTrip(dataType, timeGapInJvmZone2.withNano(0))
                .addRoundTrip(dataType, timeGapInVilnius.withNano(0))
                .addRoundTrip(dataType, timeGapInKathmandu.withNano(0))
                // test arbitrary time for all supported precisions
                .addRoundTrip(dataType, LocalDateTime.of(1970, 1, 1, 0, 0, 0))
                .execute(getQueryRunner(), session, sapHanaCreateAndInsert("tpch.test_seconddate"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testSapHanaTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        DataType<LocalDateTime> dataType = sapHanaTimestampDataType();

        DataTypeTest.create(true)
                .addRoundTrip(dataType, beforeEpoch)
                .addRoundTrip(dataType, afterEpoch)
                .addRoundTrip(dataType, timeDoubledInJvmZone)
                .addRoundTrip(dataType, timeDoubledInVilnius)
                .addRoundTrip(dataType, epoch) // epoch also is a gap in JVM zone
                .addRoundTrip(dataType, timeGapInJvmZone1)
                .addRoundTrip(dataType, timeGapInJvmZone2)
                .addRoundTrip(dataType, timeGapInVilnius)
                .addRoundTrip(dataType, timeGapInKathmandu)
                // test arbitrary time for all supported precisions
                .addRoundTrip(dataType, LocalDateTime.of(1970, 1, 1, 0, 0, 0))
                .addRoundTrip(dataType, LocalDateTime.of(1970, 1, 1, 0, 0, 0, 100_000_000))
                .addRoundTrip(dataType, LocalDateTime.of(1970, 1, 1, 0, 0, 0, 120_000_000))
                .addRoundTrip(dataType, LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_000_000))
                .addRoundTrip(dataType, LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_400_000))
                .addRoundTrip(dataType, LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_450_000))
                .addRoundTrip(dataType, LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_000))
                .addRoundTrip(dataType, LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_700))
                // before epoch with nanos
                .addRoundTrip(dataType, LocalDateTime.of(1969, 12, 31, 23, 59, 59, 123_456_000))
                .addRoundTrip(dataType, LocalDateTime.of(1969, 12, 31, 23, 59, 59, 123_456_700))
                .execute(getQueryRunner(), session, sapHanaCreateAndInsert("tpch.test_timestamp"));
    }

    /**
     * @see #testTimestampCoercion
     */
    @Test(dataProvider = "sessionZonesDataProvider")
    public void testPrestoTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        DataTypeTest dataTypeTest = DataTypeTest.create(false) // TODO true
                .addRoundTrip(prestoTimestampForSapHanaDataType(3), beforeEpoch)
                .addRoundTrip(prestoTimestampForSapHanaDataType(3), afterEpoch)
                .addRoundTrip(prestoTimestampForSapHanaDataType(3), timeDoubledInJvmZone)
                .addRoundTrip(prestoTimestampForSapHanaDataType(3), timeDoubledInVilnius)
                .addRoundTrip(prestoTimestampForSapHanaDataType(3), epoch) // epoch also is a gap in JVM zone
                .addRoundTrip(prestoTimestampForSapHanaDataType(3), timeGapInJvmZone1)
                .addRoundTrip(prestoTimestampForSapHanaDataType(3), timeGapInJvmZone2)
                .addRoundTrip(prestoTimestampForSapHanaDataType(3), timeGapInVilnius)
                .addRoundTrip(prestoTimestampForSapHanaDataType(3), timeGapInKathmandu)

                .addRoundTrip(prestoTimestampForSapHanaDataType(7), beforeEpoch)
                .addRoundTrip(prestoTimestampForSapHanaDataType(7), afterEpoch)
                .addRoundTrip(prestoTimestampForSapHanaDataType(7), timeDoubledInJvmZone)
                .addRoundTrip(prestoTimestampForSapHanaDataType(7), timeDoubledInVilnius)
                .addRoundTrip(prestoTimestampForSapHanaDataType(7), epoch) // epoch also is a gap in JVM zone
                .addRoundTrip(prestoTimestampForSapHanaDataType(7), timeGapInJvmZone1)
                .addRoundTrip(prestoTimestampForSapHanaDataType(7), timeGapInJvmZone2)
                .addRoundTrip(prestoTimestampForSapHanaDataType(7), timeGapInVilnius)
                .addRoundTrip(prestoTimestampForSapHanaDataType(7), timeGapInKathmandu)

                // test arbitrary time for all supported precisions
                .addRoundTrip(prestoTimestampForSapHanaDataType(0), LocalDateTime.of(1970, 1, 1, 0, 0, 0))
                .addRoundTrip(prestoTimestampForSapHanaDataType(1), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 100_000_000))
                .addRoundTrip(prestoTimestampForSapHanaDataType(2), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 120_000_000))
                .addRoundTrip(prestoTimestampForSapHanaDataType(3), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_000_000))
                .addRoundTrip(prestoTimestampForSapHanaDataType(4), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_400_000))
                .addRoundTrip(prestoTimestampForSapHanaDataType(5), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_450_000))
                .addRoundTrip(prestoTimestampForSapHanaDataType(6), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_000))
                .addRoundTrip(prestoTimestampForSapHanaDataType(7), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_700))
                .addRoundTrip(prestoTimestampForSapHanaDataType(9), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_789))

                // before epoch with nanos
                .addRoundTrip(prestoTimestampForSapHanaDataType(6), LocalDateTime.of(1969, 12, 31, 23, 59, 59, 123_456_000))
                .addRoundTrip(prestoTimestampForSapHanaDataType(7), LocalDateTime.of(1969, 12, 31, 23, 59, 59, 123_456_700))

                // precision loss
                .addRoundTrip(prestoTimestampForSapHanaDataType(7), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_712))
                .addRoundTrip(prestoTimestampForSapHanaDataType(7), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_789))
                .addRoundTrip(prestoTimestampForSapHanaDataType(7), LocalDateTime.of(1969, 12, 31, 23, 59, 59, 123_456_712))
                .addRoundTrip(prestoTimestampForSapHanaDataType(7), LocalDateTime.of(1969, 12, 31, 23, 59, 59, 123_456_789));

        dataTypeTest.execute(getQueryRunner(), session, prestoCreateAsSelect(session, "test_timestamp"));
        dataTypeTest.execute(getQueryRunner(), session, prestoCreateAndInsert(session, "test_timestamp"));
    }

    /**
     * Additional test supplementing {@link #testPrestoTimestamp} with timestamp precision higher than expressible with {@code LocalDateTime}.
     *
     * @see #testPrestoTimestamp
     */
    @Test
    public void testTimestampCoercion()
    {
        // precision 0 ends up as precision 0
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1970-01-01 00:00:00'", "TIMESTAMP '1970-01-01 00:00:00'");

        testCreateTableAsAndInsertConsistency("TIMESTAMP '1970-01-01 00:00:00.1'", "TIMESTAMP '1970-01-01 00:00:00.1000000'");
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1970-01-01 00:00:00.9'", "TIMESTAMP '1970-01-01 00:00:00.9000000'");
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1970-01-01 00:00:00.123'", "TIMESTAMP '1970-01-01 00:00:00.1230000'");
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1970-01-01 00:00:00.999'", "TIMESTAMP '1970-01-01 00:00:00.9990000'");
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1970-01-01 00:00:00.123456'", "TIMESTAMP '1970-01-01 00:00:00.1234560'");

        testCreateTableAsAndInsertConsistency("TIMESTAMP '2020-09-27 12:34:56.1'", "TIMESTAMP '2020-09-27 12:34:56.1000000'");
        testCreateTableAsAndInsertConsistency("TIMESTAMP '2020-09-27 12:34:56.9'", "TIMESTAMP '2020-09-27 12:34:56.9000000'");
        testCreateTableAsAndInsertConsistency("TIMESTAMP '2020-09-27 12:34:56.123'", "TIMESTAMP '2020-09-27 12:34:56.1230000'");
        testCreateTableAsAndInsertConsistency("TIMESTAMP '2020-09-27 12:34:56.999'", "TIMESTAMP '2020-09-27 12:34:56.9990000'");
        testCreateTableAsAndInsertConsistency("TIMESTAMP '2020-09-27 12:34:56.123456'", "TIMESTAMP '2020-09-27 12:34:56.1234560'");

        // max supported precision
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1970-01-01 00:00:00.1234567'", "TIMESTAMP '1970-01-01 00:00:00.1234567'");

        // round down
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1970-01-01 00:00:00.12345671'", "TIMESTAMP '1970-01-01 00:00:00.1234567'");

        // picos round up, end result rounds down
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1970-01-01 00:00:00.123456749'", "TIMESTAMP '1970-01-01 00:00:00.1234567'");
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1970-01-01 00:00:00.123456749999'", "TIMESTAMP '1970-01-01 00:00:00.1234567'");

        // round up
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1970-01-01 00:00:00.12345675'", "TIMESTAMP '1970-01-01 00:00:00.1234568'");

        // max precision
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1970-01-01 00:00:00.111222333444'", "TIMESTAMP '1970-01-01 00:00:00.1112223'");

        // round up to next second
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1970-01-01 00:00:00.99999995'", "TIMESTAMP '1970-01-01 00:00:01.0000000'");

        // round up to next day
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1970-01-01 23:59:59.99999995'", "TIMESTAMP '1970-01-02 00:00:00.0000000'");

        // negative epoch
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1969-12-31 23:59:59.99999995'", "TIMESTAMP '1970-01-01 00:00:00.0000000'");
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1969-12-31 23:59:59.999999949999'", "TIMESTAMP '1969-12-31 23:59:59.9999999'");
        testCreateTableAsAndInsertConsistency("TIMESTAMP '1969-12-31 23:59:59.99999994'", "TIMESTAMP '1969-12-31 23:59:59.9999999'");
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
        String tableName = "test_ctas_and_insert_" + randomTableSuffix();

        // CTAS
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT " + inputLiteral + " a", 1);
        assertThat(query("SELECT a FROM " + tableName))
                .matches("VALUES " + expectedResult);

        // INSERT as a control query, where the coercion is done by the engine
        server.execute("DELETE FROM tpch." + tableName);
        assertUpdate("INSERT INTO " + tableName + " (a) VALUES (" + inputLiteral + ")", 1);
        assertThat(query("SELECT a FROM " + tableName))
                .matches("VALUES " + expectedResult);

        assertUpdate("DROP TABLE " + tableName);
    }

    private DataSetup prestoCreateAsSelect(String tableNamePrefix)
    {
        return prestoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup prestoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    // TODO use in time, timestamp tests
    private DataSetup prestoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new PrestoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup sapHanaCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(server::execute, tableNamePrefix);
    }

    private static int sapHanaTextLength(String string)
    {
        if (string.codePoints().noneMatch(codePoint -> codePoint > 127)) {
            // ASCII
            return string.length();
        }

        // TODO find out exact formula
        return string.length() * 3 + 10;
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
