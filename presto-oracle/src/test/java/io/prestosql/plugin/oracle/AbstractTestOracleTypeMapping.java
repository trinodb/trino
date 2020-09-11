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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.jdbc.UnsupportedTypeHandling;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.datatype.CreateAndInsertDataSetup;
import io.prestosql.testing.datatype.CreateAsSelectDataSetup;
import io.prestosql.testing.datatype.DataSetup;
import io.prestosql.testing.datatype.DataType;
import io.prestosql.testing.datatype.DataTypeTest;
import io.prestosql.testing.sql.PrestoSqlExecutor;
import io.prestosql.testing.sql.SqlExecutor;
import io.prestosql.testing.sql.TestTable;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;

import static com.google.common.base.Verify.verify;
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.prestosql.plugin.oracle.OracleDataTypes.CharacterSemantics.BYTE;
import static io.prestosql.plugin.oracle.OracleDataTypes.CharacterSemantics.CHAR;
import static io.prestosql.plugin.oracle.OracleDataTypes.MAX_CHAR_ON_READ;
import static io.prestosql.plugin.oracle.OracleDataTypes.MAX_CHAR_ON_WRITE;
import static io.prestosql.plugin.oracle.OracleDataTypes.MAX_NCHAR;
import static io.prestosql.plugin.oracle.OracleDataTypes.MAX_NVARCHAR2;
import static io.prestosql.plugin.oracle.OracleDataTypes.MAX_VARCHAR2_ON_READ;
import static io.prestosql.plugin.oracle.OracleDataTypes.MAX_VARCHAR2_ON_WRITE;
import static io.prestosql.plugin.oracle.OracleDataTypes.binaryDoubleDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.binaryFloatDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.blobDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.booleanDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.charDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.clobDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.dateDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.doubleDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.integerDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.ncharDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.nclobDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.numberDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.nvarchar2DataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.oracleFloatDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.oracleTimestamp3TimeZoneDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.prestoTimestampWithTimeZoneDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.rawDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.realDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.tooLargeCharDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.tooLargeVarcharDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.unspecifiedNumberDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.varchar2DataType;
import static io.prestosql.plugin.oracle.OracleSessionProperties.NUMBER_DEFAULT_SCALE;
import static io.prestosql.plugin.oracle.OracleSessionProperties.NUMBER_ROUNDING_MODE;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.testing.datatype.DataType.timestampDataType;
import static io.prestosql.testing.datatype.DataType.varbinaryDataType;
import static io.prestosql.testing.datatype.DataType.varcharDataType;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_EVEN;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;

public abstract class AbstractTestOracleTypeMapping
        extends AbstractTestQueryFramework
{
    private static final String NO_SUPPORTED_COLUMNS = "Table '.*' has no supported columns \\(all \\d+ columns are not supported\\)";

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

    private final ZoneOffset fixedOffsetEast = ZoneOffset.ofHoursMinutes(2, 17);
    private final ZoneOffset fixedOffsetWest = ZoneOffset.ofHoursMinutes(-7, -31);

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

    private DataSetup prestoCreateAsSelect(String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner()), tableNamePrefix);
    }

    private DataSetup prestoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    /* Floating point types tests */

    @Test
    public void testFloatingPointMappings()
    {
        testTypeMapping("floats",
                floatTests(realDataType()),
                doubleTests(doubleDataType()));
    }

    @Test
    public void testOracleFloatingPointMappings()
    {
        testTypeReadMapping("oracle_float",
                DataTypeTest.create()
                        .addRoundTrip(oracleFloatDataType(), 1e100d)
                        .addRoundTrip(oracleFloatDataType(), 1.0)
                        .addRoundTrip(oracleFloatDataType(), 123456.123456)
                        .addRoundTrip(oracleFloatDataType(), null)
                        .addRoundTrip(oracleFloatDataType(126), 1e100d)
                        .addRoundTrip(oracleFloatDataType(126), 1.0)
                        // to test the bounds of this type we would need to go via BigDecimal or String
                        .addRoundTrip(oracleFloatDataType(126), 1234567890123456789.0123456789)
                        .addRoundTrip(oracleFloatDataType(126), null)
                        .addRoundTrip(oracleFloatDataType(1), 100000.0)
                        .addRoundTrip(oracleFloatDataType(7), 123000.0));
    }

    @Test
    public void testFloatingPointReadMappings()
    {
        testTypeReadMapping("read_floats",
                floatTests(binaryFloatDataType()),
                doubleTests(binaryDoubleDataType()));
    }

    private static DataTypeTest floatTests(DataType<Float> floatType)
    {
        return DataTypeTest.create()
                .addRoundTrip(floatType, 123.45f)
                .addRoundTrip(floatType, Float.NaN)
                .addRoundTrip(floatType, Float.NEGATIVE_INFINITY)
                .addRoundTrip(floatType, Float.POSITIVE_INFINITY)
                .addRoundTrip(floatType, null);
    }

    private static DataTypeTest doubleTests(DataType<Double> doubleType)
    {
        return DataTypeTest.create()
                .addRoundTrip(doubleType, 1.0e100d)
                .addRoundTrip(doubleType, Double.NaN)
                .addRoundTrip(doubleType, Double.POSITIVE_INFINITY)
                .addRoundTrip(doubleType, Double.NEGATIVE_INFINITY)
                .addRoundTrip(doubleType, null);
    }

    /* varchar tests */

    @Test
    public void testVarcharMapping()
    {
        testTypeMapping("varchar",
                basicCharacterTests(DataType::varcharDataType, MAX_VARCHAR2_ON_WRITE));
    }

    @Test
    public void testVarcharReadMapping()
    {
        testTypeReadMapping("read_varchar",
                basicCharacterTests(varchar2DataType(CHAR), MAX_VARCHAR2_ON_READ),
                basicCharacterTests(varchar2DataType(BYTE), MAX_VARCHAR2_ON_READ),
                basicCharacterTests(nvarchar2DataType(), MAX_NVARCHAR2));
    }

    /*
    The unicode tests assume the following Oracle database parameters:
     - NLS_NCHAR_CHARACTERSET = AL16UTF16
     - NLS_CHARACTERSET = AL32UTF8
     */
    @Test
    public void testVarcharUnicodeMapping()
    {
        testTypeMapping("varchar_unicode",
                unicodeTests(DataType::varcharDataType, codePoints(), MAX_VARCHAR2_ON_WRITE));
    }

    @Test
    public void testVarcharUnicodeReadMapping()
    {
        testTypeReadMapping("read_varchar_unicode",
                unicodeTests(varchar2DataType(CHAR), codePoints(), MAX_VARCHAR2_ON_READ),
                unicodeTests(varchar2DataType(BYTE), utf8Bytes(), MAX_VARCHAR2_ON_READ),
                unicodeTests(nvarchar2DataType(), String::length, MAX_NVARCHAR2));
    }

    @Test
    public void testUnboundedVarcharMapping()
    {
        testTypeMapping("unbounded",
                unboundedVarcharTests(varcharDataType()),
                unboundedVarcharTests(tooLargeVarcharDataType()),
                unboundedVarcharTests(tooLargeCharDataType()));
    }

    @Test
    public void testUnboundedVarcharReadMapping()
    {
        testTypeReadMapping("read_unbounded",
                unboundedVarcharTests(clobDataType()).addRoundTrip(clobDataType(), ""),
                unboundedVarcharTests(nclobDataType()).addRoundTrip(nclobDataType(), ""));
        // The tests on empty strings are read-only because Oracle treats empty
        // strings as NULL. The empty clob is generated by an Oracle function.
    }

    private static DataTypeTest unboundedVarcharTests(DataType<String> dataType)
    {
        // The string length function and max size are placeholders;
        // the data type isn't parameterized.
        return unicodeTests(ignored -> dataType, ignored -> 0, 0)
                .addRoundTrip(dataType, "clob")
                .addRoundTrip(dataType, null);
    }

    /* char tests */

    @Test
    public void testCharMapping()
    {
        testTypeMapping("char",
                basicCharacterTests(DataType::charDataType, MAX_CHAR_ON_WRITE));
    }

    @Test
    public void testCharReadMapping()
    {
        testTypeReadMapping("read_char",
                basicCharacterTests(charDataType(CHAR), MAX_CHAR_ON_READ),
                basicCharacterTests(charDataType(BYTE), MAX_CHAR_ON_READ),
                basicCharacterTests(ncharDataType(), MAX_NCHAR));
    }

    // TODO: Replace this to not take maxSize
    private static DataTypeTest basicCharacterTests(IntFunction<DataType<String>> typeConstructor, int maxSize)
    {
        return DataTypeTest.create()
                .addRoundTrip(typeConstructor.apply(10), "string 010")
                .addRoundTrip(typeConstructor.apply(20), "string 20")
                .addRoundTrip(typeConstructor.apply(maxSize), "string max size")
                .addRoundTrip(typeConstructor.apply(5), null);
    }

    @Test
    public void testCharUnicodeMapping()
    {
        testTypeMapping("char_unicode",
                unicodeTests(DataType::charDataType, codePoints(), MAX_CHAR_ON_WRITE));
    }

    @Test
    public void testCharUnicodeReadMapping()
    {
        testTypeReadMapping("read_char_unicode",
                unicodeTests(charDataType(CHAR), codePoints(), MAX_CHAR_ON_READ),
                unicodeTests(charDataType(BYTE), utf8Bytes(), MAX_CHAR_ON_READ),
                unicodeTests(ncharDataType(), String::length, MAX_NCHAR));
    }

    private static DataTypeTest unicodeTests(IntFunction<DataType<String>> typeConstructor, ToIntFunction<String> stringLength, int maxSize)
    {
        String unicodeText = "攻殻機動隊";
        String nonBmpCharacter = "\ud83d\ude02";
        int unicodeLength = stringLength.applyAsInt(unicodeText);
        int nonBmpLength = stringLength.applyAsInt(nonBmpCharacter);

        return DataTypeTest.create()
                .addRoundTrip(typeConstructor.apply(unicodeLength), unicodeText)
                .addRoundTrip(typeConstructor.apply(unicodeLength + 8), unicodeText)
                .addRoundTrip(typeConstructor.apply(maxSize), unicodeText)
                .addRoundTrip(typeConstructor.apply(nonBmpLength), nonBmpCharacter)
                .addRoundTrip(typeConstructor.apply(nonBmpLength + 5), nonBmpCharacter);
    }

    /* Varbinary tests */

    @Test
    public void testVarbinaryMapping()
    {
        testTypeMapping("varbinary", varbinaryTests(varbinaryDataType()));
    }

    @Test
    public void testVarbinaryReadMapping()
    {
        testTypeReadMapping("read_varbinary",
                varbinaryTests(blobDataType())
                        .addRoundTrip(blobDataType(), new byte[] {}),
                varbinaryTests(rawDataType(2000)));
        // The test with the empty array is read-only because Oracle treats empty
        // binary data as NULL. The empty blob is generated by an Oracle function.
    }

    private static DataTypeTest varbinaryTests(DataType<byte[]> binaryType)
    {
        return DataTypeTest.create()
                .addRoundTrip(binaryType, "varbinary".getBytes(UTF_8))
                .addRoundTrip(binaryType, "Piękna łąka w 東京都".getBytes(UTF_8))
                .addRoundTrip(binaryType, "Bag full of \ud83d\udcb0".getBytes(UTF_16LE))
                .addRoundTrip(binaryType, null)
                .addRoundTrip(binaryType, new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 13, -7, 54, 122, -89, 0, 0, 0});
    }

    /* Decimal tests */

    @Test
    public void testDecimalMapping()
    {
        testTypeMapping("decimals", numericTests(DataType::decimalDataType));
    }

    @Test
    public void testIntegerMappings()
    {
        testTypeMapping("integers",
                DataTypeTest.create()
                        .addRoundTrip(integerDataType("tinyint", 3), 0L)
                        .addRoundTrip(integerDataType("smallint", 5), 0L)
                        .addRoundTrip(integerDataType("integer", 10), 0L)
                        .addRoundTrip(integerDataType("bigint", 19), 0L));
    }

    @Test
    public void testNumberReadMapping()
    {
        testTypeReadMapping("read_decimals", numericTests(OracleDataTypes::oracleDecimalDataType));
    }

    private static DataTypeTest numericTests(BiFunction<Integer, Integer, DataType<BigDecimal>> decimalType)
    {
        return DataTypeTest.create()
                .addRoundTrip(decimalType.apply(3, 0), new BigDecimal("193")) // full p
                .addRoundTrip(decimalType.apply(3, 0), new BigDecimal("19")) // partial p
                .addRoundTrip(decimalType.apply(3, 0), new BigDecimal("-193")) // negative full p
                .addRoundTrip(decimalType.apply(3, 1), new BigDecimal("10.0")) // 0 decimal
                .addRoundTrip(decimalType.apply(3, 1), new BigDecimal("10.1")) // full ps
                .addRoundTrip(decimalType.apply(3, 1), new BigDecimal("-10.1")) // negative ps
                .addRoundTrip(decimalType.apply(4, 2), new BigDecimal("2")) //
                .addRoundTrip(decimalType.apply(4, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalType.apply(24, 2), new BigDecimal("2"))
                .addRoundTrip(decimalType.apply(24, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalType.apply(24, 2), new BigDecimal("123456789.3"))
                .addRoundTrip(decimalType.apply(24, 4), new BigDecimal("12345678901234567890.31"))
                .addRoundTrip(decimalType.apply(30, 5), new BigDecimal("3141592653589793238462643.38327"))
                .addRoundTrip(decimalType.apply(30, 5), new BigDecimal("-3141592653589793238462643.38327"))
                .addRoundTrip(decimalType.apply(38, 0), new BigDecimal("27182818284590452353602874713526624977"))
                .addRoundTrip(decimalType.apply(38, 0), new BigDecimal("-27182818284590452353602874713526624977"))
                .addRoundTrip(decimalType.apply(38, 38), new BigDecimal(".10000200003000040000500006000070000888"))
                .addRoundTrip(decimalType.apply(38, 38), new BigDecimal("-.27182818284590452353602874713526624977"))
                .addRoundTrip(decimalType.apply(10, 3), null);
    }

    @Test
    public void testNumberWithoutScaleReadMapping()
    {
        DataTypeTest.create()
                .addRoundTrip(numberDataType(1), BigDecimal.valueOf(1))
                .addRoundTrip(numberDataType(2), BigDecimal.valueOf(99))
                .addRoundTrip(numberDataType(38),
                        new BigDecimal("99999999999999999999999999999999999999")) // max
                .addRoundTrip(numberDataType(38),
                        new BigDecimal("-99999999999999999999999999999999999999")) // min
                .execute(getQueryRunner(), oracleCreateAndInsert("number_without_scale"));
    }

    @Test
    public void testNumberWithoutPrecisionAndScaleReadMapping()
    {
        DataTypeTest.create()
                .addRoundTrip(unspecifiedNumberDataType(9), BigDecimal.valueOf(1))
                .addRoundTrip(unspecifiedNumberDataType(9), BigDecimal.valueOf(99))
                .addRoundTrip(unspecifiedNumberDataType(9), new BigDecimal("9999999999999999999999999999.999999999")) // max
                .addRoundTrip(unspecifiedNumberDataType(9), new BigDecimal("-999999999999999999999999999.999999999")) // min
                .execute(getQueryRunner(), number(9), oracleCreateAndInsert("number_wo_prec_and_scale"));
    }

    @Test
    public void testRoundingOfUnspecifiedNumber()
    {
        try (TestTable table = oracleTable("rounding", "col NUMBER", "(0.123456789)")) {
            assertQuery(number(9), "SELECT * FROM " + table.getName(), "VALUES 0.123456789");
            assertQuery(number(HALF_EVEN, 6), "SELECT * FROM " + table.getName(), "VALUES 0.123457");
            assertQuery(number(HALF_EVEN, 3), "SELECT * FROM " + table.getName(), "VALUES 0.123");
            assertQueryFails(number(UNNECESSARY, 3), "SELECT * FROM " + table.getName(), "Rounding necessary");
        }

        try (TestTable table = oracleTable("rounding", "col NUMBER", "(123456789012345678901234567890.123456789)")) {
            assertQueryFails(number(9), "SELECT * FROM " + table.getName(), "Decimal overflow");
            assertQuery(number(HALF_EVEN, 8), "SELECT * FROM " + table.getName(), "VALUES 123456789012345678901234567890.12345679");
            assertQuery(number(HALF_EVEN, 6), "SELECT * FROM " + table.getName(), "VALUES 123456789012345678901234567890.123457");
            assertQuery(number(HALF_EVEN, 3), "SELECT * FROM " + table.getName(), "VALUES 123456789012345678901234567890.123");
            assertQueryFails(number(UNNECESSARY, 3), "SELECT * FROM " + table.getName(), "Rounding necessary");
        }

        try (TestTable table = oracleTable("rounding", "col NUMBER", "(123456789012345678901234567890123456789)")) {
            assertQueryFails(number(0), "SELECT * FROM " + table.getName(), "Decimal overflow");
            assertQueryFails(number(HALF_EVEN, 8), "SELECT * FROM " + table.getName(), "Decimal overflow");
            assertQueryFails(number(HALF_EVEN, 0), "SELECT * FROM " + table.getName(), "Decimal overflow");
        }
    }

    @Test
    public void testNumberNegativeScaleReadMapping()
    {
        // TODO: Add similar tests for write mappings.
        // Those tests would require the table to be created in Oracle, but values inserted
        // by Presto, which is outside the capabilities of the current DataSetup classes.
        DataTypeTest.create()
                .addRoundTrip(numberDataType(1, -1), BigDecimal.valueOf(2_0))
                .addRoundTrip(numberDataType(1, -1), BigDecimal.valueOf(3_5)) // More useful as a test for write mappings.
                .addRoundTrip(numberDataType(2, -4), BigDecimal.valueOf(47_0000))
                .addRoundTrip(numberDataType(2, -4), BigDecimal.valueOf(-8_0000))
                .addRoundTrip(numberDataType(8, -3), BigDecimal.valueOf(-88888888, -3))
                .addRoundTrip(numberDataType(8, -3), BigDecimal.valueOf(4050_000))
                .addRoundTrip(numberDataType(14, -14), BigDecimal.valueOf(14000014000014L, -14))
                .addRoundTrip(numberDataType(14, -14), BigDecimal.valueOf(1, -21))
                .addRoundTrip(numberDataType(5, -33), BigDecimal.valueOf(12345, -33))
                .addRoundTrip(numberDataType(5, -33), BigDecimal.valueOf(-12345, -33))
                .addRoundTrip(numberDataType(1, -37), BigDecimal.valueOf(1, -37))
                .addRoundTrip(numberDataType(1, -37), BigDecimal.valueOf(-1, -37))
                .addRoundTrip(numberDataType(37, -1),
                        new BigDecimal("99999999999999999999999999999999999990")) // max
                .addRoundTrip(numberDataType(37, -1),
                        new BigDecimal("-99999999999999999999999999999999999990")) // min
                .execute(getQueryRunner(), oracleCreateAndInsert("number_negative_s"));
    }

    @Test
    public void testHighNumberScale()
    {
        try (TestTable table = oracleTable("highNumberScale", "col NUMBER(38, 40)", "(0.0012345678901234567890123456789012345678)")) {
            assertQueryFails(number(UNNECESSARY), "SELECT * FROM " + table.getName(), NO_SUPPORTED_COLUMNS);
            assertQuery(number(HALF_EVEN), "SELECT * FROM " + table.getName(), "VALUES 0.00123456789012345678901234567890123457");
            assertQuery(numberConvertToVarchar(), "SELECT * FROM " + table.getName(), "VALUES '1.2345678901234567890123456789012345678E-03'");
        }

        try (TestTable table = oracleTable("highNumberScale", "col NUMBER(18, 40)", "(0.0000000000000000000000123456789012345678)")) {
            assertQueryFails(number(UNNECESSARY), "SELECT * FROM " + table.getName(), NO_SUPPORTED_COLUMNS);
            assertQuery(number(HALF_EVEN), "SELECT * FROM " + table.getName(), "VALUES 0.00000000000000000000001234567890123457");
        }

        try (TestTable table = oracleTable("highNumberScale", "col NUMBER(38, 80)", "(0.00000000000000000000000000000000000000000000012345678901234567890123456789012345678)")) {
            assertQuery(number(HALF_EVEN), "SELECT * FROM " + table.getName(), "VALUES 0");
            assertQuery(numberConvertToVarchar(), "SELECT * FROM " + table.getName(), "VALUES '1.2345678901234567890123456789012346E-46'");
        }
    }

    @Test
    public void testNumberWithHiveNegativeScaleReadMapping()
    {
        try (TestTable table = oracleTable("highNegativeNumberScale", "col NUMBER(38, -60)", "(1234567890123456789012345678901234567000000000000000000000000000000000000000000000000000000000000)")) {
            assertQuery(numberConvertToVarchar(), "SELECT * FROM " + table.getName(), "VALUES '1.234567890123456789012345678901234567E96'");
        }

        try (TestTable table = oracleTable("highNumberScale", "col NUMBER(18, 60)", "(0.000000000000000000000000000000000000000000000123456789012345678)")) {
            assertQuery(number(HALF_EVEN), "SELECT * FROM " + table.getName(), "VALUES 0");
        }
    }

    private Session number(int scale)
    {
        return number(IGNORE, UNNECESSARY, Optional.of(scale));
    }

    private Session number(RoundingMode roundingMode)
    {
        return number(IGNORE, roundingMode, Optional.empty());
    }

    private Session number(RoundingMode roundingMode, int scale)
    {
        return number(IGNORE, roundingMode, Optional.of(scale));
    }

    private Session numberConvertToVarchar()
    {
        return number(CONVERT_TO_VARCHAR, UNNECESSARY, Optional.empty());
    }

    private Session number(UnsupportedTypeHandling unsupportedTypeHandlingStrategy, RoundingMode roundingMode, Optional<Integer> scale)
    {
        Session.SessionBuilder builder = Session.builder(getSession())
                .setCatalogSessionProperty("oracle", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandlingStrategy.name())
                .setCatalogSessionProperty("oracle", NUMBER_ROUNDING_MODE, roundingMode.name());
        scale.ifPresent(value -> builder.setCatalogSessionProperty("oracle", NUMBER_DEFAULT_SCALE, value.toString()));
        return builder.build();
    }

    @Test
    public void testSpecialNumberFormats()
    {
        getOracleSqlExecutor().execute("CREATE TABLE test (num1 number)");
        getOracleSqlExecutor().execute("INSERT INTO test VALUES (12345678901234567890.12345678901234567890123456789012345678)");
        assertQuery(number(HALF_UP, 10), "SELECT * FROM test", "VALUES (12345678901234567890.1234567890)");
    }

    @Test
    public void testBooleanType()
    {
        DataTypeTest.create()
                .addRoundTrip(booleanDataType(), true)
                .addRoundTrip(booleanDataType(), false)
                .execute(getQueryRunner(), prestoCreateAsSelect("boolean_types"));
    }

    /* Datetime tests */

    @Test
    public void testLegacyDateMapping()
    {
        legacyDateTests(zone -> prestoCreateAsSelect("l_date_" + zone));
    }

    @Test
    public void testLegacyDateReadMapping()
    {
        legacyDateTests(zone -> oracleCreateAndInsert("l_read_date_" + zone));
    }

    private void legacyDateTests(Function<String, DataSetup> dataSetup)
    {
        Map<String, TimeZoneKey> zonesBySqlName = ImmutableMap.of(
                "UTC", UTC_KEY,
                "JVM", getTimeZoneKey(ZoneId.systemDefault().getId()),
                "other", getTimeZoneKey(ZoneId.of("Europe/Vilnius").getId()));

        for (Map.Entry<String, TimeZoneKey> zone : zonesBySqlName.entrySet()) {
            runLegacyTimestampTestInZone(
                    dataSetup.apply(zone.getKey()),
                    zone.getValue().getId(),
                    legacyDateTests());
        }
    }

    private static DataTypeTest legacyDateTests()
    {
        ZoneId someZone = ZoneId.of("Europe/Vilnius");

        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone =
                LocalDate.of(1983, 10, 1);

        verify(someZone.getRules().getValidOffsets(
                dateOfLocalTimeChangeBackwardAtMidnightInSomeZone
                        .atStartOfDay().minusMinutes(1)).size() == 2);

        return DataTypeTest.create()
                // before epoch
                .addRoundTrip(dateDataType(), LocalDate.of(1952, 4, 3))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 2, 3))
                // summer on northern hemisphere (possible DST)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 7, 1))
                // winter on northern hemisphere
                // (possible DST on southern hemisphere)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 1, 1))
                .addRoundTrip(dateDataType(),
                        dateOfLocalTimeChangeBackwardAtMidnightInSomeZone);
    }

    @Test
    public void testDateMapping()
    {
        DateTests(zone -> prestoCreateAsSelect("nl_date_" + zone));
    }

    @Test
    public void testDateReadMapping()
    {
        DateTests(zone -> oracleCreateAndInsert("nl_read_date_" + zone));
    }

    private void DateTests(Function<String, DataSetup> dataSetup)
    {
        Map<String, TimeZoneKey> zonesBySqlName = ImmutableMap.of(
                "UTC", UTC_KEY,
                "JVM", getTimeZoneKey(ZoneId.systemDefault().getId()),
                "other", getTimeZoneKey(ZoneId.of("Europe/Vilnius").getId()));

        for (Map.Entry<String, TimeZoneKey> zone : zonesBySqlName.entrySet()) {
            runTimestampTestInZone(
                    dataSetup.apply(zone.getKey()),
                    zone.getValue().getId(),
                    DateTests());
        }
    }

    private DataTypeTest DateTests()
    {
        // Note: these test cases are duplicates of those for PostgreSQL and MySQL.

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone =
                LocalDate.of(1970, 1, 1);

        verify(jvmZone.getRules().getValidOffsets(
                dateOfLocalTimeChangeForwardAtMidnightInJvmZone
                        .atStartOfDay()).isEmpty());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone =
                LocalDate.of(1983, 4, 1);

        verify(someZone.getRules().getValidOffsets(
                dateOfLocalTimeChangeForwardAtMidnightInSomeZone
                        .atStartOfDay()).isEmpty());

        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone =
                LocalDate.of(1983, 10, 1);

        verify(someZone.getRules().getValidOffsets(
                dateOfLocalTimeChangeBackwardAtMidnightInSomeZone
                        .atStartOfDay().minusMinutes(1)).size() == 2);

        return DataTypeTest.create()
                // before epoch
                .addRoundTrip(dateDataType(), LocalDate.of(1952, 4, 3))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 1, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 2, 3))
                // summer on northern hemisphere (possible DST)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 7, 1))
                // winter on northern hemisphere
                // (possible DST on southern hemisphere)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 1, 1))
                .addRoundTrip(dateDataType(),
                        dateOfLocalTimeChangeForwardAtMidnightInJvmZone)
                .addRoundTrip(dateDataType(),
                        dateOfLocalTimeChangeForwardAtMidnightInSomeZone)
                .addRoundTrip(dateDataType(),
                        dateOfLocalTimeChangeBackwardAtMidnightInSomeZone);
    }

    @Test(dataProvider = "testTimestampDataProvider")
    public void testTimestamp(boolean insertWithPresto, ZoneId sessionZone)
    {
        // using two non-JVM zones so that we don't need to worry what Oracle system zone is
        DataTypeTest tests = DataTypeTest.create()
                .addRoundTrip(timestampDataType(), beforeEpoch)
                .addRoundTrip(timestampDataType(), afterEpoch)
                .addRoundTrip(timestampDataType(), timeDoubledInJvmZone)
                .addRoundTrip(timestampDataType(), timeDoubledInVilnius)
                .addRoundTrip(timestampDataType(), epoch) // epoch also is a gap in JVM zone
                .addRoundTrip(timestampDataType(), timeGapInJvmZone1)
                .addRoundTrip(timestampDataType(), timeGapInJvmZone2)
                .addRoundTrip(timestampDataType(), timeGapInVilnius)
                .addRoundTrip(timestampDataType(), timeGapInKathmandu);

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(getTimeZoneKey(sessionZone.getId()))
                .build();

        if (insertWithPresto) {
            tests.execute(getQueryRunner(), session, prestoCreateAsSelect(session, "test_timestamp"));
        }
        else {
            tests.execute(getQueryRunner(), session, oracleCreateAndInsert("test_timestamp"));
        }
    }

    @DataProvider
    public Object[][] testTimestampDataProvider()
    {
        return new Object[][] {
                {true, UTC},
                {false, UTC},

                {true, jvmZone},
                {false, jvmZone},

                // using two non-JVM zones so that we don't need to worry what Oracle system zone is
                {true, vilnius},
                {false, vilnius},

                {true, kathmandu},
                {false, kathmandu},

                {true, ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
                {false, ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
        };
    }

    @Test(dataProvider = "testTimestampWithTimeZoneDataProvider")
    public void testTimestampWithTimeZone(boolean insertWithPresto)
    {
        DataType<ZonedDateTime> dataType;
        DataSetup dataSetup;
        if (insertWithPresto) {
            dataType = prestoTimestampWithTimeZoneDataType();
            dataSetup = prestoCreateAsSelect("timestamp_tz");
        }
        else {
            dataType = oracleTimestamp3TimeZoneDataType();
            dataSetup = oracleCreateAndInsert("timestamp_tz");
        }

        DataTypeTest tests = DataTypeTest.create()
                .addRoundTrip(dataType, epoch.atZone(UTC))
                .addRoundTrip(dataType, epoch.atZone(kathmandu))
                .addRoundTrip(dataType, epoch.atZone(fixedOffsetEast))
                .addRoundTrip(dataType, epoch.atZone(fixedOffsetWest))
                .addRoundTrip(dataType, beforeEpoch.atZone(UTC))
                .addRoundTrip(dataType, beforeEpoch.atZone(kathmandu))
                .addRoundTrip(dataType, beforeEpoch.atZone(fixedOffsetEast))
                .addRoundTrip(dataType, beforeEpoch.atZone(fixedOffsetWest))
                .addRoundTrip(dataType, afterEpoch.atZone(UTC))
                .addRoundTrip(dataType, afterEpoch.atZone(kathmandu))
                .addRoundTrip(dataType, afterEpoch.atZone(fixedOffsetEast))
                .addRoundTrip(dataType, afterEpoch.atZone(fixedOffsetWest))
                .addRoundTrip(dataType, timeDoubledInJvmZone.atZone(UTC))
                .addRoundTrip(dataType, timeDoubledInJvmZone.atZone(jvmZone))
                .addRoundTrip(dataType, timeDoubledInJvmZone.atZone(kathmandu))
                .addRoundTrip(dataType, timeDoubledInVilnius.atZone(UTC))
                .addRoundTrip(dataType, timeDoubledInVilnius.atZone(vilnius))
                .addRoundTrip(dataType, timeDoubledInVilnius.atZone(kathmandu))
                .addRoundTrip(dataType, timeGapInJvmZone1.atZone(UTC))
                .addRoundTrip(dataType, timeGapInJvmZone1.atZone(kathmandu))
                .addRoundTrip(dataType, timeGapInJvmZone2.atZone(UTC))
                .addRoundTrip(dataType, timeGapInJvmZone2.atZone(kathmandu))
                .addRoundTrip(dataType, timeGapInVilnius.atZone(kathmandu))
                .addRoundTrip(dataType, timeGapInKathmandu.atZone(vilnius));

        tests.execute(getQueryRunner(), dataSetup);
    }

    @DataProvider
    public Object[][] testTimestampWithTimeZoneDataProvider()
    {
        return new Object[][] {
                {true},
                {false},
        };
    }

    /* Unsupported type tests */

    @Test
    public void testUnsupportedBasicType()
    {
        testUnsupportedOracleType("BFILE"); // Never in mapping
    }

    @Test
    public void testUnsupportedNumberScale()
    {
        // Difference between precision and negative scale greater than 38
        testUnsupportedOracleType("number(20, -20)");
        testUnsupportedOracleType("number(38, -84)");
        // Scale larger than precision.
        testUnsupportedOracleType("NUMBER(2, 4)"); // Explicitly removed from mapping
    }

    /* Testing utilities */

    /**
     * Check that unsupported data types are ignored
     */
    private void testUnsupportedOracleType(String dataTypeName)
    {
        try (TestTable table = new TestTable(getOracleSqlExecutor(), "unsupported_type", format("(unsupported_type %s)", dataTypeName))) {
            assertQueryFails("SELECT * FROM " + table.getName(), NO_SUPPORTED_COLUMNS);
        }
    }

    private DataSetup oracleCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(getOracleSqlExecutor(), tableNamePrefix);
    }

    /**
     * Run {@link DataTypeTest}s, creating tables from Presto.
     */
    private void testTypeMapping(String tableNamePrefix, DataTypeTest... tests)
    {
        runTestsWithSetup(prestoCreateAsSelect(tableNamePrefix), tests);
    }

    /**
     * Run {@link DataTypeTest}s, creating tables with the JDBC.
     */
    private void testTypeReadMapping(String tableNamePrefix, DataTypeTest... tests)
    {
        runTestsWithSetup(oracleCreateAndInsert(tableNamePrefix), tests);
    }

    private void runTestsWithSetup(DataSetup dataSetup, DataTypeTest... tests)
    {
        for (DataTypeTest test : tests) {
            test.execute(getQueryRunner(), dataSetup);
        }
    }

    /**
     * Run a {@link DataTypeTest} in the given time zone, using legacy timestamps.
     * <p>
     * If the given time zone is {@code null}, use the default session time zone.
     */
    private void runLegacyTimestampTestInZone(DataSetup dataSetup, String zone, DataTypeTest test)
    {
        Session.SessionBuilder session = Session.builder(getQueryRunner().getDefaultSession());
        if (zone != null) {
            session.setTimeZoneKey(getTimeZoneKey(zone));
        }
        test.execute(getQueryRunner(), session.build(), dataSetup);
    }

    /**
     * Run a {@link DataTypeTest} in the given time zone, using non-legacy timestamps.
     * <p>
     * If the given time zone is {@code null}, use the default session time zone.
     */
    private void runTimestampTestInZone(DataSetup dataSetup, String zone, DataTypeTest test)
    {
        Session.SessionBuilder session = Session.builder(getQueryRunner().getDefaultSession());
        if (zone != null) {
            session.setTimeZoneKey(getTimeZoneKey(zone));
        }
        test.execute(getQueryRunner(), session.build(), dataSetup);
    }

    protected abstract SqlExecutor getOracleSqlExecutor();

    private static ToIntFunction<String> codePoints()
    {
        return s -> s.codePointCount(0, s.length());
    }

    private static ToIntFunction<String> utf8Bytes()
    {
        return s -> s.getBytes(UTF_8).length;
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

    private TestTable oracleTable(String tableName, String schema, String data)
    {
        return new TestTable(getOracleSqlExecutor(), tableName, format("(%s)", schema), ImmutableList.of(data));
    }
}
