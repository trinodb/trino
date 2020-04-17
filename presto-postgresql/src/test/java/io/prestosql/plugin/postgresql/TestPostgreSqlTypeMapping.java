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
package io.prestosql.plugin.postgresql;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.prestosql.Session;
import io.prestosql.plugin.jdbc.UnsupportedTypeHandling;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.datatype.CreateAndInsertDataSetup;
import io.prestosql.testing.datatype.CreateAndPrestoInsertDataSetup;
import io.prestosql.testing.datatype.CreateAsSelectDataSetup;
import io.prestosql.testing.datatype.DataSetup;
import io.prestosql.testing.datatype.DataType;
import io.prestosql.testing.datatype.DataTypeTest;
import io.prestosql.testing.sql.JdbcSqlExecutor;
import io.prestosql.testing.sql.PrestoSqlExecutor;
import io.prestosql.testing.sql.TestTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.BaseEncoding.base16;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.prestosql.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.prestosql.plugin.jdbc.DecimalConfig.DecimalMapping.STRICT;
import static io.prestosql.plugin.jdbc.DecimalSessionPropertiesProvider.DECIMAL_DEFAULT_SCALE;
import static io.prestosql.plugin.jdbc.DecimalSessionPropertiesProvider.DECIMAL_MAPPING;
import static io.prestosql.plugin.jdbc.DecimalSessionPropertiesProvider.DECIMAL_ROUNDING_MODE;
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcPropertiesProvider.UNSUPPORTED_TYPE_HANDLING;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.prestosql.plugin.postgresql.PostgreSqlConfig.ArrayMapping.AS_ARRAY;
import static io.prestosql.plugin.postgresql.PostgreSqlConfig.ArrayMapping.AS_JSON;
import static io.prestosql.plugin.postgresql.PostgreSqlQueryRunner.createPostgreSqlQueryRunner;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TypeSignature.mapType;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.datatype.DataType.bigintDataType;
import static io.prestosql.testing.datatype.DataType.booleanDataType;
import static io.prestosql.testing.datatype.DataType.dataType;
import static io.prestosql.testing.datatype.DataType.dateDataType;
import static io.prestosql.testing.datatype.DataType.decimalDataType;
import static io.prestosql.testing.datatype.DataType.doubleDataType;
import static io.prestosql.testing.datatype.DataType.formatStringLiteral;
import static io.prestosql.testing.datatype.DataType.integerDataType;
import static io.prestosql.testing.datatype.DataType.jsonDataType;
import static io.prestosql.testing.datatype.DataType.realDataType;
import static io.prestosql.testing.datatype.DataType.smallintDataType;
import static io.prestosql.testing.datatype.DataType.timeDataType;
import static io.prestosql.testing.datatype.DataType.timestampDataType;
import static io.prestosql.testing.datatype.DataType.varbinaryDataType;
import static io.prestosql.testing.datatype.DataType.varcharDataType;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.type.UuidType.UUID;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Test
public class TestPostgreSqlTypeMapping
        extends AbstractTestQueryFramework
{
    private static final LocalDate EPOCH_DAY = LocalDate.ofEpochDay(0);
    private static final JsonCodec<List<Map<String, String>>> HSTORE_CODEC = listJsonCodec(mapJsonCodec(String.class, String.class));

    private TestingPostgreSqlServer postgreSqlServer;

    private LocalDateTime beforeEpoch;
    private LocalDateTime epoch;
    private LocalDateTime afterEpoch;

    private ZoneId jvmZone;
    private LocalDateTime timeGapInJvmZone1;
    private LocalDateTime timeGapInJvmZone2;
    private LocalDateTime timeDoubledInJvmZone;

    // no DST in 1970, but has DST in later years (e.g. 2018)
    private ZoneId vilnius;
    private LocalDateTime timeGapInVilnius;
    private LocalDateTime timeDoubledInVilnius;

    // minutes offset change since 1970-01-01, no DST
    private ZoneId kathmandu;
    private LocalDateTime timeGapInKathmandu;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.postgreSqlServer = new TestingPostgreSqlServer();
        return createPostgreSqlQueryRunner(
                postgreSqlServer,
                ImmutableMap.of(),
                ImmutableMap.of("jdbc-types-mapped-to-varchar", "Tsrange, Inet" /* make sure that types are compared case insensitively */),
                ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        postgreSqlServer.close();
    }

    @BeforeClass
    public void setUp()
    {
        beforeEpoch = LocalDateTime.of(1958, 1, 1, 13, 18, 3, 123_000_000);
        epoch = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
        afterEpoch = LocalDateTime.of(2019, 03, 18, 10, 01, 17, 987_000_000);

        jvmZone = ZoneId.systemDefault();

        timeGapInJvmZone1 = LocalDateTime.of(1970, 1, 1, 0, 13, 42);
        checkIsGap(jvmZone, timeGapInJvmZone1);
        timeGapInJvmZone2 = LocalDateTime.of(2018, 4, 1, 2, 13, 55, 123_000_000);
        checkIsGap(jvmZone, timeGapInJvmZone2);
        timeDoubledInJvmZone = LocalDateTime.of(2018, 10, 28, 1, 33, 17, 456_000_000);
        checkIsDoubled(jvmZone, timeDoubledInJvmZone);

        vilnius = ZoneId.of("Europe/Vilnius");

        timeGapInVilnius = LocalDateTime.of(2018, 3, 25, 3, 17, 17);
        checkIsGap(vilnius, timeGapInVilnius);
        timeDoubledInVilnius = LocalDateTime.of(2018, 10, 28, 3, 33, 33, 333_000_000);
        checkIsDoubled(vilnius, timeDoubledInVilnius);

        kathmandu = ZoneId.of("Asia/Kathmandu");

        timeGapInKathmandu = LocalDateTime.of(1986, 1, 1, 0, 13, 7);
        checkIsGap(kathmandu, timeGapInKathmandu);

        JdbcSqlExecutor executor = new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl());
        executor.execute("CREATE EXTENSION hstore");
    }

    @Test
    public void testBasicTypes()
    {
        DataTypeTest.create()
                .addRoundTrip(booleanDataType(), true)
                .addRoundTrip(booleanDataType(), false)
                .addRoundTrip(bigintDataType(), 123_456_789_012L)
                .addRoundTrip(integerDataType(), 1_234_567_890)
                .addRoundTrip(smallintDataType(), (short) 32_456)
                .addRoundTrip(doubleDataType(), 123.45d)
                .addRoundTrip(realDataType(), 123.45f)
                .addRoundTrip(dataType("tinyint", SMALLINT, Object::toString, result -> (short) result), (byte) 5)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testVarbinary()
    {
        varbinaryTestCases(varbinaryDataType())
                .execute(getQueryRunner(), prestoCreateAsSelect("test_varbinary"));

        varbinaryTestCases(byteaDataType())
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.test_varbinary"));
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
    public void testPrestoCreatedParameterizedVarchar()
    {
        varcharDataTypeTest()
                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_parameterized_varchar"));
    }

    @Test
    public void testPostgreSqlCreatedParameterizedVarchar()
    {
        varcharDataTypeTest()
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_parameterized_varchar"));
    }

    private DataTypeTest varcharDataTypeTest()
    {
        return DataTypeTest.create()
                .addRoundTrip(varcharDataType(10), "text_a")
                .addRoundTrip(varcharDataType(255), "text_b")
                .addRoundTrip(varcharDataType(65535), "text_d")
                .addRoundTrip(varcharDataType(10485760), "text_f")
                .addRoundTrip(varcharDataType(), "unbounded");
    }

    @Test
    public void testPrestoCreatedParameterizedVarcharUnicode()
    {
        unicodeVarcharDateTypeTest()
                .execute(getQueryRunner(), prestoCreateAsSelect("postgresql_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testPostgreSqlCreatedParameterizedVarcharUnicode()
    {
        unicodeVarcharDateTypeTest()
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testPrestoCreatedParameterizedCharUnicode()
    {
        unicodeDataTypeTest(DataType::charDataType)
                .execute(getQueryRunner(), prestoCreateAsSelect("postgresql_test_parameterized_char_unicode"));
    }

    @Test
    public void testPostgreSqlCreatedParameterizedCharUnicode()
    {
        unicodeDataTypeTest(DataType::charDataType)
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_parameterized_char_unicode"));
    }

    private DataTypeTest unicodeVarcharDateTypeTest()
    {
        return unicodeDataTypeTest(DataType::varcharDataType)
                .addRoundTrip(varcharDataType(), "\u041d\u0443, \u043f\u043e\u0433\u043e\u0434\u0438!");
    }

    private DataTypeTest unicodeDataTypeTest(Function<Integer, DataType<String>> dataTypeFactory)
    {
        String sampleUnicodeText = "\u653b\u6bbb\u6a5f\u52d5\u968a";
        String sampleFourByteUnicodeCharacter = "\uD83D\uDE02";

        return DataTypeTest.create()
                .addRoundTrip(dataTypeFactory.apply(sampleUnicodeText.length()), sampleUnicodeText)
                .addRoundTrip(dataTypeFactory.apply(32), sampleUnicodeText)
                .addRoundTrip(dataTypeFactory.apply(20000), sampleUnicodeText)
                .addRoundTrip(dataTypeFactory.apply(1), sampleFourByteUnicodeCharacter);
    }

    @Test
    public void testPostgresSqlCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.test_decimal"));
    }

    @Test
    public void testPrestoCreatedDecimal()
    {
        decimalTests()
                .execute(getQueryRunner(), prestoCreateAsSelect("test_decimal"));
    }

    private DataTypeTest decimalTests()
    {
        return DataTypeTest.create()
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("193"))
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("19"))
                .addRoundTrip(decimalDataType(3, 0), new BigDecimal("-193"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("10.0"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("10.1"))
                .addRoundTrip(decimalDataType(3, 1), new BigDecimal("-10.1"))
                .addRoundTrip(decimalDataType(4, 2), new BigDecimal("2"))
                .addRoundTrip(decimalDataType(4, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("2"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalDataType(24, 2), new BigDecimal("123456789.3"))
                .addRoundTrip(decimalDataType(24, 4), new BigDecimal("12345678901234567890.31"))
                .addRoundTrip(decimalDataType(30, 5), new BigDecimal("3141592653589793238462643.38327"))
                .addRoundTrip(decimalDataType(30, 5), new BigDecimal("-3141592653589793238462643.38327"))
                .addRoundTrip(decimalDataType(38, 0), new BigDecimal("27182818284590452353602874713526624977"))
                .addRoundTrip(decimalDataType(38, 0), new BigDecimal("-27182818284590452353602874713526624977"));
    }

    @Test
    public void testForcedMappingToVarchar()
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl());
        jdbcSqlExecutor.execute("CREATE TABLE tpch.test_forced_varchar_mapping(tsrange_col tsrange, inet_col inet, tsrange_arr_col tsrange[], unsupported_nonforced_column tstzrange)");
        jdbcSqlExecutor.execute("INSERT INTO tpch.test_forced_varchar_mapping(tsrange_col, inet_col, tsrange_arr_col, unsupported_nonforced_column) " +
                "VALUES ('[2010-01-01 14:30, 2010-01-01 15:30)'::tsrange, '172.0.0.1'::inet, array['[2010-01-01 14:30, 2010-01-01 15:30)'::tsrange], '[2010-01-01 14:30, 2010-01-01 15:30)'::tstzrange)");
        try {
            assertQuery(
                    sessionWithArrayAsArray(),
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'test_forced_varchar_mapping'",
                    "VALUES ('tsrange_col','varchar'),('inet_col','varchar'),('tsrange_arr_col','array(varchar)')"); // no 'unsupported_nonforced_column'

            assertQuery(
                    sessionWithArrayAsArray(),
                    "SELECT * FROM tpch.test_forced_varchar_mapping",
                    "VALUES ('[\"2010-01-01 14:30:00\",\"2010-01-01 15:30:00\")','172.0.0.1',ARRAY['[\"2010-01-01 14:30:00\",\"2010-01-01 15:30:00\")'])");

            // test predicate pushdown to column that has forced varchar mapping
            assertQuery(
                    "SELECT 1 FROM tpch.test_forced_varchar_mapping WHERE tsrange_col = '[\"2010-01-01 14:30:00\",\"2010-01-01 15:30:00\")'",
                    "VALUES 1");
            assertQuery(
                    "SELECT 1 FROM tpch.test_forced_varchar_mapping WHERE tsrange_col = 'some value'",
                    "SELECT 1 WHERE false");

            // test insert into column that has forced varchar mapping
            assertQueryFails(
                    "INSERT INTO tpch.test_forced_varchar_mapping (tsrange_col) VALUES ('some value')",
                    "Underlying type that is mapped to VARCHAR is not supported for INSERT: tsrange");
        }
        finally {
            jdbcSqlExecutor.execute("DROP TABLE tpch.test_forced_varchar_mapping");
        }
    }

    @Test
    public void testDecimalExceedingPrecisionMaxIgnored()
    {
        testUnsupportedDataTypeAsIgnored("decimal(50,0)", "12345678901234567890123456789012345678901234567890");
    }

    @Test
    public void testDecimalExceedingPrecisionMaxConvertedToVarchar()
    {
        testUnsupportedDataTypeConvertedToVarchar(
                "decimal(50,0)",
                "12345678901234567890123456789012345678901234567890",
                "'12345678901234567890123456789012345678901234567890'");
    }

    @Test
    public void testDecimalExceedingPrecisionMaxWithExceedingIntegerValues()
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl());

        try (TestTable testTable = new TestTable(
                jdbcSqlExecutor,
                "tpch.test_exceeding_max_decimal",
                "(d_col decimal(65,25))",
                asList("1234567890123456789012345678901234567890.123456789", "-1234567890123456789012345678901234567890.123456789"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,0)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Decimal overflow");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'varchar')");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES ('1234567890123456789012345678901234567890.1234567890000000000000000'), ('-1234567890123456789012345678901234567890.1234567890000000000000000')");
        }
    }

    @Test
    public void testDecimalExceedingPrecisionMaxWithNonExceedingIntegerValues()
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl());

        try (TestTable testTable = new TestTable(
                jdbcSqlExecutor,
                "tpch.test_exceeding_max_decimal",
                "(d_col decimal(60,20))",
                asList("123456789012345678901234567890.123456789012345", "-123456789012345678901234567890.123456789012345"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,0)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (123456789012345678901234567890), (-123456789012345678901234567890)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 8),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,8)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (123456789012345678901234567890.12345679), (-123456789012345678901234567890.12345679)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 22),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,20)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 20),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Decimal overflow");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 9),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Decimal overflow");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'varchar')");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES ('123456789012345678901234567890.12345678901234500000'), ('-123456789012345678901234567890.12345678901234500000')");
        }
    }

    @Test(dataProvider = "testDecimalExceedingPrecisionMaxProvider")
    public void testDecimalExceedingPrecisionMaxWithSupportedValues(int typePrecision, int typeScale)
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl());

        try (TestTable testTable = new TestTable(
                jdbcSqlExecutor,
                "tpch.test_exceeding_max_decimal",
                format("(d_col decimal(%d,%d))", typePrecision, typeScale),
                asList("12.01", "-12.01", "123", "-123", "1.12345678", "-1.12345678"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,0)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12), (-12), (123), (-123), (1), (-1)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 3),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,3)')");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 3),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.123), (-1.123)");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 3),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 8),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col', 'decimal(38,8)')");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.12345678), (-1.12345678)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 9),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.12345678), (-1.12345678)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 8),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (12.01), (-12.01), (123), (-123), (1.12345678), (-1.12345678)");
        }
    }

    @DataProvider
    public Object[][] testDecimalExceedingPrecisionMaxProvider()
    {
        return new Object[][] {
                {40, 8},
                {50, 10},
        };
    }

    @Test
    public void testDecimalUnspecifiedPrecisionWithSupportedValues()
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl());

        try (TestTable testTable = new TestTable(
                jdbcSqlExecutor,
                "tpch.test_var_decimal",
                "(d_col decimal)",
                asList("1.12", "123456.789", "-1.12", "-123456.789"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col','decimal(38,0)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (1), (123457), (-1), (-123457)");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 1),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 1),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col','decimal(38,1)')");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 1),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (1.1), (123456.8), (-1.1), (-123456.8)");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 2),
                    "SELECT d_col FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 2),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (1.12), (123456.79), (-1.12), (-123456.79)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 3),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('d_col','decimal(38,3)')");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 3),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (1.12), (123456.789), (-1.12), (-123456.789)");
        }
    }

    @Test
    public void testDecimalUnspecifiedPrecisionWithExceedingValue()
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl());
        try (TestTable testTable = new TestTable(
                jdbcSqlExecutor,
                "tpch.test_var_decimal_with_exceeding_value",
                "(key varchar(5), d_col decimal)",
                asList("NULL, '1.12'", "NULL, '1234567890123456789012345678901234567890.1234567'"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('key', 'varchar(5)'),('d_col', 'decimal(38,0)')");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT * FROM " + testTable.getName(),
                    "Rounding necessary");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT * FROM " + testTable.getName(),
                    "Decimal overflow");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('key', 'varchar(5)'),('d_col', 'varchar')");
            assertQuery(
                    sessionWithDecimalMappingStrict(CONVERT_TO_VARCHAR),
                    "SELECT * FROM " + testTable.getName(),
                    "VALUES (NULL, '1.12'), (NULL, '1234567890123456789012345678901234567890.1234567')");
            assertQuery(
                    sessionWithDecimalMappingStrict(IGNORE),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_schema||'.'||table_name = '%s'", testTable.getName()),
                    "VALUES ('key', 'varchar(5)')");
        }
    }

    @Test
    public void testArray()
    {
        // basic types
        DataTypeTest.create()
                .addRoundTrip(arrayDataType(booleanDataType()), asList(true, false))
                .addRoundTrip(arrayDataType(bigintDataType()), asList(123_456_789_012L))
                .addRoundTrip(arrayDataType(integerDataType()), asList(1, 2, 1_234_567_890))
                .addRoundTrip(arrayDataType(smallintDataType()), asList((short) 32_456))
                .addRoundTrip(arrayDataType(doubleDataType()), asList(123.45d))
                .addRoundTrip(arrayDataType(realDataType()), asList(123.45f))
                .execute(getQueryRunner(), sessionWithArrayAsArray(), prestoCreateAsSelect(sessionWithArrayAsArray(), "test_array_basic"));

        arrayDateTest(TestPostgreSqlTypeMapping::arrayDataType)
                .execute(getQueryRunner(), sessionWithArrayAsArray(), prestoCreateAsSelect(sessionWithArrayAsArray(), "test_array_date"));
        arrayDateTest(TestPostgreSqlTypeMapping::postgresArrayDataType)
                .execute(getQueryRunner(), sessionWithArrayAsArray(), postgresCreateAndInsert("tpch.test_array_date"));

        arrayDecimalTest(TestPostgreSqlTypeMapping::arrayDataType)
                .execute(getQueryRunner(), sessionWithArrayAsArray(), prestoCreateAsSelect(sessionWithArrayAsArray(), "test_array_decimal"));
        arrayDecimalTest(TestPostgreSqlTypeMapping::postgresArrayDataType)
                .execute(getQueryRunner(), sessionWithArrayAsArray(), postgresCreateAndInsert("tpch.test_array_decimal"));

        arrayVarcharDataTypeTest(TestPostgreSqlTypeMapping::arrayDataType)
                .execute(getQueryRunner(), sessionWithArrayAsArray(), prestoCreateAsSelect(sessionWithArrayAsArray(), "test_array_varchar"));
        arrayVarcharDataTypeTest(TestPostgreSqlTypeMapping::postgresArrayDataType)
                .execute(getQueryRunner(), sessionWithArrayAsArray(), postgresCreateAndInsert("tpch.test_array_varchar"));

        arrayUnicodeDataTypeTest(TestPostgreSqlTypeMapping::arrayDataType, DataType::charDataType)
                .execute(getQueryRunner(), sessionWithArrayAsArray(), prestoCreateAsSelect(sessionWithArrayAsArray(), "test_array_parameterized_char_unicode"));
        arrayUnicodeDataTypeTest(TestPostgreSqlTypeMapping::postgresArrayDataType, DataType::charDataType)
                .execute(getQueryRunner(), sessionWithArrayAsArray(), postgresCreateAndInsert("tpch.test_array_parameterized_char_unicode"));
        arrayVarcharUnicodeDataTypeTest(TestPostgreSqlTypeMapping::arrayDataType)
                .execute(getQueryRunner(), sessionWithArrayAsArray(), prestoCreateAsSelect(sessionWithArrayAsArray(), "test_array_parameterized_varchar_unicode"));
        arrayVarcharUnicodeDataTypeTest(TestPostgreSqlTypeMapping::postgresArrayDataType)
                .execute(getQueryRunner(), sessionWithArrayAsArray(), postgresCreateAndInsert("tpch.test_array_parameterized_varchar_unicode"));
    }

    @Test
    public void testInternalArray()
    {
        DataTypeTest.create()
                .addRoundTrip(arrayDataType(integerDataType(), "_int4"), asList(1, 2, 3))
                .addRoundTrip(arrayDataType(varcharDataType(), "_text"), asList("a", "b"))
                .execute(getQueryRunner(), sessionWithArrayAsArray(), postgresCreateAndInsert("tpch.test_array_with_native_name"));
    }

    @Test
    public void testArrayEmptyOrNulls()
    {
        DataTypeTest.create()
                .addRoundTrip(arrayDataType(bigintDataType()), asList())
                .addRoundTrip(arrayDataType(booleanDataType()), null)
                .addRoundTrip(arrayDataType(realDataType()), singletonList(null))
                .addRoundTrip(arrayDataType(integerDataType()), asList(1, null, 3, null))
                .addRoundTrip(arrayDataType(timestampDataType()), asList())
                .addRoundTrip(arrayDataType(timestampDataType()), singletonList(null))
                .addRoundTrip(arrayDataType(prestoTimestampWithTimeZoneDataType()), asList())
                .addRoundTrip(arrayDataType(prestoTimestampWithTimeZoneDataType()), singletonList(null))
                .execute(getQueryRunner(), sessionWithArrayAsArray(), prestoCreateAsSelect(sessionWithArrayAsArray(), "test_array_empty_or_nulls"));
    }

    private DataTypeTest arrayDecimalTest(Function<DataType<BigDecimal>, DataType<List<BigDecimal>>> arrayTypeFactory)
    {
        return DataTypeTest.create()
                .addRoundTrip(arrayTypeFactory.apply(decimalDataType(3, 0)), asList(new BigDecimal("193"), new BigDecimal("19"), new BigDecimal("-193")))
                .addRoundTrip(arrayTypeFactory.apply(decimalDataType(3, 1)), asList(new BigDecimal("10.0"), new BigDecimal("10.1"), new BigDecimal("-10.1")))
                .addRoundTrip(arrayTypeFactory.apply(decimalDataType(4, 2)), asList(new BigDecimal("2"), new BigDecimal("2.3")))
                .addRoundTrip(arrayTypeFactory.apply(decimalDataType(24, 2)), asList(new BigDecimal("2"), new BigDecimal("2.3"), new BigDecimal("123456789.3")))
                .addRoundTrip(arrayTypeFactory.apply(decimalDataType(24, 4)), asList(new BigDecimal("12345678901234567890.31")))
                .addRoundTrip(arrayTypeFactory.apply(decimalDataType(30, 5)), asList(new BigDecimal("3141592653589793238462643.38327"), new BigDecimal("-3141592653589793238462643.38327")))
                .addRoundTrip(arrayTypeFactory.apply(decimalDataType(38, 0)), asList(
                        new BigDecimal("27182818284590452353602874713526624977"),
                        new BigDecimal("-27182818284590452353602874713526624977")));
    }

    private DataTypeTest arrayVarcharDataTypeTest(Function<DataType<String>, DataType<List<String>>> arrayTypeFactory)
    {
        return DataTypeTest.create()
                .addRoundTrip(arrayTypeFactory.apply(varcharDataType(10)), asList("text_a"))
                .addRoundTrip(arrayTypeFactory.apply(varcharDataType(255)), asList("text_b"))
                .addRoundTrip(arrayTypeFactory.apply(varcharDataType(65535)), asList("text_d"))
                .addRoundTrip(arrayTypeFactory.apply(varcharDataType(10485760)), asList("text_f"))
                .addRoundTrip(arrayTypeFactory.apply(varcharDataType()), asList("unbounded"));
    }

    private DataTypeTest arrayVarcharUnicodeDataTypeTest(Function<DataType<String>, DataType<List<String>>> arrayTypeFactory)
    {
        return arrayUnicodeDataTypeTest(arrayTypeFactory, DataType::varcharDataType)
                .addRoundTrip(arrayTypeFactory.apply(varcharDataType()), asList("\u041d\u0443, \u043f\u043e\u0433\u043e\u0434\u0438!"));
    }

    private DataTypeTest arrayUnicodeDataTypeTest(Function<DataType<String>, DataType<List<String>>> arrayTypeFactory, Function<Integer, DataType<String>> dataTypeFactory)
    {
        String sampleUnicodeText = "\u653b\u6bbb\u6a5f\u52d5\u968a";
        String sampleFourByteUnicodeCharacter = "\uD83D\uDE02";

        return DataTypeTest.create()
                .addRoundTrip(arrayTypeFactory.apply(dataTypeFactory.apply(sampleUnicodeText.length())), asList(sampleUnicodeText))
                .addRoundTrip(arrayTypeFactory.apply(dataTypeFactory.apply(32)), asList(sampleUnicodeText))
                .addRoundTrip(arrayTypeFactory.apply(dataTypeFactory.apply(20000)), asList(sampleUnicodeText))
                .addRoundTrip(arrayTypeFactory.apply(dataTypeFactory.apply(1)), asList(sampleFourByteUnicodeCharacter));
    }

    private DataTypeTest arrayDateTest(Function<DataType<LocalDate>, DataType<List<LocalDate>>> arrayTypeFactory)
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

        return DataTypeTest.create()
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(LocalDate.of(1952, 4, 3))) // before epoch
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(LocalDate.of(1970, 1, 1)))
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(LocalDate.of(1970, 2, 3)))
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(LocalDate.of(2017, 7, 1))) // summer on northern hemisphere (possible DST)
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(LocalDate.of(2017, 1, 1))) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(dateOfLocalTimeChangeForwardAtMidnightInJvmZone))
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(dateOfLocalTimeChangeForwardAtMidnightInSomeZone))
                .addRoundTrip(arrayTypeFactory.apply(dateDataType()), asList(dateOfLocalTimeChangeBackwardAtMidnightInSomeZone));
    }

    @Test
    public void testArrayMultidimensional()
    {
        // for multidimensional arrays, PostgreSQL requires subarrays to have the same dimensions, including nulls
        // e.g. [[1], [1, 2]] and [null, [1, 2]] are not allowed, but [[null, null], [1, 2]] is allowed
        DataTypeTest.create()
                .addRoundTrip(arrayDataType(arrayDataType(booleanDataType())), asList(asList(null, null, null)))
                .addRoundTrip(arrayDataType(arrayDataType(booleanDataType())), asList(asList(true, null), asList(null, null), asList(false, false)))
                .addRoundTrip(arrayDataType(arrayDataType(integerDataType())), asList(asList(1, 2), asList(null, null), asList(3, 4)))
                .addRoundTrip(arrayDataType(arrayDataType(decimalDataType(3, 0))), asList(
                        asList(new BigDecimal("193")),
                        asList(new BigDecimal("19")),
                        asList(new BigDecimal("-193"))))
                .execute(getQueryRunner(), sessionWithArrayAsArray(), prestoCreateAsSelect(sessionWithArrayAsArray(), "test_array_2d"));

        DataTypeTest.create()
                .addRoundTrip(arrayDataType(arrayDataType(arrayDataType(doubleDataType()))), asList(
                        asList(asList(123.45d), asList(678.99d)),
                        asList(asList(543.21d), asList(998.76d)),
                        asList(asList(567.123d), asList(789.12d))))
                .addRoundTrip(arrayDataType(arrayDataType(arrayDataType(dateDataType()))), asList(
                        asList(asList(LocalDate.of(1952, 4, 3), LocalDate.of(1970, 1, 1))),
                        asList(asList(null, LocalDate.of(1970, 1, 1))),
                        asList(asList(LocalDate.of(1970, 2, 3), LocalDate.of(2017, 7, 1)))))
                .execute(getQueryRunner(), sessionWithArrayAsArray(), prestoCreateAsSelect(sessionWithArrayAsArray(), "test_array_3d"));
    }

    @Test
    public void testArrayAsJson()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty("postgresql.array_mapping", AS_JSON.name())
                .build();

        DataTypeTest.create()
                .addRoundTrip(arrayAsJsonDataType("boolean[]"), null)
                .addRoundTrip(arrayAsJsonDataType("boolean[]"), "[[true,false],[false,true],[true,true]]")
                .addRoundTrip(arrayAsJsonDataType("boolean[3][2]"), "[[true,false],[false,true],[true,true]]")
                .addRoundTrip(arrayAsJsonDataType("boolean[100][100][100]"), "[true]")
                .addRoundTrip(arrayAsJsonDataType("_bool"), "[[true,false],[null,null]]")
                .addRoundTrip(arrayAsJsonDataType("_bool"), "[[[null]]]")
                .addRoundTrip(arrayAsJsonDataType("_bool"), "[]")
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_boolean_array_as_json"));

        DataTypeTest.create()
                .addRoundTrip(arrayAsJsonDataType("integer[]"), null)
                .addRoundTrip(arrayAsJsonDataType("integer[]"), "[[[1,2,3],[4,5,6]],[[7,8,9],[10,11,12]]]")
                .addRoundTrip(arrayAsJsonDataType("integer[100][100][100]"), "[0]")
                .addRoundTrip(arrayAsJsonDataType("integer[]"), "[[[null,null]]]")
                .addRoundTrip(arrayAsJsonDataType("integer[]"), "[]")
                .addRoundTrip(arrayAsJsonDataType("_int4"), "[]")
                .addRoundTrip(arrayAsJsonDataType("_int4"), "[[0],[1],[2],[3]]")
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_integer_array_as_json"));

        DataTypeTest.create()
                .addRoundTrip(arrayAsJsonDataType("double precision[]"), null)
                .addRoundTrip(arrayAsJsonDataType("double precision[]"), "[[[1.1,2.2,3.3],[4.4,5.5,6.6]]]")
                .addRoundTrip(arrayAsJsonDataType("double precision[100][100][100]"), "[42.3]")
                .addRoundTrip(arrayAsJsonDataType("double precision[]"), "[[[null,null]]]")
                .addRoundTrip(arrayAsJsonDataType("double precision[]"), "[]")
                .addRoundTrip(arrayAsJsonDataType("_float8"), "[]")
                .addRoundTrip(arrayAsJsonDataType("_float8"), "[[1.1],[2.2]]")
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_double_array_as_json"));

        DataTypeTest.create()
                .addRoundTrip(arrayAsJsonDataType("real[]"), null)
                .addRoundTrip(arrayAsJsonDataType("real[]"), "[[[1.1,2.2,3.3],[4.4,5.5,6.6]]]")
                .addRoundTrip(arrayAsJsonDataType("real[100][100][100]"), "[42.3]")
                .addRoundTrip(arrayAsJsonDataType("real[]"), "[[[null,null]]]")
                .addRoundTrip(arrayAsJsonDataType("real[]"), "[]")
                .addRoundTrip(arrayAsJsonDataType("_float4"), "[]")
                .addRoundTrip(arrayAsJsonDataType("_float4"), "[[1.1],[2.2]]")
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_real_array_as_json"));

        DataTypeTest.create()
                .addRoundTrip(arrayAsJsonDataType("varchar[]"), null)
                .addRoundTrip(arrayAsJsonDataType("varchar[]"), "[\"text\"]")
                .addRoundTrip(arrayAsJsonDataType("_text"), "[[\"one\",\"two\"],[\"three\",\"four\"]]")
                .addRoundTrip(arrayAsJsonDataType("_text"), "[[\"one\",null]]")
                .addRoundTrip(arrayAsJsonDataType("_text"), "[]")
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_varchar_array_as_json"));

        DataTypeTest.create()
                .addRoundTrip(arrayAsJsonDataType("date[]"), null)
                .addRoundTrip(arrayAsJsonDataType("date[]"), "[\"2019-01-02\"]")
                .addRoundTrip(arrayAsJsonDataType("date[]"), "[null,null]")
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_timestamp_array_as_json"));

        DataTypeTest.create()
                .addRoundTrip(arrayAsJsonDataType("timestamp[]"), null)
                .addRoundTrip(arrayAsJsonDataType("timestamp[]"), "[\"2019-01-02 03:04:05.789\"]")
                .addRoundTrip(arrayAsJsonDataType("timestamp[]"), "[null,null]")
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_timestamp_array_as_json"));

        DataTypeTest.create()
                .addRoundTrip(arrayAsJsonDataType("hstore[]"), null)
                .addRoundTrip(arrayAsJsonDataType("hstore[]"), "[]")
                .addRoundTrip(arrayAsJsonDataType("hstore[]"), "[null,null]")
                .addRoundTrip(hstoreArrayAsJsonDataType(), "[{\"a\":\"1\",\"b\":\"2\"},{\"a\":\"3\",\"d\":\"4\"}]")
                .addRoundTrip(hstoreArrayAsJsonDataType(), "[{\"a\":null,\"b\":\"2\"}]")
                .execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_hstore_array_as_json"));
    }

    private static <E> DataType<List<E>> arrayDataType(DataType<E> elementType)
    {
        return arrayDataType(elementType, format("ARRAY(%s)", elementType.getInsertType()));
    }

    private static <E> DataType<List<E>> postgresArrayDataType(DataType<E> elementType)
    {
        return arrayDataType(elementType, elementType.getInsertType() + "[]");
    }

    private static <E> DataType<List<E>> arrayDataType(DataType<E> elementType, String insertType)
    {
        return dataType(
                insertType,
                new ArrayType(elementType.getPrestoResultType()),
                valuesList -> "ARRAY" + valuesList.stream().map(elementType::toLiteral).collect(toList()),
                valuesList -> valuesList == null ? null : valuesList.stream().map(elementType::toPrestoQueryResult).collect(toList()));
    }

    private static DataType<String> arrayAsJsonDataType(String insertType)
    {
        return dataType(
                insertType,
                JSON,
                // naive conversion JSON array -> array literal, sufficient for tests
                value -> value
                        .replace("[", "ARRAY[")
                        .replace("\"", "'")
                        + "::" + insertType,
                identity());
    }

    private static DataType<String> hstoreArrayAsJsonDataType()
    {
        return dataType(
                "hstore[]",
                JSON,
                json -> HSTORE_CODEC.fromJson(json).stream()
                        .map(TestPostgreSqlTypeMapping::hstoreLiteral)
                        .collect(joining(",", "ARRAY[", "]")),
                identity());
    }

    @Test
    public void testDate()
    {
        // Note: there is identical test for MySQL

        ZoneId jvmZone = ZoneId.systemDefault();
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDate.of(1970, 1, 1);
        checkIsGap(jvmZone, dateOfLocalTimeChangeForwardAtMidnightInJvmZone.atStartOfDay());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        checkIsGap(someZone, dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        checkIsDoubled(someZone, dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1));

        DataTypeTest testCases = DataTypeTest.create()
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
            testCases.execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_date"));
            testCases.execute(getQueryRunner(), session, prestoCreateAsSelect("test_date"));
        }
    }

    @Test
    public void testEnum()
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl());
        jdbcSqlExecutor.execute("CREATE TYPE enum_t AS ENUM ('a','b','c')");
        jdbcSqlExecutor.execute("CREATE TABLE tpch.test_enum(id int, enum_column enum_t)");
        jdbcSqlExecutor.execute("INSERT INTO tpch.test_enum(id,enum_column) values (1,'a'::enum_t),(2,'b'::enum_t)");
        try {
            assertQuery(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'test_enum'",
                    "VALUES ('id','integer'),('enum_column','varchar')");
            assertQuery("SELECT * FROM tpch.test_enum", "VALUES (1,'a'),(2,'b')");
            assertQuery("SELECT * FROM tpch.test_enum WHERE enum_column='a'", "VALUES (1,'a')");
        }
        finally {
            jdbcSqlExecutor.execute("DROP TABLE tpch.test_enum");
            jdbcSqlExecutor.execute("DROP TYPE enum_t");
        }
    }

    @Test(dataProvider = "testTimestampDataProvider")
    public void testTime(boolean legacyTimestamp, boolean insertWithPresto, ZoneId sessionZone)
    {
        LocalTime timeGapInJvmZone = LocalTime.of(0, 12, 34, 567_000_000);
        checkIsGap(jvmZone, timeGapInJvmZone.atDate(EPOCH_DAY));

        DataTypeTest tests = DataTypeTest.create()
                .addRoundTrip(timeDataType(), LocalTime.of(1, 12, 34, 0))
                .addRoundTrip(timeDataType(), LocalTime.of(2, 12, 34, 0))
                .addRoundTrip(timeDataType(), LocalTime.of(2, 12, 34, 1_000_000))
                .addRoundTrip(timeDataType(), LocalTime.of(3, 12, 34, 0))
                .addRoundTrip(timeDataType(), LocalTime.of(4, 12, 34, 0))
                .addRoundTrip(timeDataType(), LocalTime.of(5, 12, 34, 0))
                .addRoundTrip(timeDataType(), LocalTime.of(6, 12, 34, 0))
                .addRoundTrip(timeDataType(), LocalTime.of(9, 12, 34, 0))
                .addRoundTrip(timeDataType(), LocalTime.of(10, 12, 34, 0))
                .addRoundTrip(timeDataType(), LocalTime.of(15, 12, 34, 567_000_000))
                .addRoundTrip(timeDataType(), LocalTime.of(23, 59, 59, 999_000_000));

        addTimeTestIfSupported(tests, legacyTimestamp, sessionZone, epoch.toLocalTime()); // epoch is also a gap in JVM zone
        addTimeTestIfSupported(tests, legacyTimestamp, sessionZone, timeGapInJvmZone);

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .setSystemProperty("legacy_timestamp", Boolean.toString(legacyTimestamp))
                .build();

        if (insertWithPresto) {
            tests.execute(getQueryRunner(), session, prestoCreateAsSelect(session, "test_time"));
        }
        else {
            tests.execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_time"));
        }
    }

    private void addTimeTestIfSupported(DataTypeTest tests, boolean legacyTimestamp, ZoneId sessionZone, LocalTime time)
    {
        if (legacyTimestamp && isGap(sessionZone, time.atDate(EPOCH_DAY))) {
            // in legacy timestamp semantics we cannot represent this time
            return;
        }

        tests.addRoundTrip(timeDataType(), time);
    }

    @Test(dataProvider = "testTimestampDataProvider")
    public void testTimestamp(boolean legacyTimestamp, boolean insertWithPresto, ZoneId sessionZone)
    {
        // using two non-JVM zones so that we don't need to worry what Postgres system zone is
        DataTypeTest tests = DataTypeTest.create()
                .addRoundTrip(timestampDataType(), beforeEpoch)
                .addRoundTrip(timestampDataType(), afterEpoch)
                .addRoundTrip(timestampDataType(), timeDoubledInJvmZone)
                .addRoundTrip(timestampDataType(), timeDoubledInVilnius);

        addTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, epoch); // epoch also is a gap in JVM zone
        addTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, timeGapInJvmZone1);
        addTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, timeGapInJvmZone2);
        addTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, timeGapInVilnius);
        addTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, timeGapInKathmandu);

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .setSystemProperty("legacy_timestamp", Boolean.toString(legacyTimestamp))
                .build();

        if (insertWithPresto) {
            tests.execute(getQueryRunner(), session, prestoCreateAsSelect(session, "test_timestamp"));
        }
        else {
            tests.execute(getQueryRunner(), session, postgresCreateAndInsert("tpch.test_timestamp"));
        }
    }

    @Test(dataProvider = "testTimestampDataProvider")
    public void testArrayTimestamp(boolean legacyTimestamp, boolean insertWithPresto, ZoneId sessionZone)
    {
        DataType<List<LocalDateTime>> dataType;
        DataSetup dataSetup;
        if (insertWithPresto) {
            dataType = arrayDataType(timestampDataType());
            dataSetup = prestoCreateAsSelect(sessionWithArrayAsArray(), "test_array_timestamp");
        }
        else {
            dataType = arrayDataType(timestampDataType(), "timestamp[]");
            dataSetup = postgresCreateAndInsert("tpch.test_array_timestamp");
        }
        DataTypeTest tests = DataTypeTest.create()
                .addRoundTrip(dataType, asList(beforeEpoch))
                .addRoundTrip(dataType, asList(afterEpoch))
                .addRoundTrip(dataType, asList(timeDoubledInJvmZone))
                .addRoundTrip(dataType, asList(timeDoubledInVilnius));

        addArrayTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, dataType, epoch);
        addArrayTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, dataType, timeGapInJvmZone1);
        addArrayTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, dataType, timeGapInJvmZone2);
        addArrayTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, dataType, timeGapInVilnius);
        addArrayTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, dataType, timeGapInKathmandu);

        Session session = Session.builder(sessionWithArrayAsArray())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .setSystemProperty("legacy_timestamp", Boolean.toString(legacyTimestamp))
                .build();

        tests.execute(getQueryRunner(), session, dataSetup);
    }

    private void addTimestampTestIfSupported(DataTypeTest tests, boolean legacyTimestamp, ZoneId sessionZone, LocalDateTime dateTime)
    {
        if (legacyTimestamp && isGap(sessionZone, dateTime)) {
            // in legacy timestamp semantics we cannot represent this dateTime
            return;
        }

        tests.addRoundTrip(timestampDataType(), dateTime);
    }

    private void addArrayTimestampTestIfSupported(DataTypeTest tests, boolean legacyTimestamp, ZoneId sessionZone, DataType<List<LocalDateTime>> dataType, LocalDateTime dateTime)
    {
        if (legacyTimestamp && isGap(sessionZone, dateTime)) {
            // in legacy timestamp semantics we cannot represent this dateTime
            return;
        }

        tests.addRoundTrip(dataType, asList(dateTime));
    }

    @Test(dataProvider = "testTimestampWithTimeZoneDataProvider")
    public void testArrayTimestampWithTimeZone(boolean insertWithPresto)
    {
        DataType<List<ZonedDateTime>> dataType;
        DataSetup dataSetup;
        if (insertWithPresto) {
            dataType = arrayDataType(prestoTimestampWithTimeZoneDataType());
            dataSetup = prestoCreateAsSelect(sessionWithArrayAsArray(), "test_array_timestamp_with_time_zone");
        }
        else {
            dataType = arrayDataType(postgreSqlTimestampWithTimeZoneDataType(), "timestamptz[]");
            dataSetup = postgresCreateAndInsert("tpch.test_array_timestamp_with_time_zone");
        }

        DataTypeTest tests = DataTypeTest.create()
                .addRoundTrip(dataType, asList(epoch.atZone(UTC), epoch.atZone(kathmandu)))
                .addRoundTrip(dataType, asList(beforeEpoch.atZone(kathmandu), beforeEpoch.atZone(UTC)))
                .addRoundTrip(dataType, asList(afterEpoch.atZone(UTC), afterEpoch.atZone(kathmandu)))
                .addRoundTrip(dataType, asList(timeDoubledInJvmZone.atZone(UTC)))
                .addRoundTrip(dataType, asList(timeDoubledInJvmZone.atZone(kathmandu)))
                .addRoundTrip(dataType, asList(timeDoubledInVilnius.atZone(UTC), timeDoubledInVilnius.atZone(vilnius), timeDoubledInVilnius.atZone(kathmandu)))
                .addRoundTrip(dataType, asList(timeGapInJvmZone1.atZone(UTC), timeGapInJvmZone1.atZone(kathmandu)))
                .addRoundTrip(dataType, asList(timeGapInJvmZone2.atZone(UTC), timeGapInJvmZone2.atZone(kathmandu)))
                .addRoundTrip(dataType, asList(timeGapInVilnius.atZone(kathmandu)))
                .addRoundTrip(dataType, asList(timeGapInKathmandu.atZone(vilnius)));
        if (!insertWithPresto) {
            // Postgres results with non-DST time (winter time) for timeDoubledInJvmZone.atZone(jvmZone) while Java results with DST time
            // When writing timestamptz arrays, Postgres JDBC driver converts java.sql.Timestamp to string representing date-time in JVM zone
            // TODO upgrade driver or find a different way to write timestamptz array elements as a point in time values with org.postgresql.jdbc.PgArray (https://github.com/pgjdbc/pgjdbc/issues/1225#issuecomment-516312324)
            tests.addRoundTrip(dataType, asList(timeDoubledInJvmZone.atZone(jvmZone)));
        }
        tests.execute(getQueryRunner(), sessionWithArrayAsArray(), dataSetup);
    }

    @DataProvider
    public Object[][] testTimestampDataProvider()
    {
        return new Object[][] {
                {true, true, ZoneOffset.UTC},
                {false, true, ZoneOffset.UTC},
                {true, false, ZoneOffset.UTC},
                {false, false, ZoneOffset.UTC},

                {true, true, jvmZone},
                {false, true, jvmZone},
                {true, false, jvmZone},
                {false, false, jvmZone},

                // using two non-JVM zones so that we don't need to worry what Postgres system zone is
                {true, true, vilnius},
                {false, true, vilnius},
                {true, false, vilnius},
                {false, false, vilnius},

                {true, true, kathmandu},
                {false, true, kathmandu},
                {true, false, kathmandu},
                {false, false, kathmandu},

                {true, true, ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
                {false, true, ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
                {true, false, ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
                {false, false, ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId())},
        };
    }

    @Test(dataProvider = "testTimestampWithTimeZoneDataProvider")
    public void testTimestampWithTimeZone(boolean insertWithPresto)
    {
        DataType<ZonedDateTime> dataType;
        DataSetup dataSetup;
        if (insertWithPresto) {
            dataType = prestoTimestampWithTimeZoneDataType();
            dataSetup = prestoCreateAsSelect("test_timestamp_with_time_zone");
        }
        else {
            dataType = postgreSqlTimestampWithTimeZoneDataType();
            dataSetup = postgresCreateAndInsert("tpch.test_timestamp_with_time_zone");
        }

        DataTypeTest tests = DataTypeTest.create()
                .addRoundTrip(dataType, epoch.atZone(UTC))
                .addRoundTrip(dataType, epoch.atZone(kathmandu))
                .addRoundTrip(dataType, beforeEpoch.atZone(UTC))
                .addRoundTrip(dataType, beforeEpoch.atZone(kathmandu))
                .addRoundTrip(dataType, afterEpoch.atZone(UTC))
                .addRoundTrip(dataType, afterEpoch.atZone(kathmandu))
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

    @Test
    public void testJson()
    {
        jsonTestCases(jsonDataType())
                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_json"));
        jsonTestCases(jsonDataType())
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_json"));
    }

    @Test
    public void testJsonb()
    {
        jsonTestCases(jsonbDataType())
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_jsonb"));
    }

    private DataTypeTest jsonTestCases(DataType<String> jsonDataType)
    {
        return DataTypeTest.create()
                .addRoundTrip(jsonDataType, "{}")
                .addRoundTrip(jsonDataType, null)
                .addRoundTrip(jsonDataType, "null")
                .addRoundTrip(jsonDataType, "123.4")
                .addRoundTrip(jsonDataType, "\"abc\"")
                .addRoundTrip(jsonDataType, "\"text with \\\" quotations and ' apostrophes\"")
                .addRoundTrip(jsonDataType, "\"\"")
                .addRoundTrip(jsonDataType, "{\"a\":1,\"b\":2}")
                .addRoundTrip(jsonDataType, "{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}")
                .addRoundTrip(jsonDataType, "[]");
    }

    @Test
    public void testHstore()
    {
        hstoreTestCases(hstoreDataType())
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_hstore"));

        hstoreTestCases(varcharMapDataType())
                .execute(getQueryRunner(), postgresCreatePrestoInsert("tpch.postgresql_test_hstore"));
    }

    private DataTypeTest hstoreTestCases(DataType<Map<String, String>> varcharMapDataType)
    {
        return DataTypeTest.create()
                .addRoundTrip(varcharMapDataType, null)
                .addRoundTrip(varcharMapDataType, ImmutableMap.of())
                .addRoundTrip(varcharMapDataType, ImmutableMap.of("key1", "value1"))
                .addRoundTrip(varcharMapDataType, ImmutableMap.of("key1", "value1", "key2", "value2", "key3", "value3"))
                .addRoundTrip(varcharMapDataType, ImmutableMap.of("key1", " \" ", "key2", " ' ", "key3", " ]) "))
                .addRoundTrip(varcharMapDataType, Collections.singletonMap("key1", null));
    }

    @Test
    public void testUuid()
    {
        uuidTestCases(uuidDataType())
                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_uuid"));
    }

    private DataTypeTest uuidTestCases(DataType<java.util.UUID> uuidDataType)
    {
        return DataTypeTest.create()
                .addRoundTrip(uuidDataType, java.util.UUID.fromString("00000000-0000-0000-0000-000000000000"))
                .addRoundTrip(uuidDataType, java.util.UUID.fromString("123e4567-e89b-12d3-a456-426655440000"));
    }

    @Test
    public void testMoney()
    {
        DataTypeTest.create()
                .addRoundTrip(moneyDataType(), null)
                .addRoundTrip(moneyDataType(), 10.)
                .addRoundTrip(moneyDataType(), 10.54)
                .addRoundTrip(moneyDataType(), 10_000_000.42)
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.presto_test_money"));
    }

    private void testUnsupportedDataTypeAsIgnored(String dataTypeName, String databaseValue)
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl());
        try (TestTable table = new TestTable(
                jdbcSqlExecutor,
                "tpch.unsupported_type",
                format("(key varchar(5), unsupported_column %s)", dataTypeName),
                ImmutableList.of(
                        "'1', NULL",
                        "'2', " + databaseValue))) {
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 1, 2");
            assertQuery(
                    "DESC " + table.getName(),
                    "VALUES ('key', 'varchar(5)','', '')"); // no 'unsupported_column'

            assertUpdate(format("INSERT INTO %s VALUES '3'", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES '1', '2', '3'");
        }
    }

    private void testUnsupportedDataTypeConvertedToVarchar(String dataTypeName, String databaseValue, String prestoValue)
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl());
        try (TestTable table = new TestTable(
                jdbcSqlExecutor,
                "tpch.unsupported_type",
                format("(key varchar(5), unsupported_column %s)", dataTypeName),
                ImmutableList.of(
                        "1, NULL",
                        "2, " + databaseValue))) {
            Session convertToVarchar = withUnsupportedType(CONVERT_TO_VARCHAR);
            assertQuery(
                    convertToVarchar,
                    "SELECT * FROM " + table.getName(),
                    format("VALUES ('1', NULL), ('2', %s)", prestoValue));
            assertQuery(
                    convertToVarchar,
                    format("SELECT key FROM %s WHERE unsupported_column = %s", table.getName(), prestoValue),
                    "VALUES '2'");
            assertQuery(
                    convertToVarchar,
                    "DESC " + table.getName(),
                    "VALUES " +
                            "('key', 'varchar(5)', '', ''), " +
                            "('unsupported_column', 'varchar', '', '')");
            assertQueryFails(
                    convertToVarchar,
                    format("INSERT INTO %s (key, unsupported_column) VALUES (3, NULL)", table.getName()),
                    "Insert query has mismatched column types: Table: \\[varchar\\(5\\), varchar\\], Query: \\[integer, unknown\\]");
            assertQueryFails(
                    convertToVarchar,
                    format("INSERT INTO %s (key, unsupported_column) VALUES (4, %s)", table.getName(), prestoValue),
                    "Insert query has mismatched column types: Table: \\[varchar\\(5\\), varchar\\], Query: \\[integer, varchar\\(50\\)\\]");
            assertUpdate(
                    convertToVarchar,
                    format("INSERT INTO %s (key) VALUES '5'", table.getName()),
                    1);
            assertQuery(
                    convertToVarchar,
                    "SELECT * FROM " + table.getName(),
                    format("VALUES ('1', NULL), ('2', %s), ('5', NULL)", prestoValue));
        }
    }

    private Session withUnsupportedType(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("postgresql", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }

    public static DataType<ZonedDateTime> prestoTimestampWithTimeZoneDataType()
    {
        return dataType(
                "timestamp with time zone",
                TIMESTAMP_WITH_TIME_ZONE,
                DateTimeFormatter.ofPattern("'TIMESTAMP '''yyyy-MM-dd HH:mm:ss.SSS VV''")::format,
                // PostgreSQL does not store zone, only the point in time
                zonedDateTime -> zonedDateTime.withZoneSameInstant(ZoneId.of("UTC")));
    }

    public static DataType<ZonedDateTime> postgreSqlTimestampWithTimeZoneDataType()
    {
        return dataType(
                "timestamp with time zone",
                TIMESTAMP_WITH_TIME_ZONE,
                // PostgreSQL never examines the content of a literal string before determining its type, so `TIMESTAMP '.... {zone}'` won't work.
                // PostgreSQL does not store zone, only the point in time
                zonedDateTime -> DateTimeFormatter.ofPattern("'TIMESTAMP WITH TIME ZONE '''yyyy-MM-dd HH:mm:ss.SSS VV''").format(zonedDateTime.withZoneSameInstant(UTC)),
                zonedDateTime -> zonedDateTime.withZoneSameInstant(ZoneId.of("UTC")));
    }

    public static DataType<String> jsonbDataType()
    {
        return dataType(
                "jsonb",
                JSON,
                value -> "JSON " + formatStringLiteral(value),
                identity());
    }

    private DataType<Map<String, String>> hstoreDataType()
    {
        return dataType(
                "hstore",
                getQueryRunner().getMetadata().getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature())),
                TestPostgreSqlTypeMapping::hstoreLiteral,
                identity());
    }

    private static String hstoreLiteral(Map<String, String> value)
    {
        return value.entrySet().stream()
                .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
                .map(input -> (input == null) ? "null" : formatStringLiteral(input))
                .collect(joining(",", "hstore(ARRAY[", "]::varchar[])"));
    }

    private DataType<Map<String, String>> varcharMapDataType()
    {
        return dataType(
                "hstore",
                getQueryRunner().getMetadata().getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature())),
                value -> {
                    List<String> formatted = value.entrySet().stream()
                            .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
                            .map(string -> {
                                if (string == null) {
                                    return "null";
                                }
                                return DataType.formatStringLiteral(string);
                            })
                            .collect(toImmutableList());
                    ImmutableList.Builder<String> keys = ImmutableList.builder();
                    ImmutableList.Builder<String> values = ImmutableList.builder();
                    for (int i = 0; i < formatted.size(); i = i + 2) {
                        keys.add(formatted.get(i));
                        values.add(formatted.get(i + 1));
                    }
                    return String.format("MAP(ARRAY[%s], ARRAY[%s])", Joiner.on(',').join(keys.build()), Joiner.on(',').join(values.build()));
                },
                identity());
    }

    public static DataType<java.util.UUID> uuidDataType()
    {
        return dataType(
                "uuid",
                UUID,
                value -> "UUID " + formatStringLiteral(value.toString()),
                identity());
    }

    private static DataType<byte[]> byteaDataType()
    {
        return dataType(
                "bytea",
                VARBINARY,
                bytes -> format("bytea E'\\\\x%s'", base16().encode(bytes)),
                identity());
    }

    private static DataType<Double> moneyDataType()
    {
        return dataType(
                "money",
                VARCHAR,
                String::valueOf,
                amount -> {
                    NumberFormat numberFormat = NumberFormat.getCurrencyInstance(Locale.US);
                    return numberFormat.format(amount);
                });
    }

    private Session sessionWithArrayAsArray()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty("postgresql.array_mapping", AS_ARRAY.name())
                .build();
    }

    private Session sessionWithDecimalMappingAllowOverflow(RoundingMode roundingMode, int scale)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty("postgresql", DECIMAL_MAPPING, ALLOW_OVERFLOW.name())
                .setCatalogSessionProperty("postgresql", DECIMAL_ROUNDING_MODE, roundingMode.name())
                .setCatalogSessionProperty("postgresql", DECIMAL_DEFAULT_SCALE, Integer.valueOf(scale).toString())
                .build();
    }

    private Session sessionWithDecimalMappingStrict(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty("postgresql", DECIMAL_MAPPING, STRICT.name())
                .setCatalogSessionProperty("postgresql", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }

    private DataSetup prestoCreateAsSelect(String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner()), tableNamePrefix);
    }

    private DataSetup prestoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup postgresCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl()), tableNamePrefix);
    }

    private DataSetup postgresCreatePrestoInsert(String tableNamePrefix)
    {
        return new CreateAndPrestoInsertDataSetup(new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl()), new PrestoSqlExecutor(getQueryRunner()), tableNamePrefix);
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
