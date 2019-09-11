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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.testing.TestingSession;
import io.prestosql.tests.AbstractTestQueryFramework;
import io.prestosql.tests.datatype.CreateAndInsertDataSetup;
import io.prestosql.tests.datatype.CreateAsSelectDataSetup;
import io.prestosql.tests.datatype.DataSetup;
import io.prestosql.tests.datatype.DataType;
import io.prestosql.tests.datatype.DataTypeTest;
import io.prestosql.tests.sql.JdbcSqlExecutor;
import io.prestosql.tests.sql.PrestoSqlExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.BaseEncoding.base16;
import static io.prestosql.plugin.postgresql.PostgreSqlQueryRunner.createPostgreSqlQueryRunner;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.tests.datatype.DataType.bigintDataType;
import static io.prestosql.tests.datatype.DataType.booleanDataType;
import static io.prestosql.tests.datatype.DataType.dataType;
import static io.prestosql.tests.datatype.DataType.dateDataType;
import static io.prestosql.tests.datatype.DataType.decimalDataType;
import static io.prestosql.tests.datatype.DataType.doubleDataType;
import static io.prestosql.tests.datatype.DataType.formatStringLiteral;
import static io.prestosql.tests.datatype.DataType.integerDataType;
import static io.prestosql.tests.datatype.DataType.jsonDataType;
import static io.prestosql.tests.datatype.DataType.realDataType;
import static io.prestosql.tests.datatype.DataType.smallintDataType;
import static io.prestosql.tests.datatype.DataType.timestampDataType;
import static io.prestosql.tests.datatype.DataType.varbinaryDataType;
import static io.prestosql.tests.datatype.DataType.varcharDataType;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.type.UuidType.UUID;
import static java.lang.String.format;
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
    private final TestingPostgreSqlServer postgreSqlServer;

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

    public TestPostgreSqlTypeMapping()
    {
        this(new TestingPostgreSqlServer());
    }

    private TestPostgreSqlTypeMapping(TestingPostgreSqlServer postgreSqlServer)
    {
        super(() -> createPostgreSqlQueryRunner(
                postgreSqlServer,
                ImmutableMap.of("postgresql.experimental.array-mapping", "AS_ARRAY",
                                "jdbc-types-mapped-to-varchar", "tsrange, inet"),
                ImmutableList.of()));
        this.postgreSqlServer = postgreSqlServer;
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
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'test_forced_varchar_mapping'",
                    "VALUES ('tsrange_col','varchar'),('inet_col','varchar'),('tsrange_arr_col','array(varchar)')"); // no 'unsupported_nonforced_column'

            assertQuery("SELECT * FROM tpch.test_forced_varchar_mapping", "VALUES ('[\"2010-01-01 14:30:00\",\"2010-01-01 15:30:00\")','172.0.0.1',ARRAY['[\"2010-01-01 14:30:00\",\"2010-01-01 15:30:00\")'])");
        }
        finally {
            jdbcSqlExecutor.execute("DROP TABLE tpch.test_forced_varchar_mapping");
        }
    }

    @Test
    public void testDecimalExceedingPrecisionMax()
    {
        testUnsupportedDataType("decimal(50,0)");
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
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_basic"));

        arrayDateTest(TestPostgreSqlTypeMapping::arrayDataType)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_date"));
        arrayDateTest(TestPostgreSqlTypeMapping::postgresArrayDataType)
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.test_array_date"));

        arrayDecimalTest(TestPostgreSqlTypeMapping::arrayDataType)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_decimal"));
        arrayDecimalTest(TestPostgreSqlTypeMapping::postgresArrayDataType)
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.test_array_decimal"));

        arrayVarcharDataTypeTest(TestPostgreSqlTypeMapping::arrayDataType)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_varchar"));
        arrayVarcharDataTypeTest(TestPostgreSqlTypeMapping::postgresArrayDataType)
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.test_array_varchar"));

        arrayUnicodeDataTypeTest(TestPostgreSqlTypeMapping::arrayDataType, DataType::charDataType)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_parameterized_char_unicode"));
        arrayUnicodeDataTypeTest(TestPostgreSqlTypeMapping::postgresArrayDataType, DataType::charDataType)
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.test_array_parameterized_char_unicode"));
        arrayVarcharUnicodeDataTypeTest(TestPostgreSqlTypeMapping::arrayDataType)
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_parameterized_varchar_unicode"));
        arrayVarcharUnicodeDataTypeTest(TestPostgreSqlTypeMapping::postgresArrayDataType)
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.test_array_parameterized_varchar_unicode"));
    }

    @Test
    public void testInternalArray()
    {
        DataTypeTest.create()
                .addRoundTrip(arrayDataType(integerDataType(), "_int4"), asList(1, 2, 3))
                .addRoundTrip(arrayDataType(varcharDataType(), "_text"), asList("a", "b"))
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.test_array_with_native_name"));
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
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_empty_or_nulls"));
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
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_2d"));

        DataTypeTest.create()
                .addRoundTrip(arrayDataType(arrayDataType(arrayDataType(doubleDataType()))), asList(
                        asList(asList(123.45d), asList(678.99d)),
                        asList(asList(543.21d), asList(998.76d)),
                        asList(asList(567.123d), asList(789.12d))))
                .addRoundTrip(arrayDataType(arrayDataType(arrayDataType(dateDataType()))), asList(
                        asList(asList(LocalDate.of(1952, 4, 3), LocalDate.of(1970, 1, 1))),
                        asList(asList(null, LocalDate.of(1970, 1, 1))),
                        asList(asList(LocalDate.of(1970, 2, 3), LocalDate.of(2017, 7, 1)))))
                .execute(getQueryRunner(), prestoCreateAsSelect("test_array_3d"));
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
    public void testTimestamp(boolean legacyTimestamp, boolean insertWithPresto)
    {
        // using two non-JVM zones so that we don't need to worry what Postgres system zone is
        for (ZoneId sessionZone : ImmutableList.of(ZoneOffset.UTC, jvmZone, vilnius, kathmandu, ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId()))) {
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
    }

    @Test(dataProvider = "testTimestampDataProvider")
    public void testArrayTimestamp(boolean legacyTimestamp, boolean insertWithPresto)
    {
        DataType<List<LocalDateTime>> dataType;
        DataSetup dataSetup;
        if (insertWithPresto) {
            dataType = arrayDataType(timestampDataType());
            dataSetup = prestoCreateAsSelect("test_array_timestamp");
        }
        else {
            dataType = arrayDataType(timestampDataType(), "timestamp[]");
            dataSetup = postgresCreateAndInsert("tpch.test_array_timestamp");
        }
        for (ZoneId sessionZone : ImmutableList.of(ZoneOffset.UTC, jvmZone, vilnius, kathmandu, ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId()))) {
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

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .setSystemProperty("legacy_timestamp", Boolean.toString(legacyTimestamp))
                    .build();

            tests.execute(getQueryRunner(), session, dataSetup);
        }
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
            dataSetup = prestoCreateAsSelect("test_array_timestamp_with_time_zone");
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
        tests.execute(getQueryRunner(), dataSetup);
    }

    @DataProvider
    public Object[][] testTimestampDataProvider()
    {
        return new Object[][] {
                {true, true},
                {false, true},
                {true, false},
                {false, false},
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
        DataTypeTest.create()
                .addRoundTrip(hstoreDataType(), null)
                .addRoundTrip(hstoreDataType(), ImmutableMap.of())
                .addRoundTrip(hstoreDataType(), ImmutableMap.of("key1", "value1"))
                .addRoundTrip(hstoreDataType(), ImmutableMap.of("key1", "value1", "key2", "value2", "key3", "value3"))
                .addRoundTrip(hstoreDataType(), ImmutableMap.of("key1", " \" ", "key2", " ' ", "key3", " ]) "))
                .addRoundTrip(hstoreDataType(), Collections.singletonMap("key1", null))
                .execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_hstore"));
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

    private void testUnsupportedDataType(String databaseDataType)
    {
        JdbcSqlExecutor jdbcSqlExecutor = new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl());
        jdbcSqlExecutor.execute(format("CREATE TABLE tpch.test_unsupported_data_type(key varchar(5), unsupported_column %s)", databaseDataType));
        try {
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'test_unsupported_data_type'",
                    "VALUES 'key'"); // no 'unsupported_column'
        }
        finally {
            jdbcSqlExecutor.execute("DROP TABLE tpch.test_unsupported_data_type");
        }
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
                getQueryRunner().getMetadata().getType(parseTypeSignature("map(varchar, varchar)")),
                value -> value.entrySet().stream()
                        .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
                        .map(string -> {
                            if (string == null) {
                                return "null";
                            }
                            return DataType.formatStringLiteral(string);
                        })
                        .collect(joining(",", "hstore(ARRAY[", "]::varchar[])")),
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
