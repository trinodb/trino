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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.datatype.CreateAndInsertDataSetup;
import io.prestosql.testing.datatype.CreateAsSelectDataSetup;
import io.prestosql.testing.datatype.DataSetup;
import io.prestosql.testing.datatype.DataType;
import io.prestosql.testing.datatype.DataTypeTest;
import io.prestosql.testing.sql.PrestoSqlExecutor;
import io.prestosql.testing.sql.SqlExecutor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.testing.datatype.DataType.booleanDataType;
import static io.prestosql.testing.datatype.DataType.dateDataType;
import static io.prestosql.testing.datatype.DataType.stringDataType;
import static io.prestosql.testing.datatype.DataType.timestampDataType;
import static io.prestosql.testing.datatype.DataType.varcharDataType;
import static io.prestosql.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.function.Function.identity;

public abstract class BaseSnowflakeTypeMappingTest
        extends AbstractTestQueryFramework
{
    protected static final int MAX_VARCHAR = 16777216;

    protected final SnowflakeServer server = new SnowflakeServer();

    private LocalDateTime dateTimeBeforeEpoch;
    private LocalDateTime dateTimeEpoch;
    private LocalDateTime dateTimeAfterEpoch;
    private ZoneId jvmZone;
    private LocalDateTime dateTimeGapInJvmZone1;
    private LocalDateTime dateTimeGapInJvmZone2;
    private LocalDateTime dateTimeDoubledInJvmZone;

    // no DST in 1970, but has DST in later years (e.g. 2018)
    private ZoneId vilnius;
    private LocalDateTime dateTimeGapInVilnius;
    private LocalDateTime dateTimeDoubledInVilnius;

    // minutes offset change since 1970-01-01, no DST
    private ZoneId kathmandu;
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

    @Test
    public void booleanMappings()
    {
        testTypeMapping(DataTypeTest.create()
                .addRoundTrip(booleanDataType(), true)
                .addRoundTrip(booleanDataType(), false));
    }

    @Test
    public void floatingPointMappings()
    {
        DataType<Double> dataType = doubleDataType();
        testTypeMapping(DataTypeTest.create()
                .addRoundTrip(dataType, 1.0e100d)
                .addRoundTrip(dataType, Double.NaN)
                .addRoundTrip(dataType, Double.POSITIVE_INFINITY)
                .addRoundTrip(dataType, Double.NEGATIVE_INFINITY)
                .addRoundTrip(dataType, null));
    }

    @Test
    public void snowflakeFloatingPointMappings()
    {
        testTypeReadMapping(
                DataTypeTest.create()
                        .addRoundTrip(dataType("double precision", DoubleType.DOUBLE), 1.0e100d)
                        .addRoundTrip(dataType("double", DoubleType.DOUBLE), 1.0)
                        .addRoundTrip(dataType("real", DoubleType.DOUBLE), 123456.123456)
                        .addRoundTrip(dataType("float", DoubleType.DOUBLE), null)
                        .addRoundTrip(dataType("float8", DoubleType.DOUBLE), 1.0e15d)
                        .addRoundTrip(dataType("float4", DoubleType.DOUBLE), 1.0)
                        .addRoundTrip(dataType("float8", DoubleType.DOUBLE), 1234567890.01234)
                        .addRoundTrip(dataType("real", DoubleType.DOUBLE), null)
                        .addRoundTrip(dataType("double", DoubleType.DOUBLE), 100000.0)
                        .addRoundTrip(dataType("double precision", DoubleType.DOUBLE), 123000.0));
    }

    @Test
    public void varcharMapping()
    {
        testTypeMapping(
                DataTypeTest.create()
                        .addRoundTrip(varcharDataType(10), "string 010")
                        .addRoundTrip(varcharDataType(20), "string 020")
                        .addRoundTrip(varcharDataType(MAX_VARCHAR), "string max size")
                        .addRoundTrip(varcharDataType(5), null)
                        .addRoundTrip(varcharDataType(213), "攻殻機動隊")
                        .addRoundTrip(varcharDataType(42), null));
    }

    @Test
    public void varcharReadMapping()
    {
        testTypeReadMapping(
                DataTypeTest.create()
                        .addRoundTrip(stringDataType("varchar(10)", VarcharType.createVarcharType(10)), "string 010")
                        .addRoundTrip(stringDataType("varchar(20)", VarcharType.createVarcharType(20)), "string 020")
                        .addRoundTrip(stringDataType(format("varchar(%s)", MAX_VARCHAR), VarcharType.createVarcharType(MAX_VARCHAR)), "string max size")
                        .addRoundTrip(stringDataType("character(10)", VarcharType.createVarcharType(10)), null)
                        .addRoundTrip(stringDataType("char(100)", VarcharType.createVarcharType(100)), "攻殻機動隊")
                        .addRoundTrip(stringDataType("text", VarcharType.createVarcharType(MAX_VARCHAR)), "攻殻機動隊")
                        .addRoundTrip(stringDataType("string", VarcharType.createVarcharType(MAX_VARCHAR)), "攻殻機動隊"));
    }

    @Test
    public void charMapping()
    {
        testTypeMapping(
                DataTypeTest.create()
                        .addRoundTrip(stringDataType("char(10)", VarcharType.createVarcharType(10)), "string 010")
                        .addRoundTrip(stringDataType("char(20)", VarcharType.createVarcharType(20)), "string 020          ")
                        .addRoundTrip(stringDataType("char(10)", VarcharType.createVarcharType(10)), null));
    }

    @Test
    public void charReadMapping()
    {
        testTypeReadMapping(
                DataTypeTest.create()
                        .addRoundTrip(stringDataType("char(10)", VarcharType.createVarcharType(10)), "string 010")
                        .addRoundTrip(stringDataType("char(20)", VarcharType.createVarcharType(20)), "string 020")
                        .addRoundTrip(stringDataType("char(5)", VarcharType.createVarcharType(5)), null));
    }

    @Test
    public void decimalMapping()
    {
        testTypeMapping(numericTests((precision, scale) -> decimalDataType("decimal", precision, scale)));
    }

    @Test
    public void decimalReadMapping()
    {
        testTypeReadMapping(numericTests((precision, scale) -> decimalDataType("decimal", precision, scale)));
        testTypeReadMapping(numericTests((precision, scale) -> decimalDataType("numeric", precision, scale)));
        testTypeReadMapping(numericTests((precision, scale) -> decimalDataType("number", precision, scale)));
    }

    @Test
    public void integerMappings()
    {
        testTypeMapping(
                DataTypeTest.create()
                        .addRoundTrip(integerDataType("TINYINT"), new BigDecimal(0))
                        .addRoundTrip(integerDataType("TINYINT"), null)
                        .addRoundTrip(integerDataType("SMALLINT"), new BigDecimal(0))
                        .addRoundTrip(integerDataType("SMALLINT"), new BigDecimal(-32768))
                        .addRoundTrip(integerDataType("SMALLINT"), new BigDecimal(32767))
                        .addRoundTrip(integerDataType("SMALLINT"), null)
                        .addRoundTrip(integerDataType("INTEGER"), new BigDecimal(0))
                        .addRoundTrip(integerDataType("INTEGER"), new BigDecimal(0x80000000))
                        .addRoundTrip(integerDataType("INTEGER"), new BigDecimal(0x7fffffff))
                        .addRoundTrip(integerDataType("INTEGER"), null)
                        .addRoundTrip(integerDataType("BIGINT"), new BigDecimal(0L))
                        .addRoundTrip(integerDataType("BIGINT"), new BigDecimal(0x8000000000000000L + 1))
                        .addRoundTrip(integerDataType("BIGINT"), new BigDecimal(0x7fffffffffffffffL))
                        .addRoundTrip(integerDataType("BIGINT"), null));
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
                .addRoundTrip(decimalType.apply(38, 37), new BigDecimal(".1000020000300004000050000600007000088"))
                .addRoundTrip(decimalType.apply(38, 37), new BigDecimal("-.2718281828459045235360287471352662497"))
                .addRoundTrip(decimalType.apply(10, 3), null);
    }

    @Test
    public void testDateMapping()
    {
        testTypeMapping(dateTests());
    }

    @Test
    public void testDateReadMapping()
    {
        testTypeReadMapping(dateTests());
    }

    private static DataTypeTest dateTests()
    {
        return DataTypeTest.create()
                .addRoundTrip(dateDataType(), null)
                .addRoundTrip(dateDataType(), LocalDate.of(1952, 4, 3))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 1, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 2, 3))
                .addRoundTrip(dateDataType(), LocalDate.of(1983, 4, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(1983, 10, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 1, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 7, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 1, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 1, 1));
    }

    @Test
    public void testVariantReadMapping()
    {
        testTypeReadMapping(DataTypeTest.create()
                .addRoundTrip(variantDataType("hello world"), "'hello world'")
                .addRoundTrip(variantDataType("42"), 42)
                .addRoundTrip(variantDataType("{\"key1\":42,\"key2\":54}"), "OBJECT_CONSTRUCT('key1', 42, 'key2', 54)"));
    }

    @Test
    public void testObjectReadMapping()
    {
        testTypeReadMapping(DataTypeTest.create()
                .addRoundTrip(objectDataType("{\"key1\":42,\"key2\":54}"), ImmutableMap.of("key1", 42, "key2", 54))
                .addRoundTrip(objectDataType("{\"key1\":\"foo\",\"key2\":\"bar\"}"), ImmutableMap.of("key1", "'foo'", "key2", "'bar'")));
    }

    @Test
    public void testArrayReadMapping()
    {
        testTypeReadMapping(DataTypeTest.create()
                .addRoundTrip(arrayDataType("[42,54]"), ImmutableList.of(42, 54))
                .addRoundTrip(arrayDataType("[\"foo\",\"bar\"]"), ImmutableList.of("'foo'", "'bar'")));
    }

    @Test(dataProvider = "testTimestampDataProvider")
    public void testTime(boolean legacyTimestamp, boolean insertWithPresto)
    {
        // using two non-JVM zones so that we don't need to worry what Postgres system zone is
        for (ZoneId sessionZone : ImmutableList.of(UTC, jvmZone, vilnius, kathmandu, ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId()))) {
            DataTypeTest tests = DataTypeTest.create()
                    .addRoundTrip(timeDataType(), LocalTime.of(13, 18, 3, 123_000_000))
                    .addRoundTrip(timeDataType(), LocalTime.of(14, 18, 3, 423_000_000))
                    .addRoundTrip(timeDataType(), LocalTime.of(15, 18, 3, 523_000_000))
                    .addRoundTrip(timeDataType(), LocalTime.of(16, 18, 3, 623_000_000))
                    .addRoundTrip(timeDataType(), LocalTime.of(10, 1, 17, 987_000_000))
                    .addRoundTrip(timeDataType(), LocalTime.of(19, 1, 17, 987_000_000))
                    .addRoundTrip(timeDataType(), LocalTime.of(20, 1, 17, 987_000_000))
                    .addRoundTrip(timeDataType(), LocalTime.of(21, 1, 17, 987_000_000))
                    .addRoundTrip(timeDataType(), LocalTime.of(1, 33, 17, 456_000_000))
                    .addRoundTrip(timeDataType(), LocalTime.of(3, 17, 17))
                    .addRoundTrip(timeDataType(), LocalTime.of(22, 59, 59, 0))
                    .addRoundTrip(timeDataType(), LocalTime.of(22, 59, 59, 999_000_000));

            if (!legacyTimestamp) {
                // 0 hour TIME values are not valid in Bahia_Banderas timezone in legacy timestamp semantics
                tests.addRoundTrip(timeDataType(), LocalTime.of(0, 0, 0))
                        .addRoundTrip(timeDataType(), LocalTime.of(0, 13, 42));
            }

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .setSystemProperty("legacy_timestamp", Boolean.toString(legacyTimestamp))
                    .build();

            if (insertWithPresto) {
                tests.execute(getQueryRunner(), session, prestoCreateAsSelect(session));
            }
            else {
                tests.execute(getQueryRunner(), session, snowflakeCreateAndInsert());
            }
        }
    }

    @Test(dataProvider = "testTimestampArrayDataProvider")
    public void testTimeArray(boolean legacyTimestamp)
    {
        // using two non-JVM zones so that we don't need to worry what Postgres system zone is
        for (ZoneId sessionZone : ImmutableList.of(UTC, jvmZone, vilnius, kathmandu, ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId()))) {
            DataTypeTest tests = DataTypeTest.create()
                    .addRoundTrip(timeArrayDataType("[" +
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
                                    "\"22:59:59.999000000\"]"),
                            ImmutableList.of(
                                    LocalTime.of(13, 18, 3, 123_000_000),
                                    LocalTime.of(14, 18, 3, 423_000_000),
                                    LocalTime.of(15, 18, 3, 523_000_000),
                                    LocalTime.of(16, 18, 3, 623_000_000),
                                    LocalTime.of(10, 1, 17, 987_000_000),
                                    LocalTime.of(19, 1, 17, 987_000_000),
                                    LocalTime.of(20, 1, 17, 987_000_000),
                                    LocalTime.of(21, 1, 17, 987_000_000),
                                    LocalTime.of(1, 33, 17, 456_000_000),
                                    LocalTime.of(3, 17, 17),
                                    LocalTime.of(22, 59, 59, 999_000_000)));

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .setSystemProperty("legacy_timestamp", Boolean.toString(legacyTimestamp))
                    .build();

            tests.execute(getQueryRunner(), session, snowflakeCreateAsSelect());
        }
    }

    @Test(dataProvider = "testTimestampDataProvider")
    public void testTimestamp(boolean legacyTimestamp, boolean insertWithPresto)
    {
        // using two non-JVM zones so that we don't need to worry what Postgres system zone is
        for (ZoneId sessionZone : ImmutableList.of(UTC, jvmZone, vilnius, kathmandu, ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId()))) {
            DataTypeTest tests = DataTypeTest.create()
                    .addRoundTrip(timestampDataType(), dateTimeBeforeEpoch)
                    .addRoundTrip(timestampDataType(), dateTimeAfterEpoch)
                    .addRoundTrip(timestampDataType(), dateTimeDoubledInJvmZone)
                    .addRoundTrip(timestampDataType(), dateTimeDoubledInVilnius);

            if (!insertWithPresto) {
                addTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, dateTimeEpoch); // epoch also is a gap in JVM zone
                addTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, dateTimeGapInJvmZone1);
                addTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, dateTimeGapInJvmZone2);
            }

            addTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, dateTimeGapInVilnius);
            addTimestampTestIfSupported(tests, legacyTimestamp, sessionZone, dateTimeGapInKathmandu);

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .setSystemProperty("legacy_timestamp", Boolean.toString(legacyTimestamp))
                    .build();

            if (insertWithPresto) {
                tests.execute(getQueryRunner(), session, prestoCreateAsSelect(session));
            }
            else {
                tests.execute(getQueryRunner(), session, snowflakeCreateAndInsert());
            }
        }
    }

    @Test(dataProvider = "testTimestampArrayDataProvider")
    public void testTimestampArray(boolean legacyTimestamp)
    {
        // using two non-JVM zones so that we don't need to worry what Postgres system zone is
        for (ZoneId sessionZone : ImmutableList.of(UTC, jvmZone, vilnius, kathmandu, ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId()))) {
            DataTypeTest tests = DataTypeTest.create()
                    .addRoundTrip(timestampArrayDataType("[" +
                                    "\"1958-01-01T13:18:03.123000000Z\"," +
                                    "\"1970-01-01T00:00:00.000000000Z\"," +
                                    "\"2019-03-18T10:01:17.987000000Z\"," +
                                    "\"1970-01-01T00:13:42.000000000Z\"," +
                                    "\"2018-04-01T02:13:55.123000000Z\"," +
                                    "\"2018-10-28T01:33:17.456000000Z\"," +
                                    "\"2018-03-25T03:17:17.000000000Z\"," +
                                    "\"1986-01-01T00:13:07.000000000Z\"," +
                                    "\"2018-10-28T03:33:33.333000000Z\"]"),
                            ImmutableList.of(
                                    dateTimeBeforeEpoch,
                                    dateTimeEpoch,
                                    dateTimeAfterEpoch,
                                    dateTimeGapInJvmZone1,
                                    dateTimeGapInJvmZone2,
                                    dateTimeDoubledInJvmZone,
                                    dateTimeGapInVilnius,
                                    dateTimeGapInKathmandu,
                                    dateTimeDoubledInVilnius));

            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                    .setSystemProperty("legacy_timestamp", Boolean.toString(legacyTimestamp))
                    .build();

            tests.execute(getQueryRunner(), session, snowflakeCreateAsSelect());
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

    @Test(dataProvider = "testTimestampWithTimeZoneDataProvider")
    public void testTimestampWithTimeZone(boolean insertWithPresto, String timestampType, ZoneId resultZone)
    {
        DataType<ZonedDateTime> dataType;
        DataSetup dataSetup;

        LocalDateTime minSnowflakeDate = LocalDateTime.of(1, 1, 1, 0, 0, 0);
        LocalDateTime maxSnowflakeDate = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999000000);
        if (insertWithPresto) {
            dataType = prestoTimestampWithTimeZoneDataType();
            dataSetup = prestoCreateAsSelect();
        }
        else {
            dataType = snowflakeSqlTimestampWithTimeZoneDataType(timestampType, resultZone);
            dataSetup = snowflakeCreateAsSelect();
        }

        DataTypeTest tests;
        if (timestampType.equals("TIMESTAMP_LTZ") && !insertWithPresto) {
            // TODO: improve tests for TIMESTAMP_LTZ
            tests = DataTypeTest.create()
                    .addRoundTrip(dataType, dateTimeEpoch.atZone(UTC))
                    .addRoundTrip(dataType, dateTimeEpoch.atZone(kathmandu))
                    .addRoundTrip(dataType, dateTimeBeforeEpoch.atZone(UTC))
                    .addRoundTrip(dataType, dateTimeBeforeEpoch.atZone(kathmandu));
        }
        else {
            tests = DataTypeTest.create()
                    .addRoundTrip(dataType, dateTimeEpoch.atZone(UTC))
                    .addRoundTrip(dataType, dateTimeEpoch.atZone(kathmandu))
                    .addRoundTrip(dataType, dateTimeBeforeEpoch.atZone(UTC))
                    .addRoundTrip(dataType, dateTimeBeforeEpoch.atZone(kathmandu))
                    .addRoundTrip(dataType, dateTimeAfterEpoch.atZone(UTC))
                    .addRoundTrip(dataType, dateTimeAfterEpoch.atZone(kathmandu))
                    .addRoundTrip(dataType, dateTimeDoubledInJvmZone.atZone(UTC))
                    .addRoundTrip(dataType, dateTimeDoubledInJvmZone.atZone(jvmZone))
                    .addRoundTrip(dataType, dateTimeDoubledInJvmZone.atZone(kathmandu))
                    .addRoundTrip(dataType, dateTimeDoubledInVilnius.atZone(UTC))
                    .addRoundTrip(dataType, dateTimeDoubledInVilnius.atZone(vilnius))
                    .addRoundTrip(dataType, dateTimeDoubledInVilnius.atZone(kathmandu))
                    .addRoundTrip(dataType, dateTimeGapInJvmZone1.atZone(UTC))
                    .addRoundTrip(dataType, dateTimeGapInJvmZone1.atZone(kathmandu))
                    .addRoundTrip(dataType, dateTimeGapInJvmZone2.atZone(UTC))
                    .addRoundTrip(dataType, dateTimeGapInJvmZone2.atZone(kathmandu))
                    .addRoundTrip(dataType, dateTimeGapInVilnius.atZone(kathmandu))
                    .addRoundTrip(dataType, dateTimeGapInKathmandu.atZone(vilnius))
                    .addRoundTrip(dataType, maxSnowflakeDate.atZone(UTC))
                    .addRoundTrip(dataType, maxSnowflakeDate.atZone(kathmandu))
                    .addRoundTrip(dataType, maxSnowflakeDate.atZone(vilnius))
                    .addRoundTrip(dataType, minSnowflakeDate.atZone(UTC));
        }

        tests.execute(getQueryRunner(), dataSetup);
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

    @DataProvider
    public Object[][] testTimestampDataProvider()
    {
        return new Object[][] {
                {true, true},
                {false, true},
                {true, false},
                {false, false}
        };
    }

    @DataProvider
    public Object[][] testTimestampArrayDataProvider()
    {
        return new Object[][] {
                {true},
                {false}
        };
    }

    private static DataType<ZonedDateTime> prestoTimestampWithTimeZoneDataType()
    {
        return DataType.dataType(
                "timestamp with time zone",
                TIMESTAMP_WITH_TIME_ZONE,
                DateTimeFormatter.ofPattern("'TIMESTAMP '''yyyy-MM-dd HH:mm:ss.SSS VV''")::format,
                zonedDateTime -> {
                    if (zonedDateTime.getOffset().getTotalSeconds() == 0) {
                        // convert to UTC for testing purposes
                        return zonedDateTime.withZoneSameInstant(ZoneId.of("UTC"));
                    }

                    return zonedDateTime.withFixedOffsetZone();
                });
    }

    private static DataType<ZonedDateTime> snowflakeSqlTimestampWithTimeZoneDataType(String timestampType, ZoneId resultZone)
    {
        return DataType.dataType(
                timestampType,
                TIMESTAMP_WITH_TIME_ZONE,
                zonedDateTime -> DateTimeFormatter.ofPattern(format("'TO_%s('''yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX''')'", timestampType)).format(zonedDateTime),
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

    private static DataType<LocalTime> timeDataType()
    {
        return DataType.dataType(
                "TIME",
                TIME,
                DateTimeFormatter.ofPattern("'TIME '''HH:mm:ss.SSS''")::format,
                identity());
    }

    private static DataType<List<LocalTime>> timeArrayDataType(String expectedResult)
    {
        return DataType.dataType(
                "ARRAY",
                VarcharType.createUnboundedVarcharType(),
                value -> value.stream()
                        .map(DateTimeFormatter.ofPattern("'TIME '''HH:mm:ss.SSS''")::format)
                        .collect(Collectors.joining(",", "ARRAY_CONSTRUCT(", ")")),
                value -> expectedResult);
    }

    private static DataType<List<LocalDateTime>> timestampArrayDataType(String expectedResult)
    {
        return DataType.dataType(
                "ARRAY",
                VarcharType.createUnboundedVarcharType(),
                value -> value.stream()
                        .map(DateTimeFormatter.ofPattern("'TIMESTAMP '''yyyy-MM-dd HH:mm:ss.SSS''")::format)
                        .collect(Collectors.joining(",", "ARRAY_CONSTRUCT(", ")")),
                value -> expectedResult);
    }

    private static DataType<BigDecimal> integerDataType(String insertType)
    {
        return decimalDataType(insertType, createDecimalType(38, 0));
    }

    private static DataType<BigDecimal> decimalDataType(String typeName, int precision, int scale)
    {
        return decimalDataType(format("%s(%s, %s)", typeName, precision, scale), createDecimalType(precision, scale));
    }

    private static DataType<BigDecimal> decimalDataType(String insertType, DecimalType decimalType)
    {
        return DataType.dataType(
                insertType,
                decimalType,
                bigDecimal -> format("CAST('%s' AS %s)", bigDecimal, insertType),
                bigDecimal -> bigDecimal.setScale(decimalType.getScale(), UNNECESSARY));
    }

    private static DataType<Double> doubleDataType()
    {
        return dataType("double", DoubleType.DOUBLE,
                d -> {
                    if (Double.isFinite(d)) {
                        return d.toString();
                    }
                    else if (Double.isNaN(d)) {
                        return "nan()";
                    }
                    else {
                        return format("%sinfinity()", d > 0 ? "+" : "-");
                    }
                });
    }

    private static <T> DataType<T> variantDataType(String expectedResult)
    {
        return DataType.dataType(
                "VARIANT",
                VarcharType.createUnboundedVarcharType(),
                value -> "to_variant(" + value + ")",
                // snowflake will wrap value in double quota
                value -> expectedResult);
    }

    private static <T> DataType<Map<String, T>> objectDataType(String expectedResult)
    {
        return DataType.dataType(
                "OBJECT",
                VarcharType.createUnboundedVarcharType(),
                value -> "OBJECT_CONSTRUCT(" + value.entrySet().stream()
                        .map(entry -> "'" + entry.getKey() + "'," + entry.getValue())
                        .collect(Collectors.joining(",")) + ")",
                value -> expectedResult);
    }

    private static <T> DataType<List<T>> arrayDataType(String expectedResult)
    {
        return DataType.dataType(
                "ARRAY",
                VarcharType.createUnboundedVarcharType(),
                value -> "ARRAY_CONSTRUCT(" + value.stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(",")) + ")",
                value -> expectedResult);
    }

    private static <T> DataType<T> dataType(String insertType, Type prestoResultType)
    {
        return dataType(insertType, prestoResultType, Object::toString);
    }

    private static <T> DataType<T> dataType(String insertType, Type prestoResultType, Function<T, String> toLiteral)
    {
        return DataType.dataType(insertType, prestoResultType, toLiteral, identity());
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

    protected void testTypeMapping(DataTypeTest... tests)
    {
        runTestsWithSetup(prestoCreateAsSelect(), tests);
    }

    protected void testTypeReadMapping(DataTypeTest... tests)
    {
        runTestsWithSetup(snowflakeCreateAsSelect(), tests);
    }

    private void runTestsWithSetup(DataSetup dataSetup, DataTypeTest... tests)
    {
        for (DataTypeTest test : tests) {
            test.execute(getQueryRunner(), dataSetup);
        }
    }

    private DataSetup prestoCreateAsSelect()
    {
        return new CreateAsSelectDataSetup(
                new PrestoSqlExecutor(getQueryRunner()),
                "test_table_" + randomTableSuffix());
    }

    private DataSetup prestoCreateAsSelect(Session session)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner(), session), "test_table_" + randomTableSuffix());
    }

    private DataSetup snowflakeCreateAsSelect()
    {
        return new CreateAsSelectDataSetup(
                getSqlExecutor(),
                "test_table_" + randomTableSuffix());
    }

    private DataSetup snowflakeCreateAndInsert()
    {
        return new CreateAndInsertDataSetup(
                getSqlExecutor(),
                "test_table_" + randomTableSuffix());
    }

    protected SqlExecutor getSqlExecutor()
    {
        return sql -> {
            try {
                server.execute(format("USE SCHEMA %s", TEST_SCHEMA), sql);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
