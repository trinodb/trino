/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import io.prestosql.Session;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.datatype.CreateAndInsertDataSetup;
import io.prestosql.testing.datatype.CreateAndPrestoInsertDataSetup;
import io.prestosql.testing.datatype.CreateAsSelectDataSetup;
import io.prestosql.testing.datatype.DataSetup;
import io.prestosql.testing.datatype.DataType;
import io.prestosql.testing.datatype.DataTypeTest;
import io.prestosql.testing.sql.PrestoSqlExecutor;
import io.prestosql.testing.sql.TestTable;
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
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createRemotePrestoQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.prestoConnectorConnectionUrl;
import static io.airlift.testing.Closeables.closeAll;
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.testing.datatype.DataType.bigintDataType;
import static io.prestosql.testing.datatype.DataType.booleanDataType;
import static io.prestosql.testing.datatype.DataType.dateDataType;
import static io.prestosql.testing.datatype.DataType.decimalDataType;
import static io.prestosql.testing.datatype.DataType.doubleDataType;
import static io.prestosql.testing.datatype.DataType.integerDataType;
import static io.prestosql.testing.datatype.DataType.realDataType;
import static io.prestosql.testing.datatype.DataType.smallintDataType;
import static io.prestosql.testing.datatype.DataType.timeDataType;
import static io.prestosql.testing.datatype.DataType.timestampDataType;
import static io.prestosql.testing.datatype.DataType.tinyintDataType;
import static io.prestosql.testing.datatype.DataType.varbinaryDataType;
import static io.prestosql.testing.datatype.DataType.varcharDataType;
import static io.prestosql.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPrestoConnectorTypeMapping
        extends AbstractTestQueryFramework
{
    private static final LocalDate EPOCH_DAY = LocalDate.ofEpochDay(0);

    private DistributedQueryRunner remotePresto;
    private PrestoSqlExecutor remoteExecutor;

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
        remotePresto = createRemotePrestoQueryRunner(
                Map.of(),
                List.of());
        return createPrestoConnectorQueryRunner(
                true,
                Map.of(),
                Map.of(
                        "connection-url", prestoConnectorConnectionUrl(remotePresto, "memory"),
                        "allow-drop-table", "true",
                        // TODO use synthetic testing type installed on remote Presto only
                        "jdbc-types-mapped-to-varchar", "IPAddress"));
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

        remoteExecutor = new PrestoSqlExecutor(
                remotePresto,
                Session.builder(remotePresto.getDefaultSession())
                        .setCatalog("memory")
                        .setSchema("tiny")
                        .build());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        closeAll(
                remotePresto,
                () -> {
                    remotePresto = null;
                    remoteExecutor = null;
                });
    }

    @Test
    public void testBasicTypes()
    {
        DataTypeTest testCases = DataTypeTest.create(true)
                .addRoundTrip(booleanDataType(), true)
                .addRoundTrip(booleanDataType(), false)
                .addRoundTrip(tinyintDataType(), (byte) -42)
                .addRoundTrip(tinyintDataType(), (byte) 42)
                .addRoundTrip(smallintDataType(), (short) 32_456)
                .addRoundTrip(integerDataType(), 1_234_567_890)
                .addRoundTrip(bigintDataType(), 123_456_789_012L)
                .addRoundTrip(realDataType(), 123.45f)
                .addRoundTrip(doubleDataType(), 123.45d);

        testCases.execute(getQueryRunner(), remotePrestoCreated("test_basic_types"));
        testCases.execute(getQueryRunner(), remotePrestoCreatedPrestoConnectorInserted("test_basic_types"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAsSelect("test_basic_types"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAndInsert("test_basic_types"));
    }

    @Test
    public void testReal()
    {
        DataType<Float> dataType = realDataType();
        DataTypeTest testCases = DataTypeTest.create(true)
                .addRoundTrip(dataType, 3.14f)
                .addRoundTrip(dataType, 3.1415927f)
                .addRoundTrip(dataType, Float.NaN)
                .addRoundTrip(dataType, Float.NEGATIVE_INFINITY)
                .addRoundTrip(dataType, Float.POSITIVE_INFINITY)
                .addRoundTrip(dataType, null);

        testCases.execute(getQueryRunner(), remotePrestoCreated("test_real"));
        testCases.execute(getQueryRunner(), remotePrestoCreatedPrestoConnectorInserted("test_real"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAsSelect("test_real"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAndInsert("test_real"));
    }

    @Test
    public void testDouble()
    {
        DataType<Double> dataType = doubleDataType();
        DataTypeTest testCases = DataTypeTest.create(true)
                .addRoundTrip(dataType, 1.0e100d)
                .addRoundTrip(dataType, Double.NaN)
                .addRoundTrip(dataType, Double.POSITIVE_INFINITY)
                .addRoundTrip(dataType, Double.NEGATIVE_INFINITY)
                .addRoundTrip(dataType, null);

        testCases.execute(getQueryRunner(), remotePrestoCreated("test_double"));
        testCases.execute(getQueryRunner(), remotePrestoCreatedPrestoConnectorInserted("test_double"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAsSelect("test_double"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAndInsert("test_double"));
    }

    @Test
    public void testDecimal()
    {
        DataTypeTest testCases = DataTypeTest.create()
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

        testCases.execute(getQueryRunner(), remotePrestoCreated("test_decimal"));
        testCases.execute(getQueryRunner(), remotePrestoCreatedPrestoConnectorInserted("test_decimal"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAsSelect("test_decimal"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAndInsert("test_decimal"));
    }

    @Test
    public void testChar()
    {
        DataTypeTest testCases = characterDataTypeTest(DataType::charDataType);

        testCases.execute(getQueryRunner(), remotePrestoCreated("test_char"));
        testCases.execute(getQueryRunner(), remotePrestoCreatedPrestoConnectorInserted("test_char"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAsSelect("test_char"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAndInsert("test_char"));
    }

    @Test
    public void testVarchar()
    {
        // varchar(n)
        DataTypeTest testCases = varcharDataTypeTest(DataType::varcharDataType);

        testCases.execute(getQueryRunner(), remotePrestoCreated("test_varchar"));
        testCases.execute(getQueryRunner(), remotePrestoCreatedPrestoConnectorInserted("test_varchar"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAsSelect("test_varchar"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAndInsert("test_varchar"));

        // varchar unbounded
        testCases = varcharDataTypeTest(length -> varcharDataType());

        testCases.execute(getQueryRunner(), remotePrestoCreated("test_varchar_unbounded"));
        testCases.execute(getQueryRunner(), remotePrestoCreatedPrestoConnectorInserted("test_varchar_unbounded"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAsSelect("test_varchar_unbounded"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAndInsert("test_varchar_unbounded"));
    }

    private static DataTypeTest varcharDataTypeTest(Function<Integer, DataType<String>> dataTypeFactory)
    {
        return characterDataTypeTest(dataTypeFactory)
                .addRoundTrip(dataTypeFactory.apply(10485760), "text_f"); // too long for a char in Presto
    }

    private static DataTypeTest characterDataTypeTest(Function<Integer, DataType<String>> dataTypeFactory)
    {
        String sampleUnicodeText = "\u653b\u6bbb\u6a5f\u52d5\u968a";
        String sampleFourByteUnicodeCharacter = "\uD83D\uDE02";

        return DataTypeTest.create()
                .addRoundTrip(dataTypeFactory.apply(10), "text_a")
                .addRoundTrip(dataTypeFactory.apply(255), "text_b")
                .addRoundTrip(dataTypeFactory.apply(65535), "text_d")

                .addRoundTrip(dataTypeFactory.apply(sampleUnicodeText.length()), sampleUnicodeText)
                .addRoundTrip(dataTypeFactory.apply(32), sampleUnicodeText)
                .addRoundTrip(dataTypeFactory.apply(20000), sampleUnicodeText)
                .addRoundTrip(dataTypeFactory.apply(1), sampleFourByteUnicodeCharacter)
                .addRoundTrip(dataTypeFactory.apply(77), "\u041d\u0443, \u043f\u043e\u0433\u043e\u0434\u0438!");
    }

    @Test
    public void testVarbinary()
    {
        DataType<byte[]> dataType = varbinaryDataType();
        DataTypeTest testCases = DataTypeTest.create()
                .addRoundTrip(dataType, "hello".getBytes(UTF_8))
                .addRoundTrip(dataType, "Piękna łąka w 東京都".getBytes(UTF_8))
                .addRoundTrip(dataType, "Bag full of 💰".getBytes(UTF_16LE))
                .addRoundTrip(dataType, null)
                .addRoundTrip(dataType, new byte[] {})
                .addRoundTrip(dataType, new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 13, -7, 54, 122, -89, 0, 0, 0});

        testCases.execute(getQueryRunner(), remotePrestoCreated("test_varbinary"));
        testCases.execute(getQueryRunner(), remotePrestoCreatedPrestoConnectorInserted("test_varbinary"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAsSelect("test_varbinary"));
        testCases.execute(getQueryRunner(), prestoConnectorCreateAndInsert("test_varbinary"));
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
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeBackwardAtMidnightInSomeZone)
                // historical date, surprisingly common in actual data
                .addRoundTrip(dateDataType(), LocalDate.of(1, 1, 1));

        for (String timeZoneId : List.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            testCases.execute(getQueryRunner(), session, remotePrestoCreated("test_date"));
            testCases.execute(getQueryRunner(), session, remotePrestoCreatedPrestoConnectorInserted(session, "test_date"));
            testCases.execute(getQueryRunner(), session, prestoConnectorCreateAsSelect(session, "test_date"));
            testCases.execute(getQueryRunner(), session, prestoConnectorCreateAndInsert(session, "test_date"));
        }
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTime(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        LocalTime timeGapInJvmZone = LocalTime.of(0, 12, 34);
        checkIsGap(jvmZone, timeGapInJvmZone.atDate(EPOCH_DAY));

        DataTypeTest testCases = DataTypeTest.create()
                .addRoundTrip(timeDataType(0), LocalTime.of(1, 12, 34, 0))
                .addRoundTrip(timeDataType(9), LocalTime.of(23, 59, 59))
                .addRoundTrip(timeDataType(9), LocalTime.of(23, 59, 59, 123_456_789))
                .addRoundTrip(timeDataType(1), LocalTime.of(2, 12, 34, 100_000_000))
                .addRoundTrip(timeDataType(2), LocalTime.of(2, 12, 34, 10_000_000))
                .addRoundTrip(timeDataType(3), LocalTime.of(2, 12, 34, 1_000_000))
                .addRoundTrip(timeDataType(4), LocalTime.of(2, 12, 34, 100_000))
                .addRoundTrip(timeDataType(5), LocalTime.of(2, 12, 34, 10_000))
                .addRoundTrip(timeDataType(6), LocalTime.of(2, 12, 34, 1_000))
                .addRoundTrip(timeDataType(7), LocalTime.of(2, 12, 34, 100))
                .addRoundTrip(timeDataType(8), LocalTime.of(2, 12, 34, 10))
                .addRoundTrip(timeDataType(9), LocalTime.of(2, 12, 34, 1))
                // maximum possible value for given precision
                .addRoundTrip(timeDataType(0), LocalTime.of(23, 59, 59))
                .addRoundTrip(timeDataType(3), LocalTime.of(23, 59, 59, 999_000_000))
                .addRoundTrip(timeDataType(6), LocalTime.of(23, 59, 59, 999_999_000))
                .addRoundTrip(timeDataType(9), LocalTime.of(23, 59, 59, 999_999_999))
                // epoch is also a gap in JVM zone
                .addRoundTrip(timeDataType(0), epoch.toLocalTime())
                .addRoundTrip(timeDataType(3), epoch.toLocalTime())
                .addRoundTrip(timeDataType(6), epoch.toLocalTime())
                .addRoundTrip(timeDataType(9), epoch.toLocalTime())
                .addRoundTrip(timeDataType(0), timeGapInJvmZone.withNano(0))
                .addRoundTrip(timeDataType(3), timeGapInJvmZone.withNano(567_000_000))
                .addRoundTrip(timeDataType(6), timeGapInJvmZone.withNano(567_123_000))
                .addRoundTrip(timeDataType(9), timeGapInJvmZone.withNano(567_123_456));

        testCases.execute(getQueryRunner(), session, remotePrestoCreated("test_time"));
        testCases.execute(getQueryRunner(), session, remotePrestoCreatedPrestoConnectorInserted(session, "test_time"));
        testCases.execute(getQueryRunner(), session, prestoConnectorCreateAsSelect(session, "test_time"));
        testCases.execute(getQueryRunner(), session, prestoConnectorCreateAndInsert(session, "test_time"));
    }

    /**
     * Additional test supplementing {@link #testTime} with timestamp precision higher than expressible with {@code LocalTime}.
     *
     * @see #testTime
     */
    @Test
    public void testTimePrecision()
    {
        testTimePrecision("TIME '00:00:00'");
        testTimePrecision("TIME '00:34:56'");
        testTimePrecision("TIME '01:34:56'");
        testTimePrecision("TIME '12:34:56'");
        testTimePrecision("TIME '23:59:59'");
        testTimePrecision("TIME '00:00:00.123456'");

        // minimum possible non-zero value for given precision
        testTimePrecision("TIME '00:00:00.1'");
        testTimePrecision("TIME '00:00:00.01'");
        testTimePrecision("TIME '00:00:00.001'");
        testTimePrecision("TIME '00:00:00.0001'");
        testTimePrecision("TIME '00:00:00.00001'");
        testTimePrecision("TIME '00:00:00.000001'");
        testTimePrecision("TIME '00:00:00.0000001'");
        testTimePrecision("TIME '00:00:00.00000001'");
        testTimePrecision("TIME '00:00:00.000000001'");
        testTimePrecision("TIME '00:00:00.0000000001'");
        testTimePrecision("TIME '00:00:00.00000000001'");
        testTimePrecision("TIME '00:00:00.000000000001'");

        // maximum possible value for given precision
        testTimePrecision("TIME '23:59:59.9'");
        testTimePrecision("TIME '23:59:59.99'");
        testTimePrecision("TIME '23:59:59.999'");
        testTimePrecision("TIME '23:59:59.9999'");
        testTimePrecision("TIME '23:59:59.99999'");
        testTimePrecision("TIME '23:59:59.999999'");
        testTimePrecision("TIME '23:59:59.9999999'");
        testTimePrecision("TIME '23:59:59.99999999'");
        testTimePrecision("TIME '23:59:59.999999999'");
        testTimePrecision("TIME '23:59:59.9999999999'");
        testTimePrecision("TIME '23:59:59.99999999999'");
        testTimePrecision("TIME '23:59:59.999999999999'");
    }

    private void testTimePrecision(String literal)
    {
        testCreateTableAsAndInsertConsistency(literal, literal);
    }

    /**
     * @see #testTimestampPrecision
     */
    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        DataTypeTest testCases = DataTypeTest.create()
                .addRoundTrip(timestampDataType(3), beforeEpoch)
                .addRoundTrip(timestampDataType(3), afterEpoch)
                .addRoundTrip(timestampDataType(3), timeDoubledInJvmZone)
                .addRoundTrip(timestampDataType(3), timeDoubledInVilnius)
                .addRoundTrip(timestampDataType(3), epoch) // epoch also is a gap in JVM zone
                .addRoundTrip(timestampDataType(3), timeGapInJvmZone1)
                .addRoundTrip(timestampDataType(3), timeGapInJvmZone2)
                .addRoundTrip(timestampDataType(3), timeGapInVilnius)
                .addRoundTrip(timestampDataType(3), timeGapInKathmandu)

                .addRoundTrip(timestampDataType(7), beforeEpoch)
                .addRoundTrip(timestampDataType(7), afterEpoch)
                .addRoundTrip(timestampDataType(7), timeDoubledInJvmZone)
                .addRoundTrip(timestampDataType(7), timeDoubledInVilnius)
                .addRoundTrip(timestampDataType(7), epoch) // epoch also is a gap in JVM zone
                .addRoundTrip(timestampDataType(7), timeGapInJvmZone1)
                .addRoundTrip(timestampDataType(7), timeGapInJvmZone2)
                .addRoundTrip(timestampDataType(7), timeGapInVilnius)
                .addRoundTrip(timestampDataType(7), timeGapInKathmandu)

                // test some arbitrary time for all supported precisions
                .addRoundTrip(timestampDataType(0), LocalDateTime.of(1970, 1, 1, 0, 0, 0))
                .addRoundTrip(timestampDataType(1), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 100_000_000))
                .addRoundTrip(timestampDataType(2), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 120_000_000))
                .addRoundTrip(timestampDataType(3), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_000_000))
                .addRoundTrip(timestampDataType(4), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_400_000))
                .addRoundTrip(timestampDataType(5), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_450_000))
                .addRoundTrip(timestampDataType(6), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_000))
                .addRoundTrip(timestampDataType(7), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_700))
                .addRoundTrip(timestampDataType(8), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_780))
                .addRoundTrip(timestampDataType(9), LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123_456_789))

                // before epoch with nanos
                .addRoundTrip(timestampDataType(6), LocalDateTime.of(1969, 12, 31, 23, 59, 59, 123_456_000))
                .addRoundTrip(timestampDataType(7), LocalDateTime.of(1969, 12, 31, 23, 59, 59, 123_456_700));

        testCases.execute(getQueryRunner(), session, remotePrestoCreated("test_timestamp"));
        testCases.execute(getQueryRunner(), session, remotePrestoCreatedPrestoConnectorInserted(session, "test_timestamp"));
        testCases.execute(getQueryRunner(), session, prestoConnectorCreateAsSelect(session, "test_timestamp"));
        testCases.execute(getQueryRunner(), session, prestoConnectorCreateAndInsert(session, "test_timestamp"));
    }

    /**
     * Additional test supplementing {@link #testTimestamp} with timestamp precision higher than expressible with {@code LocalDateTime}.
     *
     * @see #testTimestamp
     */
    @Test
    public void testTimestampPrecision()
    {
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00'");

        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.1'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.9'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.123'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.999'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.123456'");

        testTimestampPrecision("TIMESTAMP '2020-09-27 12:34:56.1'");
        testTimestampPrecision("TIMESTAMP '2020-09-27 12:34:56.9'");
        testTimestampPrecision("TIMESTAMP '2020-09-27 12:34:56.123'");
        testTimestampPrecision("TIMESTAMP '2020-09-27 12:34:56.999'");
        testTimestampPrecision("TIMESTAMP '2020-09-27 12:34:56.123456'");

        // minimum possible positive value for given precision
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.1'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.01'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.001'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.0001'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.00001'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.000001'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.0000001'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.00000001'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.000000001'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.0000000001'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.00000000001'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 00:00:00.000000000001'");

        // maximum possible time of the day
        testTimestampPrecision("TIMESTAMP '1970-01-01 23:59:59'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 23:59:59.9'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 23:59:59.99'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 23:59:59.999'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 23:59:59.9999'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 23:59:59.99999'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 23:59:59.999999'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 23:59:59.9999999'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 23:59:59.99999999'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 23:59:59.999999999'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 23:59:59.9999999999'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 23:59:59.99999999999'");
        testTimestampPrecision("TIMESTAMP '1970-01-01 23:59:59.999999999999'");

        // negative epoch
        testTimestampPrecision("TIMESTAMP '1969-12-31 23:59:59'");
        testTimestampPrecision("TIMESTAMP '1969-12-31 23:59:59.999'");
        testTimestampPrecision("TIMESTAMP '1969-12-31 23:59:59.999999'");
        testTimestampPrecision("TIMESTAMP '1969-12-31 23:59:59.999999999'");
        testTimestampPrecision("TIMESTAMP '1969-12-31 23:59:59.999999999999'");

        // historical date, surprisingly common in actual data
        testTimestampPrecision("TIMESTAMP '0001-01-01 00:00:00'");
        testTimestampPrecision("TIMESTAMP '0001-01-01 00:00:00.000'");
        testTimestampPrecision("TIMESTAMP '0001-01-01 00:00:00.000000'");
        testTimestampPrecision("TIMESTAMP '0001-01-01 00:00:00.000000000'");
        testTimestampPrecision("TIMESTAMP '0001-01-01 00:00:00.000000000000'");

        // negative year
        testTimestampPrecision("TIMESTAMP '-0042-01-01 01:23:45.123'");
        testTimestampPrecision("TIMESTAMP '-0042-01-01 01:23:45.123456789012'");

        // beyond four-digit year, rendered with a plus sign in various places, including Presto response
        testTimestampPrecision("TIMESTAMP '123456-01-01 01:23:45.123'");
        testTimestampPrecision("TIMESTAMP '123456-01-01 01:23:45.123456789012'");
    }

    private void testTimestampPrecision(String a)
    {
        testCreateTableAsAndInsertConsistency(a, a);
    }

    @Test
    public void testForcedMappingToVarchar()
    {
        try (TestTable table = new TestTable(
                remoteExecutor,
                "test_forced_mapping_to_varchar",
                // TODO use synthetic testing type installed on remote Presto only
                "(key varchar(5), unsupported_column ipaddress)",
                List.of(
                        "'1', NULL",
                        "'2', IPADDRESS '2001:db8::1'"))) {
            String tableName = table.getName();

            assertThat(query("SELECT unsupported_column FROM " + tableName))
                    .matches("VALUES NULL, CAST('2001:db8::1' AS varchar)");

            // test predicate pushdown to column that has forced varchar mapping
            assertThat(query("SELECT 1 FROM " + tableName + " WHERE unsupported_column = '2001:db8::1'"))
                    .matches("VALUES 1")
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT 1 FROM " + tableName + " WHERE unsupported_column = 'some value'"))
                    .returnsEmptyResult()
                    .isNotFullyPushedDown(FilterNode.class);

            // test insert into column that has forced varchar mapping
            assertQueryFails(
                    "INSERT INTO " + tableName + " (unsupported_column) VALUES ('some value')",
                    "Underlying type that is mapped to VARCHAR is not supported for INSERT: ipaddress");
        }
    }

    @Test
    public void testUnsupportedDataType()
    {
        // TODO use synthetic testing type installed on remote Presto only; or use ColorType (requires fixing its representation in JDBC)
        String unsupportedDataType = "uuid";
        String exampleValue = "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'";

        testUnsupportedDataTypeAsIgnored(getSession(), unsupportedDataType, exampleValue);
        testUnsupportedDataTypeConvertedToVarchar(getSession(), unsupportedDataType, exampleValue, "'12151fd2-7586-11e9-8f9e-2a86e4085a59'");
    }

    private void testUnsupportedDataTypeAsIgnored(Session session, String dataTypeName, String databaseValue)
    {
        try (TestTable table = new TestTable(
                remoteExecutor,
                "test_unsupported_type",
                format("(key varchar(5), unsupported_column %s)", dataTypeName),
                List.of(
                        "'null', NULL",
                        "'value', " + databaseValue))) {
            assertQuery(session, "SELECT * FROM " + table.getName(), "VALUES 'null', 'value'");
            assertQuery(
                    session,
                    "DESC " + table.getName(),
                    "VALUES ('key', 'varchar(5)','', '')"); // no 'unsupported_column'

            assertUpdate(session, format("INSERT INTO %s VALUES 'third'", table.getName()), 1);
            assertQuery(session, "SELECT * FROM " + table.getName(), "VALUES 'null', 'value', 'third'");
        }
    }

    private void testUnsupportedDataTypeConvertedToVarchar(Session session, String dataTypeName, String databaseValue, String expectedReturnedValue)
    {
        try (TestTable table = new TestTable(
                remoteExecutor,
                "test_unsupported_type_converted",
                format("(key varchar(30), unsupported_column %s)", dataTypeName),
                List.of(
                        format("'null', CAST(NULL AS %s)", dataTypeName),
                        "'value', " + databaseValue))) {
            Session convertToVarchar = Session.builder(session)
                    .setCatalogSessionProperty("p2p_remote", UNSUPPORTED_TYPE_HANDLING, CONVERT_TO_VARCHAR.name())
                    .build();
            assertQuery(
                    convertToVarchar,
                    "SELECT * FROM " + table.getName(),
                    format("VALUES ('null', NULL), ('value', %s)", expectedReturnedValue));
            assertQuery(
                    convertToVarchar,
                    format("SELECT key FROM %s WHERE unsupported_column = %s", table.getName(), expectedReturnedValue),
                    "VALUES 'value'");
            assertQuery(
                    convertToVarchar,
                    "DESC " + table.getName(),
                    "VALUES " +
                            "('key', 'varchar(30)', '', ''), " +
                            "('unsupported_column', 'varchar', '', '')");
            assertUpdate(
                    convertToVarchar,
                    format("INSERT INTO %s (key, unsupported_column) VALUES ('inserted null', NULL)", table.getName()),
                    1);
            assertQueryFails(
                    convertToVarchar,
                    format("INSERT INTO %s (key, unsupported_column) VALUES ('inserted value', %s)", table.getName(), expectedReturnedValue),
                    "\\QUnderlying type that is mapped to VARCHAR is not supported for INSERT: " + dataTypeName);
            assertUpdate(
                    convertToVarchar,
                    format("INSERT INTO %s (key) VALUES ('inserted implicit null')", table.getName()),
                    1);

            assertQuery(
                    convertToVarchar,
                    "SELECT * FROM " + table.getName(),
                    format("VALUES ('null', NULL), ('value', %s), ('inserted null', NULL), ('inserted implicit null', NULL)", expectedReturnedValue));
        }
    }

    private void testCreateTableAsAndInsertConsistency(String inputLiteral, String expectedResult)
    {
        String tableName = "test_ctas_and_insert_" + randomTableSuffix();

        // CTAS
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT " + inputLiteral + " a", 1);
        assertThat(query("SELECT a FROM " + tableName))
                .matches("VALUES " + expectedResult);
        assertUpdate("DROP TABLE " + tableName);

        // INSERT as a control query, where the coercion is done by the engine
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT " + inputLiteral + " a WITH NO DATA", 0);
        assertUpdate("INSERT INTO " + tableName + " (a) VALUES (" + inputLiteral + ")", 1);
        assertThat(query("SELECT a FROM " + tableName))
                .matches("VALUES " + expectedResult);

        // opportunistically test predicate pushdown if applies to given type
        assertThat(query("SELECT count(*) FROM " + tableName + " WHERE a = " + expectedResult))
                .matches("VALUES BIGINT '1'")
                .isFullyPushedDown();

        assertUpdate("DROP TABLE " + tableName);
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

    private DataSetup remotePrestoCreated(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(remoteExecutor, tableNamePrefix);
    }

    private DataSetup remotePrestoCreatedPrestoConnectorInserted(String tableNamePrefix)
    {
        return remotePrestoCreatedPrestoConnectorInserted(getSession(), tableNamePrefix);
    }

    private DataSetup remotePrestoCreatedPrestoConnectorInserted(Session session, String tableNamePrefix)
    {
        return new CreateAndPrestoInsertDataSetup(remoteExecutor, new PrestoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup prestoConnectorCreateAsSelect(String tableNamePrefix)
    {
        return prestoConnectorCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup prestoConnectorCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup prestoConnectorCreateAndInsert(String tableNamePrefix)
    {
        return prestoConnectorCreateAndInsert(getSession(), tableNamePrefix);
    }

    private DataSetup prestoConnectorCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new PrestoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
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
