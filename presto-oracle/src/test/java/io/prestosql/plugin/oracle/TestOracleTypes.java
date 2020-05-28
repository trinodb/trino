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

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.plugin.jdbc.UnsupportedTypeHandling;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.datatype.CreateAndInsertDataSetup;
import io.prestosql.testing.datatype.CreateAsSelectDataSetup;
import io.prestosql.testing.datatype.DataSetup;
import io.prestosql.testing.datatype.DataType;
import io.prestosql.testing.datatype.DataTypeTest;
import io.prestosql.testing.sql.JdbcSqlExecutor;
import io.prestosql.testing.sql.PrestoSqlExecutor;
import org.testng.annotations.AfterClass;
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
import java.util.Properties;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcPropertiesProvider.UNSUPPORTED_TYPE_HANDLING;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.prestosql.plugin.oracle.OracleDataTypes.dateDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.oracleTimestamp3TimeZoneDataType;
import static io.prestosql.plugin.oracle.OracleDataTypes.prestoTimestampWithTimeZoneDataType;
import static io.prestosql.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static io.prestosql.plugin.oracle.OracleSessionProperties.NUMBER_DEFAULT_SCALE;
import static io.prestosql.plugin.oracle.OracleSessionProperties.NUMBER_ROUNDING_MODE;
import static io.prestosql.plugin.oracle.TestingOracleServer.TEST_PASS;
import static io.prestosql.plugin.oracle.TestingOracleServer.TEST_USER;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.datatype.DataType.stringDataType;
import static io.prestosql.testing.datatype.DataType.timestampDataType;
import static io.prestosql.testing.datatype.DataType.varcharDataType;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.time.ZoneOffset.UTC;

public class TestOracleTypes
        extends AbstractTestQueryFramework
{
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

    private TestingOracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.oracleServer = new TestingOracleServer();
        return createOracleQueryRunner(oracleServer);
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

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (oracleServer != null) {
            oracleServer.close();
        }
    }

    private DataSetup prestoCreateAsSelect(String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner()), tableNamePrefix);
    }

    private DataSetup prestoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    @Test
    public void testBooleanType()
    {
        DataTypeTest.create()
                .addRoundTrip(booleanOracleType(), true)
                .addRoundTrip(booleanOracleType(), false)
                .execute(getQueryRunner(), prestoCreateAsSelect("boolean_types"));
    }

    @Test
    public void testSpecialNumberFormats()
    {
        oracleServer.execute("CREATE TABLE test (num1 number)");
        oracleServer.execute("INSERT INTO test VALUES (12345678901234567890.12345678901234567890123456789012345678)");
        assertQuery(number(HALF_UP, 10), "SELECT * FROM test", "VALUES (12345678901234567890.1234567890)");
    }

    private Session number(RoundingMode roundingMode, int scale)
    {
        return number(IGNORE, roundingMode, Optional.of(scale));
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
    public void testVarcharType()
    {
        DataTypeTest.create()
                .addRoundTrip(varcharDataType(10), "test")
                .addRoundTrip(stringDataType("varchar", createVarcharType(4000)), "test")
                .addRoundTrip(stringDataType("varchar(5000)", createUnboundedVarcharType()), "test")
                .addRoundTrip(varcharDataType(3), String.valueOf('\u2603'))
                .execute(getQueryRunner(), prestoCreateAsSelect("varchar_types"));
    }

    @Test
    public void testNumericTypes()
    {
        DataTypeTest.create()
                .addRoundTrip(numberOracleType("tinyint", BigintType.BIGINT), 123L)
                .addRoundTrip(numberOracleType("tinyint", BigintType.BIGINT), null)
                .addRoundTrip(numberOracleType("smallint", BigintType.BIGINT), 123L)
                .addRoundTrip(numberOracleType("integer", BigintType.BIGINT), 123L)
                .addRoundTrip(numberOracleType("bigint", BigintType.BIGINT), 123L)
                .addRoundTrip(numberOracleType("decimal", BigintType.BIGINT), 123L)
                .addRoundTrip(numberOracleType("decimal(20)", BigintType.BIGINT), 123L)
                .addRoundTrip(numberOracleType("decimal(20,0)", BigintType.BIGINT), 123L)
                .addRoundTrip(numberOracleType(createDecimalType(5, 1)), BigDecimal.valueOf(123))
                .addRoundTrip(numberOracleType(createDecimalType(5, 2)), BigDecimal.valueOf(123))
                .addRoundTrip(numberOracleType(createDecimalType(5, 2)), BigDecimal.valueOf(123.046))
                .execute(getQueryRunner(), prestoCreateAsSelect("numeric_types"));
    }

    private static DataType<Boolean> booleanOracleType()
    {
        return DataType.dataType(
                "boolean",
                BigintType.BIGINT,
                val -> val ? "1" : "0",
                val -> val ? 1L : 0L);
    }

    private static DataType<BigDecimal> numberOracleType(DecimalType type)
    {
        String databaseType = format("decimal(%s, %s)", type.getPrecision(), type.getScale());
        return numberOracleType(databaseType, type);
    }

    private static <T> DataType<T> numberOracleType(String inputType, Type resultType)
    {
        Function<T, ?> queryResult = (Function<T, Object>) value ->
                (value instanceof BigDecimal && resultType instanceof DecimalType)
                    ? ((BigDecimal) value).setScale(((DecimalType) resultType).getScale(), HALF_UP)
                    : value;

        return DataType.dataType(
                inputType,
                resultType,
                value -> format("CAST('%s' AS %s)", value, resultType),
                queryResult);
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
    public void testNonLegacyDateMapping()
    {
        nonLegacyDateTests(zone -> prestoCreateAsSelect("nl_date_" + zone));
    }

    @Test
    public void testNonLegacyDateReadMapping()
    {
        nonLegacyDateTests(zone -> oracleCreateAndInsert("nl_read_date_" + zone));
    }

    void nonLegacyDateTests(Function<String, DataSetup> dataSetup)
    {
        Map<String, TimeZoneKey> zonesBySqlName = ImmutableMap.of(
                "UTC", UTC_KEY,
                "JVM", getTimeZoneKey(ZoneId.systemDefault().getId()),
                "other", getTimeZoneKey(ZoneId.of("Europe/Vilnius").getId()));

        for (Map.Entry<String, TimeZoneKey> zone : zonesBySqlName.entrySet()) {
            runNonLegacyTimestampTestInZone(
                    dataSetup.apply(zone.getKey()),
                    zone.getValue().getId(),
                    nonLegacyDateTests());
        }
    }

    private DataTypeTest nonLegacyDateTests()
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
    public void testTimestamp(boolean legacyTimestamp, boolean insertWithPresto, ZoneId sessionZone)
    {
        // using two non-JVM zones so that we don't need to worry what Oracle system zone is
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
            tests.execute(getQueryRunner(), session, oracleCreateAndInsert("test_timestamp"));
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

                // using two non-JVM zones so that we don't need to worry what Oracle system zone is
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

    private DataSetup oracleCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(getSqlExecutor(), tableNamePrefix);
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
    private void runNonLegacyTimestampTestInZone(DataSetup dataSetup, String zone, DataTypeTest test)
    {
        Session.SessionBuilder session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(SystemSessionProperties.LEGACY_TIMESTAMP, "false");
        if (zone != null) {
            session.setTimeZoneKey(getTimeZoneKey(zone));
        }
        test.execute(getQueryRunner(), session.build(), dataSetup);
    }

    private JdbcSqlExecutor getSqlExecutor()
    {
        Properties properties = new Properties();
        properties.setProperty("user", TEST_USER);
        properties.setProperty("password", TEST_PASS);
        return new JdbcSqlExecutor(oracleServer.getJdbcUrl(), properties);
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
