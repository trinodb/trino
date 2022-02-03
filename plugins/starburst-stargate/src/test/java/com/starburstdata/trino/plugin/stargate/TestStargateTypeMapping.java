/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAndTrinoInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import io.trino.type.JsonType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createRemoteStarburstQueryRunnerWithMemory;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createStargateQueryRunner;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.stargateConnectionUrl;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
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
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStargateTypeMapping
        extends AbstractTestQueryFramework
{
    private static final LocalDate EPOCH_DAY = LocalDate.ofEpochDay(0);

    private DistributedQueryRunner starburstEnterprise;
    private TrinoSqlExecutor remoteExecutor;

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
        starburstEnterprise = createRemoteStarburstQueryRunnerWithMemory(
                List.of(),
                Optional.empty());
        return createStargateQueryRunner(
                true,
                Map.of(
                        "connection-url", stargateConnectionUrl(starburstEnterprise, "memory"),
                        // TODO use synthetic testing type installed on remote Trino only
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

        remoteExecutor = new TrinoSqlExecutor(
                starburstEnterprise,
                Session.builder(starburstEnterprise.getDefaultSession())
                        .setCatalog("memory")
                        .setSchema("tiny")
                        .build());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        closeAll(
                starburstEnterprise,
                () -> {
                    starburstEnterprise = null;
                    remoteExecutor = null;
                });
    }

    @Test
    public void testBasicTypes()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN)
                .addRoundTrip("boolean", "false", BOOLEAN)
                .addRoundTrip("tinyint", "-42", TINYINT, "TINYINT '-42'")
                .addRoundTrip("tinyint", "42", TINYINT, "TINYINT '42'")
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("integer", "1234567890", INTEGER)
                .addRoundTrip("bigint", "123456789012", BIGINT)
                .addRoundTrip("real", "123.45", REAL, "REAL '123.45'")
                .addRoundTrip("double", "123.45", DOUBLE, "DOUBLE '123.45'")
                .execute(getQueryRunner(), remoteConnectorCreated("test_basic_types"))
                .execute(getQueryRunner(), remoteTrinoCreatedRemoteConnectorInserted("test_basic_types"))
                .execute(getQueryRunner(), remoteConnectorCreateAsSelect("test_basic_types"))
                .execute(getQueryRunner(), remoteConnectorCreateAndInsert("test_basic_types"));
    }

    @Test
    public void testReal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS REAL)")
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'")
                .addRoundTrip("real", "nan()", REAL, "CAST(nan() AS REAL)")
                .addRoundTrip("real", "-infinity()", REAL, "CAST(-infinity() AS REAL)")
                .addRoundTrip("real", "+infinity()", REAL, "CAST(+infinity() AS REAL)")
                .execute(getQueryRunner(), remoteConnectorCreated("test_real"))
                .execute(getQueryRunner(), remoteTrinoCreatedRemoteConnectorInserted("test_real"))
                .execute(getQueryRunner(), remoteConnectorCreateAsSelect("test_real"))
                .execute(getQueryRunner(), remoteConnectorCreateAndInsert("test_real"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "nan()", DOUBLE, "nan()")
                .addRoundTrip("double", "+infinity()", DOUBLE, "+infinity()")
                .addRoundTrip("double", "-infinity()", DOUBLE, "-infinity()")
                .execute(getQueryRunner(), remoteConnectorCreated("test_double"))
                .execute(getQueryRunner(), remoteTrinoCreatedRemoteConnectorInserted("test_double"))
                .execute(getQueryRunner(), remoteConnectorCreateAsSelect("test_double"))
                .execute(getQueryRunner(), remoteConnectorCreateAndInsert("test_double"));
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "NULL", createDecimalType(3, 0), "CAST(NULL AS decimal(3, 0))")
                .addRoundTrip("decimal(38, 0)", "NULL", createDecimalType(38, 0), "CAST(NULL AS decimal(38, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('19' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('19' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('-193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('-193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.0' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "CAST('-10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('-10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(4, 2)", "CAST('2' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2' AS decimal(4, 2))")
                .addRoundTrip("decimal(4, 2)", "CAST('2.3' AS decimal(4, 2))", createDecimalType(4, 2), "CAST('2.3' AS decimal(4, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('2.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('2.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "CAST('123456789.3' AS decimal(24, 2))", createDecimalType(24, 2), "CAST('123456789.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 4)", "CAST('12345678901234567890.31' AS decimal(24, 4))", createDecimalType(24, 4), "CAST('12345678901234567890.31' AS decimal(24, 4))")
                .addRoundTrip("decimal(30, 5)", "CAST('3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(30, 5)", "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(38, 0)", "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('27182818284590452353602874713526624977' AS decimal(38, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('-27182818284590452353602874713526624977' AS decimal(38, 0))")
                .execute(getQueryRunner(), remoteConnectorCreated("test_decimal"))
                .execute(getQueryRunner(), remoteTrinoCreatedRemoteConnectorInserted("test_decimal"))
                .execute(getQueryRunner(), remoteConnectorCreateAsSelect("test_decimal"))
                .execute(getQueryRunner(), remoteConnectorCreateAndInsert("test_decimal"));
    }

    @Test
    public void testChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char(10)", "'text_a'", createCharType(10), "CAST('text_a' AS char(10))")
                .addRoundTrip("char(255)", "'text_b'", createCharType(255), "CAST('text_b' AS char(255))")
                .addRoundTrip("char(65535)", "'text_d'", createCharType(65535), "CAST('text_d' AS char(65535))")
                .addRoundTrip("char(5)", "'攻殻機動隊'", createCharType(5), "CAST('攻殻機動隊' AS char(5))")
                .addRoundTrip("char(32)", "'攻殻機動隊'", createCharType(32), "CAST('攻殻機動隊' AS char(32))")
                .addRoundTrip("char(20000)", "'攻殻機動隊'", createCharType(20000), "CAST('攻殻機動隊' AS char(20000))")
                .addRoundTrip("char(1)", "'😂'", createCharType(1), "CAST('😂' AS char(1))")
                .addRoundTrip("char(77)", "'Ну, погоди!'", createCharType(77), "CAST('Ну, погоди!' AS char(77))")
                .execute(getQueryRunner(), remoteConnectorCreated("test_char"))
                .execute(getQueryRunner(), remoteTrinoCreatedRemoteConnectorInserted("test_char"))
                .execute(getQueryRunner(), remoteConnectorCreateAsSelect("test_char"))
                .execute(getQueryRunner(), remoteConnectorCreateAndInsert("test_char"));
    }

    @Test
    public void testVarchar()
    {
        // varchar(n)
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("varchar(65535)", "'text_d'", createVarcharType(65535), "CAST('text_d' AS varchar(65535))")
                .addRoundTrip("varchar(5)", "'攻殻機動隊'", createVarcharType(5), "CAST('攻殻機動隊' AS varchar(5))")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", createVarcharType(32), "CAST('攻殻機動隊' AS varchar(32))")
                .addRoundTrip("varchar(20000)", "'攻殻機動隊'", createVarcharType(20000), "CAST('攻殻機動隊' AS varchar(20000))")
                .addRoundTrip("varchar(1)", "'😂'", createVarcharType(1), "CAST('😂' AS varchar(1))")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS varchar(77))")
                .addRoundTrip("varchar(10485760)", "'text_f'", createVarcharType(10485760), "CAST('text_f' AS varchar(10485760))")
                .execute(getQueryRunner(), remoteConnectorCreated("test_varchar"))
                .execute(getQueryRunner(), remoteTrinoCreatedRemoteConnectorInserted("test_varchar"))
                .execute(getQueryRunner(), remoteConnectorCreateAsSelect("test_varchar"))
                .execute(getQueryRunner(), remoteConnectorCreateAndInsert("test_varchar"));

        // varchar unbounded
        SqlDataTypeTest.create()
                .addRoundTrip("varchar", "'text_a'", createUnboundedVarcharType(), "CAST('text_a' AS varchar)")
                .addRoundTrip("varchar", "'text_b'", createUnboundedVarcharType(), "CAST('text_b' AS varchar)")
                .addRoundTrip("varchar", "'text_d'", createUnboundedVarcharType(), "CAST('text_d' AS varchar)")
                .addRoundTrip("varchar", "'攻殻機動隊'", createUnboundedVarcharType(), "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar", "'攻殻機動隊'", createUnboundedVarcharType(), "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar", "'攻殻機動隊'", createUnboundedVarcharType(), "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("varchar", "'😂'", createUnboundedVarcharType(), "CAST('😂' AS varchar)")
                .addRoundTrip("varchar", "'Ну, погоди!'", createUnboundedVarcharType(), "CAST('Ну, погоди!' AS varchar)")
                .addRoundTrip("varchar", "'text_f'", createUnboundedVarcharType(), "CAST('text_f' AS varchar)")
                .execute(getQueryRunner(), remoteConnectorCreated("test_varchar_unbounded"))
                .execute(getQueryRunner(), remoteTrinoCreatedRemoteConnectorInserted("test_varchar_unbounded"))
                .execute(getQueryRunner(), remoteConnectorCreateAsSelect("test_varchar_unbounded"))
                .execute(getQueryRunner(), remoteConnectorCreateAndInsert("test_varchar_unbounded"));
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
                .execute(getQueryRunner(), remoteConnectorCreated("test_varbinary"))
                .execute(getQueryRunner(), remoteTrinoCreatedRemoteConnectorInserted("test_varbinary"))
                .execute(getQueryRunner(), remoteConnectorCreateAsSelect("test_varbinary"))
                .execute(getQueryRunner(), remoteConnectorCreateAndInsert("test_varbinary"));
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

        SqlDataTypeTest testCases = SqlDataTypeTest.create()
                .addRoundTrip("date", "DATE '-5877641-06-23'", DATE, "DATE '-5877641-06-23'") // min value in Trino (the epoch is integer min)
                .addRoundTrip("date", "DATE '1582-10-04'", DATE, "DATE '1582-10-04'")
                .addRoundTrip("date", "DATE '1582-10-05'", DATE, "DATE '1582-10-05'") // begin julian->gregorian switch
                .addRoundTrip("date", "DATE '1582-10-14'", DATE, "DATE '1582-10-14'") // end julian->gregorian switch
                .addRoundTrip("date", "DATE '1582-10-15'", DATE, "DATE '1582-10-15'")
                .addRoundTrip("date", "DATE '1952-04-03'", DATE, "DATE '1952-04-03'") // before epoch
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'") // date of local time change forward at midnight in JVM zone
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'") // date of local time change forward at midnight in some zone
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'") // date of local time change backward at midnight in some zone
                .addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'") // historical date, surprisingly common in actual data
                .addRoundTrip("date", "DATE '9999-12-31'", DATE, "DATE '9999-12-31'")
                .addRoundTrip("date", "DATE '5881580-07-11'", DATE, "DATE '5881580-07-11'"); // max value in Trino (the epoch is integer max)
        for (String timeZoneId : List.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            testCases
                    .execute(getQueryRunner(), session, remoteConnectorCreated("test_date"))
                    .execute(getQueryRunner(), session, remoteTrinoCreatedRemoteConnectorInserted(session, "test_date"))
                    .execute(getQueryRunner(), session, remoteConnectorCreateAsSelect(session, "test_date"))
                    .execute(getQueryRunner(), session, remoteConnectorCreateAndInsert(session, "test_date"));
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

        SqlDataTypeTest.create()
                .addRoundTrip("time(9)", "TIME '23:59:59.000000000'", createTimeType(9), "TIME '23:59:59.000000000'")
                .addRoundTrip("time(9)", "TIME '23:59:59.123456789'", createTimeType(9), "TIME '23:59:59.123456789'")

                // minimum possible non-zero value for given precision
                .addRoundTrip("time(0)", "TIME '00:00:00'", createTimeType(0), "TIME '00:00:00'")
                .addRoundTrip("time(1)", "TIME '00:00:00.1'", createTimeType(1), "TIME '00:00:00.1'")
                .addRoundTrip("time(2)", "TIME '00:00:00.01'", createTimeType(2), "TIME '00:00:00.01'")
                .addRoundTrip("time(3)", "TIME '00:00:00.001'", createTimeType(3), "TIME '00:00:00.001'")
                .addRoundTrip("time(4)", "TIME '00:00:00.0001'", createTimeType(4), "TIME '00:00:00.0001'")
                .addRoundTrip("time(5)", "TIME '00:00:00.00001'", createTimeType(5), "TIME '00:00:00.00001'")
                .addRoundTrip("time(6)", "TIME '00:00:00.000001'", createTimeType(6), "TIME '00:00:00.000001'")
                .addRoundTrip("time(7)", "TIME '00:00:00.0000001'", createTimeType(7), "TIME '00:00:00.0000001'")
                .addRoundTrip("time(8)", "TIME '00:00:00.00000001'", createTimeType(8), "TIME '00:00:00.00000001'")
                .addRoundTrip("time(9)", "TIME '00:00:00.000000001'", createTimeType(9), "TIME '00:00:00.000000001'")
                .addRoundTrip("time(10)", "TIME '00:00:00.0000000001'", createTimeType(10), "TIME '00:00:00.0000000001'")
                .addRoundTrip("time(11)", "TIME '00:00:00.00000000001'", createTimeType(11), "TIME '00:00:00.00000000001'")
                .addRoundTrip("time(12)", "TIME '00:00:00.000000000001'", createTimeType(12), "TIME '00:00:00.000000000001'")

                // maximum possible value for given precision
                .addRoundTrip("time(0)", "TIME '23:59:59'", createTimeType(0), "TIME '23:59:59'")
                .addRoundTrip("time(1)", "TIME '23:59:59.9'", createTimeType(1), "TIME '23:59:59.9'")
                .addRoundTrip("time(2)", "TIME '23:59:59.99'", createTimeType(2), "TIME '23:59:59.99'")
                .addRoundTrip("time(3)", "TIME '23:59:59.999'", createTimeType(3), "TIME '23:59:59.999'")
                .addRoundTrip("time(4)", "TIME '23:59:59.9999'", createTimeType(4), "TIME '23:59:59.9999'")
                .addRoundTrip("time(5)", "TIME '23:59:59.99999'", createTimeType(5), "TIME '23:59:59.99999'")
                .addRoundTrip("time(6)", "TIME '23:59:59.999999'", createTimeType(6), "TIME '23:59:59.999999'")
                .addRoundTrip("time(7)", "TIME '23:59:59.9999999'", createTimeType(7), "TIME '23:59:59.9999999'")
                .addRoundTrip("time(8)", "TIME '23:59:59.99999999'", createTimeType(8), "TIME '23:59:59.99999999'")
                .addRoundTrip("time(9)", "TIME '23:59:59.999999999'", createTimeType(9), "TIME '23:59:59.999999999'")
                .addRoundTrip("time(10)", "TIME '23:59:59.9999999999'", createTimeType(10), "TIME '23:59:59.9999999999'")
                .addRoundTrip("time(11)", "TIME '23:59:59.99999999999'", createTimeType(11), "TIME '23:59:59.99999999999'")
                .addRoundTrip("time(12)", "TIME '23:59:59.999999999999'", createTimeType(12), "TIME '23:59:59.999999999999'")

                // epoch is also a gap in JVM zone
                .addRoundTrip("time(0)", "TIME '00:00:00'", createTimeType(0), "TIME '00:00:00'")
                .addRoundTrip("time(3)", "TIME '00:00:00.000'", createTimeType(3), "TIME '00:00:00.000'")
                .addRoundTrip("time(6)", "TIME '00:00:00.000000'", createTimeType(6), "TIME '00:00:00.000000'")
                .addRoundTrip("time(9)", "TIME '00:00:00.000000000'", createTimeType(9), "TIME '00:00:00.000000000'")

                // time gap in JVM zone
                .addRoundTrip("time(0)", "TIME '00:12:34'", createTimeType(0), "TIME '00:12:34'")
                .addRoundTrip("time(3)", "TIME '00:12:34.567'", createTimeType(3), "TIME '00:12:34.567'")
                .addRoundTrip("time(6)", "TIME '00:12:34.567123'", createTimeType(6), "TIME '00:12:34.567123'")
                .addRoundTrip("time(9)", "TIME '00:12:34.567123456'", createTimeType(9), "TIME '00:12:34.567123456'")

                .execute(getQueryRunner(), session, remoteConnectorCreated("test_time"))
                .execute(getQueryRunner(), session, remoteTrinoCreatedRemoteConnectorInserted(session, "test_time"))
                .execute(getQueryRunner(), session, remoteConnectorCreateAsSelect(session, "test_time"))
                .execute(getQueryRunner(), session, remoteConnectorCreateAndInsert(session, "test_time"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimeWithTimeZone(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        LocalTime timeGapInJvmZone = LocalTime.of(0, 12, 34);
        checkIsGap(jvmZone, timeGapInJvmZone.atDate(EPOCH_DAY));

        SqlDataTypeTest.create()
                .addRoundTrip("time(0) with time zone", "TIME '01:12:34+00:00'", createTimeWithTimeZoneType(0), "TIME '01:12:34+00:00'")
                .addRoundTrip("time(0) with time zone", "TIME '01:12:34+07:35'", createTimeWithTimeZoneType(0), "TIME '01:12:34+07:35'")
                .addRoundTrip("time(0) with time zone", "TIME '01:12:34-07:35'", createTimeWithTimeZoneType(0), "TIME '01:12:34-07:35'")

                .addRoundTrip("time(9) with time zone", "TIME '23:59:59.000000000+00:00'", createTimeWithTimeZoneType(9), "TIME '23:59:59.000000000+00:00'")
                .addRoundTrip("time(9) with time zone", "TIME '23:59:59.123456789+00:00'", createTimeWithTimeZoneType(9), "TIME '23:59:59.123456789+00:00'")

                // zero time in various zones
                .addRoundTrip("time(0) with time zone", "TIME '00:00:00 +00:00'", createTimeWithTimeZoneType(0), "TIME '00:00:00 +00:00'")
                .addRoundTrip("time(0) with time zone", "TIME '00:00:00 -00:00'", createTimeWithTimeZoneType(0), "TIME '00:00:00 -00:00'")
                .addRoundTrip("time(0) with time zone", "TIME '00:00:00 +02:00'", createTimeWithTimeZoneType(0), "TIME '00:00:00 +02:00'")
                .addRoundTrip("time(0) with time zone", "TIME '00:00:00 -08:00'", createTimeWithTimeZoneType(0), "TIME '00:00:00 -08:00'")
                .addRoundTrip("time(0) with time zone", "TIME '00:00:00 +00:00'", createTimeWithTimeZoneType(0), "TIME '00:00:00 +00:00'")
                .addRoundTrip("time(0) with time zone", "TIME '00:00:00 +03:34'", createTimeWithTimeZoneType(0), "TIME '00:00:00 +03:34'")
                .addRoundTrip("time(0) with time zone", "TIME '00:00:00 -07:12'", createTimeWithTimeZoneType(0), "TIME '00:00:00 -07:12'")

                // minimum possible positive value for given precision
                .addRoundTrip("time(0) with time zone", "TIME '00:00:00 +05:45'", createTimeWithTimeZoneType(0), "TIME '00:00:00 +05:45'")
                .addRoundTrip("time(1) with time zone", "TIME '00:00:00.1 +05:45'", createTimeWithTimeZoneType(1), "TIME '00:00:00.1 +05:45'")
                .addRoundTrip("time(2) with time zone", "TIME '00:00:00.01 +05:45'", createTimeWithTimeZoneType(2), "TIME '00:00:00.01 +05:45'")
                .addRoundTrip("time(3) with time zone", "TIME '00:00:00.001 +05:45'", createTimeWithTimeZoneType(3), "TIME '00:00:00.001 +05:45'")
                .addRoundTrip("time(4) with time zone", "TIME '00:00:00.0001 +05:45'", createTimeWithTimeZoneType(4), "TIME '00:00:00.0001 +05:45'")
                .addRoundTrip("time(5) with time zone", "TIME '00:00:00.00001 +05:45'", createTimeWithTimeZoneType(5), "TIME '00:00:00.00001 +05:45'")
                .addRoundTrip("time(6) with time zone", "TIME '00:00:00.000001 +05:45'", createTimeWithTimeZoneType(6), "TIME '00:00:00.000001 +05:45'")
                .addRoundTrip("time(7) with time zone", "TIME '00:00:00.0000001 +05:45'", createTimeWithTimeZoneType(7), "TIME '00:00:00.0000001 +05:45'")
                .addRoundTrip("time(8) with time zone", "TIME '00:00:00.00000001 +05:45'", createTimeWithTimeZoneType(8), "TIME '00:00:00.00000001 +05:45'")
                .addRoundTrip("time(9) with time zone", "TIME '00:00:00.000000001 +05:45'", createTimeWithTimeZoneType(9), "TIME '00:00:00.000000001 +05:45'")
                .addRoundTrip("time(10) with time zone", "TIME '00:00:00.0000000001 +05:45'", createTimeWithTimeZoneType(10), "TIME '00:00:00.0000000001 +05:45'")
                .addRoundTrip("time(11) with time zone", "TIME '00:00:00.00000000001 +05:45'", createTimeWithTimeZoneType(11), "TIME '00:00:00.00000000001 +05:45'")
                .addRoundTrip("time(12) with time zone", "TIME '00:00:00.000000000001 +05:45'", createTimeWithTimeZoneType(12), "TIME '00:00:00.000000000001 +05:45'")

                // maximum possible value for given precision
                .addRoundTrip("time(0) with time zone", "TIME '23:59:59 +05:45'", createTimeWithTimeZoneType(0), "TIME '23:59:59 +05:45'")
                .addRoundTrip("time(1) with time zone", "TIME '23:59:59.9 +05:45'", createTimeWithTimeZoneType(1), "TIME '23:59:59.9 +05:45'")
                .addRoundTrip("time(2) with time zone", "TIME '23:59:59.99 +05:45'", createTimeWithTimeZoneType(2), "TIME '23:59:59.99 +05:45'")
                .addRoundTrip("time(3) with time zone", "TIME '23:59:59.999 +05:45'", createTimeWithTimeZoneType(3), "TIME '23:59:59.999 +05:45'")
                .addRoundTrip("time(4) with time zone", "TIME '23:59:59.9999 +05:45'", createTimeWithTimeZoneType(4), "TIME '23:59:59.9999 +05:45'")
                .addRoundTrip("time(5) with time zone", "TIME '23:59:59.99999 +05:45'", createTimeWithTimeZoneType(5), "TIME '23:59:59.99999 +05:45'")
                .addRoundTrip("time(6) with time zone", "TIME '23:59:59.999999 +05:45'", createTimeWithTimeZoneType(6), "TIME '23:59:59.999999 +05:45'")
                .addRoundTrip("time(7) with time zone", "TIME '23:59:59.9999999 +05:45'", createTimeWithTimeZoneType(7), "TIME '23:59:59.9999999 +05:45'")
                .addRoundTrip("time(8) with time zone", "TIME '23:59:59.99999999 +05:45'", createTimeWithTimeZoneType(8), "TIME '23:59:59.99999999 +05:45'")
                .addRoundTrip("time(9) with time zone", "TIME '23:59:59.999999999 +05:45'", createTimeWithTimeZoneType(9), "TIME '23:59:59.999999999 +05:45'")
                .addRoundTrip("time(10) with time zone", "TIME '23:59:59.9999999999 +05:45'", createTimeWithTimeZoneType(10), "TIME '23:59:59.9999999999 +05:45'")
                .addRoundTrip("time(11) with time zone", "TIME '23:59:59.99999999999 +05:45'", createTimeWithTimeZoneType(11), "TIME '23:59:59.99999999999 +05:45'")
                .addRoundTrip("time(12) with time zone", "TIME '23:59:59.999999999999 +05:45'", createTimeWithTimeZoneType(12), "TIME '23:59:59.999999999999 +05:45'")

                // epoch is also a gap in JVM zone
                .addRoundTrip("time(0) with time zone", "TIME '00:00:00+00:00'", createTimeWithTimeZoneType(0), "TIME '00:00:00+00:00'")
                .addRoundTrip("time(3) with time zone", "TIME '00:00:00.000+00:00'", createTimeWithTimeZoneType(3), "TIME '00:00:00.000+00:00'")
                .addRoundTrip("time(6) with time zone", "TIME '00:00:00.000000+00:00'", createTimeWithTimeZoneType(6), "TIME '00:00:00.000000+00:00'")
                .addRoundTrip("time(9) with time zone", "TIME '00:00:00.000000000+00:00'", createTimeWithTimeZoneType(9), "TIME '00:00:00.000000000+00:00'")

                .addRoundTrip("time(0) with time zone", "TIME '00:00:00+07:35'", createTimeWithTimeZoneType(0), "TIME '00:00:00+07:35'")
                .addRoundTrip("time(3) with time zone", "TIME '00:00:00.000+07:35'", createTimeWithTimeZoneType(3), "TIME '00:00:00.000+07:35'")
                .addRoundTrip("time(6) with time zone", "TIME '00:00:00.000000+07:35'", createTimeWithTimeZoneType(6), "TIME '00:00:00.000000+07:35'")
                .addRoundTrip("time(9) with time zone", "TIME '00:00:00.000000000+07:35'", createTimeWithTimeZoneType(9), "TIME '00:00:00.000000000+07:35'")

                // time gap in JVM zone
                .addRoundTrip("time(3) with time zone", "TIME '00:12:34.567+00:00'", createTimeWithTimeZoneType(3), "TIME '00:12:34.567+00:00'")
                .addRoundTrip("time(9) with time zone", "TIME '00:12:34.567123456+00:00'", createTimeWithTimeZoneType(9), "TIME '00:12:34.567123456+00:00'")
                .addRoundTrip("time(3) with time zone", "TIME '00:12:34.567+07:35'", createTimeWithTimeZoneType(3), "TIME '00:12:34.567+07:35'")
                .addRoundTrip("time(9) with time zone", "TIME '00:12:34.567123456+07:35'", createTimeWithTimeZoneType(9), "TIME '00:12:34.567123456+07:35'")

                .execute(getQueryRunner(), session, remoteConnectorCreated("test_time_with_time_zone"))
                .execute(getQueryRunner(), session, remoteTrinoCreatedRemoteConnectorInserted(session, "test_time_with_time_zone"))
                .execute(getQueryRunner(), session, remoteConnectorCreateAsSelect(session, "test_time_with_time_zone"))
                .execute(getQueryRunner(), session, remoteConnectorCreateAndInsert(session, "test_time_with_time_zone"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestamp(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1958-01-01 13:18:03.123'", createTimestampType(3), "TIMESTAMP '1958-01-01 13:18:03.123'") // before epoch
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", createTimestampType(3), "TIMESTAMP '2019-03-18 10:01:17.987'") // after epoch
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", createTimestampType(3), "TIMESTAMP '2018-10-28 01:33:17.456'") // time doubled in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 03:33:33.333'", createTimestampType(3), "TIMESTAMP '2018-10-28 03:33:33.333'") // time doubled in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:00.000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.000'") // epoch is also a gap in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:13:42.000'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:13:42.000'") // time gap in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-04-01 02:13:55.123'", createTimestampType(3), "TIMESTAMP '2018-04-01 02:13:55.123'") // time gap in JVM zone
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-03-25 03:17:17.000'", createTimestampType(3), "TIMESTAMP '2018-03-25 03:17:17.000'") // time gap in Vilnius
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1986-01-01 00:13:07.000'", createTimestampType(3), "TIMESTAMP '1986-01-01 00:13:07.000'") // time gap in Kathmandu

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
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01 00:00:00'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:00'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 00:00:00.1'", createTimestampType(1), "TIMESTAMP '1970-01-01 00:00:00.1'")
                .addRoundTrip("timestamp(2)", "TIMESTAMP '1970-01-01 00:00:00.12'", createTimestampType(2), "TIMESTAMP '1970-01-01 00:00:00.12'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:00.123'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.123'")
                .addRoundTrip("timestamp(4)", "TIMESTAMP '1970-01-01 00:00:00.1234'", createTimestampType(4), "TIMESTAMP '1970-01-01 00:00:00.1234'")
                .addRoundTrip("timestamp(5)", "TIMESTAMP '1970-01-01 00:00:00.12345'", createTimestampType(5), "TIMESTAMP '1970-01-01 00:00:00.12345'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.123456'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.123456'")
                .addRoundTrip("timestamp(7)", "TIMESTAMP '1970-01-01 00:00:00.1234567'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.1234567'")
                .addRoundTrip("timestamp(8)", "TIMESTAMP '1970-01-01 00:00:00.12345678'", createTimestampType(8), "TIMESTAMP '1970-01-01 00:00:00.12345678'")
                .addRoundTrip("timestamp(9)", "TIMESTAMP '1970-01-01 00:00:00.123456789'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.123456789'")

                // minimum possible positive value for given precision
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01 00:00:00'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:00'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 00:00:00.1'", createTimestampType(1), "TIMESTAMP '1970-01-01 00:00:00.1'")
                .addRoundTrip("timestamp(2)", "TIMESTAMP '1970-01-01 00:00:00.01'", createTimestampType(2), "TIMESTAMP '1970-01-01 00:00:00.01'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 00:00:00.001'", createTimestampType(3), "TIMESTAMP '1970-01-01 00:00:00.001'")
                .addRoundTrip("timestamp(4)", "TIMESTAMP '1970-01-01 00:00:00.0001'", createTimestampType(4), "TIMESTAMP '1970-01-01 00:00:00.0001'")
                .addRoundTrip("timestamp(5)", "TIMESTAMP '1970-01-01 00:00:00.00001'", createTimestampType(5), "TIMESTAMP '1970-01-01 00:00:00.00001'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.000001'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.000001'")
                .addRoundTrip("timestamp(7)", "TIMESTAMP '1970-01-01 00:00:00.0000001'", createTimestampType(7), "TIMESTAMP '1970-01-01 00:00:00.0000001'")
                .addRoundTrip("timestamp(8)", "TIMESTAMP '1970-01-01 00:00:00.00000001'", createTimestampType(8), "TIMESTAMP '1970-01-01 00:00:00.00000001'")
                .addRoundTrip("timestamp(9)", "TIMESTAMP '1970-01-01 00:00:00.000000001'", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.000000001'")
                .addRoundTrip("timestamp(10)", "TIMESTAMP '1970-01-01 00:00:00.0000000001'", createTimestampType(10), "TIMESTAMP '1970-01-01 00:00:00.0000000001'")
                .addRoundTrip("timestamp(11)", "TIMESTAMP '1970-01-01 00:00:00.00000000001'", createTimestampType(11), "TIMESTAMP '1970-01-01 00:00:00.00000000001'")
                .addRoundTrip("timestamp(12)", "TIMESTAMP '1970-01-01 00:00:00.000000000001'", createTimestampType(12), "TIMESTAMP '1970-01-01 00:00:00.000000000001'")

                // maximum possible time of the day
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1970-01-01 23:59:59'", createTimestampType(0), "TIMESTAMP '1970-01-01 23:59:59'")
                .addRoundTrip("timestamp(1)", "TIMESTAMP '1970-01-01 23:59:59.9'", createTimestampType(1), "TIMESTAMP '1970-01-01 23:59:59.9'")
                .addRoundTrip("timestamp(2)", "TIMESTAMP '1970-01-01 23:59:59.99'", createTimestampType(2), "TIMESTAMP '1970-01-01 23:59:59.99'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1970-01-01 23:59:59.999'", createTimestampType(3), "TIMESTAMP '1970-01-01 23:59:59.999'")
                .addRoundTrip("timestamp(4)", "TIMESTAMP '1970-01-01 23:59:59.9999'", createTimestampType(4), "TIMESTAMP '1970-01-01 23:59:59.9999'")
                .addRoundTrip("timestamp(5)", "TIMESTAMP '1970-01-01 23:59:59.99999'", createTimestampType(5), "TIMESTAMP '1970-01-01 23:59:59.99999'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1970-01-01 23:59:59.999999'", createTimestampType(6), "TIMESTAMP '1970-01-01 23:59:59.999999'")
                .addRoundTrip("timestamp(7)", "TIMESTAMP '1970-01-01 23:59:59.9999999'", createTimestampType(7), "TIMESTAMP '1970-01-01 23:59:59.9999999'")
                .addRoundTrip("timestamp(8)", "TIMESTAMP '1970-01-01 23:59:59.99999999'", createTimestampType(8), "TIMESTAMP '1970-01-01 23:59:59.99999999'")
                .addRoundTrip("timestamp(9)", "TIMESTAMP '1970-01-01 23:59:59.999999999'", createTimestampType(9), "TIMESTAMP '1970-01-01 23:59:59.999999999'")
                .addRoundTrip("timestamp(10)", "TIMESTAMP '1970-01-01 23:59:59.9999999999'", createTimestampType(10), "TIMESTAMP '1970-01-01 23:59:59.9999999999'")
                .addRoundTrip("timestamp(11)", "TIMESTAMP '1970-01-01 23:59:59.99999999999'", createTimestampType(11), "TIMESTAMP '1970-01-01 23:59:59.99999999999'")
                .addRoundTrip("timestamp(12)", "TIMESTAMP '1970-01-01 23:59:59.999999999999'", createTimestampType(12), "TIMESTAMP '1970-01-01 23:59:59.999999999999'")

                // before epoch
                .addRoundTrip("timestamp(0)", "TIMESTAMP '1969-12-31 23:59:59'", createTimestampType(0), "TIMESTAMP '1969-12-31 23:59:59'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '1969-12-31 23:59:59.999'", createTimestampType(3), "TIMESTAMP '1969-12-31 23:59:59.999'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '1969-12-31 23:59:59.999999'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999999'")
                .addRoundTrip("timestamp(9)", "TIMESTAMP '1969-12-31 23:59:59.999999999'", createTimestampType(9), "TIMESTAMP '1969-12-31 23:59:59.999999999'")
                .addRoundTrip("timestamp(12)", "TIMESTAMP '1969-12-31 23:59:59.999999999999'", createTimestampType(12), "TIMESTAMP '1969-12-31 23:59:59.999999999999'")

                // historical date, surprisingly common in actual data
                .addRoundTrip("timestamp(0)", "TIMESTAMP '0001-01-01 00:00:00'", createTimestampType(0), "TIMESTAMP '0001-01-01 00:00:00'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '0001-01-01 00:00:00.000'", createTimestampType(3), "TIMESTAMP '0001-01-01 00:00:00.000'")
                .addRoundTrip("timestamp(6)", "TIMESTAMP '0001-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '0001-01-01 00:00:00.000000'")
                .addRoundTrip("timestamp(9)", "TIMESTAMP '0001-01-01 00:00:00.000000000'", createTimestampType(9), "TIMESTAMP '0001-01-01 00:00:00.000000000'")
                .addRoundTrip("timestamp(12)", "TIMESTAMP '0001-01-01 00:00:00.000000000000'", createTimestampType(12), "TIMESTAMP '0001-01-01 00:00:00.000000000000'")

                // negative year
                .addRoundTrip("timestamp(3)", "TIMESTAMP '-0042-01-01 01:23:45.123'", createTimestampType(3), "TIMESTAMP '-0042-01-01 01:23:45.123'")
                .addRoundTrip("timestamp(12)", "TIMESTAMP '-0042-01-01 01:23:45.123456789012'", createTimestampType(12), "TIMESTAMP '-0042-01-01 01:23:45.123456789012'")

                // beyond four-digit year, rendered with a plus sign in various places, including Trino response
                .addRoundTrip("timestamp(3)", "TIMESTAMP '123456-01-01 01:23:45.123'", createTimestampType(3), "TIMESTAMP '123456-01-01 01:23:45.123'")
                .addRoundTrip("timestamp(12)", "TIMESTAMP '123456-01-01 01:23:45.123456789012'", createTimestampType(12), "TIMESTAMP '123456-01-01 01:23:45.123456789012'")

                .execute(getQueryRunner(), session, remoteConnectorCreated("test_timestamp"))
                .execute(getQueryRunner(), session, remoteTrinoCreatedRemoteConnectorInserted(session, "test_timestamp"))
                .execute(getQueryRunner(), session, remoteConnectorCreateAsSelect(session, "test_timestamp"))
                .execute(getQueryRunner(), session, remoteConnectorCreateAndInsert(session, "test_timestamp"));
    }

    @Test(dataProvider = "sessionZonesDataProvider")
    public void testTimestampWithTimeZone(ZoneId sessionZone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                // epoch
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.000 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:00.000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.000 Asia/Kathmandu'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:00.000 Asia/Kathmandu'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.000 +02:17'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:00.000 +02:17'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.000 -07:31'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:00.000 -07:31'")

                // before epoch
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1958-01-01 13:18:03.123 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1958-01-01 13:18:03.123 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1958-01-01 13:18:03.123 Asia/Kathmandu'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1958-01-01 13:18:03.123 Asia/Kathmandu'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1958-01-01 13:18:03.123 +02:17'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1958-01-01 13:18:03.123 +02:17'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1958-01-01 13:18:03.123 -07:31'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1958-01-01 13:18:03.123 -07:31'")

                // after epoch
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2019-03-18 10:01:17.987 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2019-03-18 10:01:17.987 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2019-03-18 10:01:17.987 Asia/Kathmandu'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2019-03-18 10:01:17.987 Asia/Kathmandu'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2019-03-18 10:01:17.987 +02:17'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2019-03-18 10:01:17.987 +02:17'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2019-03-18 10:01:17.987 -07:31'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2019-03-18 10:01:17.987 -07:31'")

                // time doubled in JVM zone
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2018-10-28 01:33:17.456 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-10-28 01:33:17.456 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2018-10-28 01:33:17.456 America/Bahia_Banderas'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-10-28 01:33:17.456 America/Bahia_Banderas'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2018-10-28 01:33:17.456 Asia/Kathmandu'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-10-28 01:33:17.456 Asia/Kathmandu'")

                // time doubled in Vilnius
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2018-10-28 03:33:33.333 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-10-28 03:33:33.333 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2018-10-28 03:33:33.333 Europe/Vilnius'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-10-28 03:33:33.333 Europe/Vilnius'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2018-10-28 03:33:33.333 Asia/Kathmandu'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-10-28 03:33:33.333 Asia/Kathmandu'")

                // time gap in JVM zone
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:13:42.000 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:13:42.000 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:13:42.000 Asia/Kathmandu'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:13:42.000 Asia/Kathmandu'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2018-04-01 02:13:55.123 UTC'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-04-01 02:13:55.123 UTC'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2018-04-01 02:13:55.123 Asia/Kathmandu'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-04-01 02:13:55.123 Asia/Kathmandu'")

                // time gap in Vilnius
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '2018-03-25 03:17:17.000 Asia/Kathmandu'", createTimestampWithTimeZoneType(3), "TIMESTAMP '2018-03-25 03:17:17.000 Asia/Kathmandu'")

                // time gap in Kathmandu
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1986-01-01 00:13:07.000 Europe/Vilnius'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1986-01-01 00:13:07.000 Europe/Vilnius'")

                // minimum possible positive value for given precision
                .addRoundTrip("timestamp(0) with time zone", "TIMESTAMP '1970-01-01 00:00:00 Asia/Kathmandu'", createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 00:00:00 Asia/Kathmandu'")
                .addRoundTrip("timestamp(1) with time zone", "TIMESTAMP '1970-01-01 00:00:00.1 Asia/Kathmandu'", createTimestampWithTimeZoneType(1), "TIMESTAMP '1970-01-01 00:00:00.1 Asia/Kathmandu'")
                .addRoundTrip("timestamp(2) with time zone", "TIMESTAMP '1970-01-01 00:00:00.01 Asia/Kathmandu'", createTimestampWithTimeZoneType(2), "TIMESTAMP '1970-01-01 00:00:00.01 Asia/Kathmandu'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 00:00:00.001 Asia/Kathmandu'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 00:00:00.001 Asia/Kathmandu'")
                .addRoundTrip("timestamp(4) with time zone", "TIMESTAMP '1970-01-01 00:00:00.0001 Asia/Kathmandu'", createTimestampWithTimeZoneType(4), "TIMESTAMP '1970-01-01 00:00:00.0001 Asia/Kathmandu'")
                .addRoundTrip("timestamp(5) with time zone", "TIMESTAMP '1970-01-01 00:00:00.00001 Asia/Kathmandu'", createTimestampWithTimeZoneType(5), "TIMESTAMP '1970-01-01 00:00:00.00001 Asia/Kathmandu'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 00:00:00.000001 Asia/Kathmandu'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-01 00:00:00.000001 Asia/Kathmandu'")
                .addRoundTrip("timestamp(7) with time zone", "TIMESTAMP '1970-01-01 00:00:00.0000001 Asia/Kathmandu'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1970-01-01 00:00:00.0000001 Asia/Kathmandu'")
                .addRoundTrip("timestamp(8) with time zone", "TIMESTAMP '1970-01-01 00:00:00.00000001 Asia/Kathmandu'", createTimestampWithTimeZoneType(8), "TIMESTAMP '1970-01-01 00:00:00.00000001 Asia/Kathmandu'")
                .addRoundTrip("timestamp(9) with time zone", "TIMESTAMP '1970-01-01 00:00:00.000000001 Asia/Kathmandu'", createTimestampWithTimeZoneType(9), "TIMESTAMP '1970-01-01 00:00:00.000000001 Asia/Kathmandu'")
                .addRoundTrip("timestamp(10) with time zone", "TIMESTAMP '1970-01-01 00:00:00.0000000001 Asia/Kathmandu'", createTimestampWithTimeZoneType(10), "TIMESTAMP '1970-01-01 00:00:00.0000000001 Asia/Kathmandu'")
                .addRoundTrip("timestamp(11) with time zone", "TIMESTAMP '1970-01-01 00:00:00.00000000001 Asia/Kathmandu'", createTimestampWithTimeZoneType(11), "TIMESTAMP '1970-01-01 00:00:00.00000000001 Asia/Kathmandu'")
                .addRoundTrip("timestamp(12) with time zone", "TIMESTAMP '1970-01-01 00:00:00.000000000001 Asia/Kathmandu'", createTimestampWithTimeZoneType(12), "TIMESTAMP '1970-01-01 00:00:00.000000000001 Asia/Kathmandu'")

                // maximum possible time of the day
                .addRoundTrip("timestamp(0) with time zone", "TIMESTAMP '1970-01-01 23:59:59 Asia/Kathmandu'", createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 23:59:59 Asia/Kathmandu'")
                .addRoundTrip("timestamp(1) with time zone", "TIMESTAMP '1970-01-01 23:59:59.9 Asia/Kathmandu'", createTimestampWithTimeZoneType(1), "TIMESTAMP '1970-01-01 23:59:59.9 Asia/Kathmandu'")
                .addRoundTrip("timestamp(2) with time zone", "TIMESTAMP '1970-01-01 23:59:59.99 Asia/Kathmandu'", createTimestampWithTimeZoneType(2), "TIMESTAMP '1970-01-01 23:59:59.99 Asia/Kathmandu'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1970-01-01 23:59:59.999 Asia/Kathmandu'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1970-01-01 23:59:59.999 Asia/Kathmandu'")
                .addRoundTrip("timestamp(4) with time zone", "TIMESTAMP '1970-01-01 23:59:59.9999 Asia/Kathmandu'", createTimestampWithTimeZoneType(4), "TIMESTAMP '1970-01-01 23:59:59.9999 Asia/Kathmandu'")
                .addRoundTrip("timestamp(5) with time zone", "TIMESTAMP '1970-01-01 23:59:59.99999 Asia/Kathmandu'", createTimestampWithTimeZoneType(5), "TIMESTAMP '1970-01-01 23:59:59.99999 Asia/Kathmandu'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1970-01-01 23:59:59.999999 Asia/Kathmandu'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1970-01-01 23:59:59.999999 Asia/Kathmandu'")
                .addRoundTrip("timestamp(7) with time zone", "TIMESTAMP '1970-01-01 23:59:59.9999999 Asia/Kathmandu'", createTimestampWithTimeZoneType(7), "TIMESTAMP '1970-01-01 23:59:59.9999999 Asia/Kathmandu'")
                .addRoundTrip("timestamp(8) with time zone", "TIMESTAMP '1970-01-01 23:59:59.99999999 Asia/Kathmandu'", createTimestampWithTimeZoneType(8), "TIMESTAMP '1970-01-01 23:59:59.99999999 Asia/Kathmandu'")
                .addRoundTrip("timestamp(9) with time zone", "TIMESTAMP '1970-01-01 23:59:59.999999999 Asia/Kathmandu'", createTimestampWithTimeZoneType(9), "TIMESTAMP '1970-01-01 23:59:59.999999999 Asia/Kathmandu'")
                .addRoundTrip("timestamp(10) with time zone", "TIMESTAMP '1970-01-01 23:59:59.9999999999 Asia/Kathmandu'", createTimestampWithTimeZoneType(10), "TIMESTAMP '1970-01-01 23:59:59.9999999999 Asia/Kathmandu'")
                .addRoundTrip("timestamp(11) with time zone", "TIMESTAMP '1970-01-01 23:59:59.99999999999 Asia/Kathmandu'", createTimestampWithTimeZoneType(11), "TIMESTAMP '1970-01-01 23:59:59.99999999999 Asia/Kathmandu'")
                .addRoundTrip("timestamp(12) with time zone", "TIMESTAMP '1970-01-01 23:59:59.999999999999 Asia/Kathmandu'", createTimestampWithTimeZoneType(12), "TIMESTAMP '1970-01-01 23:59:59.999999999999 Asia/Kathmandu'")

                // negative epoch
                .addRoundTrip("timestamp(0) with time zone", "TIMESTAMP '1969-12-31 23:59:59 Asia/Kathmandu'", createTimestampWithTimeZoneType(0), "TIMESTAMP '1969-12-31 23:59:59 Asia/Kathmandu'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '1969-12-31 23:59:59.999 Asia/Kathmandu'", createTimestampWithTimeZoneType(3), "TIMESTAMP '1969-12-31 23:59:59.999 Asia/Kathmandu'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '1969-12-31 23:59:59.999999 Asia/Kathmandu'", createTimestampWithTimeZoneType(6), "TIMESTAMP '1969-12-31 23:59:59.999999 Asia/Kathmandu'")
                .addRoundTrip("timestamp(9) with time zone", "TIMESTAMP '1969-12-31 23:59:59.999999999 Asia/Kathmandu'", createTimestampWithTimeZoneType(9), "TIMESTAMP '1969-12-31 23:59:59.999999999 Asia/Kathmandu'")
                .addRoundTrip("timestamp(12) with time zone", "TIMESTAMP '1969-12-31 23:59:59.999999999999 Asia/Kathmandu'", createTimestampWithTimeZoneType(12), "TIMESTAMP '1969-12-31 23:59:59.999999999999 Asia/Kathmandu'")

                // historical date, surprisingly common in actual data
                .addRoundTrip("timestamp(0) with time zone", "TIMESTAMP '0001-01-01 00:00:00 Asia/Kathmandu'", createTimestampWithTimeZoneType(0), "TIMESTAMP '0001-01-01 00:00:00 Asia/Kathmandu'")
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '0001-01-01 00:00:00.000 Asia/Kathmandu'", createTimestampWithTimeZoneType(3), "TIMESTAMP '0001-01-01 00:00:00.000 Asia/Kathmandu'")
                .addRoundTrip("timestamp(6) with time zone", "TIMESTAMP '0001-01-01 00:00:00.000000 Asia/Kathmandu'", createTimestampWithTimeZoneType(6), "TIMESTAMP '0001-01-01 00:00:00.000000 Asia/Kathmandu'")
                .addRoundTrip("timestamp(9) with time zone", "TIMESTAMP '0001-01-01 00:00:00.000000000 Asia/Kathmandu'", createTimestampWithTimeZoneType(9), "TIMESTAMP '0001-01-01 00:00:00.000000000 Asia/Kathmandu'")
                .addRoundTrip("timestamp(12) with time zone", "TIMESTAMP '0001-01-01 00:00:00.000000000000 Asia/Kathmandu'", createTimestampWithTimeZoneType(12), "TIMESTAMP '0001-01-01 00:00:00.000000000000 Asia/Kathmandu'")

                // negative year
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '-0042-01-01 01:23:45.123 Asia/Kathmandu'", createTimestampWithTimeZoneType(3), "TIMESTAMP '-0042-01-01 01:23:45.123 Asia/Kathmandu'")
                .addRoundTrip("timestamp(12) with time zone", "TIMESTAMP '-0042-01-01 01:23:45.123456789012 Asia/Kathmandu'", createTimestampWithTimeZoneType(12), "TIMESTAMP '-0042-01-01 01:23:45.123456789012 Asia/Kathmandu'")

                // beyond four-digit year, rendered with a plus sign in various places, including Trino response
                .addRoundTrip("timestamp(3) with time zone", "TIMESTAMP '12345-01-01 01:23:45.123 Asia/Kathmandu'", createTimestampWithTimeZoneType(3), "TIMESTAMP '12345-01-01 01:23:45.123 Asia/Kathmandu'")
                .addRoundTrip("timestamp(12) with time zone", "TIMESTAMP '12345-01-01 01:23:45.123456789012 Asia/Kathmandu'", createTimestampWithTimeZoneType(12), "TIMESTAMP '12345-01-01 01:23:45.123456789012 Asia/Kathmandu'")

                // different forms of time zones
                .addRoundTrip("timestamp(0) with time zone", "TIMESTAMP '1970-01-01 00:00:00 Z'", createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 00:00:00 Z'")
                .addRoundTrip("timestamp(0) with time zone", "TIMESTAMP '1970-01-01 00:00:00 UTC'", createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 00:00:00 UTC'")
                .addRoundTrip("timestamp(0) with time zone", "TIMESTAMP '1970-01-01 00:00:00 Europe/Warsaw'", createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 00:00:00 Europe/Warsaw'")
                .addRoundTrip("timestamp(0) with time zone", "TIMESTAMP '1970-01-01 00:00:00 America/Los_Angeles'", createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 00:00:00 America/Los_Angeles'")
                .addRoundTrip("timestamp(0) with time zone", "TIMESTAMP '1970-01-01 00:00:00 +00:00'", createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 00:00:00 +00:00'")
                .addRoundTrip("timestamp(0) with time zone", "TIMESTAMP '1970-01-01 00:00:00 +03:34'", createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 00:00:00 +03:34'")
                .addRoundTrip("timestamp(0) with time zone", "TIMESTAMP '1970-01-01 00:00:00 -07:12'", createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 00:00:00 -07:12'")

                .execute(getQueryRunner(), session, remoteConnectorCreated("test_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, remoteTrinoCreatedRemoteConnectorInserted(session, "test_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, remoteConnectorCreateAsSelect(session, "test_timestamp_with_time_zone"))
                .execute(getQueryRunner(), session, remoteConnectorCreateAndInsert(session, "test_timestamp_with_time_zone"));
    }

    @Test
    public void testForcedMappingToVarchar()
    {
        try (TestTable table = new TestTable(
                remoteExecutor,
                "test_forced_mapping_to_varchar",
                // TODO use synthetic testing type installed on remote Trino only
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
        // TODO use synthetic testing type installed on remote Trino only; or use ColorType (requires fixing its representation in JDBC)
        String unsupportedDataType = "uuid";
        String exampleValue = "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'";

        testUnsupportedDataTypeAsIgnored(getSession(), unsupportedDataType, exampleValue);
        testUnsupportedDataTypeConvertedToVarchar(getSession(), unsupportedDataType, exampleValue, "'12151fd2-7586-11e9-8f9e-2a86e4085a59'");
    }

    @Test
    public void testJson()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("json", "CAST(NULL AS JSON)", JsonType.JSON)
                .addRoundTrip("json", "JSON '{}'", JsonType.JSON)
                .addRoundTrip("json", "JSON 'null'", JsonType.JSON)
                .addRoundTrip("json", "JSON '123.4'", JsonType.JSON)
                .addRoundTrip("json", "JSON '\"text with \\\" quotations and '' apostrophes\"'", JsonType.JSON)
                .addRoundTrip("json", "JSON '\"\"'", JsonType.JSON)
                .addRoundTrip("json", "JSON '{\"a\":1,\"b\":2}'", JsonType.JSON)
                .addRoundTrip("json", "JSON '{\"a\":[1,2,3],\"b\":{\"aa\":11,\"bb\":[{\"a\":1,\"b\":2},{\"a\":0}]}}'", JsonType.JSON)
                .addRoundTrip("json", "JSON '[]'", JsonType.JSON)
                .execute(getQueryRunner(), remoteConnectorCreated("test_json"))
                .execute(getQueryRunner(), remoteTrinoCreatedRemoteConnectorInserted("test_json"))
                .execute(getQueryRunner(), remoteConnectorCreateAsSelect("test_json"))
                .execute(getQueryRunner(), remoteConnectorCreateAndInsert("test_json"));
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

    private DataSetup remoteConnectorCreated(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(remoteExecutor, tableNamePrefix);
    }

    private DataSetup remoteTrinoCreatedRemoteConnectorInserted(String tableNamePrefix)
    {
        return remoteTrinoCreatedRemoteConnectorInserted(getSession(), tableNamePrefix);
    }

    private DataSetup remoteTrinoCreatedRemoteConnectorInserted(Session session, String tableNamePrefix)
    {
        return new CreateAndTrinoInsertDataSetup(remoteExecutor, new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup remoteConnectorCreateAsSelect(String tableNamePrefix)
    {
        return remoteConnectorCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup remoteConnectorCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup remoteConnectorCreateAndInsert(String tableNamePrefix)
    {
        return remoteConnectorCreateAndInsert(getSession(), tableNamePrefix);
    }

    private DataSetup remoteConnectorCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
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
