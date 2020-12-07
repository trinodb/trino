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
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
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
import static io.prestosql.testing.datatype.DataType.tinyintDataType;
import static io.prestosql.testing.datatype.DataType.varbinaryDataType;
import static io.prestosql.testing.datatype.DataType.varcharDataType;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPrestoConnectorTypeMapping
        extends AbstractTestQueryFramework
{
    private DistributedQueryRunner remotePresto;
    private PrestoSqlExecutor remoteExecutor;

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
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeBackwardAtMidnightInSomeZone);

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
