/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.dynamodb;

import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.plugin.dynamodb.testing.DynamoDbDataSetup;
import io.trino.Session;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import io.trino.tpch.TpchTable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
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
import static io.trino.type.JsonType.JSON;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// DynamoDB may miss new tables when running in multi threads
@Test(singleThreaded = true)
public class TestDynamoDbTypeMapping
        extends AbstractTestQueryFramework
{
    private DynamoDbConfig dynamoDbConfig;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingDynamoDbServer server = closeAfterClass(new TestingDynamoDbServer());

        dynamoDbConfig = new DynamoDbConfig()
                .setAwsAccessKey("access-key")
                .setAwsSecretKey("secret-key")
                .setAwsRegion("us-east-2")
                .setEndpointUrl(server.getEndpointUrl())
                .setSchemaDirectory(server.getSchemaDirectory().getAbsolutePath());

        return DynamoDbQueryRunner.builder(server.getEndpointUrl(), server.getSchemaDirectory())
                // copy smallest table to enforce DynamicCatalogManager load catalogs
                .setTables(ImmutableList.of(TpchTable.REGION))
                .setFirstColumnAsPrimaryKeyEnabled(true)
                .enableWrites()
                .build();
    }

    @Test
    public void testBoolean()
    {
        // First type here used as Dynamo primary key, cannot use boolean as primary key
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(255)", "'id'", createVarcharType(255), "CAST('id' AS VARCHAR(255))")
                .addRoundTrip("boolean", "true", BOOLEAN, "true")
                .addRoundTrip("boolean", "false", BOOLEAN, "false")
                .addRoundTrip("boolean", "null", BOOLEAN, "CAST(NULL as BOOLEAN)")
                .execute(getQueryRunner(), dynamoDbCreateAndInsert("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"));
    }

    @Test
    public void testTinyInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", Byte.toString(Byte.MIN_VALUE), TINYINT, format("CAST(%s AS TINYINT)", Byte.MIN_VALUE))
                .addRoundTrip("tinyint", Byte.toString(Byte.MAX_VALUE), TINYINT, format("CAST(%s AS TINYINT)", Byte.MAX_VALUE))
                .addRoundTrip("tinyint", "null", TINYINT, "CAST(NULL as TINYINT)")
                .execute(getQueryRunner(), dynamoDbCreateAndInsert("test_tinyint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"));
    }

    @Test
    public void testSmallInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", Short.toString(Short.MIN_VALUE), SMALLINT, format("CAST(%s AS SMALLINT)", Short.MIN_VALUE))
                .addRoundTrip("smallint", Short.toString(Short.MAX_VALUE), SMALLINT, format("CAST(%s AS SMALLINT)", Short.MAX_VALUE))
                .addRoundTrip("smallint", "null", SMALLINT, "CAST(NULL as SMALLINT)")
                .execute(getQueryRunner(), dynamoDbCreateAndInsert("test_smallint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_smallint"));
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", Integer.toString(Integer.MIN_VALUE), INTEGER, Integer.toString(Integer.MIN_VALUE))
                .addRoundTrip("integer", Integer.toString(Integer.MAX_VALUE), INTEGER, Integer.toString(Integer.MAX_VALUE))
                .addRoundTrip("integer", "null", INTEGER, "CAST(NULL as INTEGER)")
                .execute(getQueryRunner(), dynamoDbCreateAndInsert("test_integer"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_integer"));
    }

    @Test
    public void testBigInt()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", Long.toString(Long.MIN_VALUE), BIGINT, Long.toString(Long.MIN_VALUE))
                .addRoundTrip("bigint", Long.toString(Long.MAX_VALUE), BIGINT, Long.toString(Long.MAX_VALUE))
                .addRoundTrip("bigint", "null", BIGINT, "CAST(NULL as BIGINT)")
                .execute(getQueryRunner(), dynamoDbCreateAndInsert("test_bigint"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_bigint"));
    }

    @Test
    public void testReal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'")
                .addRoundTrip("real", "null", REAL, "CAST(NULL as REAL)")
                .execute(getQueryRunner(), dynamoDbCreateAndInsert("test_real"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_real"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "-1.0E100", DOUBLE, "-1.0E100")
                .addRoundTrip("double", "null", DOUBLE, "CAST(NULL as DOUBLE)")
                .execute(getQueryRunner(), dynamoDbCreateAndInsert("test_double"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_double"));
    }

    @Test
    public void testDecimal()
    {
        // Decimal types show up as VARCHAR
        testUnsupportedValue(
                "decimal(3, 0)",
                "CAST('193' AS decimal)",
                createDecimalType(3, 0),
                "expected:<[decimal(3,]0)> but was:<[varchar(200]0)>",
                "Unsupported column type: decimal");
    }

    @Test
    public void testChar()
    {
        // Driver converts CHAR types to VARCHAR(2000), so flag as unsupported
        testUnsupportedValue("char", "'a'", createCharType(1), "expected:<[char(1])> but was:<[varchar(2000])>", "Unsupported column type: char");
    }

    @Test
    public void testVarchar()
    {
        String sampleUnicodeText = "\u653b\u6bbb\u6a5f\u52d5\u968a";
        String sampleFourByteUnicodeCharacter = "\uD83D\uDE02";
        String nuPogodi = "\u041d\u0443, \u043f\u043e\u0433\u043e\u0434\u0438!";

        SqlDataTypeTest.create()
                .addRoundTrip("varchar(" + sampleUnicodeText.length() + ")", "'" + sampleUnicodeText + "'", createVarcharType(sampleUnicodeText.length()), "CAST('" + sampleUnicodeText + "' AS varchar(" + sampleUnicodeText.length() + "))")
                .addRoundTrip("varchar(32)", "'" + sampleUnicodeText + "'", createVarcharType(32), "CAST('" + sampleUnicodeText + "' AS varchar(32))")
                .addRoundTrip("varchar(16000)", "'" + sampleUnicodeText + "'", createVarcharType(16000), "CAST('" + sampleUnicodeText + "' AS varchar(16000))")
                .addRoundTrip("varchar(1)", "'" + sampleFourByteUnicodeCharacter + "'", createVarcharType(1), "CAST('" + sampleFourByteUnicodeCharacter + "' AS varchar(1))")
                .addRoundTrip("varchar(77)", "'" + nuPogodi + "'", createVarcharType(77), "CAST('" + nuPogodi + "' AS varchar(77))")
                .execute(getQueryRunner(), dynamoDbCreateAndInsert("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"));

        // Unbounded varchars get reported as varchar(2000) by CData driver
        testUnsupportedValue(
                "varchar",
                "'a'",
                createUnboundedVarcharType(),
                "expected:<varchar[]> but was:<varchar[(2000)]>",
                "Unsupported column type: varchar. Only bounded varchars are supported");
    }

    @Test
    public void testVarbinary()
    {
        // The JDBC Driver does not like using plain INSERT statements with binary types
        SqlDataTypeTest.create()
                .addRoundTrip("varbinary", "X'000000000000'", VARBINARY, "X'000000000000'")
                .addRoundTrip("varbinary", "NULL", VARBINARY, "CAST(NULL AS VARBINARY)")
//                .addRoundTrip("varbinary", "X''", VARBINARY, "X''") Empty array is not stored in DynamoDB and read back as NULL
                .addRoundTrip("varbinary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("varbinary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varbinary", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbinary", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varbinary"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varbinary"));
    }

    @Test
    public void testJson()
    {
        // Driver converts JSON types to VARCHAR(2000), so flag as unsupported
        testUnsupportedValue(
                "json",
                "'a'",
                JSON,
                "expected:<[json]> but was:<[varchar(2000)]>",
                "Unsupported column type: json");
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

        String dateOfLocalTimeChangeForwardAtMidnightInJvmZoneLiteral = format("DATE '%s'", ISO_DATE.format(dateOfLocalTimeChangeForwardAtMidnightInJvmZone));
        String dateOfLocalTimeChangeForwardAtMidnightInSomeZoneLiteral = format("DATE '%s'", ISO_DATE.format(dateOfLocalTimeChangeForwardAtMidnightInSomeZone));
        String dateOfLocalTimeChangeBackwardAtMidnightInSomeZoneLiteral = format("DATE '%s'", ISO_DATE.format(dateOfLocalTimeChangeBackwardAtMidnightInSomeZone));

        SqlDataTypeTest testCases = SqlDataTypeTest.create()
                .addRoundTrip("DATE", "DATE '0001-01-01'", DateType.DATE, "DATE '0001-01-01'") // min value in DynamoDB
                .addRoundTrip("DATE", "DATE '1582-10-04'", DateType.DATE, "DATE '1582-10-04'") // before julian->gregorian switch
                .addRoundTrip("DATE", "DATE '1582-10-15'", DateType.DATE, "DATE '1582-10-15'") // after julian->gregorian switch
                .addRoundTrip("DATE", "DATE '1952-04-03'", DateType.DATE, "DATE '1952-04-03'") // before epoch
                .addRoundTrip("DATE", "DATE '1970-01-01'", DateType.DATE, "DATE '1970-01-01'")
                .addRoundTrip("DATE", "DATE '1970-02-03'", DateType.DATE, "DATE '1970-02-03'")
                .addRoundTrip("DATE", "DATE '2017-07-01'", DateType.DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("DATE", "DATE '2017-01-01'", DateType.DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("DATE", dateOfLocalTimeChangeForwardAtMidnightInJvmZoneLiteral, DateType.DATE, dateOfLocalTimeChangeForwardAtMidnightInJvmZoneLiteral)
                .addRoundTrip("DATE", dateOfLocalTimeChangeForwardAtMidnightInSomeZoneLiteral, DateType.DATE, dateOfLocalTimeChangeForwardAtMidnightInSomeZoneLiteral)
                .addRoundTrip("DATE", dateOfLocalTimeChangeBackwardAtMidnightInSomeZoneLiteral, DateType.DATE, dateOfLocalTimeChangeBackwardAtMidnightInSomeZoneLiteral)
                .addRoundTrip("DATE", "DATE '9999-12-31'", DateType.DATE, "DATE '9999-12-31'"); // max value in DynamoDB

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getQueryRunner().getDefaultSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            testCases.execute(getQueryRunner(), dynamoDbCreateAndInsert("test_date"));
            testCases.execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"));
            testCases.execute(getQueryRunner(), session, trinoCreateAndInsert("test_date"));
        }
    }

    @Test
    public void testNullDate()
    {
        // First column becomes by default partition key (not nullable), to test null values we should use next columns
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_unsupported_date", "(a smallint, dt date)")) {
            assertQuerySucceeds(format("INSERT INTO %s VALUES (1, CAST(NULL AS date))", table.getName()));
        }
    }

    @Test(dataProvider = "unsupportedDateDataProvider")
    public void testUnsupportedDate(String unsupportedDate)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_unsupported_date", "(dt date)")) {
            // DynamoDB accepts writing these values, but CData driver throws an exception in ResultSet getters
            // > Error obtaining value 'yyyy-MM-dd' for column 'dt'. Failed to convert  to datatype: 'string'.
            // > The original datatype of the column is: 'date'. Original Exception: Error parsing date value [yyyy-MM-dd].
            assertUpdate(format("INSERT INTO %s VALUES (date '%s')", table.getName(), unsupportedDate), 1);
            assertQueryFails("SELECT * FROM " + table.getName(), ".*Error obtaining value.*");
        }
    }

    @DataProvider
    public Object[][] unsupportedDateDataProvider()
    {
        return new Object[][] {
                {"-0000-01-01"},
                {"0000-12-31"},
                {"1582-10-05"}, // begin julian->gregorian switch
                {"1582-10-14"}, // end julian->gregorian switch
                {"10000-01-01"},
        };
    }

    @Test
    public void testTime()
    {
        // Driver converts the TIME types to VARCHAR, so flag as unsupported
        testUnsupportedValue(
                "time(3)",
                "TIME '00:00:00.000'",
                createTimeType(3),
                "Illegal value for type varchar(3): '00:00:00.000'",
                "Unsupported column type: time");
    }

    @Test
    public void testTimeWithTimeZone()
    {
        // Driver converts TIME WITH TIME ZONE types to VARCHAR(2000), so flag as unsupported
        testUnsupportedValue(
                "time(3) with time zone",
                "TIME '00:00:00.000 +00:00'",
                createTimeWithTimeZoneType(3),
                "expected:<[time(3) with time zone]> but was:<[varchar(2000)]>",
                "Unsupported column type: time(3) with time zone");
    }

    @Test
    public void testTimestamp()
    {
        // Driver converts TIMESTAMP types to VARCHAR, so flag as unsupported
        testUnsupportedValue(
                "timestamp(3)",
                "TIMESTAMP '1970-01-01 00:00:00.000'",
                createTimestampType(3),
                "Illegal value for type varchar(3): '1970-01-01 00:00:00.000'",
                "Unsupported column type: timestamp(3)");
    }

    @Test
    public void testTimestampWithTimeZone()
    {
        // Driver converts TIMESTAMP WITH TIME ZONE types to VARCHAR(2000), so flag as unsupported
        testUnsupportedValue(
                "timestamp(3) with time zone",
                "TIMESTAMP '1970-01-01 00:00:00.000 UTC'",
                createTimestampWithTimeZoneType(3),
                "expected:<[timestamp(3) with time zone]> but was:<[varchar(2000)]>",
                "Unsupported column type: timestamp(3) with time zone");
    }

    private <T extends Type> void testUnsupportedValue(String inputType, String inputLiteral, T expectedType, String dynamoMessage, String trinoMessage)
    {
        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("integer", "1", INTEGER) // Primary key column
                        .addRoundTrip(inputType, inputLiteral, expectedType)
                        .execute(getQueryRunner(), dynamoDbCreateAndInsert("test_unsupported")))
                .hasStackTraceContaining(dynamoMessage);

        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("integer", "1", INTEGER) // Primary key column
                        .addRoundTrip(inputType, inputLiteral, expectedType)
                        .execute(getQueryRunner(), trinoCreateAsSelect("test_unsupported")))
                .hasStackTraceContaining(trinoMessage);

        assertThatThrownBy(() ->
                SqlDataTypeTest.create()
                        .addRoundTrip("integer", "1", INTEGER) // Primary key column
                        .addRoundTrip(inputType, inputLiteral, expectedType)
                        .execute(getQueryRunner(), trinoCreateAndInsert("test_unsupported")))
                .hasStackTraceContaining(trinoMessage);
    }

    protected DataSetup trinoCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), getSession()), tableNamePrefix);
    }

    protected DataSetup dynamoDbCreateAndInsert(String tableNamePrefix)
    {
        return new DynamoDbDataSetup(dynamoDbConfig, new JdbcSqlExecutor(DynamoDbConnectionFactory.getConnectionUrl(dynamoDbConfig)), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner()), tableNamePrefix);
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
