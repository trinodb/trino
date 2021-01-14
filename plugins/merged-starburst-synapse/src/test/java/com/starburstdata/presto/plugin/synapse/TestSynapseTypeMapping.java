/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.DataType;
import io.trino.testing.datatype.DataTypeTest;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.createSynapseQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.datatype.DataType.charDataType;
import static io.trino.testing.datatype.DataType.dataType;
import static io.trino.testing.datatype.DataType.varcharDataType;
import static java.util.function.Function.identity;

// TODO(https://starburstdata.atlassian.net/browse/PRESTO-5088) Extend Synapse tests form SQL server tests
// TODO(https://starburstdata.atlassian.net/browse/PRESTO-5087) Port Synapse type mapping tests to OSS SQL server
public class TestSynapseTypeMapping
        extends AbstractTestQueryFramework
{
    protected final SynapseServer synapseServer = new SynapseServer();

    private static final int SYNAPSE_MAX_NVARCHAR = 4000;
    private static final int SYNAPSE_MAX_NCHAR = 4000;

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
        SynapseServer synapseServer = new SynapseServer();
        return createSynapseQueryRunner(synapseServer, true, Map.of(), List.of());
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
    public void testVarbinary()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varbinary", "X''", VARBINARY, "X''")
                .addRoundTrip("varbinary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("varbinary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varbinary", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbinary", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("varbinary", "X'000000000000'", VARBINARY, "X'000000000000'")
                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_varbinary"));
//     TODO(https://starburstdata.atlassian.net/browse/PRESTO-5089) prestoCreateAsSelect to correct
//                .execute(getQueryRunner(), synapseCreateAndInsert("test_varbinary"));
    }

    @Test
    public void testBasicTypes()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN)
                .addRoundTrip("boolean", "false", BOOLEAN)
                .addRoundTrip("bigint", "123456789012", BIGINT)
                .addRoundTrip("integer", "123456789", INTEGER)
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("real", "123.456", REAL, "REAL '123.456'")
                .addRoundTrip("double", "123.456789", DOUBLE, "DOUBLE '123.456789'")
                .execute(getQueryRunner(), prestoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testReal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS real)")
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'")
                .execute(getQueryRunner(), synapseCreateAndInsert("test_real"));

        SqlDataTypeTest.create()
                .addRoundTrip("real", "NULL", REAL, "CAST(NULL AS real)")
                .addRoundTrip("real", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("real", "3.1415927", REAL, "REAL '3.1415927'")
                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_real"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double precision", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double precision", "1.0E100", DOUBLE, "1.0E100")
                .execute(getQueryRunner(), synapseCreateAndInsert("test_double"));

        SqlDataTypeTest.create()
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_double"));
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "193", createDecimalType(3, 0), "CAST('193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "19", createDecimalType(3, 0), "CAST('19' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "-193", createDecimalType(3, 0), "CAST('-193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "10.0", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "10.1", createDecimalType(3, 1), "CAST('10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "-10.1", createDecimalType(3, 1), "CAST('-10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(4, 2)", "2", createDecimalType(4, 2), "CAST('2' AS decimal(4, 2))")
                .addRoundTrip("decimal(4, 2)", "2.3", createDecimalType(4, 2), "CAST('2.3' AS decimal(4, 2))")
                .addRoundTrip("decimal(24, 2)", "2", createDecimalType(24, 2), "CAST('2' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "2.3", createDecimalType(24, 2), "CAST('2.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 2)", "123456789.3", createDecimalType(24, 2), "CAST('123456789.3' AS decimal(24, 2))")
                .addRoundTrip("decimal(24, 4)", "12345678901234567890.31", createDecimalType(24, 4), "CAST('12345678901234567890.31' AS decimal(24, 4))")
                .addRoundTrip("decimal(30, 5)", "3141592653589793238462643.38327", createDecimalType(30, 5), "CAST('3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(30, 5)", "-3141592653589793238462643.38327", createDecimalType(30, 5), "CAST('-3141592653589793238462643.38327' AS decimal(30, 5))")
                .execute(getQueryRunner(), synapseCreateAndInsert("test_decimal"))
                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_decimal"));
    }

    @Test
    public void testVarchar()
    {
        DataType<String> unboundedPrestoVarchar = DataType.dataType("varchar", createVarcharType(SYNAPSE_MAX_NVARCHAR), DataType::formatStringLiteral);
        varcharDataTypeTest(DataType::varcharDataType, true)
                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_varchar"));

        varcharDataTypeTest(size -> unboundedPrestoVarchar, true)
                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_varchar"));

        // TODO(https://starburstdata.atlassian.net/browse/PRESTO-5085) Test varchars with unicode with data setup done by synapse
        varcharDataTypeTest(DataType::varcharDataType, false)
                .execute(getQueryRunner(), synapseCreateAndInsert("presto_test_varchar"));

        // Synapse does not support unbounded nvarchar. Testing with maximum supported length
        varcharDataTypeTest(size -> varcharDataType(SYNAPSE_MAX_NVARCHAR), false)
                .execute(getQueryRunner(), synapseCreateAndInsert("presto_test_varchar"));
    }

    private DataTypeTest varcharDataTypeTest(Function<Integer, DataType<String>> dataTypeFactory, boolean includeUnicode)
    {
        DataTypeTest dataTypeTest = DataTypeTest.create()
                .addRoundTrip(dataTypeFactory.apply(SYNAPSE_MAX_NVARCHAR), "nvarchar max")
                .addRoundTrip(dataTypeFactory.apply(10), "text_a")
                .addRoundTrip(dataTypeFactory.apply(255), "text_b");
        if (includeUnicode) {
            dataTypeTest.addRoundTrip(dataTypeFactory.apply(40), "\u653b\u6bbb\u6a5f\u52d5\u968a")
                    .addRoundTrip(dataTypeFactory.apply(8), "\u968a")
                    .addRoundTrip(dataTypeFactory.apply(16), "\uD83D\uDE02")
                    .addRoundTrip(dataTypeFactory.apply(88), "\u041d\u0443, \u043f\u043e\u0433\u043e\u0434\u0438!");
        }
        return dataTypeTest;
    }

    @Test
    public void testChar()
    {
        // testing NCHAR(4000) separately as there is 8k limit on row size in Synapse and we cannot have more columns
        DataTypeTest ncharMaxTest = DataTypeTest.create()
                .addRoundTrip(charDataType(SYNAPSE_MAX_NCHAR), "nchar max");
        ncharMaxTest.execute(getQueryRunner(), prestoCreateAsSelect("presto_test_char_max"));
        ncharMaxTest.execute(getQueryRunner(), synapseCreateAndInsert("synapse_test_char_max"));

        charDataTypeTest(true).execute(getQueryRunner(), prestoCreateAsSelect("presto_test_char"));
        charDataTypeTest(false).execute(getQueryRunner(), synapseCreateAndInsert("synapse_test_char"));
    }

    private DataTypeTest charDataTypeTest(boolean includeUnicode)
    {
        DataTypeTest dataTypeTest = DataTypeTest.create()
                .addRoundTrip(charDataType(10), "text_a")
                .addRoundTrip(charDataType(255), "text_b");
        if (includeUnicode) {
            dataTypeTest.addRoundTrip(charDataType(40), "\u653b\u6bbb\u6a5f\u52d5\u968a")
                    .addRoundTrip(charDataType(8), "\u968a")
                    .addRoundTrip(charDataType(16), "\uD83D\uDE02")
                    .addRoundTrip(charDataType(88), "\u041d\u0443, \u043f\u043e\u0433\u043e\u0434\u0438!");
        }
        return dataTypeTest;
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

                // TODO(https://starburstdata.atlassian.net/browse/PRESTO-5097) Add support for dates < 1583 (gregorian switch)
                .addRoundTrip(dateDataType(), LocalDate.of(1952, 4, 3)) // before epoch
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 1, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 2, 3))
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 7, 1)) // summer on northern hemisphere (possible DST)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 1, 1)) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeForwardAtMidnightInJvmZone)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeForwardAtMidnightInSomeZone)
                .addRoundTrip(dateDataType(), dateOfLocalTimeChangeBackwardAtMidnightInSomeZone);

        for (String timeZoneId : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            Session session = Session.builder(getSession())
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZoneId))
                    .build();
            testCases.execute(getQueryRunner(), session, synapseCreateAndInsert("test_date"));
            testCases.execute(getQueryRunner(), session, prestoCreateAsSelect(session, "presto_test_date"));
        }
    }

    public static DataType<LocalDate> dateDataType()
    {
        return dataType(
                "date",
                DATE,
                DateTimeFormatter.ofPattern("''uuuu-MM-dd''")::format,
                DateTimeFormatter.ofPattern("'DATE '''uuuu-MM-dd''")::format,
                identity());
    }

    @Test
    public void testTime()
    {
        // TODO(https://starburstdata.atlassian.net/browse/PRESTO-5084) Add support for TIME mapping for Synapse and SQL server connectors
        throw new SkipException("not supported");
    }

    @Test
    public void testTimestamp()
    {
        // TODO(https://starburstdata.atlassian.net/browse/PRESTO-5074) Add support for DATETIME2 mapping for Synapse Connector
        throw new SkipException("not supported");
    }

    private DataSetup prestoCreateAsSelect(String tableNamePrefix)
    {
        return prestoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup prestoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup synapseCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(getJdbcExecutor(), tableNamePrefix);
    }

    private static void checkIsDoubled(ZoneId zone, LocalDateTime dateTime)
    {
        verify(zone.getRules().getValidOffsets(dateTime).size() == 2, "Expected %s to be doubled in %s", dateTime, zone);
    }

    private static boolean isGap(ZoneId zone, LocalDateTime dateTime)
    {
        return zone.getRules().getValidOffsets(dateTime).isEmpty();
    }

    private static void checkIsGap(ZoneId zone, LocalDateTime dateTime)
    {
        verify(isGap(zone, dateTime), "Expected %s to be a gap in %s", dateTime, zone);
    }

    private SqlExecutor getJdbcExecutor()
    {
        return synapseServer::execute;
    }
}
