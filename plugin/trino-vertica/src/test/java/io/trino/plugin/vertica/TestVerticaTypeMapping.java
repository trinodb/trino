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
package io.trino.plugin.vertica;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.STRICT;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_DEFAULT_SCALE;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_MAPPING;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_ROUNDING_MODE;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.vertica.VerticaQueryRunner.createVerticaQueryRunner;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;

public class TestVerticaTypeMapping
        extends AbstractTestQueryFramework
{
    private final ZoneId jvmZone = ZoneId.systemDefault();
    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");
    // minutes offset change since 1970-01-01, no DST
    private final ZoneId kathmandu = ZoneId.of("Asia/Kathmandu");

    private TestingVerticaServer verticaServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verticaServer = closeAfterClass(new TestingVerticaServer());
        return createVerticaQueryRunner(
                verticaServer,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of());
    }

    @BeforeAll
    public void setUp()
    {
        checkIsGap(jvmZone, LocalDateTime.of(1970, 1, 1, 0, 13, 42));
        checkIsGap(jvmZone, LocalDateTime.of(2018, 4, 1, 2, 13, 55, 123_000_000));
        checkIsDoubled(jvmZone, LocalDateTime.of(2018, 10, 28, 1, 33, 17, 456_000_000));

        checkIsGap(vilnius, LocalDateTime.of(2018, 3, 25, 3, 17, 17));
        checkIsDoubled(vilnius, LocalDateTime.of(2018, 10, 28, 3, 33, 33, 333_000_000));

        checkIsGap(kathmandu, LocalDateTime.of(1986, 1, 1, 0, 13, 7));
    }

    @Test
    public void testBasicTypes()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN)
                .addRoundTrip("boolean", "false", BOOLEAN)
                .addRoundTrip("bigint", "123456789012", BIGINT)
                .addRoundTrip("integer", "123456789", BIGINT, "BIGINT '123456789'")
                .addRoundTrip("smallint", "32456", BIGINT, "BIGINT '32456'")
                .addRoundTrip("tinyint", "5", BIGINT, "BIGINT '5'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_basic_types"));
    }

    @Test
    public void testChar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("char", "'a'", createCharType(1), "CAST('a' AS char(1))")
                .addRoundTrip("char(10)", "'text_a'", createCharType(10), "CAST('text_a' AS char(10))")
                .addRoundTrip("char(255)", "'text_b'", createCharType(255), "CAST('text_b' AS char(255))")
                .addRoundTrip("char(15)", "'攻殻機動隊'", createCharType(15), "CAST('攻殻機動隊' AS char(15))")
                .addRoundTrip("char(32)", "'攻殻機動隊'", createCharType(32), "CAST('攻殻機動隊' AS char(32))")
                .addRoundTrip("char(4)", "'😂'", createCharType(4), "CAST('😂' AS char(4))")
                .addRoundTrip("char(77)", "'Ну, погоди!'", createCharType(77), "CAST('Ну, погоди!' AS char(77))")
                .execute(getQueryRunner(), verticaCreateAndInsert("tpch.test_char"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_char"));
    }

    @Test
    public void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar(10)", "'text_a'", createVarcharType(10), "CAST('text_a' AS varchar(10))")
                .addRoundTrip("varchar(255)", "'text_b'", createVarcharType(255), "CAST('text_b' AS varchar(255))")
                .addRoundTrip("varchar(15)", "'攻殻機動隊'", createVarcharType(15), "CAST('攻殻機動隊' AS varchar(15))")
                .addRoundTrip("varchar(32)", "'攻殻機動隊'", createVarcharType(32), "CAST('攻殻機動隊' AS varchar(32))")
                .addRoundTrip("varchar(4)", "'😂'", createVarcharType(4), "CAST('😂' AS varchar(4))")
                .addRoundTrip("varchar(77)", "'Ну, погоди!'", createVarcharType(77), "CAST('Ну, погоди!' AS varchar(77))")
                .execute(getQueryRunner(), verticaCreateAndInsert("tpch.test_varchar"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"));

        SqlDataTypeTest.create()
                .addRoundTrip("varchar", "'text_default'", createVarcharType(80), "CAST('text_default' AS varchar(80))")
                .execute(getQueryRunner(), verticaCreateAndInsert("tpch.test_default_varchar"));

        SqlDataTypeTest.create()
                .addRoundTrip("varchar", "'text_default'", createVarcharType(1048576), "CAST('text_default' AS varchar(1048576))")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_default_varchar"));
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "NULL", createDecimalType(3, 0), "CAST(NULL AS decimal(3, 0))")
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
                .execute(getQueryRunner(), verticaCreateAndInsert("tpch.test_decimal"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"));
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
                getSession(),
                "decimal(50,0)",
                "12345678901234567890123456789012345678901234567890",
                "'12345678901234567890123456789012345678901234567890'");
    }

    @Test
    public void testDecimalExceedingPrecisionMaxWithExceedingIntegerValues()
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
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
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
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

    @Test
    public void testDecimalExceedingPrecisionMaxWithSupportedValues()
    {
        testDecimalExceedingPrecisionMaxWithSupportedValues(40, 8);
        testDecimalExceedingPrecisionMaxWithSupportedValues(50, 10);
    }

    private void testDecimalExceedingPrecisionMaxWithSupportedValues(int typePrecision, int typeScale)
    {
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
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

    @Test
    public void testDate()
    {
        testDate(UTC);
        testDate(jvmZone);
        // using two non-JVM zones so that we don't need to worry what Vertica system zone is
        testDate(vilnius);
        testDate(kathmandu);
        testDate(ZoneId.of(TestingSession.DEFAULT_TIME_ZONE_KEY.getId()));
    }

    private void testDate(ZoneId sessionZone)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(sessionZone.getId()))
                .build();

        SqlDataTypeTest.create()
                .addRoundTrip("DATE", "NULL", DATE, "CAST(NULL AS DATE)")
                .addRoundTrip("DATE", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'")
                // julian->gregorian switch
                .addRoundTrip("DATE", "DATE '1582-10-04'", DATE, "DATE '1582-10-04'")
                // .addRoundTrip("DATE", "DATE '1582-10-05'", DATE, "DATE '1582-10-05'") // Vertica plus 10 days
                // .addRoundTrip("DATE", "DATE '1582-10-14'", DATE, "DATE '1582-10-14'") // Vertica plus 10 days
                .addRoundTrip("DATE", "DATE '1582-10-15'", DATE, "DATE '1582-10-15'")
                // before epoch
                .addRoundTrip("DATE", "DATE '1952-04-03'", DATE, "DATE '1952-04-03'")
                .addRoundTrip("DATE", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("DATE", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                // summer on northern hemisphere (possible DST)
                .addRoundTrip("DATE", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'")
                // winter on northern hemisphere
                // (possible DST on southern hemisphere)
                .addRoundTrip("DATE", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'")
                .addRoundTrip("DATE", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                .addRoundTrip("DATE", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'")
                // some large dates
                .addRoundTrip("DATE", "DATE '9999-12-31'", DATE, "DATE '9999-12-31'")
                .addRoundTrip("DATE", "DATE '5881580-07-11'", DATE, "DATE '5881580-07-11'") // The epoch is integer max
                .execute(getQueryRunner(), session, verticaCreateAndInsert("tpch.test_date"))
                .execute(getQueryRunner(), session, trinoCreateAsSelect("test_date"))
                .execute(getQueryRunner(), session, trinoCreateAndInsert(getSession(), "test_date"));
    }

    @Test
    public void testReal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("real", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("real", "3.14", DOUBLE, "DOUBLE '3.14'")
                .addRoundTrip("real", "3.1415927", DOUBLE, "DOUBLE '3.1415927'")
                .addRoundTrip("real", "'NaN'::real", DOUBLE, "CAST(nan() AS double)")
                .addRoundTrip("real", "'-Infinity'::real", DOUBLE, "CAST(-infinity() AS double)")
                .addRoundTrip("real", "'+Infinity'::real", DOUBLE, "CAST(+infinity() AS double)")
                .execute(getQueryRunner(), verticaCreateAndInsert("tpch.vertica_test_real"));

        SqlDataTypeTest.create()
                .addRoundTrip("real", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("real", "3.14", DOUBLE, "DOUBLE '3.14'")
                .addRoundTrip("real", "3.1415927", DOUBLE, "DOUBLE '3.1415927'")
                .addRoundTrip("real", "nan()", DOUBLE, "CAST(nan() AS double)")
                .addRoundTrip("real", "-infinity()", DOUBLE, "CAST(-infinity() AS double)")
                .addRoundTrip("real", "+infinity()", DOUBLE, "CAST(+infinity() AS double)")
                .execute(getQueryRunner(), trinoCreateAsSelect("presto_test_real"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double precision", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double precision", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double precision", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("double precision", "'NaN'::double precision", DOUBLE, "nan()")
                .addRoundTrip("double precision", "'+Infinity'::double precision", DOUBLE, "+infinity()")
                .addRoundTrip("double precision", "'-Infinity'::double precision", DOUBLE, "-infinity()")
                .execute(getQueryRunner(), verticaCreateAndInsert("tpch.vertica_test_double"));

        SqlDataTypeTest.create()
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS double)")
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("double", "nan()", DOUBLE, "nan()")
                .addRoundTrip("double", "+infinity()", DOUBLE, "+infinity()")
                .addRoundTrip("double", "-infinity()", DOUBLE, "-infinity()")
                .execute(getQueryRunner(), trinoCreateAsSelect("presto_test_double"));
    }

    private void testUnsupportedDataTypeAsIgnored(String dataTypeName, String databaseValue)
    {
        testUnsupportedDataTypeAsIgnored(getSession(), dataTypeName, databaseValue);
    }

    private void testUnsupportedDataTypeAsIgnored(Session session, String dataTypeName, String databaseValue)
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.unsupported_type",
                format("(key varchar(5), unsupported_column %s)", dataTypeName),
                ImmutableList.of(
                        "'1', NULL",
                        "'2', " + databaseValue))) {
            assertQuery(session, "SELECT * FROM " + table.getName(), "VALUES 1, 2");
            assertQuery(
                    session,
                    "DESC " + table.getName(),
                    "VALUES ('key', 'varchar(5)','', '')"); // no 'unsupported_column'

            assertUpdate(session, format("INSERT INTO %s VALUES '3'", table.getName()), 1);
            assertQuery(session, "SELECT * FROM " + table.getName(), "VALUES '1', '2', '3'");
        }
    }

    private void testUnsupportedDataTypeConvertedToVarchar(Session session, String dataTypeName, String databaseValue, String trinoValue)
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.unsupported_type",
                format("(key varchar(5), unsupported_column %s)", dataTypeName),
                ImmutableList.of(
                        "1, NULL",
                        "2, " + databaseValue))) {
            Session convertToVarchar = Session.builder(session)
                    .setCatalogSessionProperty("vertica", UNSUPPORTED_TYPE_HANDLING, CONVERT_TO_VARCHAR.name())
                    .build();
            assertQuery(
                    convertToVarchar,
                    "SELECT * FROM " + table.getName(),
                    format("VALUES ('1', NULL), ('2', %s)", trinoValue));
            assertQuery(
                    convertToVarchar,
                    format("SELECT key FROM %s WHERE unsupported_column = %s", table.getName(), trinoValue),
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
                    format("INSERT INTO %s (key, unsupported_column) VALUES (4, %s)", table.getName(), trinoValue),
                    "Insert query has mismatched column types: Table: \\[varchar\\(5\\), varchar\\], Query: \\[integer, varchar\\(\\d+\\)\\]");
            assertUpdate(
                    convertToVarchar,
                    format("INSERT INTO %s (key) VALUES '5'", table.getName()),
                    1);
            assertQuery(
                    convertToVarchar,
                    "SELECT * FROM " + table.getName(),
                    format("VALUES ('1', NULL), ('2', %s), ('5', NULL)", trinoValue));
        }
    }

    private Session sessionWithDecimalMappingAllowOverflow(RoundingMode roundingMode, int scale)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("vertica", DECIMAL_MAPPING, ALLOW_OVERFLOW.name())
                .setCatalogSessionProperty("vertica", DECIMAL_ROUNDING_MODE, roundingMode.name())
                .setCatalogSessionProperty("vertica", DECIMAL_DEFAULT_SCALE, Integer.valueOf(scale).toString())
                .build();
    }

    private Session sessionWithDecimalMappingStrict(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("vertica", DECIMAL_MAPPING, STRICT.name())
                .setCatalogSessionProperty("vertica", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }

    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup verticaCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(onRemoteDatabase(), tableNamePrefix);
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

    private SqlExecutor onRemoteDatabase()
    {
        return verticaServer.getSqlExecutor();
    }
}
