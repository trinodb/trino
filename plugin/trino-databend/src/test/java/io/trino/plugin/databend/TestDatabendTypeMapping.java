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
package io.trino.plugin.databend;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.time.LocalDate;
import java.time.ZoneId;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDatabendTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingDatabendServer databendServer;

    private final ZoneId jvmZone = ZoneId.systemDefault();
    // no DST in 1970, but has DST in later years (e.g. 2018)
    private final ZoneId vilnius = ZoneId.of("Europe/Vilnius");

    @BeforeAll
    public void setUp()
    {
        checkState(jvmZone.getId().equals("America/Bahia_Banderas"), "This test assumes certain JVM time zone");

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDate.of(1983, 4, 1);
        verify(vilnius.getRules().getValidOffsets(dateOfLocalTimeChangeForwardAtMidnightInSomeZone.atStartOfDay()).isEmpty());
        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDate.of(1983, 10, 1);
        verify(vilnius.getRules().getValidOffsets(dateOfLocalTimeChangeBackwardAtMidnightInSomeZone.atStartOfDay().minusMinutes(1)).size() == 2);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        databendServer = closeAfterClass(new TestingDatabendServer());
        return DatabendQueryRunner.builder(databendServer).addConnectorProperty("connection-url", databendServer.getJdbcUrl())
                .build();
    }

    @Test
    public void testTinyint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .addRoundTrip("tinyint", "-128", TINYINT, "TINYINT '-128'") // min value in Databend and Trino
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("tinyint", "127", TINYINT, "TINYINT '127'") // max value in Databend and Trino
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.test_tinyint"));
    }

    @Test
    public void testUnsupportedTinyint()
    {
        try (TestTable table = new TestTable(databendServer::execute, "tpch.test_unsupported_tinyint", "(data tinyint)")) {
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (-129)", // min - 1
                    "Query failed");
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (128)", // max + 1
                    "Query failed");
        }
    }

    @Test
    public void testSmallint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'") // min value in Databend and Trino
                .addRoundTrip("smallint", "32456", SMALLINT, "SMALLINT '32456'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'") // max value in Databend and Trino
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.test_smallint"));
    }

    @Test
    public void testUnsupportedSmallint()
    {
        try (TestTable table = new TestTable(databendServer::execute, "tpch.test_unsupported_smallint", "(data smallint)")) {
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (-32769)", // min - 1
                    "Query failed");
            assertDatabendQueryFails(
                    format("INSERT INTO %s VALUES (32768)", table.getName()), // max + 1
                    "Query failed");
        }
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .addRoundTrip("integer", "-2147483648", INTEGER, "-2147483648") // min value in Databend and Trino
                .addRoundTrip("integer", "1234567890", INTEGER, "1234567890")
                .addRoundTrip("integer", "2147483647", INTEGER, "2147483647") // max value in Databend and Trino
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.test_int"));
//                .execute(getQueryRunner(), trinoCreateAndInsert("test_int"));
    }

    @Test
    public void testUnsupportedInteger()
    {
        try (TestTable table = new TestTable(databendServer::execute, "tpch.test_unsupported_integer", "(data integer)")) {
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (-2147483649)", // min - 1
                    "Query failed");
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (2147483648)", // max + 1
                    "Query failed");
        }
    }

    @Test
    public void testBigint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808") // min value in Databend and Trino
                .addRoundTrip("bigint", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807") // max value in Databend and Trino
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.test_bigint"));
    }

    @Test
    public void testUnsupportedBigint()
    {
        try (TestTable table = new TestTable(databendServer::execute, "tpch.test_unsupported_bigint", "(data bigint)")) {
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (-92233720368547758090)",
                    "Query failed");
            assertDatabendQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (92233720368547758080)",
                    "Query failed");
        }
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create()
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
                .addRoundTrip("decimal(3, 0)", "CAST(NULL AS decimal(3, 0))", createDecimalType(3, 0), "CAST(NULL AS decimal(3, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST(NULL AS decimal(38, 0))", createDecimalType(38, 0), "CAST(NULL AS decimal(38, 0))")
                .addRoundTrip("decimal(38, 0)", "CAST('99999999999999999999999999999999999999' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('99999999999999999999999999999999999999' AS decimal(38, 0))")
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.test_decimal"));
    }

    @Test
    public void testFloat()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("float", "3.14", REAL, "REAL '3.14'")
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.databend_test_float"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double", "1.23456E12", DOUBLE, "1.23456E12")
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.databend_test_double"));
    }

    @Test
    public void testUnsignedTypes()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("TINYINT UNSIGNED", "255", SMALLINT, "SMALLINT '255'")
                .addRoundTrip("SMALLINT UNSIGNED", "65535", INTEGER, "65535")
                .addRoundTrip("INT UNSIGNED", "4294967295", BIGINT, "4294967295")
                .addRoundTrip("INTEGER UNSIGNED", "4294967295", BIGINT, "4294967295")
                .addRoundTrip("BIGINT UNSIGNED", "18446744073709551615", createDecimalType(20, 0), "DECIMAL '18446744073709551615'")
                .execute(getQueryRunner(), databendCreateAndInsert("tpch.databend_test_unsigned"));
    }

    private DataSetup databendCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(databendServer::execute, tableNamePrefix);
    }

    private void assertDatabendQueryFails(@Language("SQL") String sql, String expectedMessage)
    {
        assertThatThrownBy(() -> databendServer.execute(sql))
                .cause()
                .hasMessageContaining(expectedMessage);
    }
}
