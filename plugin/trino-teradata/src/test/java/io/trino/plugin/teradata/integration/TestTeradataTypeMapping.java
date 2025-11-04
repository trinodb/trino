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
package io.trino.plugin.teradata.integration;

import io.trino.plugin.teradata.integration.clearscape.ClearScapeEnvironmentUtils;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.datatype.SqlDataTypeTest.create;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

final class TestTeradataTypeMapping
        extends AbstractTestQueryFramework
{
    private final String envName;
    private TestingTeradataServer database;

    public TestTeradataTypeMapping()
    {
        envName = ClearScapeEnvironmentUtils.generateUniqueEnvName(TestTeradataTypeMapping.class);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        database = closeAfterClass(new TestingTeradataServer(envName));
        // Register this specific instance for this test class
        return TeradataQueryRunner.builder(database).build();
    }

    @AfterAll
    void cleanupTestClass()
    {
        database = null;
    }

    @Test
    void testByteint()
    {
        create()
                .addRoundTrip("byteint", "0", TINYINT, "CAST(0 AS TINYINT)")
                .addRoundTrip("byteint", "127", TINYINT, "CAST(127 AS TINYINT)")
                .addRoundTrip("byteint", "-128", TINYINT, "CAST(-128 AS TINYINT)")
                .addRoundTrip("byteint", "null", TINYINT, "CAST(null AS TINYINT)")
                .execute(getQueryRunner(), teradataJDBCCreateAndInsert("byteint"));
    }

    @Test
    void testSmallint()
    {
        create()
                .addRoundTrip("smallint", "0", SMALLINT, "CAST(0 AS SMALLINT)")
                .addRoundTrip("smallint", "32767", SMALLINT, "CAST(32767 AS SMALLINT)")
                .addRoundTrip("smallint", "-32768", SMALLINT, "CAST(-32768 AS SMALLINT)")
                .addRoundTrip("smallint", "null", SMALLINT, "CAST(null AS SMALLINT)")
                .execute(getQueryRunner(), teradataJDBCCreateAndInsert("smallint"));
    }

    @Test
    void testInteger()
    {
        create()
                .addRoundTrip("integer", "0", INTEGER, "0")
                .addRoundTrip("integer", "2147483647", INTEGER, "2147483647")
                .addRoundTrip("integer", "-2147483648", INTEGER, "-2147483648")
                .addRoundTrip("integer", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .execute(getQueryRunner(), teradataJDBCCreateAndInsert("integer"));
    }

    @Test
    void testBigint()
    {
        create()
                .addRoundTrip("bigint", "0", BIGINT, "CAST(0 AS BIGINT)")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807")
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808")
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), teradataJDBCCreateAndInsert("bigint"));
    }

    @Test
    void testFloat()
    {
        create()
                .addRoundTrip("float", "0", DOUBLE, "CAST(0 AS DOUBLE)")
                .addRoundTrip("real", "0", DOUBLE, "CAST(0 AS DOUBLE)")
                .addRoundTrip("double precision", "0", DOUBLE, "CAST(0 AS DOUBLE)")
                .addRoundTrip("float", "1.797e308", DOUBLE, "1.797e308")
                .addRoundTrip("real", "1.797e308", DOUBLE, "1.797e308")
                .addRoundTrip("double precision", "1.797e308", DOUBLE, "1.797e308")
                .addRoundTrip("float", "2.226e-308", DOUBLE, "2.226e-308")
                .addRoundTrip("real", "2.226e-308", DOUBLE, "2.226e-308")
                .addRoundTrip("double precision", "2.226e-308", DOUBLE, "2.226e-308")
                .addRoundTrip("float", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("real", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("double precision", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .execute(getQueryRunner(), teradataJDBCCreateAndInsert("float"));
    }

    @Test
    void testDecimal()
    {
        create()
                .addRoundTrip("decimal(3, 0)", "0", createDecimalType(3, 0), "CAST('0' AS decimal(3, 0))")
                .addRoundTrip("numeric(3, 0)", "0", createDecimalType(3, 0), "CAST('0' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "0.0", createDecimalType(3, 1), "CAST('0.0' AS decimal(3, 1))")
                .addRoundTrip("numeric(3, 1)", "0.0", createDecimalType(3, 1), "CAST('0.0' AS decimal(3, 1))")
                .addRoundTrip("decimal(1, 0)", "1", createDecimalType(1, 0), "CAST('1' AS decimal(1, 0))")
                .addRoundTrip("numeric(1, 0)", "1", createDecimalType(1, 0), "CAST('1' AS decimal(1, 0))")
                .addRoundTrip("decimal(1, 0)", "-1", createDecimalType(1, 0), "CAST('-1' AS decimal(1, 0))")
                .addRoundTrip("numeric(1, 0)", "-1", createDecimalType(1, 0), "CAST('-1' AS decimal(1, 0))")
                .addRoundTrip("decimal(3, 0)", "1", createDecimalType(3, 0), "CAST('1' AS decimal(3, 0))")
                .addRoundTrip("numeric(3, 0)", "1", createDecimalType(3, 0), "CAST('1' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "-1", createDecimalType(3, 0), "CAST('-1' AS decimal(3, 0))")
                .addRoundTrip("numeric(3, 0)", "-1", createDecimalType(3, 0), "CAST('-1' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "123", createDecimalType(3, 0), "CAST('123' AS decimal(3, 0))")
                .addRoundTrip("numeric(3, 0)", "123", createDecimalType(3, 0), "CAST('123' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "-123", createDecimalType(3, 0), "CAST('-123' AS decimal(3, 0))")
                .addRoundTrip("numeric(3, 0)", "-123", createDecimalType(3, 0), "CAST('-123' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "10.0", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))")
                .addRoundTrip("numeric(3, 1)", "10.0", createDecimalType(3, 1), "CAST('10.0' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "12.3", createDecimalType(3, 1), "CAST('12.3' AS decimal(3, 1))")
                .addRoundTrip("numeric(3, 1)", "12.3", createDecimalType(3, 1), "CAST('12.3' AS decimal(3, 1))")
                .addRoundTrip("decimal(3, 1)", "-12.3", createDecimalType(3, 1), "CAST('-12.3' AS decimal(3, 1))")
                .addRoundTrip("numeric(3, 1)", "-12.3", createDecimalType(3, 1), "CAST('-12.3' AS decimal(3, 1))")
                .addRoundTrip("decimal(38, 0)", "12345678901234567890123456789012345678", createDecimalType(38, 0), "CAST('12345678901234567890123456789012345678' AS decimal(38, 0))")
                .addRoundTrip("numeric(38, 0)", "12345678901234567890123456789012345678", createDecimalType(38, 0), "CAST('12345678901234567890123456789012345678' AS decimal(38, 0))")
                .addRoundTrip("decimal(38, 0)", "-12345678901234567890123456789012345678", createDecimalType(38, 0), "CAST('-12345678901234567890123456789012345678' AS decimal(38, 0))")
                .addRoundTrip("numeric(38, 0)", "-12345678901234567890123456789012345678", createDecimalType(38, 0), "CAST('-12345678901234567890123456789012345678' AS decimal(38, 0))")
                .addRoundTrip("decimal(1, 0)", "null", createDecimalType(1, 0), "CAST(null AS decimal(1, 0))")
                .execute(getQueryRunner(), teradataJDBCCreateAndInsert("decimal"));
    }

    @Test
    void testNumber()
    {
        create()
                .addRoundTrip("numeric(3)", "0", createDecimalType(3, 0), "CAST('0' AS decimal(3, 0))")
                .addRoundTrip("number(5,2)", "0", createDecimalType(5, 2), "CAST('0' AS decimal(5, 2))")
                .addRoundTrip("number(38)", "0", createDecimalType(38, 0), "CAST('0' AS decimal(38, 0))")
                .addRoundTrip("number(38,2)", "123456789012345678901234567890123456.78", createDecimalType(38, 2), "CAST('123456789012345678901234567890123456.78' AS decimal(38, 2))")
                .addRoundTrip("numeric(38)", "12345678901234567890123456789012345678", createDecimalType(38, 0), "CAST('12345678901234567890123456789012345678' AS decimal(38, 0))")
                .addRoundTrip("numeric(3)", "null", createDecimalType(3, 0), "CAST(null AS decimal(3, 0))")
                .execute(getQueryRunner(), teradataJDBCCreateAndInsert("number"));
    }

    @Test
    void testChar()
    {
        create()
                .addRoundTrip("char(3)", "''", createCharType(3), "CAST('' AS char(3))")
                .addRoundTrip("char(3)", "' '", createCharType(3), "CAST(' ' AS char(3))")
                .addRoundTrip("char(3)", "'  '", createCharType(3), "CAST('  ' AS char(3))")
                .addRoundTrip("char(3)", "'   '", createCharType(3), "CAST('   ' AS char(3))")
                .addRoundTrip("char(3)", "'A'", createCharType(3), "CAST('A' AS char(3))")
                .addRoundTrip("char(3)", "'A  '", createCharType(3), "CAST('A  ' AS char(3))")
                .addRoundTrip("char(3)", "' B '", createCharType(3), "CAST(' B ' AS char(3))")
                .addRoundTrip("char(3)", "'  C'", createCharType(3), "CAST('  C' AS char(3))")
                .addRoundTrip("char(3)", "'AB'", createCharType(3), "CAST('AB' AS char(3))")
                .addRoundTrip("char(3)", "'ABC'", createCharType(3), "CAST('ABC' AS char(3))")
                .addRoundTrip("char(3)", "'A C'", createCharType(3), "CAST('A C' AS char(3))")
                .addRoundTrip("char(3)", "' BC'", createCharType(3), "CAST(' BC' AS char(3))")
                .addRoundTrip("char(3)", "null", createCharType(3), "CAST(null AS char(3))")
                .execute(getQueryRunner(), teradataJDBCCreateAndInsert("char"));
        String tmode = database.getTMode();
        if (tmode.equals("TERA")) {
            // truncation
            create()
                    .addRoundTrip("char(3)", "'ABCD'", createCharType(3), "CAST('ABCD' AS char(3))")
                    .execute(getQueryRunner(), teradataJDBCCreateAndInsert("chart"));
        }
        else {
            // Error on truncation
            assertThatThrownBy(() ->
                    create()
                            .addRoundTrip("char(3)", "'ABCD'", createCharType(3), "CAST('ABCD' AS char(3))")
                            .execute(getQueryRunner(), teradataJDBCCreateAndInsert("chart")))
                    .isInstanceOf(RuntimeException.class)
                    .hasCauseInstanceOf(SQLException.class)
                    .cause()
                    .hasMessageContaining("Right truncation of string data");
        }
        // max-size
        create()
                .addRoundTrip("char(64000)", "'max'", createCharType(64000), "CAST('max' AS char(64000))")
                .execute(getQueryRunner(), teradataJDBCCreateAndInsert("charl"));
    }

    @Test
    void testVarchar()
    {
        create()
                .addRoundTrip("varchar(32)", "''", createVarcharType(32), "CAST('' AS varchar(32))")
                .addRoundTrip("varchar(32)", "' '", createVarcharType(32), "CAST(' ' AS varchar(32))")
                .addRoundTrip("varchar(32)", "' '", createVarcharType(32), "CAST(' ' AS varchar(32))")
                .addRoundTrip("varchar(32)", "'  '", createVarcharType(32), "CAST('  ' AS varchar(32))")
                .addRoundTrip("varchar(32)", "'   '", createVarcharType(32), "CAST('   ' AS varchar(32))")
                .addRoundTrip("varchar(32)", "'A'", createVarcharType(32), "CAST('A' AS varchar(32))")
                .addRoundTrip("varchar(32)", "'A  '", createVarcharType(32), "CAST('A  ' AS varchar(32))")
                .addRoundTrip("varchar(32)", "' B '", createVarcharType(32), "CAST(' B ' AS varchar(32))")
                .addRoundTrip("varchar(32)", "'  C'", createVarcharType(32), "CAST('  C' AS varchar(32))")
                .addRoundTrip("varchar(32)", "'AB'", createVarcharType(32), "CAST('AB' AS varchar(32))")
                .addRoundTrip("varchar(32)", "'ABC'", createVarcharType(32), "CAST('ABC' AS varchar(32))")
                .addRoundTrip("varchar(32)", "'A C'", createVarcharType(32), "CAST('A C' AS varchar(32))")
                .addRoundTrip("varchar(32)", "' BC'", createVarcharType(32), "CAST(' BC' AS varchar(32))")
                .addRoundTrip("varchar(32)", "null", createVarcharType(32), "CAST(null AS varchar(32))")
                .execute(getQueryRunner(), teradataJDBCCreateAndInsert("varchar"));
        String teraMode = database.getTMode();
        if (teraMode.equals("TERA")) {
            // truncation
            create()
                    .addRoundTrip("varchar(3)", "'ABCD'", createVarcharType(3), "CAST('ABCD' AS varchar(3))")
                    .execute(getQueryRunner(), teradataJDBCCreateAndInsert("varchart"));
        }
        else {
            // Error on truncation
            assertThatThrownBy(() ->
                    create()
                            .addRoundTrip("varchar(3)", "'ABCD'", createVarcharType(3), "CAST('ABCD' AS varchar(3))")
                            .execute(getQueryRunner(), teradataJDBCCreateAndInsert("varchart")))
                    .isInstanceOf(RuntimeException.class)
                    .hasCauseInstanceOf(SQLException.class)
                    .cause()
                    .hasMessageContaining("Right truncation of string data");
        }
        // max-size
        create()
                .addRoundTrip("long varchar", "'max'", createVarcharType(64000), "CAST('max' AS varchar(64000))")
                .execute(getQueryRunner(), teradataJDBCCreateAndInsert("varcharl"));
    }

    @Test
    void testDate()
    {
        create()
                .addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'")
                .addRoundTrip("date", "DATE '0012-12-12'", DATE, "DATE '0012-12-12'")
                .addRoundTrip("date", "DATE '1500-01-01'", DATE, "DATE '1500-01-01'")
                .addRoundTrip("date", "DATE '1582-10-04'", DATE, "DATE '1582-10-04'")
                .addRoundTrip("date", "DATE '1582-10-15'", DATE, "DATE '1582-10-15'")
                .addRoundTrip("date", "DATE '1952-04-03'", DATE, "DATE '1952-04-03'")
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'")
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'")
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'")
                .addRoundTrip("date", "DATE '2024-02-29'", DATE, "DATE '2024-02-29'")
                .addRoundTrip("date", "DATE '9999-12-30'", DATE, "DATE '9999-12-30'")
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                .execute(getQueryRunner(), teradataJDBCCreateAndInsert("date"));
    }

    private DataSetup teradataJDBCCreateAndInsert(String tableNamePrefix)
    {
        String prefix = String.format("%s.%s", database.getDatabaseName(), tableNamePrefix);
        return new CreateAndInsertDataSetup(database, prefix);
    }
}
