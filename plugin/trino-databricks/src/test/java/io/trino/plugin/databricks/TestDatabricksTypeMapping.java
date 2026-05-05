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
package io.trino.plugin.databricks;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDatabricksTypeMapping
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DatabricksQueryRunner.builder()
                .build();
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN, "true")
                .addRoundTrip("boolean", "false", BOOLEAN, "false")
                .addRoundTrip("boolean", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .execute(getQueryRunner(), databricksCreateAndInsert("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_boolean"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_boolean"));
    }

    @Test
    public void testTinyint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "NULL", TINYINT, "CAST(NULL AS TINYINT)")
                .addRoundTrip("tinyint", "-128", TINYINT, "TINYINT '-128'")
                .addRoundTrip("tinyint", "5", TINYINT, "TINYINT '5'")
                .addRoundTrip("tinyint", "127", TINYINT, "TINYINT '127'")
                .execute(getQueryRunner(), databricksCreateAndInsert("test_tinyint"));
    }

    @Test
    public void testSmallint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "NULL", SMALLINT, "CAST(NULL AS SMALLINT)")
                .addRoundTrip("smallint", "-32768", SMALLINT, "SMALLINT '-32768'")
                .addRoundTrip("smallint", "32767", SMALLINT, "SMALLINT '32767'")
                .execute(getQueryRunner(), databricksCreateAndInsert("test_smallint"));
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("int", "NULL", INTEGER, "CAST(NULL AS INTEGER)")
                .addRoundTrip("int", "-2147483648", INTEGER, "INTEGER '-2147483648'")
                .addRoundTrip("int", "2147483647", INTEGER, "INTEGER '2147483647'")
                .execute(getQueryRunner(), databricksCreateAndInsert("test_integer"));
    }

    @Test
    public void testBigint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "BIGINT '-9223372036854775808'")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "BIGINT '9223372036854775807'")
                .execute(getQueryRunner(), databricksCreateAndInsert("test_bigint"));
    }

    @Test
    public void testFloat()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("float", "NULL", REAL, "CAST(NULL AS REAL)")
                .addRoundTrip("float", "3.14", REAL, "REAL '3.14'")
                .addRoundTrip("float", "CAST('NaN' AS float)", REAL, "nan()")
                .execute(getQueryRunner(), databricksCreateAndInsert("test_float"));
    }

    @Test
    public void testDouble()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("double", "3.14", DOUBLE, "DOUBLE '3.14'")
                .addRoundTrip("double", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("double", "CAST('NaN' AS DOUBLE)", DOUBLE, "nan()")
                .addRoundTrip("double", "CAST('Infinity' AS DOUBLE)", DOUBLE, "+infinity()")
                .addRoundTrip("double", "CAST('-Infinity' AS DOUBLE)", DOUBLE, "-infinity()")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_double"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_double"));
    }

    @Test
    public void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("decimal(3, 0)", "NULL", createDecimalType(3, 0), "CAST(NULL AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 0)", "CAST('193' AS decimal(3, 0))", createDecimalType(3, 0), "CAST('193' AS decimal(3, 0))")
                .addRoundTrip("decimal(3, 1)", "CAST('10.1' AS decimal(3, 1))", createDecimalType(3, 1), "CAST('10.1' AS decimal(3, 1))")
                .addRoundTrip("decimal(30, 5)", "CAST('3141592653589793238462643.38327' AS decimal(30, 5))", createDecimalType(30, 5), "CAST('3141592653589793238462643.38327' AS decimal(30, 5))")
                .addRoundTrip("decimal(38, 0)", "CAST('99999999999999999999999999999999999999' AS decimal(38, 0))", createDecimalType(38, 0), "CAST('99999999999999999999999999999999999999' AS decimal(38, 0))")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_decimal"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_decimal"));
    }

    @Test
    public void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("string", "''\''test string'\\'''", createUnboundedVarcharType(), "CAST('test string'' AS VARCHAR)")
                .addRoundTrip("string", "'NULL'", createUnboundedVarcharType(), "CAST('NULL' AS VARCHAR)")
                .execute(getQueryRunner(), databricksCreateAndInsert("test_varchar"));

        SqlDataTypeTest.create()
                .addRoundTrip("varchar", "NULL", createUnboundedVarcharType(), "CAST(NULL AS VARCHAR)")
                .addRoundTrip("varchar", "'hello'", createUnboundedVarcharType(), "CAST('hello' AS VARCHAR)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_varchar"));
    }

    @Test
    public void testDate()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'")
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'")
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_date"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_date"));
    }

    @Test
    public void testTimestamp()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2019-03-18 10:01:17.987'", createTimestampType(3), "TIMESTAMP '2019-03-18 10:01:17.987'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2018-10-28 01:33:17.456'", createTimestampType(3), "TIMESTAMP '2018-10-28 01:33:17.456'")
                .addRoundTrip("timestamp(3)", "TIMESTAMP '2038-01-19 03:14:07.000'", createTimestampType(3), "TIMESTAMP '2038-01-19 03:14:07.000'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test_timestamp"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test_timestamp"));
    }

    private DataSetup trinoCreateAsSelect(String tableNamePrefix)
    {
        return trinoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(String tableNamePrefix)
    {
        return trinoCreateAndInsert(getSession(), tableNamePrefix);
    }

    private DataSetup trinoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new TrinoSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup databricksCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(TestingDatabricksServer::execute, tableNamePrefix);
    }
}
