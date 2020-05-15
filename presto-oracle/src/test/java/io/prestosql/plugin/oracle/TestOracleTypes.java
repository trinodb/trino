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

import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.datatype.CreateAsSelectDataSetup;
import io.prestosql.testing.datatype.DataSetup;
import io.prestosql.testing.datatype.DataType;
import io.prestosql.testing.datatype.DataTypeTest;
import io.prestosql.testing.sql.PrestoSqlExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;

import static io.prestosql.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.datatype.DataType.stringDataType;
import static io.prestosql.testing.datatype.DataType.timestampDataType;
import static io.prestosql.testing.datatype.DataType.varcharDataType;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;

public class TestOracleTypes
        extends AbstractTestQueryFramework
{
    private TestingOracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.oracleServer = new TestingOracleServer();
        return createOracleQueryRunner(oracleServer);
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
        oracleServer.execute("CREATE TABLE test (num1 number, num2 number(*,-2))");
        oracleServer.execute("INSERT INTO test VALUES (12345678901234567890.12345678901234567890123456789012345678, 1234567890.123)");
        assertQuery("SELECT * FROM test", "VALUES (12345678901234567890.1234567890, 1234567900.0000000000)");
    }

    @Test
    public void testDateTimeType()
    {
        DataTypeTest.create()
                .addRoundTrip(dateOracleType(), LocalDate.of(2020, 1, 1))
                .addRoundTrip(timestampDataType(), LocalDateTime.of(2020, 1, 1, 13, 10, 1))
                .execute(getQueryRunner(), prestoCreateAsSelect("datetime_types"));
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

    public static DataType<LocalDate> dateOracleType()
    {
        return DataType.dataType(
                "date",
                TimestampType.TIMESTAMP,
                DateTimeFormatter.ofPattern("'DATE '''yyyy-MM-dd''")::format,
                LocalDate::atStartOfDay);
    }
}
