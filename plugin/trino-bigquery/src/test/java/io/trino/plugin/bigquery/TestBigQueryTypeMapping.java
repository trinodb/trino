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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;

public class TestBigQueryTypeMapping
        extends AbstractTestQueryFramework
{
    private BigQueryQueryRunner.BigQuerySqlExecutor bigQuerySqlExecutor;

    @BeforeClass(alwaysRun = true)
    public void initBigQueryExecutor()
    {
        bigQuerySqlExecutor = new BigQueryQueryRunner.BigQuerySqlExecutor();
        bigQuerySqlExecutor.deleteSelfCreatedDatasets();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BigQueryQueryRunner.createQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of());
    }

    @Test
    public void testFloat()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("float64", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("float64", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("float64", "123.456E10", DOUBLE, "123.456E10")
                .addRoundTrip("float64", "CAST('NaN' AS float64)", DOUBLE, "nan()")
                .addRoundTrip("float64", "CAST('Infinity' AS float64)", DOUBLE, "+infinity()")
                .addRoundTrip("float64", "CAST('-Infinity' AS float64)", DOUBLE, "-infinity()")
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.float"));
    }

    @Test
    public void testNumericMapping()
    {
        // Max precision is 29 when the scale is 0 in BigQuery
        // Valid scale range is between 0 and 9 in BigQuery

        // Use "NUMERIC 'value'" style because BigQuery doesn't accept parameterized cast "CAST (... AS NUMERIC(p, s))"
        SqlDataTypeTest.create()
                .addRoundTrip("NUMERIC(3, 0)", "NUMERIC '193'", createDecimalType(3, 0), "CAST(193 AS DECIMAL(3, 0))")
                .addRoundTrip("NUMERIC(3, 0)", "NUMERIC '19'", createDecimalType(3, 0), "CAST(19 AS DECIMAL(3, 0))")
                .addRoundTrip("NUMERIC(3, 0)", "NUMERIC '-193'", createDecimalType(3, 0), "CAST(-193 AS DECIMAL(3, 0))")
                .addRoundTrip("NUMERIC(3, 1)", "NUMERIC '10.0'", createDecimalType(3, 1), "CAST(10.0 AS DECIMAL(3, 1))")
                .addRoundTrip("NUMERIC(3, 1)", "NUMERIC '10.1'", createDecimalType(3, 1), "CAST(10.1 AS DECIMAL(3, 1))")
                .addRoundTrip("NUMERIC(3, 1)", "NUMERIC '-10.1'", createDecimalType(3, 1), "CAST(-10.1 AS DECIMAL(3, 1))")
                .addRoundTrip("NUMERIC(4, 2)", "NUMERIC '2'", createDecimalType(4, 2), "CAST(2 AS DECIMAL(4, 2))")
                .addRoundTrip("NUMERIC(4, 2)", "NUMERIC '2.3'", createDecimalType(4, 2), "CAST(2.3 AS DECIMAL(4, 2))")
                .addRoundTrip("NUMERIC(24, 2)", "NUMERIC '2'", createDecimalType(24, 2), "CAST(2 AS DECIMAL(24, 2))")
                .addRoundTrip("NUMERIC(24, 2)", "NUMERIC '2.3'", createDecimalType(24, 2), "CAST(2.3 AS DECIMAL(24, 2))")
                .addRoundTrip("NUMERIC(24, 2)", "NUMERIC '123456789.3'", createDecimalType(24, 2), "CAST(123456789.3 AS DECIMAL(24, 2))")
                .addRoundTrip("NUMERIC(24, 4)", "NUMERIC '12345678901234567890.31'", createDecimalType(24, 4), "CAST(12345678901234567890.31 AS DECIMAL(24, 4))")
                .addRoundTrip("NUMERIC(29, 0)", "NUMERIC '27182818284590452353602874713'", createDecimalType(29, 0), "CAST('27182818284590452353602874713' AS DECIMAL(29, 0))")
                .addRoundTrip("NUMERIC(29, 0)", "NUMERIC '-27182818284590452353602874713'", createDecimalType(29, 0), "CAST('-27182818284590452353602874713' AS DECIMAL(29, 0))")
                .addRoundTrip("NUMERIC(30, 5)", "NUMERIC '3141592653589793238462643.38327'", createDecimalType(30, 5), "CAST(3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip("NUMERIC(30, 5)", "NUMERIC '-3141592653589793238462643.38327'", createDecimalType(30, 5), "CAST(-3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip("NUMERIC(38, 9)", "NUMERIC '100000000020000000001234567.123456789'", createDecimalType(38, 9), "CAST(100000000020000000001234567.123456789 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(38, 9)", "NUMERIC '-100000000020000000001234567.123456789'", createDecimalType(38, 9), "CAST(-100000000020000000001234567.123456789 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(10, 3)", "CAST(NULL AS NUMERIC)", createDecimalType(10, 3), "CAST(NULL AS DECIMAL(10, 3))")
                .addRoundTrip("NUMERIC(38, 9)", "CAST(NULL AS NUMERIC)", createDecimalType(38, 9), "CAST(NULL AS DECIMAL(38, 9))")
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.numeric"));
    }

    private DataSetup bigqueryCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(getBigQuerySqlExecutor(), tableNamePrefix);
    }

    private SqlExecutor getBigQuerySqlExecutor()
    {
        return sql -> bigQuerySqlExecutor.execute(sql);
    }
}
