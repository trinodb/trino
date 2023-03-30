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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.CreateAndTrinoInsertDataSetup;
import io.trino.testing.datatype.CreateAsSelectDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @see <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types">BigQuery data types</a>
 */
public abstract class BaseBigQueryTypeMapping
        extends AbstractTestQueryFramework
{
    private BigQueryQueryRunner.BigQuerySqlExecutor bigQuerySqlExecutor;

    @BeforeClass(alwaysRun = true)
    public void initBigQueryExecutor()
    {
        bigQuerySqlExecutor = new BigQueryQueryRunner.BigQuerySqlExecutor();
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN, "true")
                .addRoundTrip("boolean", "false", BOOLEAN, "false")
                .addRoundTrip("boolean", "NULL", BOOLEAN, "CAST(NULL AS BOOLEAN)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test.boolean"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test.boolean"))
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.boolean"))
                .execute(getQueryRunner(), bigqueryViewCreateAndInsert("test.boolean"));
    }

    @Test
    public void testBytes()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bytes", "NULL", VARBINARY, "CAST(NULL AS VARBINARY)")
                .addRoundTrip("bytes", "b''", VARBINARY, "X''")
                .addRoundTrip("bytes", "from_hex('68656C6C6F')", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("bytes", "from_hex('5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD')", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("bytes", "from_hex('4261672066756C6C206F6620F09F92B0')", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("bytes", "from_hex('0001020304050607080DF9367AA7000000')", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("bytes", "from_hex('000000000000')", VARBINARY, "X'000000000000'")
                .addRoundTrip("bytes(10)", "from_hex('68656C6C6F')", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("bytes(4001)", "from_hex('68656C6C6F')", VARBINARY, "to_utf8('hello')")
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.bytes"))
                .execute(getQueryRunner(), bigqueryViewCreateAndInsert("test.bytes"));

        SqlDataTypeTest.create()
                .addRoundTrip("varbinary", "NULL", VARBINARY, "CAST(NULL AS VARBINARY)")
                .addRoundTrip("varbinary", "X''", VARBINARY, "X''")
                .addRoundTrip("varbinary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .addRoundTrip("varbinary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'", VARBINARY, "to_utf8('Piękna łąka w 東京都')")
                .addRoundTrip("varbinary", "X'4261672066756C6C206F6620F09F92B0'", VARBINARY, "to_utf8('Bag full of 💰')")
                .addRoundTrip("varbinary", "X'0001020304050607080DF9367AA7000000'", VARBINARY, "X'0001020304050607080DF9367AA7000000'") // non-text
                .addRoundTrip("varbinary", "X'000000000000'", VARBINARY, "X'000000000000'")
                .addRoundTrip("varbinary", "X'68656C6C6F'", VARBINARY, "to_utf8('hello')")
                .execute(getQueryRunner(), trinoCreateAsSelect("test.varbinary"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test.varbinary"));
    }

    @Test(dataProvider = "bigqueryIntegerTypeProvider")
    public void testInt64(String inputType)
    {
        SqlDataTypeTest.create()
                .addRoundTrip(inputType, "-9223372036854775808", BIGINT, "-9223372036854775808")
                .addRoundTrip(inputType, "9223372036854775807", BIGINT, "9223372036854775807")
                .addRoundTrip(inputType, "0", BIGINT, "CAST(0 AS BIGINT)")
                .addRoundTrip(inputType, "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.integer"))
                .execute(getQueryRunner(), bigqueryViewCreateAndInsert("test.integer"));
    }

    @DataProvider
    public Object[][] bigqueryIntegerTypeProvider()
    {
        // BYTEINT, TINYINT, SMALLINT, INTEGER, INT and BIGINT are aliases for INT64 in BigQuery
        return new Object[][] {
                {"BYTEINT"},
                {"TINYINT"},
                {"SMALLINT"},
                {"INTEGER"},
                {"INT64"},
                {"INT"},
                {"BIGINT"},
        };
    }

    @Test
    public void testTinyint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("tinyint", "-128", BIGINT, "BIGINT '-128'")
                .addRoundTrip("tinyint", "5", BIGINT, "BIGINT '5'")
                .addRoundTrip("tinyint", "127", BIGINT, "BIGINT '127'")
                .addRoundTrip("tinyint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test.tinyint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test.tinyint"));
    }

    @Test
    public void testSmallint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("smallint", "-32768", BIGINT, "BIGINT '-32768'")
                .addRoundTrip("smallint", "32456", BIGINT, "BIGINT '32456'")
                .addRoundTrip("smallint", "32767", BIGINT, "BIGINT '32767'")
                .addRoundTrip("smallint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test.smallint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test.smallint"));
    }

    @Test
    public void testInteger()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "-2147483648", BIGINT, "BIGINT '-2147483648'")
                .addRoundTrip("integer", "1234567890", BIGINT, "BIGINT '1234567890'")
                .addRoundTrip("integer", "2147483647", BIGINT, "BIGINT '2147483647'")
                .addRoundTrip("integer", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test.integer"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test.integer"));
    }

    @Test
    public void testBigint()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("bigint", "-9223372036854775808", BIGINT, "-9223372036854775808")
                .addRoundTrip("bigint", "9223372036854775807", BIGINT, "9223372036854775807")
                .addRoundTrip("bigint", "0", BIGINT, "CAST(0 AS BIGINT)")
                .addRoundTrip("bigint", "NULL", BIGINT, "CAST(NULL AS BIGINT)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test.bigint"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test.bigint"));
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
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.float"))
                .execute(getQueryRunner(), bigqueryViewCreateAndInsert("test.float"));
    }

    @Test
    public void testDouble()
    {
        // TODO: Add nan, infinity, -infinity cases. Currently, it fails by IllegalArgumentException without helpful message
        SqlDataTypeTest.create()
                .addRoundTrip("double", "NULL", DOUBLE, "CAST(NULL AS DOUBLE)")
                .addRoundTrip("double", "double '1.0E100'", DOUBLE, "1.0E100")
                .addRoundTrip("double", "double '123.456E10'", DOUBLE, "123.456E10")
                .execute(getQueryRunner(), trinoCreateAsSelect("test.double"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test.double"));
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

    @Test
    public void testNumericWriteMapping()
    {
        // Max precision is 29 when the scale is 0 in BigQuery
        // Valid scale range is between 0 and 9 in BigQuery

        SqlDataTypeTest.create()
                .addRoundTrip("NUMERIC(3, 0)", "CAST(193 AS DECIMAL(3, 0))", createDecimalType(3, 0), "CAST(193 AS DECIMAL(3, 0))")
                .addRoundTrip("NUMERIC(3, 0)", "CAST(19 AS DECIMAL(3, 0))", createDecimalType(3, 0), "CAST(19 AS DECIMAL(3, 0))")
                .addRoundTrip("NUMERIC(3, 0)", "CAST(-193 AS DECIMAL(3, 0))", createDecimalType(3, 0), "CAST(-193 AS DECIMAL(3, 0))")
                .addRoundTrip("NUMERIC(3, 1)", "CAST(10.0 AS DECIMAL(3, 1))", createDecimalType(3, 1), "CAST(10.0 AS DECIMAL(3, 1))")
                .addRoundTrip("NUMERIC(3, 1)", "CAST(10.1 AS DECIMAL(3, 1))", createDecimalType(3, 1), "CAST(10.1 AS DECIMAL(3, 1))")
                .addRoundTrip("NUMERIC(3, 1)", "CAST(-10.1 AS DECIMAL(3, 1))", createDecimalType(3, 1), "CAST(-10.1 AS DECIMAL(3, 1))")
                .addRoundTrip("NUMERIC(4, 2)", "CAST(2 AS DECIMAL(4, 2))", createDecimalType(4, 2), "CAST(2 AS DECIMAL(4, 2))")
                .addRoundTrip("NUMERIC(4, 2)", "CAST(2.3 AS DECIMAL(4, 2))", createDecimalType(4, 2), "CAST(2.3 AS DECIMAL(4, 2))")
                .addRoundTrip("NUMERIC(24, 2)", "CAST(2 AS DECIMAL(24, 2))", createDecimalType(24, 2), "CAST(2 AS DECIMAL(24, 2))")
                .addRoundTrip("NUMERIC(24, 2)", "CAST(2.3 AS DECIMAL(24, 2))", createDecimalType(24, 2), "CAST(2.3 AS DECIMAL(24, 2))")
                .addRoundTrip("NUMERIC(24, 2)", "CAST(123456789.3 AS DECIMAL(24, 2))", createDecimalType(24, 2), "CAST(123456789.3 AS DECIMAL(24, 2))")
                .addRoundTrip("NUMERIC(24, 4)", "CAST(12345678901234567890.31 AS DECIMAL(24, 4))", createDecimalType(24, 4), "CAST(12345678901234567890.31 AS DECIMAL(24, 4))")
                .addRoundTrip("NUMERIC(29, 0)", "CAST('27182818284590452353602874713' AS DECIMAL(29, 0))", createDecimalType(29, 0), "CAST('27182818284590452353602874713' AS DECIMAL(29, 0))")
                .addRoundTrip("NUMERIC(29, 0)", "CAST('-27182818284590452353602874713' AS DECIMAL(29, 0))", createDecimalType(29, 0), "CAST('-27182818284590452353602874713' AS DECIMAL(29, 0))")
                .addRoundTrip("NUMERIC(30, 5)", "CAST(3141592653589793238462643.38327 AS DECIMAL(30, 5))", createDecimalType(30, 5), "CAST(3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip("NUMERIC(30, 5)", "CAST(-3141592653589793238462643.38327 AS DECIMAL(30, 5))", createDecimalType(30, 5), "CAST(-3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip("NUMERIC(38, 9)", "CAST(100000000020000000001234567.123456789 AS DECIMAL(38, 9))", createDecimalType(38, 9), "CAST(100000000020000000001234567.123456789 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(38, 9)", "CAST(-100000000020000000001234567.123456789 AS DECIMAL(38, 9))", createDecimalType(38, 9), "CAST(-100000000020000000001234567.123456789 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(10, 3)", "CAST(NULL AS DECIMAL(10, 3))", createDecimalType(10, 3), "CAST(NULL AS DECIMAL(10, 3))")
                .addRoundTrip("NUMERIC(38, 9)", "CAST(NULL AS DECIMAL(38, 9))", createDecimalType(38, 9), "CAST(NULL AS DECIMAL(38, 9))")
                .execute(getQueryRunner(), bigqueryCreateAndTrinoInsert("test.writenumeric"));
    }

    @Test
    public void testNumericMappingView()
    {
        // BigQuery views always return DECIMAL(38, 9)
        SqlDataTypeTest.create()
                .addRoundTrip("NUMERIC(3, 0)", "NUMERIC '193'", createDecimalType(38, 9), "CAST(193 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(3, 0)", "NUMERIC '19'", createDecimalType(38, 9), "CAST(19 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(3, 0)", "NUMERIC '-193'", createDecimalType(38, 9), "CAST(-193 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(3, 1)", "NUMERIC '10.0'", createDecimalType(38, 9), "CAST(10.0 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(3, 1)", "NUMERIC '10.1'", createDecimalType(38, 9), "CAST(10.1 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(3, 1)", "NUMERIC '-10.1'", createDecimalType(38, 9), "CAST(-10.1 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(4, 2)", "NUMERIC '2'", createDecimalType(38, 9), "CAST(2 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(4, 2)", "NUMERIC '2.3'", createDecimalType(38, 9), "CAST(2.3 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(24, 2)", "NUMERIC '2'", createDecimalType(38, 9), "CAST(2 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(24, 2)", "NUMERIC '2.3'", createDecimalType(38, 9), "CAST(2.3 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(24, 2)", "NUMERIC '123456789.3'", createDecimalType(38, 9), "CAST(123456789.3 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(24, 4)", "NUMERIC '12345678901234567890.31'", createDecimalType(38, 9), "CAST(12345678901234567890.31 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(29, 0)", "NUMERIC '27182818284590452353602874713'", createDecimalType(38, 9), "CAST('27182818284590452353602874713' AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(29, 0)", "NUMERIC '-27182818284590452353602874713'", createDecimalType(38, 9), "CAST('-27182818284590452353602874713' AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(30, 5)", "NUMERIC '3141592653589793238462643.38327'", createDecimalType(38, 9), "CAST(3141592653589793238462643.38327 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(30, 5)", "NUMERIC '-3141592653589793238462643.38327'", createDecimalType(38, 9), "CAST(-3141592653589793238462643.38327 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(38, 9)", "NUMERIC '100000000020000000001234567.123456789'", createDecimalType(38, 9), "CAST(100000000020000000001234567.123456789 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(38, 9)", "NUMERIC '-100000000020000000001234567.123456789'", createDecimalType(38, 9), "CAST(-100000000020000000001234567.123456789 AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(10, 3)", "CAST(NULL AS NUMERIC)", createDecimalType(38, 9), "CAST(NULL AS DECIMAL(38, 9))")
                .addRoundTrip("NUMERIC(38, 9)", "CAST(NULL AS NUMERIC)", createDecimalType(38, 9), "CAST(NULL AS DECIMAL(38, 9))")
                .execute(getQueryRunner(), bigqueryViewCreateAndInsert("test.numeric"));
    }

    @Test
    public void testInvalidNumericScaleType()
    {
        String tableName = "test.invalid_numeric_scale_" + randomNameSuffix();
        try {
            assertThatThrownBy(() -> getBigQuerySqlExecutor().execute(format("CREATE TABLE %s (invalid_type NUMERIC(38, 10))", tableName)))
                    .hasMessageContaining("In NUMERIC(P, S), S must be between 0 and 9");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testBigNumericMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("BIGNUMERIC(3, 0)", "BIGNUMERIC '193'", createDecimalType(3, 0), "CAST(193 AS DECIMAL(3, 0))")
                .addRoundTrip("BIGNUMERIC(3, 0)", "BIGNUMERIC '19'", createDecimalType(3, 0), "CAST(19 AS DECIMAL(3, 0))")
                .addRoundTrip("BIGNUMERIC(3, 0)", "BIGNUMERIC '-193'", createDecimalType(3, 0), "CAST(-193 AS DECIMAL(3, 0))")
                .addRoundTrip("BIGNUMERIC(3, 1)", "BIGNUMERIC '10.0'", createDecimalType(3, 1), "CAST(10.0 AS DECIMAL(3, 1))")
                .addRoundTrip("BIGNUMERIC(3, 1)", "BIGNUMERIC '10.1'", createDecimalType(3, 1), "CAST(10.1 AS DECIMAL(3, 1))")
                .addRoundTrip("BIGNUMERIC(3, 1)", "BIGNUMERIC '-10.1'", createDecimalType(3, 1), "CAST(-10.1 AS DECIMAL(3, 1))")
                .addRoundTrip("BIGNUMERIC(4, 2)", "BIGNUMERIC '2'", createDecimalType(4, 2), "CAST(2 AS DECIMAL(4, 2))")
                .addRoundTrip("BIGNUMERIC(4, 2)", "BIGNUMERIC '2.3'", createDecimalType(4, 2), "CAST(2.3 AS DECIMAL(4, 2))")
                .addRoundTrip("BIGNUMERIC(24, 2)", "BIGNUMERIC '2'", createDecimalType(24, 2), "CAST(2 AS DECIMAL(24, 2))")
                .addRoundTrip("BIGNUMERIC(24, 2)", "BIGNUMERIC '2.3'", createDecimalType(24, 2), "CAST(2.3 AS DECIMAL(24, 2))")
                .addRoundTrip("BIGNUMERIC(24, 2)", "BIGNUMERIC '123456789.3'", createDecimalType(24, 2), "CAST(123456789.3 AS DECIMAL(24, 2))")
                .addRoundTrip("BIGNUMERIC(24, 4)", "BIGNUMERIC '12345678901234567890.31'", createDecimalType(24, 4), "CAST(12345678901234567890.31 AS DECIMAL(24, 4))")
                .addRoundTrip("BIGNUMERIC(29, 0)", "BIGNUMERIC '27182818284590452353602874713'", createDecimalType(29, 0), "CAST('27182818284590452353602874713' AS DECIMAL(29, 0))")
                .addRoundTrip("BIGNUMERIC(29, 0)", "BIGNUMERIC '-27182818284590452353602874713'", createDecimalType(29, 0), "CAST('-27182818284590452353602874713' AS DECIMAL(29, 0))")
                .addRoundTrip("BIGNUMERIC(30, 5)", "BIGNUMERIC '3141592653589793238462643.38327'", createDecimalType(30, 5), "CAST(3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip("BIGNUMERIC(30, 5)", "BIGNUMERIC '-3141592653589793238462643.38327'", createDecimalType(30, 5), "CAST(-3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip("BIGNUMERIC(38, 9)", "BIGNUMERIC '100000000020000000001234567.123456789'", createDecimalType(38, 9), "CAST(100000000020000000001234567.123456789 AS DECIMAL(38, 9))")
                .addRoundTrip("BIGNUMERIC(38, 9)", "BIGNUMERIC '-100000000020000000001234567.123456789'", createDecimalType(38, 9), "CAST(-100000000020000000001234567.123456789 AS DECIMAL(38, 9))")
                .addRoundTrip("BIGNUMERIC(10, 3)", "CAST(NULL AS BIGNUMERIC)", createDecimalType(10, 3), "CAST(NULL AS DECIMAL(10, 3))")
                .addRoundTrip("BIGNUMERIC(38, 9)", "CAST(NULL AS BIGNUMERIC)", createDecimalType(38, 9), "CAST(NULL AS DECIMAL(38, 9))")
                .addRoundTrip("BIGNUMERIC(1)", "BIGNUMERIC '1'", createDecimalType(1, 0), "CAST(1 AS DECIMAL(1, 0))")
                .addRoundTrip("BIGNUMERIC(1)", "BIGNUMERIC '-1'", createDecimalType(1, 0), "CAST(-1 AS DECIMAL(1, 0))")
                .addRoundTrip("BIGNUMERIC(38)", "BIGNUMERIC '10000000002000000000300000000012345678'", createDecimalType(38, 0), "CAST('10000000002000000000300000000012345678' AS DECIMAL(38, 0))")
                .addRoundTrip("BIGNUMERIC(38)", "BIGNUMERIC '-10000000002000000000300000000012345678'", createDecimalType(38, 0), "CAST('-10000000002000000000300000000012345678' AS DECIMAL(38, 0))")
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.bignumeric"));
        // TODO (https://github.com/trinodb/trino/pull/12210) Add support for bigquery type in views
    }

    @Test
    public void testBigNumericWriteMapping()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("BIGNUMERIC(3, 0)", "CAST(193 AS DECIMAL(3, 0))", createDecimalType(3, 0), "CAST(193 AS DECIMAL(3, 0))")
                .addRoundTrip("BIGNUMERIC(3, 0)", "CAST(19 AS DECIMAL(3, 0))", createDecimalType(3, 0), "CAST(19 AS DECIMAL(3, 0))")
                .addRoundTrip("BIGNUMERIC(3, 0)", "CAST(-193 AS DECIMAL(3, 0))", createDecimalType(3, 0), "CAST(-193 AS DECIMAL(3, 0))")
                .addRoundTrip("BIGNUMERIC(3, 1)", "CAST(10.0 AS DECIMAL(3, 1))", createDecimalType(3, 1), "CAST(10.0 AS DECIMAL(3, 1))")
                .addRoundTrip("BIGNUMERIC(3, 1)", "CAST(10.1 AS DECIMAL(3, 1))", createDecimalType(3, 1), "CAST(10.1 AS DECIMAL(3, 1))")
                .addRoundTrip("BIGNUMERIC(3, 1)", "CAST(-10.1 AS DECIMAL(3, 1))", createDecimalType(3, 1), "CAST(-10.1 AS DECIMAL(3, 1))")
                .addRoundTrip("BIGNUMERIC(4, 2)", "CAST(2 AS DECIMAL(4, 2))", createDecimalType(4, 2), "CAST(2 AS DECIMAL(4, 2))")
                .addRoundTrip("BIGNUMERIC(4, 2)", "CAST(2.3 AS DECIMAL(4, 2))", createDecimalType(4, 2), "CAST(2.3 AS DECIMAL(4, 2))")
                .addRoundTrip("BIGNUMERIC(24, 2)", "CAST(2 AS DECIMAL(24, 2))", createDecimalType(24, 2), "CAST(2 AS DECIMAL(24, 2))")
                .addRoundTrip("BIGNUMERIC(24, 2)", "CAST(2.3 AS DECIMAL(24, 2))", createDecimalType(24, 2), "CAST(2.3 AS DECIMAL(24, 2))")
                .addRoundTrip("BIGNUMERIC(24, 2)", "CAST(123456789.3 AS DECIMAL(24, 2))", createDecimalType(24, 2), "CAST(123456789.3 AS DECIMAL(24, 2))")
                .addRoundTrip("BIGNUMERIC(24, 4)", "CAST(12345678901234567890.31 AS DECIMAL(24, 4))", createDecimalType(24, 4), "CAST(12345678901234567890.31 AS DECIMAL(24, 4))")
                .addRoundTrip("BIGNUMERIC(29, 0)", "CAST('27182818284590452353602874713' AS DECIMAL(29, 0))", createDecimalType(29, 0), "CAST('27182818284590452353602874713' AS DECIMAL(29, 0))")
                .addRoundTrip("BIGNUMERIC(29, 0)", "CAST('-27182818284590452353602874713' AS DECIMAL(29, 0))", createDecimalType(29, 0), "CAST('-27182818284590452353602874713' AS DECIMAL(29, 0))")
                .addRoundTrip("BIGNUMERIC(30, 5)", "CAST(3141592653589793238462643.38327 AS DECIMAL(30, 5))", createDecimalType(30, 5), "CAST(3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip("BIGNUMERIC(30, 5)", "CAST(-3141592653589793238462643.38327 AS DECIMAL(30, 5))", createDecimalType(30, 5), "CAST(-3141592653589793238462643.38327 AS DECIMAL(30, 5))")
                .addRoundTrip("BIGNUMERIC(38, 9)", "CAST(100000000020000000001234567.123456789 AS DECIMAL(38, 9))", createDecimalType(38, 9), "CAST(100000000020000000001234567.123456789 AS DECIMAL(38, 9))")
                .addRoundTrip("BIGNUMERIC(38, 9)", "CAST(-100000000020000000001234567.123456789 AS DECIMAL(38, 9))", createDecimalType(38, 9), "CAST(-100000000020000000001234567.123456789 AS DECIMAL(38, 9))")
                .addRoundTrip("BIGNUMERIC(10, 3)", "CAST(NULL AS DECIMAL(10, 3))", createDecimalType(10, 3), "CAST(NULL AS DECIMAL(10, 3))")
                .addRoundTrip("BIGNUMERIC(38, 9)", "CAST(NULL AS DECIMAL(38, 9))", createDecimalType(38, 9), "CAST(NULL AS DECIMAL(38, 9))")
                .addRoundTrip("BIGNUMERIC(1)", "CAST(1 AS DECIMAL(1, 0))", createDecimalType(1, 0), "CAST(1 AS DECIMAL(1, 0))")
                .addRoundTrip("BIGNUMERIC(1)", "CAST(-1 AS DECIMAL(1, 0))", createDecimalType(1, 0), "CAST(-1 AS DECIMAL(1, 0))")
                .execute(getQueryRunner(), bigqueryCreateAndTrinoInsert("test.writebignumeric"));
    }

    @Test
    public void testUnsupportedBigNumericMappingView()
    {
        assertThatThrownBy(() -> SqlDataTypeTest.create()
                .addRoundTrip("BIGNUMERIC(3, 0)", "BIGNUMERIC '193'", createDecimalType(3, 0), "CAST(193 AS DECIMAL(3, 0))")
                .execute(getQueryRunner(), bigqueryViewCreateAndInsert("test.bignumeric")))
                .hasMessageContaining("SELECT * not allowed from relation that has no columns");
    }

    @Test(dataProvider = "bigqueryUnsupportedBigNumericTypeProvider")
    public void testUnsupportedBigNumericMapping(String unsupportedTypeName)
    {
        try (TestTable table = new TestTable(getBigQuerySqlExecutor(), "test.unsupported_bignumeric", format("(supported_column INT64, unsupported_column %s)", unsupportedTypeName))) {
            assertQuery(
                    "DESCRIBE " + table.getName(),
                    "VALUES ('supported_column', 'bigint', '', '')");
        }
    }

    @DataProvider
    public Object[][] bigqueryUnsupportedBigNumericTypeProvider()
    {
        return new Object[][] {
                {"BIGNUMERIC"},
                {"BIGNUMERIC(40,2)"},
        };
    }

    @Test
    public void testDate()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("date", "NULL", DATE, "CAST(NULL AS DATE)")
                .addRoundTrip("date", "DATE '0001-01-01'", DATE, "DATE '0001-01-01'") // min value in BigQuery
                .addRoundTrip("date", "DATE '0012-12-12'", DATE, "DATE '0012-12-12'")
                .addRoundTrip("date", "DATE '1500-01-01'", DATE, "DATE '1500-01-01'")
                .addRoundTrip("date", "DATE '1582-10-04'", DATE, "DATE '1582-10-04'")
                .addRoundTrip("date", "DATE '1582-10-05'", DATE, "DATE '1582-10-05'") // begin julian->gregorian switch
                .addRoundTrip("date", "DATE '1582-10-14'", DATE, "DATE '1582-10-14'") // end julian->gregorian switch
                .addRoundTrip("date", "DATE '1582-10-15'", DATE, "DATE '1582-10-15'")
                .addRoundTrip("date", "DATE '1952-04-03'", DATE, "DATE '1952-04-03'")
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1970-02-03'", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "DATE '1970-01-01'", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "DATE '1983-04-01'", DATE, "DATE '1983-04-01'")
                .addRoundTrip("date", "DATE '1983-10-01'", DATE, "DATE '1983-10-01'")
                .addRoundTrip("date", "DATE '2017-07-01'", DATE, "DATE '2017-07-01'")
                .addRoundTrip("date", "DATE '2017-01-01'", DATE, "DATE '2017-01-01'")
                .addRoundTrip("date", "DATE '9999-12-31'", DATE, "DATE '9999-12-31'") // max value in BigQuery
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.date"))
                .execute(getQueryRunner(), bigqueryViewCreateAndInsert("test.date"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test.date"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test.date"));
    }

    @Test
    public void testTimestamp()
    {
        timestampTypeTest("timestamp(6)", "timestamp")
                .execute(getQueryRunner(), trinoCreateAsSelect("test.timestamp"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test.timestamp"));
    }

    @Test
    public void testDatetime()
    {
        timestampTypeTest("datetime", "datetime")
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.datetime"))
                .execute(getQueryRunner(), bigqueryViewCreateAndInsert("test.datetime"));
    }

    private SqlDataTypeTest timestampTypeTest(String inputType, String literalPrefix)
    {
        return SqlDataTypeTest.create()
                // min value in BigQuery
                .addRoundTrip(inputType, literalPrefix + " '0001-01-01 00:00:00.000'", createTimestampType(6), "TIMESTAMP '0001-01-01 00:00:00.000000'")
                // before epoch
                .addRoundTrip(inputType, literalPrefix + " '1958-01-01 13:18:03.123'", createTimestampType(6), "TIMESTAMP '1958-01-01 13:18:03.123000'")
                // after epoch
                .addRoundTrip(inputType, literalPrefix + " '2019-03-18 10:01:17.987'", createTimestampType(6), "TIMESTAMP '2019-03-18 10:01:17.987000'")
                .addRoundTrip(inputType, literalPrefix + " '2018-10-28 01:33:17.456'", createTimestampType(6), "TIMESTAMP '2018-10-28 01:33:17.456000'")
                .addRoundTrip(inputType, literalPrefix + " '2018-10-28 03:33:33.333'", createTimestampType(6), "TIMESTAMP '2018-10-28 03:33:33.333000'")
                // epoch
                .addRoundTrip(inputType, literalPrefix + " '1970-01-01 00:00:00.000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.000000'")
                .addRoundTrip(inputType, literalPrefix + " '1970-01-01 00:13:42.000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:13:42.000000'")
                .addRoundTrip(inputType, literalPrefix + " '2018-04-01 02:13:55.123'", createTimestampType(6), "TIMESTAMP '2018-04-01 02:13:55.123000'")
                .addRoundTrip(inputType, literalPrefix + " '2018-03-25 03:17:17.000'", createTimestampType(6), "TIMESTAMP '2018-03-25 03:17:17.000000'")
                .addRoundTrip(inputType, literalPrefix + " '1986-01-01 00:13:07.000'", createTimestampType(6), "TIMESTAMP '1986-01-01 00:13:07.000000'")

                // same as above but with higher precision
                .addRoundTrip(inputType, literalPrefix + " '1958-01-01 13:18:03.123456'", createTimestampType(6), "TIMESTAMP '1958-01-01 13:18:03.123456'")
                .addRoundTrip(inputType, literalPrefix + " '2019-03-18 10:01:17.987654'", createTimestampType(6), "TIMESTAMP '2019-03-18 10:01:17.987654'")
                .addRoundTrip(inputType, literalPrefix + " '2018-10-28 01:33:17.123456'", createTimestampType(6), "TIMESTAMP '2018-10-28 01:33:17.123456'")
                .addRoundTrip(inputType, literalPrefix + " '2018-10-28 03:33:33.333333'", createTimestampType(6), "TIMESTAMP '2018-10-28 03:33:33.333333'")
                .addRoundTrip(inputType, literalPrefix + " '1970-01-01 00:00:00.000000'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:00.000000'")
                .addRoundTrip(inputType, literalPrefix + " '1970-01-01 00:13:42.123456'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:13:42.123456'")
                .addRoundTrip(inputType, literalPrefix + " '2018-04-01 02:13:55.123456'", createTimestampType(6), "TIMESTAMP '2018-04-01 02:13:55.123456'")
                .addRoundTrip(inputType, literalPrefix + " '2018-03-25 03:17:17.456789'", createTimestampType(6), "TIMESTAMP '2018-03-25 03:17:17.456789'")
                .addRoundTrip(inputType, literalPrefix + " '1986-01-01 00:13:07.456789'", createTimestampType(6), "TIMESTAMP '1986-01-01 00:13:07.456789'")
                .addRoundTrip(inputType, literalPrefix + " '2021-09-07 23:59:59.999999'", createTimestampType(6), "TIMESTAMP '2021-09-07 23:59:59.999999'")

                // test arbitrary time for all supported precisions
                .addRoundTrip(inputType, literalPrefix + " '1970-01-01 00:00:01'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.000000'")
                .addRoundTrip(inputType, literalPrefix + " '1970-01-01 00:00:01.1'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.100000'")
                .addRoundTrip(inputType, literalPrefix + " '1970-01-01 00:00:01.12'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.120000'")
                .addRoundTrip(inputType, literalPrefix + " '1970-01-01 00:00:01.123'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.123000'")
                .addRoundTrip(inputType, literalPrefix + " '1970-01-01 00:00:01.1234'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.123400'")
                .addRoundTrip(inputType, literalPrefix + " '1970-01-01 00:00:01.12345'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.123450'")
                .addRoundTrip(inputType, literalPrefix + " '1970-01-01 00:00:01.123456'", createTimestampType(6), "TIMESTAMP '1970-01-01 00:00:01.123456'")

                // negative epoch
                .addRoundTrip(inputType, literalPrefix + " '1969-12-31 23:59:59.999995'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999995'")
                .addRoundTrip(inputType, literalPrefix + " '1969-12-31 23:59:59.999949'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999949'")
                .addRoundTrip(inputType, literalPrefix + " '1969-12-31 23:59:59.999994'", createTimestampType(6), "TIMESTAMP '1969-12-31 23:59:59.999994'")

                // max value in BigQuery
                .addRoundTrip(inputType, literalPrefix + " '9999-12-31 23:59:59.999999'", createTimestampType(6), "TIMESTAMP '9999-12-31 23:59:59.999999'");
    }

    @Test
    public void testUnsupportedDatetime()
    {
        try (TestTable table = new TestTable(getBigQuerySqlExecutor(), "test.unsupported_datetime", "(col datetime)")) {
            assertQueryFails("INSERT INTO " + table.getName() + " VALUES (timestamp '-0001-01-01 00:00:00.000000')", "Failed to insert rows.*");
            assertQueryFails("INSERT INTO " + table.getName() + " VALUES (timestamp '0000-12-31 23:59:59.999999')", "Failed to insert rows.*");
            assertQueryFails("INSERT INTO " + table.getName() + " VALUES (timestamp '10000-01-01 00:00:00.000000')", "Failed to insert rows.*");

            assertThatThrownBy(() -> getBigQuerySqlExecutor().execute("INSERT INTO " + table.getName() + " VALUES (datetime '-0001-01-01 00:00:00.000000')"))
                    .hasMessageContaining("Invalid DATETIME literal");
            assertThatThrownBy(() -> getBigQuerySqlExecutor().execute("INSERT INTO " + table.getName() + " VALUES (datetime '0000-12-31 23:59:59.999999')"))
                    .hasMessageContaining("Invalid DATETIME literal");
            assertThatThrownBy(() -> getBigQuerySqlExecutor().execute("INSERT INTO " + table.getName() + " VALUES (datetime '10000-01-01 00:00:00.000000')"))
                    .hasMessageContaining("Invalid DATETIME literal");
        }
    }

    @Test
    public void testTime()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("time", "'00:00:00'", createTimeType(6), "TIME '00:00:00.000000'")
                .addRoundTrip("time", "'00:00:00.000000'", createTimeType(6), "TIME '00:00:00.000000'")
                .addRoundTrip("time", "'00:00:00.123456'", createTimeType(6), "TIME '00:00:00.123456'")
                .addRoundTrip("time", "'12:34:56'", createTimeType(6), "TIME '12:34:56.000000'")
                .addRoundTrip("time", "'12:34:56.123456'", createTimeType(6), "TIME '12:34:56.123456'")

                // maximal value for a precision
                .addRoundTrip("time", "'23:59:59'", createTimeType(6), "TIME '23:59:59.000000'")
                .addRoundTrip("time", "'23:59:59.9'", createTimeType(6), "TIME '23:59:59.900000'")
                .addRoundTrip("time", "'23:59:59.99'", createTimeType(6), "TIME '23:59:59.990000'")
                .addRoundTrip("time", "'23:59:59.999'", createTimeType(6), "TIME '23:59:59.999000'")
                .addRoundTrip("time", "'23:59:59.9999'", createTimeType(6), "TIME '23:59:59.999900'")
                .addRoundTrip("time", "'23:59:59.99999'", createTimeType(6), "TIME '23:59:59.999990'")
                .addRoundTrip("time", "'23:59:59.999999'", createTimeType(6), "TIME '23:59:59.999999'")

                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.time"))
                .execute(getQueryRunner(), bigqueryViewCreateAndInsert("test.time"));
    }

    @Test
    public void testTimestampWithTimeZone()
    {
        SqlDataTypeTest.create()
                // min value in BigQuery
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '0001-01-01 00:00:00.000000 UTC'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '0001-01-01 00:00:00.000000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.000000 UTC'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 00:00:00.000000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.000000 Asia/Kathmandu'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1969-12-31 18:30:00.000000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.000000+02:17'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1969-12-31 21:43:00.000000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1970-01-01 00:00:00.000000-07:31'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1970-01-01 07:31:00.000000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1958-01-01 13:18:03.123456 UTC'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1958-01-01 13:18:03.123456 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1958-01-01 13:18:03.123000 Asia/Kathmandu'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1958-01-01 07:48:03.123000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1958-01-01 13:18:03.123000+02:17'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1958-01-01 11:01:03.123000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '1958-01-01 13:18:03.123000-07:31'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '1958-01-01 20:49:03.123000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2019-03-18 10:01:17.987654 UTC'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '2019-03-18 10:01:17.987654 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2019-03-18 10:01:17.987000 Asia/Kathmandu'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '2019-03-18 04:16:17.987000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2019-03-18 10:01:17.987000+02:17'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '2019-03-18 07:44:17.987000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2019-03-18 10:01:17.987000-07:31'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '2019-03-18 17:32:17.987000 UTC'")
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '2021-09-07 23:59:59.999999-00:00'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '2021-09-07 23:59:59.999999 UTC'")
                // max value in BigQuery
                .addRoundTrip("TIMESTAMP", "TIMESTAMP '9999-12-31 23:59:59.999999-00:00'",
                        TIMESTAMP_TZ_MICROS, "TIMESTAMP '9999-12-31 23:59:59.999999 UTC'")
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.timestamp_tz"));
        // TODO (https://github.com/trinodb/trino/pull/12210) Add support for timestamp with time zone type in views
    }

    @Test
    public void testString()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("STRING", "NULL", VARCHAR, "CAST(NULL AS VARCHAR)")
                .addRoundTrip("STRING", "'text_a'", VARCHAR, "VARCHAR 'text_a'")
                .addRoundTrip("STRING", "'攻殻機動隊'", VARCHAR, "VARCHAR '攻殻機動隊'")
                .addRoundTrip("STRING", "'😂'", VARCHAR, "VARCHAR '😂'")
                .addRoundTrip("STRING", "'Ну, погоди!'", VARCHAR, "VARCHAR 'Ну, погоди!'")
                .addRoundTrip("STRING(255)", "'text_b'", VARCHAR, "VARCHAR 'text_b'")
                .addRoundTrip("STRING(4001)", "'text_c'", VARCHAR, "VARCHAR 'text_c'")
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.string"))
                .execute(getQueryRunner(), bigqueryViewCreateAndInsert("test.string"));

        SqlDataTypeTest.create()
                .addRoundTrip("varchar", "NULL", VARCHAR, "CAST(NULL AS VARCHAR)")
                .addRoundTrip("varchar", "'text_a'", VARCHAR, "VARCHAR 'text_a'")
                .addRoundTrip("varchar", "'攻殻機動隊'", VARCHAR, "VARCHAR '攻殻機動隊'")
                .addRoundTrip("varchar", "'😂'", VARCHAR, "VARCHAR '😂'")
                .addRoundTrip("varchar", "'Ну, погоди!'", VARCHAR, "VARCHAR 'Ну, погоди!'")
                .addRoundTrip("varchar(255)", "'text_b'", VARCHAR, "VARCHAR 'text_b'")
                .addRoundTrip("varchar(4001)", "'text_c'", VARCHAR, "VARCHAR 'text_c'")
                .execute(getQueryRunner(), trinoCreateAsSelect("test.varchar"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test.varchar"));
    }

    @Test
    public void testGeography()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("GEOGRAPHY", "ST_GeogPoint(0, 0)", VARCHAR, "VARCHAR 'POINT(0 0)'")
                .addRoundTrip("GEOGRAPHY", "ST_GeogPoint(90, -90)", VARCHAR, "VARCHAR 'POINT(90 -90)'")
                .addRoundTrip("GEOGRAPHY", "NULL", VARCHAR, "CAST(NULL AS VARCHAR)")
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.geography"))
                .execute(getQueryRunner(), bigqueryViewCreateAndInsert("test.geography"));
    }

    @Test
    public void testArray()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("ARRAY(BOOLEAN)", "ARRAY[true]", new ArrayType(BOOLEAN), "ARRAY[true]")
                .addRoundTrip("ARRAY(INT)", "ARRAY[1]", new ArrayType(BIGINT), "ARRAY[BIGINT '1']")
                .addRoundTrip("ARRAY(VARCHAR)", "ARRAY['string']", new ArrayType(VARCHAR), "ARRAY[VARCHAR 'string']")
                .addRoundTrip("ARRAY(ROW(x INT, y VARCHAR))",
                        "ARRAY[ROW(1, 'string')]",
                        new ArrayType(RowType.from(ImmutableList.of(new Field(Optional.of("x"), BIGINT), new Field(Optional.of("y"), VARCHAR)))),
                        "ARRAY[CAST(ROW(1, 'string') AS ROW(x BIGINT, y VARCHAR))]")
                .addRoundTrip("ARRAY(BOOLEAN)", "ARRAY[]", new ArrayType(BOOLEAN), "CAST(ARRAY[] AS ARRAY<BOOLEAN>)")
                .execute(getQueryRunner(), trinoCreateAsSelect("test.array"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test.array"));

        SqlDataTypeTest.create()
                .addRoundTrip("ARRAY<BOOLEAN>", "[true]", new ArrayType(BOOLEAN), "ARRAY[true]")
                .addRoundTrip("ARRAY<INT64>", "[1]", new ArrayType(BIGINT), "ARRAY[BIGINT '1']")
                .addRoundTrip("ARRAY<STRING>", "['string']", new ArrayType(VARCHAR), "ARRAY[VARCHAR 'string']")
                .addRoundTrip("ARRAY<STRUCT<x INT64, y STRING>>",
                        "[(1, 'string')]",
                        new ArrayType(RowType.from(ImmutableList.of(new Field(Optional.of("x"), BIGINT), new Field(Optional.of("y"), VARCHAR)))),
                        "ARRAY[CAST(ROW(1, 'string') AS ROW(x BIGINT, y VARCHAR))]")
                .addRoundTrip("ARRAY<BOOLEAN>", "[]", new ArrayType(BOOLEAN), "CAST(ARRAY[] AS ARRAY<BOOLEAN>)")
                .addRoundTrip("ARRAY<BOOLEAN>", "NULL", new ArrayType(BOOLEAN), "CAST(ARRAY[] AS ARRAY<BOOLEAN>)")
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.array"))
                .execute(getQueryRunner(), bigqueryViewCreateAndInsert("test.array"));
    }

    @Test
    public void testUnsupportedNullArray()
    {
        // BigQuery translates a NULL ARRAY into an empty ARRAY in the query result
        // This test ensures that the connector disallows writing a NULL ARRAY
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test.test_null_array", "(col ARRAY(INT))")) {
            assertQueryFails("INSERT INTO " + table.getName() + " VALUES (NULL)", "NULL value not allowed for NOT NULL column: col");
        }
    }

    @Test
    public void testStruct()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("ROW(x INT)",
                        "ROW(1)",
                        RowType.from(ImmutableList.of(new Field(Optional.of("x"), BIGINT))),
                        "CAST(ROW(1) AS ROW(x BIGINT))")
                .addRoundTrip("ROW(x INT, y VARCHAR)",
                        "(1, 'string')",
                        RowType.from(ImmutableList.of(new Field(Optional.of("x"), BIGINT), new Field(Optional.of("y"), VARCHAR))),
                        "CAST(ROW(1, 'string') AS ROW(x BIGINT, y VARCHAR))")
                .addRoundTrip("ROW(x ROW(y VARCHAR))",
                        "ROW(ROW('nested'))",
                        RowType.from(ImmutableList.of(new Field(Optional.of("x"), RowType.from(ImmutableList.of(new Field(Optional.of("y"), VARCHAR)))))),
                        "CAST(ROW(ROW('nested')) AS ROW(X ROW(Y VARCHAR)))")
                .addRoundTrip("ROW(x INT)",
                        "NULL",
                        RowType.from(ImmutableList.of(new Field(Optional.of("x"), BIGINT))),
                        "CAST(NULL AS ROW(x BIGINT))")
                .execute(getQueryRunner(), trinoCreateAsSelect("test.row"))
                .execute(getQueryRunner(), trinoCreateAndInsert("test.row"));

        SqlDataTypeTest.create()
                .addRoundTrip("STRUCT<x INT64>",
                        "STRUCT(1)",
                        RowType.from(ImmutableList.of(new Field(Optional.of("x"), BIGINT))),
                        "CAST(ROW(1) AS ROW(x BIGINT))")
                .addRoundTrip("STRUCT<x INT64, y STRING>",
                        "(1, 'string')",
                        RowType.from(ImmutableList.of(new Field(Optional.of("x"), BIGINT), new Field(Optional.of("y"), VARCHAR))),
                        "CAST(ROW(1, 'string') AS ROW(x BIGINT, y VARCHAR))")
                .addRoundTrip("STRUCT<x STRUCT<y STRING>>",
                        "STRUCT(STRUCT('nested' AS y) AS x)",
                        RowType.from(ImmutableList.of(new Field(Optional.of("x"), RowType.from(ImmutableList.of(new Field(Optional.of("y"), VARCHAR)))))),
                        "CAST(ROW(ROW('nested')) AS ROW(X ROW(Y VARCHAR)))")
                .addRoundTrip("STRUCT<x INT64>",
                        "NULL",
                        RowType.from(ImmutableList.of(new Field(Optional.of("x"), BIGINT))),
                        "CAST(NULL AS ROW(x BIGINT))")
                .execute(getQueryRunner(), bigqueryCreateAndInsert("test.struct"))
                .execute(getQueryRunner(), bigqueryViewCreateAndInsert("test.struct"));
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

    private DataSetup bigqueryCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(getBigQuerySqlExecutor(), tableNamePrefix);
    }

    private DataSetup bigqueryCreateAndTrinoInsert(String tableNamePrefix)
    {
        return new CreateAndTrinoInsertDataSetup(getBigQuerySqlExecutor(), new TrinoSqlExecutor(getQueryRunner()), tableNamePrefix);
    }

    private DataSetup bigqueryViewCreateAndInsert(String tableNamePrefix)
    {
        return new BigQueryViewCreateAndInsertDataSetup(getBigQuerySqlExecutor(), tableNamePrefix);
    }

    private SqlExecutor getBigQuerySqlExecutor()
    {
        return sql -> bigQuerySqlExecutor.execute(sql);
    }
}
