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
package io.trino.plugin.ydb;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.JdbcSqlExecutor;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

final class TestYdbTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingYdbServer ydbServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ydbServer = closeAfterClass(new TestingYdbServer());
        return YdbQueryRunner.builder(ydbServer).build();
    }

    @Test
    void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Bool", "true", BOOLEAN)
                .addRoundTrip("Bool", "false", BOOLEAN)
                .addRoundTrip("Bool", "NULL", BOOLEAN, "CAST(NULL AS boolean)")
                .execute(getQueryRunner(), ydbCreateAndInsert("test_boolean"));
    }

    @Test
    void testSignedIntegers()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Int8", "CAST(-128 AS Int8)", TINYINT, "TINYINT '-128'")
                .addRoundTrip("Int8", "CAST(127 AS Int8)", TINYINT, "TINYINT '127'")
                .addRoundTrip("Int16", "CAST(-32768 AS Int16)", SMALLINT, "SMALLINT '-32768'")
                .addRoundTrip("Int16", "CAST(32767 AS Int16)", SMALLINT, "SMALLINT '32767'")
                .addRoundTrip("Int32", "-2147483648", INTEGER, "-2147483648")
                .addRoundTrip("Int32", "2147483647", INTEGER, "2147483647")
                .addRoundTrip("Int64", "-9223372036854775808l", BIGINT, "-9223372036854775808")
                .addRoundTrip("Int64", "9223372036854775807l", BIGINT, "9223372036854775807")
                .addRoundTrip("Int64", "NULL", BIGINT, "CAST(NULL AS bigint)")
                .execute(getQueryRunner(), ydbCreateAndInsert("test_signed_integers"));
    }

    @Test
    void testUnsignedIntegers()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Uint8", "0ut", SMALLINT, "SMALLINT '0'")
                .addRoundTrip("Uint8", "255ut", SMALLINT, "SMALLINT '255'")
                .addRoundTrip("Uint16", "65535us", INTEGER, "65535")
                .addRoundTrip("Uint32", "4294967295u", BIGINT, "BIGINT '4294967295'")
                .addRoundTrip("Uint64", "0ul", createDecimalType(20, 0), "CAST(0 AS decimal(20, 0))")
                .addRoundTrip("Uint64", "18446744073709551615ul", createDecimalType(20, 0), "CAST('18446744073709551615' AS decimal(20, 0))")
                .addRoundTrip("Uint64", "NULL", createDecimalType(20, 0), "CAST(NULL AS decimal(20, 0))")
                .execute(getQueryRunner(), ydbCreateAndInsert("test_unsigned_integers"));
    }

    @Test
    void testFloatingPoint()
    {
        SqlDataTypeTest.create()
                // YDB does not allow Float or Double primary key columns
                .addRoundTrip("Int64", "1", BIGINT, "BIGINT '1'")
                .addRoundTrip("Float", "Float('1.5')", REAL, "REAL '1.5'")
                .addRoundTrip("Float", "Float('nan')", REAL, "CAST(nan() AS real)")
                .addRoundTrip("Float", "Float('inf')", REAL, "CAST(infinity() AS real)")
                .addRoundTrip("Float", "Float('-inf')", REAL, "CAST(-infinity() AS real)")
                .addRoundTrip("Float", "NULL", REAL, "CAST(NULL AS real)")
                .addRoundTrip("Double", "Double('1.0E100')", DOUBLE, "1.0E100")
                .addRoundTrip("Double", "Double('nan')", DOUBLE, "nan()")
                .addRoundTrip("Double", "Double('inf')", DOUBLE, "infinity()")
                .addRoundTrip("Double", "Double('-inf')", DOUBLE, "-infinity()")
                .addRoundTrip("Double", "NULL", DOUBLE, "CAST(NULL AS double)")
                .execute(getQueryRunner(), ydbCreateAndInsert("test_floating_point"));
    }

    @Test
    void testDecimal()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Decimal(22, 9)", "Decimal('1.25', 22, 9)", createDecimalType(22, 9), "CAST('1.25' AS decimal(22, 9))")
                .addRoundTrip("Decimal(22, 9)", "Decimal('-3.5', 22, 9)", createDecimalType(22, 9), "CAST('-3.5' AS decimal(22, 9))")
                .addRoundTrip("Decimal(22, 9)", "Decimal('9999999999999.999999999', 22, 9)", createDecimalType(22, 9), "CAST('9999999999999.999999999' AS decimal(22, 9))")
                .addRoundTrip("Decimal(22, 9)", "NULL", createDecimalType(22, 9), "CAST(NULL AS decimal(22, 9))")
                .addRoundTrip("Decimal(10, 2)", "Decimal('12345678.90', 10, 2)", createDecimalType(10, 2), "CAST('12345678.90' AS decimal(10, 2))")
                .addRoundTrip("Decimal(35, 10)", "Decimal('-1234567890123456789012345.0123456789', 35, 10)", createDecimalType(35, 10), "CAST('-1234567890123456789012345.0123456789' AS decimal(35, 10))")
                .execute(getQueryRunner(), ydbCreateAndInsert("test_decimal"));
    }

    @Test
    void testDate()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Date", "Date('1970-01-01')", DATE, "DATE '1970-01-01'")
                .addRoundTrip("Date", "Date('2105-12-31')", DATE, "DATE '2105-12-31'")
                .addRoundTrip("Date", "NULL", DATE, "CAST(NULL AS date)")
                .addRoundTrip("Date32", "Date32('1969-12-31')", DATE, "DATE '1969-12-31'")
                .addRoundTrip("Date32", "Date32('1582-10-04')", DATE, "DATE '1582-10-04'")
                .addRoundTrip("Date32", "Date32('2200-01-01')", DATE, "DATE '2200-01-01'")
                .addRoundTrip("Date32", "NULL", DATE, "CAST(NULL AS date)")
                .execute(getQueryRunner(), ydbCreateAndInsert("test_date"));
    }

    @Test
    void testVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("Text", "'text'u", VARCHAR, "CAST('text' AS varchar)")
                .addRoundTrip("Text", "''u", VARCHAR, "CAST('' AS varchar)")
                .addRoundTrip("Text", "'攻殻機動隊'u", VARCHAR, "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("Text", "'😂'u", VARCHAR, "CAST('😂' AS varchar)")
                .addRoundTrip("Text", "NULL", VARCHAR, "CAST(NULL AS varchar)")
                .execute(getQueryRunner(), ydbCreateAndInsert("test_varchar"));
    }

    private DataSetup ydbCreateAndInsert(String tableNamePrefix)
    {
        return new YdbCreateAndInsertDataSetup(new JdbcSqlExecutor(ydbServer.getJdbcUrl()), tableNamePrefix);
    }
}
