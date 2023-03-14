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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.ArrayType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.StandardErrorCode.TOO_MANY_ARGUMENTS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.SqlDecimal.decimal;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestMathFunctions
{
    private static final double[] DOUBLE_VALUES = {123, -123, 123.45, -123.45, 0};
    private static final int[] intLefts = {9, 10, 11, -9, -10, -11, 0};
    private static final int[] intRights = {3, -3};
    private static final double[] doubleLefts = {9, 10, 11, -9, -10, -11, 9.1, 10.1, 11.1, -9.1, -10.1, -11.1};
    private static final double[] doubleRights = {3, -3, 3.1, -3.1};
    private static final double GREATEST_DOUBLE_LESS_THAN_HALF = 0x1.fffffffffffffp-2;

    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testAbs()
    {
        assertThat(assertions.function("abs", "TINYINT '123'"))
                .isEqualTo((byte) 123);

        assertThat(assertions.function("abs", "TINYINT '-123'"))
                .isEqualTo((byte) 123);

        assertThat(assertions.function("abs", "CAST(NULL AS TINYINT)"))
                .isNull(TINYINT);

        assertThat(assertions.function("abs", "SMALLINT '123'"))
                .isEqualTo((short) 123);

        assertThat(assertions.function("abs", "SMALLINT '-123'"))
                .isEqualTo((short) 123);

        assertThat(assertions.function("abs", "CAST(NULL AS SMALLINT)"))
                .isNull(SMALLINT);

        assertThat(assertions.function("abs", "123"))
                .isEqualTo(123);

        assertThat(assertions.function("abs", "-123"))
                .isEqualTo(123);

        assertThat(assertions.function("abs", "CAST(NULL AS INTEGER)"))
                .isNull(INTEGER);

        assertThat(assertions.function("abs", "BIGINT '123'"))
                .isEqualTo(123L);

        assertThat(assertions.function("abs", "BIGINT '-123'"))
                .isEqualTo(123L);

        assertThat(assertions.function("abs", "12300000000"))
                .isEqualTo(12300000000L);

        assertThat(assertions.function("abs", "-12300000000"))
                .isEqualTo(12300000000L);

        assertThat(assertions.function("abs", "CAST(NULL AS BIGINT)"))
                .isNull(BIGINT);

        assertThat(assertions.function("abs", "123.0E0"))
                .isEqualTo(123.0);

        assertThat(assertions.function("abs", "-123.0E0"))
                .isEqualTo(123.0);

        assertThat(assertions.function("abs", "123.45E0"))
                .isEqualTo(123.45);

        assertThat(assertions.function("abs", "-123.45E0"))
                .isEqualTo(123.45);

        assertThat(assertions.function("abs", "CAST(NULL AS DOUBLE)"))
                .isNull(DOUBLE);

        assertThat(assertions.function("abs", "REAL '-754.1985'"))
                .isEqualTo(754.1985f);

        assertTrinoExceptionThrownBy(() -> assertions.function("abs", "TINYINT '%s'".formatted(Byte.MIN_VALUE)).evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("abs", "SMALLINT '%s'".formatted(Short.MIN_VALUE)).evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("abs", "INTEGER '%s'".formatted(Integer.MIN_VALUE)).evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("abs", "-9223372036854775807 - if(rand() < 10, 1, 1)").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertThat(assertions.function("abs", "DECIMAL '123.45'"))
                .isEqualTo(decimal("12345", createDecimalType(5, 2)));

        assertThat(assertions.function("abs", "DECIMAL '-123.45'"))
                .isEqualTo(decimal("12345", createDecimalType(5, 2)));

        assertThat(assertions.function("abs", "DECIMAL '1234567890123456.78'"))
                .isEqualTo(decimal("123456789012345678", createDecimalType(18, 2)));

        assertThat(assertions.function("abs", "DECIMAL '-1234567890123456.78'"))
                .isEqualTo(decimal("123456789012345678", createDecimalType(18, 2)));

        assertThat(assertions.function("abs", "DECIMAL '12345678901234560.78'"))
                .isEqualTo(decimal("1234567890123456078", createDecimalType(19, 2)));

        assertThat(assertions.function("abs", "DECIMAL '-12345678901234560.78'"))
                .isEqualTo(decimal("1234567890123456078", createDecimalType(19, 2)));

        assertThat(assertions.function("abs", "CAST(NULL AS DECIMAL(1,0))"))
                .isNull(createDecimalType(1, 0));
    }

    @Test
    public void testAcos()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("acos", Double.toString(doubleValue)))
                    .isEqualTo(Math.acos(doubleValue));

            assertThat(assertions.function("acos", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.acos((float) doubleValue));
        }

        assertThat(assertions.function("acos", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testAsin()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("asin", Double.toString(doubleValue)))
                    .isEqualTo(Math.asin(doubleValue));

            assertThat(assertions.function("asin", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.asin((float) doubleValue));
        }

        assertThat(assertions.function("asin", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testAtan()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("atan", Double.toString(doubleValue)))
                    .isEqualTo(Math.atan(doubleValue));

            assertThat(assertions.function("atan", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.atan((float) doubleValue));
        }

        assertThat(assertions.function("atan", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testAtan2()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("atan2", Double.toString(doubleValue), Double.toString(doubleValue)))
                    .isEqualTo(Math.atan2(doubleValue, doubleValue));

            assertThat(assertions.function("atan2", "REAL '%s'".formatted((float) doubleValue), "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.atan2((float) doubleValue, (float) doubleValue));
        }

        assertThat(assertions.function("atan2", "NULL", "NULL"))
                .isNull(DOUBLE);

        assertThat(assertions.function("atan2", "1.0E0", "NULL"))
                .isNull(DOUBLE);

        assertThat(assertions.function("atan2", "NULL", "1.0E0"))
                .isNull(DOUBLE);
    }

    @Test
    public void testCbrt()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("cbrt", Double.toString(doubleValue)))
                    .isEqualTo(Math.cbrt(doubleValue));

            assertThat(assertions.function("cbrt", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.cbrt((float) doubleValue));
        }

        assertThat(assertions.function("cbrt", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testCeil()
    {
        assertThat(assertions.function("ceil", "TINYINT '123'"))
                .isEqualTo((byte) 123);

        assertThat(assertions.function("ceil", "TINYINT '-123'"))
                .isEqualTo((byte) -123);

        assertThat(assertions.function("ceil", "CAST(NULL AS TINYINT)"))
                .isNull(TINYINT);

        assertThat(assertions.function("ceil", "SMALLINT '123'"))
                .isEqualTo((short) 123);

        assertThat(assertions.function("ceil", "SMALLINT '-123'"))
                .isEqualTo((short) -123);

        assertThat(assertions.function("ceil", "CAST(NULL AS SMALLINT)"))
                .isNull(SMALLINT);

        assertThat(assertions.function("ceil", "123"))
                .isEqualTo(123);

        assertThat(assertions.function("ceil", "-123"))
                .isEqualTo(-123);

        assertThat(assertions.function("ceil", "CAST(NULL AS INTEGER)"))
                .isNull(INTEGER);

        assertThat(assertions.function("ceil", "BIGINT '123'"))
                .isEqualTo(123L);

        assertThat(assertions.function("ceil", "BIGINT '-123'"))
                .isEqualTo(-123L);

        assertThat(assertions.function("ceil", "12300000000"))
                .isEqualTo(12300000000L);

        assertThat(assertions.function("ceil", "-12300000000"))
                .isEqualTo(-12300000000L);

        assertThat(assertions.function("ceil", "CAST(NULL as BIGINT)"))
                .isNull(BIGINT);

        assertThat(assertions.function("ceil", "123.0E0"))
                .isEqualTo(123.0);

        assertThat(assertions.function("ceil", "-123.0E0"))
                .isEqualTo(-123.0);

        assertThat(assertions.function("ceil", "123.45E0"))
                .isEqualTo(124.0);

        assertThat(assertions.function("ceil", "-123.45E0"))
                .isEqualTo(-123.0);

        assertThat(assertions.function("ceil", "CAST(NULL as DOUBLE)"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ceil", "REAL '123.0'"))
                .isEqualTo(123.0f);

        assertThat(assertions.function("ceil", "REAL '-123.0'"))
                .isEqualTo(-123.0f);

        assertThat(assertions.function("ceil", "REAL '123.45'"))
                .isEqualTo(124.0f);

        assertThat(assertions.function("ceil", "REAL '-123.45'"))
                .isEqualTo(-123.0f);

        assertThat(assertions.function("ceiling", "12300000000"))
                .isEqualTo(12300000000L);

        assertThat(assertions.function("ceiling", "-12300000000"))
                .isEqualTo(-12300000000L);

        assertThat(assertions.function("ceiling", "CAST(NULL AS BIGINT)"))
                .isNull(BIGINT);

        assertThat(assertions.function("ceiling", "123.0E0"))
                .isEqualTo(123.0);

        assertThat(assertions.function("ceiling", "-123.0E0"))
                .isEqualTo(-123.0);

        assertThat(assertions.function("ceiling", "123.45E0"))
                .isEqualTo(124.0);

        assertThat(assertions.function("ceiling", "-123.45E0"))
                .isEqualTo(-123.0);

        assertThat(assertions.function("ceiling", "REAL '123.0'"))
                .isEqualTo(123.0f);

        assertThat(assertions.function("ceiling", "REAL '-123.0'"))
                .isEqualTo(-123.0f);

        assertThat(assertions.function("ceiling", "REAL '123.45'"))
                .isEqualTo(124.0f);

        assertThat(assertions.function("ceiling", "REAL '-123.45'"))
                .isEqualTo(-123.0f);

        // short DECIMAL -> short DECIMAL
        assertThat(assertions.function("ceiling", "DECIMAL '0'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '0.00' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("0", createDecimalType(2)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '0.00' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("0", createDecimalType(2)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '0.01' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("1", createDecimalType(2)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '-0.01' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("0", createDecimalType(2)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '0.49' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("1", createDecimalType(2)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '-0.49' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("0", createDecimalType(2)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '0.50' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("1", createDecimalType(2)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '-0.50' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("0", createDecimalType(2)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '0.99' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("1", createDecimalType(2)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '-0.99' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("0", createDecimalType(2)));

        assertThat(assertions.function("ceiling", "DECIMAL '123'"))
                .isEqualTo(decimal("123", createDecimalType(3)));

        assertThat(assertions.function("ceiling", "DECIMAL '-123'"))
                .isEqualTo(decimal("-123", createDecimalType(3)));

        assertThat(assertions.function("ceiling", "DECIMAL '123.00'"))
                .isEqualTo(decimal("123", createDecimalType(4)));

        assertThat(assertions.function("ceiling", "DECIMAL '-123.00'"))
                .isEqualTo(decimal("-123", createDecimalType(4)));

        assertThat(assertions.function("ceiling", "DECIMAL '123.01'"))
                .isEqualTo(decimal("124", createDecimalType(4)));

        assertThat(assertions.function("ceiling", "DECIMAL '-123.01'"))
                .isEqualTo(decimal("-123", createDecimalType(4)));

        assertThat(assertions.function("ceiling", "DECIMAL '123.45'"))
                .isEqualTo(decimal("124", createDecimalType(4)));

        assertThat(assertions.function("ceiling", "DECIMAL '-123.45'"))
                .isEqualTo(decimal("-123", createDecimalType(4)));

        assertThat(assertions.function("ceiling", "DECIMAL '123.49'"))
                .isEqualTo(decimal("124", createDecimalType(4)));

        assertThat(assertions.function("ceiling", "DECIMAL '-123.49'"))
                .isEqualTo(decimal("-123", createDecimalType(4)));

        assertThat(assertions.function("ceiling", "DECIMAL '123.50'"))
                .isEqualTo(decimal("124", createDecimalType(4)));

        assertThat(assertions.function("ceiling", "DECIMAL '-123.50'"))
                .isEqualTo(decimal("-123", createDecimalType(4)));

        assertThat(assertions.function("ceiling", "DECIMAL '123.99'"))
                .isEqualTo(decimal("124", createDecimalType(4)));

        assertThat(assertions.function("ceiling", "DECIMAL '-123.99'"))
                .isEqualTo(decimal("-123", createDecimalType(4)));

        assertThat(assertions.function("ceiling", "DECIMAL '999.9'"))
                .isEqualTo(decimal("1000", createDecimalType(4)));

        // long DECIMAL -> long DECIMAL
        assertThat(assertions.function("ceiling", "CAST(DECIMAL '0000000000000000000' AS DECIMAL(19,0))"))
                .isEqualTo(decimal("0", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '000000000000000000.00' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("0", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '000000000000000000.01' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("1", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '-000000000000000000.01' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("0", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '000000000000000000.49' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("1", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '-000000000000000000.49' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("0", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '000000000000000000.50' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("1", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '-000000000000000000.50' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("0", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '000000000000000000.99' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("1", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "CAST(DECIMAL '-000000000000000000.99' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("0", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "DECIMAL '123456789012345678'"))
                .isEqualTo(decimal("123456789012345678", createDecimalType(18)));

        assertThat(assertions.function("ceiling", "DECIMAL '-123456789012345678'"))
                .isEqualTo(decimal("-123456789012345678", createDecimalType(18)));

        assertThat(assertions.function("ceiling", "DECIMAL '123456789012345678.00'"))
                .isEqualTo(decimal("123456789012345678", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "DECIMAL '-123456789012345678.00'"))
                .isEqualTo(decimal("-123456789012345678", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "DECIMAL '123456789012345678.01'"))
                .isEqualTo(decimal("123456789012345679", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "DECIMAL '-123456789012345678.01'"))
                .isEqualTo(decimal("-123456789012345678", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "DECIMAL '123456789012345678.99'"))
                .isEqualTo(decimal("123456789012345679", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "DECIMAL '-123456789012345678.99'"))
                .isEqualTo(decimal("-123456789012345678", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "DECIMAL '123456789012345678.49'"))
                .isEqualTo(decimal("123456789012345679", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "DECIMAL '-123456789012345678.49'"))
                .isEqualTo(decimal("-123456789012345678", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "DECIMAL '123456789012345678.50'"))
                .isEqualTo(decimal("123456789012345679", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "DECIMAL '-123456789012345678.50'"))
                .isEqualTo(decimal("-123456789012345678", createDecimalType(19)));

        assertThat(assertions.function("ceiling", "DECIMAL '999999999999999999.9'"))
                .isEqualTo(decimal("1000000000000000000", createDecimalType(19)));

        // long DECIMAL -> short DECIMAL
        assertThat(assertions.function("ceiling", "DECIMAL '1234567890123456.78'"))
                .isEqualTo(decimal("1234567890123457", createDecimalType(17)));

        assertThat(assertions.function("ceiling", "DECIMAL '-1234567890123456.78'"))
                .isEqualTo(decimal("-1234567890123456", createDecimalType(17)));

        assertThat(assertions.function("ceiling", "CAST(NULL AS DOUBLE)"))
                .isNull(DOUBLE);

        assertThat(assertions.function("ceiling", "CAST(NULL AS REAL)"))
                .isNull(REAL);

        assertThat(assertions.function("ceiling", "CAST(NULL AS DECIMAL(1,0))"))
                .isNull(createDecimalType(1));

        assertThat(assertions.function("ceiling", "CAST(NULL AS DECIMAL(25,5))"))
                .isNull(createDecimalType(21));
    }

    @Test
    public void testTruncate()
    {
        // DOUBLE
        assertThat(assertions.function("truncate", "17.18E0"))
                .isEqualTo(17.0);

        assertThat(assertions.function("truncate", "-17.18E0"))
                .isEqualTo(-17.0);

        assertThat(assertions.function("truncate", "17.88E0"))
                .isEqualTo(17.0);

        assertThat(assertions.function("truncate", "-17.88E0"))
                .isEqualTo(-17.0);

        assertThat(assertions.function("truncate", "REAL '17.18'"))
                .isEqualTo(17.0f);

        assertThat(assertions.function("truncate", "REAL '-17.18'"))
                .isEqualTo(-17.0f);

        assertThat(assertions.function("truncate", "REAL '17.88'"))
                .isEqualTo(17.0f);

        assertThat(assertions.function("truncate", "REAL '-17.88'"))
                .isEqualTo(-17.0f);

        assertThat(assertions.function("truncate", "DOUBLE '%s'".formatted(Double.MAX_VALUE)))
                .isEqualTo(Double.MAX_VALUE);

        assertThat(assertions.function("truncate", "DOUBLE '%s'".formatted(-Double.MAX_VALUE)))
                .isEqualTo(-Double.MAX_VALUE);

        // TRUNCATE short DECIMAL -> short DECIMAL
        assertThat(assertions.function("truncate", "DECIMAL '1234'"))
                .isEqualTo(decimal("1234", createDecimalType(4)));

        assertThat(assertions.function("truncate", "DECIMAL '-1234'"))
                .isEqualTo(decimal("-1234", createDecimalType(4)));

        assertThat(assertions.function("truncate", "DECIMAL '1234.56'"))
                .isEqualTo(decimal("1234", createDecimalType(4)));

        assertThat(assertions.function("truncate", "DECIMAL '-1234.56'"))
                .isEqualTo(decimal("-1234", createDecimalType(4)));

        assertThat(assertions.function("truncate", "DECIMAL '123456789123456.999'"))
                .isEqualTo(decimal("123456789123456", createDecimalType(15)));

        assertThat(assertions.function("truncate", "DECIMAL '-123456789123456.999'"))
                .isEqualTo(decimal("-123456789123456", createDecimalType(15)));

        // TRUNCATE long DECIMAL -> short DECIMAL
        assertThat(assertions.function("truncate", "DECIMAL '1.99999999999999999999999999'"))
                .isEqualTo(decimal("1", createDecimalType(1)));

        assertThat(assertions.function("truncate", "DECIMAL '-1.99999999999999999999999999'"))
                .isEqualTo(decimal("-1", createDecimalType(1)));

        // TRUNCATE long DECIMAL -> long DECIMAL
        assertThat(assertions.function("truncate", "DECIMAL '1234567890123456789012'"))
                .isEqualTo(decimal("1234567890123456789012", createDecimalType(22)));

        assertThat(assertions.function("truncate", "DECIMAL '-1234567890123456789012'"))
                .isEqualTo(decimal("-1234567890123456789012", createDecimalType(22)));

        assertThat(assertions.function("truncate", "DECIMAL '1234567890123456789012.999'"))
                .isEqualTo(decimal("1234567890123456789012", createDecimalType(22)));

        assertThat(assertions.function("truncate", "DECIMAL '-1234567890123456789012.999'"))
                .isEqualTo(decimal("-1234567890123456789012", createDecimalType(22)));

        // TRUNCATE_N short DECIMAL -> short DECIMAL
        assertThat(assertions.function("truncate", "DECIMAL '1234'", "1"))
                .isEqualTo(decimal("1234", createDecimalType(4)));

        assertThat(assertions.function("truncate", "DECIMAL '1234'", "-1"))
                .isEqualTo(decimal("1230", createDecimalType(4)));

        assertThat(assertions.function("truncate", "DECIMAL '1234.56'", "1"))
                .isEqualTo(decimal("1234.50", createDecimalType(6, 2)));

        assertThat(assertions.function("truncate", "DECIMAL '1234.56'", "-1"))
                .isEqualTo(decimal("1230.00", createDecimalType(6, 2)));

        assertThat(assertions.function("truncate", "DECIMAL '-1234.56'", "1"))
                .isEqualTo(decimal("-1234.50", createDecimalType(6, 2)));

        assertThat(assertions.function("truncate", "DECIMAL '1239.99'", "1"))
                .isEqualTo(decimal("1239.90", createDecimalType(6, 2)));

        assertThat(assertions.function("truncate", "DECIMAL '-1239.99'", "1"))
                .isEqualTo(decimal("-1239.90", createDecimalType(6, 2)));

        assertThat(assertions.function("truncate", "DECIMAL '1239.999'", "2"))
                .isEqualTo(decimal("1239.990", createDecimalType(7, 3)));

        assertThat(assertions.function("truncate", "DECIMAL '1239.999'", "-2"))
                .isEqualTo(decimal("1200.000", createDecimalType(7, 3)));

        assertThat(assertions.function("truncate", "DECIMAL '123456789123456.999'", "2"))
                .isEqualTo(decimal("123456789123456.990", createDecimalType(18, 3)));

        assertThat(assertions.function("truncate", "DECIMAL '123456789123456.999'", "-2"))
                .isEqualTo(decimal("123456789123400.000", createDecimalType(18, 3)));

        assertThat(assertions.function("truncate", "DECIMAL '1234'", "-4"))
                .isEqualTo(decimal("0000", createDecimalType(4)));

        assertThat(assertions.function("truncate", "DECIMAL '1234.56'", "-4"))
                .isEqualTo(decimal("0000.00", createDecimalType(6, 2)));

        assertThat(assertions.function("truncate", "DECIMAL '-1234.56'", "-4"))
                .isEqualTo(decimal("0000.00", createDecimalType(6, 2)));

        assertThat(assertions.function("truncate", "DECIMAL '1234.56'", "3"))
                .isEqualTo(decimal("1234.56", createDecimalType(6, 2)));

        assertThat(assertions.function("truncate", "DECIMAL '-1234.56'", "3"))
                .isEqualTo(decimal("-1234.56", createDecimalType(6, 2)));

        // TRUNCATE_N long DECIMAL -> long DECIMAL
        assertThat(assertions.function("truncate", "DECIMAL '1234567890123456789012'", "1"))
                .isEqualTo(decimal("1234567890123456789012", createDecimalType(22)));

        assertThat(assertions.function("truncate", "DECIMAL '1234567890123456789012'", "-1"))
                .isEqualTo(decimal("1234567890123456789010", createDecimalType(22)));

        assertThat(assertions.function("truncate", "DECIMAL '1234567890123456789012.23'", "1"))
                .isEqualTo(decimal("1234567890123456789012.20", createDecimalType(24, 2)));

        assertThat(assertions.function("truncate", "DECIMAL '1234567890123456789012.23'", "-1"))
                .isEqualTo(decimal("1234567890123456789010.00", createDecimalType(24, 2)));

        assertThat(assertions.function("truncate", "DECIMAL '123456789012345678999.99'", "-1"))
                .isEqualTo(decimal("123456789012345678990.00", createDecimalType(23, 2)));

        assertThat(assertions.function("truncate", "DECIMAL '-123456789012345678999.99'", "-1"))
                .isEqualTo(decimal("-123456789012345678990.00", createDecimalType(23, 2)));

        assertThat(assertions.function("truncate", "DECIMAL '123456789012345678999.999'", "2"))
                .isEqualTo(decimal("123456789012345678999.990", createDecimalType(24, 3)));

        assertThat(assertions.function("truncate", "DECIMAL '123456789012345678999.999'", "-2"))
                .isEqualTo(decimal("123456789012345678900.000", createDecimalType(24, 3)));

        assertThat(assertions.function("truncate", "DECIMAL '123456789012345678901'", "-21"))
                .isEqualTo(decimal("000000000000000000000", createDecimalType(21)));

        assertThat(assertions.function("truncate", "DECIMAL '123456789012345678901.23'", "-21"))
                .isEqualTo(decimal("000000000000000000000.00", createDecimalType(23, 2)));

        assertThat(assertions.function("truncate", "DECIMAL '123456789012345678901.23'", "3"))
                .isEqualTo(decimal("123456789012345678901.23", createDecimalType(23, 2)));

        assertThat(assertions.function("truncate", "DECIMAL '-123456789012345678901.23'", "3"))
                .isEqualTo(decimal("-123456789012345678901.23", createDecimalType(23, 2)));

        // NULL
        assertThat(assertions.function("truncate", "CAST(NULL AS DOUBLE)"))
                .isNull(DOUBLE);

        assertThat(assertions.function("truncate", "CAST(NULL AS DECIMAL(1,0))", "-1"))
                .isNull(createDecimalType(1, 0));

        assertThat(assertions.function("truncate", "CAST(NULL AS DECIMAL(1,0))"))
                .isNull(createDecimalType(1, 0));

        assertThat(assertions.function("truncate", "CAST(NULL AS DECIMAL(18,5))"))
                .isNull(createDecimalType(13, 0));

        assertThat(assertions.function("truncate", "CAST(NULL AS DECIMAL(25,2))"))
                .isNull(createDecimalType(23, 0));

        assertThat(assertions.function("truncate", "NULL", "NULL"))
                .isNull(createDecimalType(1, 0));
    }

    @Test
    public void testCos()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("cos", Double.toString(doubleValue)))
                    .isEqualTo(Math.cos(doubleValue));

            assertThat(assertions.function("cos", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.cos((float) doubleValue));
        }

        assertThat(assertions.function("cos", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testCosh()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("cosh", Double.toString(doubleValue)))
                    .isEqualTo(Math.cosh(doubleValue));

            assertThat(assertions.function("cosh", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.cosh((float) doubleValue));
        }

        assertThat(assertions.function("cosh", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testDegrees()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("degrees", Double.toString(doubleValue)))
                    .isEqualTo(Math.toDegrees(doubleValue));

            assertThat(assertions.function("degrees", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.toDegrees((float) doubleValue));
        }

        assertThat(assertions.function("degrees", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testE()
    {
        assertThat(assertions.function("e"))
                .isEqualTo(Math.E);
    }

    @Test
    public void testExp()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("exp", Double.toString(doubleValue)))
                    .isEqualTo(Math.exp(doubleValue));

            assertThat(assertions.function("exp", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.exp((float) doubleValue));
        }

        assertThat(assertions.function("exp", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testFloor()
    {
        assertThat(assertions.function("floor", "TINYINT '123'"))
                .isEqualTo((byte) 123);

        assertThat(assertions.function("floor", "TINYINT '-123'"))
                .isEqualTo((byte) -123);

        assertThat(assertions.function("floor", "CAST(NULL AS TINYINT)"))
                .isNull(TINYINT);

        assertThat(assertions.function("floor", "SMALLINT '123'"))
                .isEqualTo((short) 123);

        assertThat(assertions.function("floor", "SMALLINT '-123'"))
                .isEqualTo((short) -123);

        assertThat(assertions.function("floor", "CAST(NULL AS SMALLINT)"))
                .isNull(SMALLINT);

        assertThat(assertions.function("floor", "123"))
                .isEqualTo(123);

        assertThat(assertions.function("floor", "-123"))
                .isEqualTo(-123);

        assertThat(assertions.function("floor", "CAST(NULL AS INTEGER)"))
                .isNull(INTEGER);

        assertThat(assertions.function("floor", "BIGINT '123'"))
                .isEqualTo(123L);

        assertThat(assertions.function("floor", "BIGINT '-123'"))
                .isEqualTo(-123L);

        assertThat(assertions.function("floor", "12300000000"))
                .isEqualTo(12300000000L);

        assertThat(assertions.function("floor", "-12300000000"))
                .isEqualTo(-12300000000L);

        assertThat(assertions.function("floor", "CAST(NULL as BIGINT)"))
                .isNull(BIGINT);

        assertThat(assertions.function("floor", "123.0E0"))
                .isEqualTo(123.0);

        assertThat(assertions.function("floor", "-123.0E0"))
                .isEqualTo(-123.0);

        assertThat(assertions.function("floor", "123.45E0"))
                .isEqualTo(123.0);

        assertThat(assertions.function("floor", "-123.45E0"))
                .isEqualTo(-124.0);

        assertThat(assertions.function("floor", "REAL '123.0'"))
                .isEqualTo(123.0f);

        assertThat(assertions.function("floor", "REAL '-123.0'"))
                .isEqualTo(-123.0f);

        assertThat(assertions.function("floor", "REAL '123.45'"))
                .isEqualTo(123.0f);

        assertThat(assertions.function("floor", "REAL '-123.45'"))
                .isEqualTo(-124.0f);

        // short DECIMAL -> short DECIMAL
        assertThat(assertions.function("floor", "DECIMAL '0'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '0.00' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("0", createDecimalType(2)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '0.00' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("0", createDecimalType(2)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '0.01' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("0", createDecimalType(2)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '-0.01' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("-1", createDecimalType(2)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '0.49' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("0", createDecimalType(2)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '-0.49' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("-1", createDecimalType(2)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '0.50' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("0", createDecimalType(2)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '-0.50' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("-1", createDecimalType(2)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '0.99' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("0", createDecimalType(2)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '-0.99' AS DECIMAL(3,2))"))
                .isEqualTo(decimal("-1", createDecimalType(2)));

        assertThat(assertions.function("floor", "DECIMAL '123'"))
                .isEqualTo(decimal("123", createDecimalType(3)));

        assertThat(assertions.function("floor", "DECIMAL '-123'"))
                .isEqualTo(decimal("-123", createDecimalType(3)));

        assertThat(assertions.function("floor", "DECIMAL '123.00'"))
                .isEqualTo(decimal("123", createDecimalType(4)));

        assertThat(assertions.function("floor", "DECIMAL '-123.00'"))
                .isEqualTo(decimal("-123", createDecimalType(4)));

        assertThat(assertions.function("floor", "DECIMAL '123.01'"))
                .isEqualTo(decimal("123", createDecimalType(4)));

        assertThat(assertions.function("floor", "DECIMAL '-123.01'"))
                .isEqualTo(decimal("-124", createDecimalType(4)));

        assertThat(assertions.function("floor", "DECIMAL '123.45'"))
                .isEqualTo(decimal("123", createDecimalType(4)));

        assertThat(assertions.function("floor", "DECIMAL '-123.45'"))
                .isEqualTo(decimal("-124", createDecimalType(4)));

        assertThat(assertions.function("floor", "DECIMAL '123.49'"))
                .isEqualTo(decimal("123", createDecimalType(4)));

        assertThat(assertions.function("floor", "DECIMAL '-123.49'"))
                .isEqualTo(decimal("-124", createDecimalType(4)));

        assertThat(assertions.function("floor", "DECIMAL '123.50'"))
                .isEqualTo(decimal("123", createDecimalType(4)));

        assertThat(assertions.function("floor", "DECIMAL '-123.50'"))
                .isEqualTo(decimal("-124", createDecimalType(4)));

        assertThat(assertions.function("floor", "DECIMAL '123.99'"))
                .isEqualTo(decimal("123", createDecimalType(4)));

        assertThat(assertions.function("floor", "DECIMAL '-123.99'"))
                .isEqualTo(decimal("-124", createDecimalType(4)));

        assertThat(assertions.function("floor", "DECIMAL '-999.9'"))
                .isEqualTo(decimal("-1000", createDecimalType(4)));

        // long DECIMAL -> long DECIMAL
        assertThat(assertions.function("floor", "CAST(DECIMAL '0000000000000000000' AS DECIMAL(19,0))"))
                .isEqualTo(decimal("0", createDecimalType(19)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '000000000000000000.00' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("0", createDecimalType(19)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '000000000000000000.01' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("0", createDecimalType(19)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '-000000000000000000.01' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("-1", createDecimalType(19)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '000000000000000000.49' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("0", createDecimalType(19)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '-000000000000000000.49' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("-1", createDecimalType(19)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '000000000000000000.50' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("0", createDecimalType(19)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '-000000000000000000.50' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("-1", createDecimalType(19)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '000000000000000000.99' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("0", createDecimalType(19)));

        assertThat(assertions.function("floor", "CAST(DECIMAL '-000000000000000000.99' AS DECIMAL(20,2))"))
                .isEqualTo(decimal("-1", createDecimalType(19)));

        assertThat(assertions.function("floor", "DECIMAL '123456789012345678'"))
                .isEqualTo(decimal("123456789012345678", createDecimalType(18)));

        assertThat(assertions.function("floor", "DECIMAL '-123456789012345678'"))
                .isEqualTo(decimal("-123456789012345678", createDecimalType(18)));

        assertThat(assertions.function("floor", "DECIMAL '123456789012345678.00'"))
                .isEqualTo(decimal("123456789012345678", createDecimalType(19)));

        assertThat(assertions.function("floor", "DECIMAL '-123456789012345678.00'"))
                .isEqualTo(decimal("-123456789012345678", createDecimalType(19)));

        assertThat(assertions.function("floor", "DECIMAL '123456789012345678.01'"))
                .isEqualTo(decimal("123456789012345678", createDecimalType(19)));

        assertThat(assertions.function("floor", "DECIMAL '-123456789012345678.01'"))
                .isEqualTo(decimal("-123456789012345679", createDecimalType(19)));

        assertThat(assertions.function("floor", "DECIMAL '123456789012345678.99'"))
                .isEqualTo(decimal("123456789012345678", createDecimalType(19)));

        assertThat(assertions.function("floor", "DECIMAL '-123456789012345678.49'"))
                .isEqualTo(decimal("-123456789012345679", createDecimalType(19)));

        assertThat(assertions.function("floor", "DECIMAL '123456789012345678.49'"))
                .isEqualTo(decimal("123456789012345678", createDecimalType(19)));

        assertThat(assertions.function("floor", "DECIMAL '-123456789012345678.50'"))
                .isEqualTo(decimal("-123456789012345679", createDecimalType(19)));

        assertThat(assertions.function("floor", "DECIMAL '123456789012345678.50'"))
                .isEqualTo(decimal("123456789012345678", createDecimalType(19)));

        assertThat(assertions.function("floor", "DECIMAL '-123456789012345678.99'"))
                .isEqualTo(decimal("-123456789012345679", createDecimalType(19)));

        assertThat(assertions.function("floor", "DECIMAL '-999999999999999999.9'"))
                .isEqualTo(decimal("-1000000000000000000", createDecimalType(19)));

        // long DECIMAL -> short DECIMAL
        assertThat(assertions.function("floor", "DECIMAL '1234567890123456.78'"))
                .isEqualTo(decimal("1234567890123456", createDecimalType(17)));

        assertThat(assertions.function("floor", "DECIMAL '-1234567890123456.78'"))
                .isEqualTo(decimal("-1234567890123457", createDecimalType(17)));

        assertThat(assertions.function("floor", "CAST(NULL as REAL)"))
                .isNull(REAL);

        assertThat(assertions.function("floor", "CAST(NULL as DOUBLE)"))
                .isNull(DOUBLE);

        assertThat(assertions.function("floor", "CAST(NULL as DECIMAL(1,0))"))
                .isNull(createDecimalType(1));

        assertThat(assertions.function("floor", "CAST(NULL as DECIMAL(25,5))"))
                .isNull(createDecimalType(21));
    }

    @Test
    public void testLn()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("ln", Double.toString(doubleValue)))
                    .isEqualTo(Math.log(doubleValue));
        }

        assertThat(assertions.function("ln", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testLog2()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("log2", Double.toString(doubleValue)))
                    .isEqualTo(Math.log(doubleValue) / Math.log(2));
        }

        assertThat(assertions.function("log2", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testLog10()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("log10", Double.toString(doubleValue)))
                    .isEqualTo(Math.log10(doubleValue));
        }

        assertThat(assertions.function("log10", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testLog()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            for (double base : DOUBLE_VALUES) {
                assertThat(assertions.function("log", Double.toString(base), Double.toString(doubleValue)))
                        .isEqualTo(Math.log(doubleValue) / Math.log(base));

                assertThat(assertions.function("log", "REAL '%s'".formatted((float) base), "REAL '%s'".formatted((float) doubleValue)))
                        .isEqualTo(Math.log((float) doubleValue) / Math.log((float) base));
            }
        }

        assertThat(assertions.function("log", "NULL", "NULL"))
                .isNull(DOUBLE);

        assertThat(assertions.function("log", "5.0E0", "NULL"))
                .isNull(DOUBLE);

        assertThat(assertions.function("log", "NULL", "5.0E0"))
                .isNull(DOUBLE);
    }

    @Test
    public void testMod()
    {
        for (int left : intLefts) {
            for (int right : intRights) {
                assertThat(assertions.function("mod", Integer.toString(left), Integer.toString(right)))
                        .isEqualTo((left % right));
            }
        }

        for (int left : intLefts) {
            for (int right : intRights) {
                assertThat(assertions.function("mod", "BIGINT '%s'".formatted(left), "BIGINT '%s'".formatted(right)))
                        .isEqualTo((long) (left % right));
            }
        }

        for (long left : intLefts) {
            for (long right : intRights) {
                assertThat(assertions.function("mod", Long.toString(left * 10000000000L), Long.toString(right * 10000000000L)))
                        .isEqualTo((left * 10000000000L) % (right * 10000000000L));
            }
        }

        for (int left : intLefts) {
            for (double right : doubleRights) {
                assertThat(assertions.function("mod", Integer.toString(left), "DOUBLE '%s'".formatted(right)))
                        .isEqualTo(left % right);
            }
        }

        for (int left : intLefts) {
            for (double right : doubleRights) {
                assertThat(assertions.function("mod", Integer.toString(left), "REAL '%s'".formatted((float) right)))
                        .isEqualTo(left % (float) right);
            }
        }

        for (double left : doubleLefts) {
            for (long right : intRights) {
                assertThat(assertions.function("mod", "DOUBLE '%s'".formatted(left), Long.toString(right)))
                        .isEqualTo(left % right);
            }
        }

        for (double left : doubleLefts) {
            for (long right : intRights) {
                assertThat(assertions.function("mod", "REAL '%s'".formatted((float) left), Long.toString(right)))
                        .isEqualTo((float) left % right);
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertThat(assertions.function("mod", "DOUBLE '%s'".formatted(left), "DOUBLE '%s'".formatted(right)))
                        .isEqualTo(left % right);
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertThat(assertions.function("mod", "REAL '%s'".formatted((float) left), "REAL '%s'".formatted((float) right)))
                        .isEqualTo((float) left % (float) right);
            }
        }

        assertThat(assertions.function("mod", "5.0E0", "NULL"))
                .isNull(DOUBLE);

        assertThat(assertions.function("mod", "NULL", "5.0E0"))
                .isNull(DOUBLE);

        assertThat(assertions.function("mod", "DECIMAL '0.0'", "DECIMAL '2.0'"))
                .isEqualTo(decimal("0", createDecimalType(1, 1)));

        assertThat(assertions.function("mod", "DECIMAL '13.0'", "DECIMAL '5.0'"))
                .isEqualTo(decimal("3.0", createDecimalType(2, 1)));

        assertThat(assertions.function("mod", "DECIMAL '-13.0'", "DECIMAL '5.0'"))
                .isEqualTo(decimal("-3.0", createDecimalType(2, 1)));

        assertThat(assertions.function("mod", "DECIMAL '13.0'", "DECIMAL '-5.0'"))
                .isEqualTo(decimal("3.0", createDecimalType(2, 1)));

        assertThat(assertions.function("mod", "DECIMAL '-13.0'", "DECIMAL '-5.0'"))
                .isEqualTo(decimal("-3.0", createDecimalType(2, 1)));

        assertThat(assertions.function("mod", "DECIMAL '5.0'", "DECIMAL '2.5'"))
                .isEqualTo(decimal("0.0", createDecimalType(2, 1)));

        assertThat(assertions.function("mod", "DECIMAL '5.0'", "DECIMAL '2.05'"))
                .isEqualTo(decimal("0.90", createDecimalType(3, 2)));

        assertThat(assertions.function("mod", "DECIMAL '5.0'", "DECIMAL '2.55'"))
                .isEqualTo(decimal("2.45", createDecimalType(3, 2)));

        assertThat(assertions.function("mod", "DECIMAL '5.0001'", "DECIMAL '2.55'"))
                .isEqualTo(decimal("2.4501", createDecimalType(5, 4)));

        assertThat(assertions.function("mod", "DECIMAL '123456789012345670'", "DECIMAL '123456789012345669'"))
                .isEqualTo(decimal("0.01", createDecimalType(18)));

        assertThat(assertions.function("mod", "DECIMAL '12345678901234567.90'", "DECIMAL '12345678901234567.89'"))
                .isEqualTo(decimal("0.01", createDecimalType(19, 2)));

        assertThat(assertions.function("mod", "DECIMAL '5.0'", "CAST(NULL as DECIMAL(1,0))"))
                .isNull(createDecimalType(2, 1));

        assertThat(assertions.function("mod", "CAST(NULL as DECIMAL(1,0))", "DECIMAL '5.0'"))
                .isNull(createDecimalType(2, 1));

        assertTrinoExceptionThrownBy(() -> assertions.function("mod", "DECIMAL '5.0'", "DECIMAL '0'").evaluate())
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testPi()
    {
        assertThat(assertions.function("pi"))
                .isEqualTo(Math.PI);
    }

    @Test
    public void testNaN()
    {
        assertThat(assertions.function("nan"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.expression("a / b")
                .binding("a", "0.0E0")
                .binding("b", "0.0E0"))
                .isEqualTo(Double.NaN);
    }

    @Test
    public void testInfinity()
    {
        assertThat(assertions.function("infinity"))
                .isEqualTo(Double.POSITIVE_INFINITY);

        assertThat(assertions.expression("-rand() / a")
                .binding("a", "0.0E0"))
                .isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testIsInfinite()
    {
        assertThat(assertions.function("is_infinite", "1.0E0 / 0.0E0"))
                .isEqualTo(true);

        assertThat(assertions.function("is_infinite", "0.0E0 / 0.0E0"))
                .isEqualTo(false);

        assertThat(assertions.function("is_infinite", "1.0E0 / 1.0E0"))
                .isEqualTo(false);

        assertThat(assertions.function("is_infinite", "REAL '1.0' / REAL '0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("is_infinite", "REAL '0.0' / REAL '0.0'"))
                .isEqualTo(false);

        assertThat(assertions.function("is_infinite", "REAL '1.0' / REAL '1.0'"))
                .isEqualTo(false);

        assertThat(assertions.function("is_infinite", "NULL"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testIsFinite()
    {
        assertThat(assertions.function("is_finite", "100000"))
                .isEqualTo(true);

        assertThat(assertions.function("is_finite", "rand() / 0.0E0"))
                .isEqualTo(false);

        assertThat(assertions.function("is_finite", "REAL '754.2008E0'"))
                .isEqualTo(true);

        assertThat(assertions.function("is_finite", "rand() / REAL '0.0E0'"))
                .isEqualTo(false);

        assertThat(assertions.function("is_finite", "NULL"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testIsNaN()
    {
        assertThat(assertions.function("is_nan", "0.0E0 / 0.0E0"))
                .isEqualTo(true);

        assertThat(assertions.function("is_nan", "0.0E0 / 1.0E0"))
                .isEqualTo(false);

        assertThat(assertions.function("is_nan", "infinity() / infinity()"))
                .isEqualTo(true);

        assertThat(assertions.function("is_nan", "nan()"))
                .isEqualTo(true);

        assertThat(assertions.function("is_nan", "REAL 'NaN'"))
                .isEqualTo(true);

        assertThat(assertions.function("is_nan", "REAL '0.0' / REAL '0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("is_nan", "REAL '0.0' / 1.0E0"))
                .isEqualTo(false);

        assertThat(assertions.function("is_nan", "infinity() / infinity()"))
                .isEqualTo(true);

        assertThat(assertions.function("is_nan", "nan()"))
                .isEqualTo(true);

        assertThat(assertions.function("is_nan", "NULL"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testPower()
    {
        for (long left : intLefts) {
            for (long right : intRights) {
                assertThat(assertions.function("power", Double.toString(left), Double.toString(right)))
                        .isEqualTo(Math.pow(left, right));
            }
        }

        for (int left : intLefts) {
            for (int right : intRights) {
                assertThat(assertions.function("power", "BIGINT '%s'".formatted(left), "BIGINT '%s'".formatted(right)))
                        .isEqualTo(Math.pow(left, right));
            }
        }

        for (long left : intLefts) {
            for (long right : intRights) {
                assertThat(assertions.function("power", "" + left * 10000000000L + "", Long.toString(right)))
                        .isEqualTo(Math.pow(left * 10000000000L, right));
            }
        }

        for (long left : intLefts) {
            for (double right : doubleRights) {
                assertThat(assertions.function("power", Long.toString(left), Double.toString(right)))
                        .isEqualTo(Math.pow(left, right));

                assertThat(assertions.function("power", Long.toString(left), "REAL '%s'".formatted((float) right)))
                        .isEqualTo(Math.pow(left, (float) right));
            }
        }

        for (double left : doubleLefts) {
            for (long right : intRights) {
                assertThat(assertions.function("power", Double.toString(left), Long.toString(right)))
                        .isEqualTo(Math.pow(left, right));

                assertThat(assertions.function("power", "REAL '%s'".formatted((float) left), Long.toString(right)))
                        .isEqualTo(Math.pow((float) left, right));
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertThat(assertions.function("power", Double.toString(left), Double.toString(right)))
                        .isEqualTo(Math.pow(left, right));

                assertThat(assertions.function("power", "REAL '%s'".formatted(left), "REAL '%s'".formatted(right)))
                        .isEqualTo(Math.pow((float) left, (float) right));
            }
        }

        assertThat(assertions.function("power", "NULL", "NULL"))
                .isNull(DOUBLE);

        assertThat(assertions.function("power", "5.0E0", "NULL"))
                .isNull(DOUBLE);

        assertThat(assertions.function("power", "NULL", "5.0E0"))
                .isNull(DOUBLE);

        // test alias
        assertThat(assertions.function("pow", "5.0E0", "2.0E0"))
                .isEqualTo(25.0);
    }

    @Test
    public void testRadians()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("radians", Double.toString(doubleValue)))
                    .isEqualTo(Math.toRadians(doubleValue));

            assertThat(assertions.function("radians", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.toRadians((float) doubleValue));
        }

        assertThat(assertions.function("radians", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testRandom()
    {
        // random is non-deterministic
        assertThat(assertions.function("rand"))
                .hasType(DOUBLE);

        assertThat(assertions.function("random"))
                .hasType(DOUBLE);

        assertThat(assertions.expression("rand(1000)"))
                .hasType(INTEGER);

        assertThat(assertions.expression("random(TINYINT '3', TINYINT '5')"))
                .hasType(TINYINT);

        assertThat(assertions.expression("random(TINYINT '-3', TINYINT '-1')"))
                .hasType(TINYINT);

        assertThat(assertions.expression("random(TINYINT '-3', TINYINT '5')"))
                .hasType(TINYINT);

        assertThat(assertions.expression("random(SMALLINT '20000', SMALLINT '30000')"))
                .hasType(SMALLINT);

        assertThat(assertions.expression("random(SMALLINT '-20000', SMALLINT '-10000')"))
                .hasType(SMALLINT);

        assertThat(assertions.expression("random(SMALLINT '-20000', SMALLINT '30000')"))
                .hasType(SMALLINT);

        assertThat(assertions.expression("random(1000, 2000)"))
                .hasType(INTEGER);

        assertThat(assertions.expression("random(-10, -5)"))
                .hasType(INTEGER);

        assertThat(assertions.expression("random(-10, 10)"))
                .hasType(INTEGER);

        assertThat(assertions.expression("random(2000)"))
                .hasType(INTEGER);

        assertThat(assertions.expression("random(3000000000)"))
                .hasType(BIGINT);

        assertThat(assertions.expression("random(3000000000, 5000000000)"))
                .hasType(BIGINT);

        assertThat(assertions.expression("random(-3000000000, -2000000000)"))
                .hasType(BIGINT);

        assertThat(assertions.expression("random(-3000000000, 5000000000)"))
                .hasType(BIGINT);

        assertTrinoExceptionThrownBy(() -> assertions.function("rand", "-1").evaluate())
                .hasMessage("bound must be positive");

        assertTrinoExceptionThrownBy(() -> assertions.function("rand", "-3000000000").evaluate())
                .hasMessage("bound must be positive");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "TINYINT '5'", "TINYINT '3'").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "TINYINT '5'", "TINYINT '5'").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "TINYINT '-5'", "TINYINT '-10'").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "TINYINT '-5'", "TINYINT '-5'").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "SMALLINT '30000'", "SMALLINT '10000'").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "SMALLINT '30000'", "SMALLINT '30000'").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "SMALLINT '-30000'", "SMALLINT '-31000'").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "SMALLINT '-30000'", "SMALLINT '-30000'").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "1000", "500").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "500", "500").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "-500", "-600").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "-500", "-500").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "3000000000", "1000000000").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "3000000000", "3000000000").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "-3000000000", "-4000000000").evaluate())
                .hasMessage("start value must be less than stop value");

        assertTrinoExceptionThrownBy(() -> assertions.function("random", "-3000000000", "-3000000000").evaluate())
                .hasMessage("start value must be less than stop value");
    }

    @Test
    public void testRound()
    {
        assertThat(assertions.function("round", "TINYINT '3'"))
                .isEqualTo((byte) 3);

        assertThat(assertions.function("round", "TINYINT '-3'"))
                .isEqualTo((byte) -3);

        assertThat(assertions.function("round", "CAST(NULL as TINYINT)"))
                .isNull(TINYINT);

        assertThat(assertions.function("round", "SMALLINT '3'"))
                .isEqualTo((short) 3);

        assertThat(assertions.function("round", "SMALLINT '-3'"))
                .isEqualTo((short) -3);

        assertThat(assertions.function("round", "CAST(NULL as SMALLINT)"))
                .isNull(SMALLINT);

        assertThat(assertions.function("round", "3"))
                .isEqualTo(3);

        assertThat(assertions.function("round", "-3"))
                .isEqualTo(-3);

        assertThat(assertions.function("round", "CAST(NULL as INTEGER)"))
                .isNull(INTEGER);

        assertThat(assertions.function("round", "BIGINT '3'"))
                .isEqualTo(3L);

        assertThat(assertions.function("round", "BIGINT '-3'"))
                .isEqualTo(-3L);

        assertThat(assertions.function("round", "CAST(NULL as BIGINT)"))
                .isNull(BIGINT);

        assertThat(assertions.function("round", " 3000000000"))
                .isEqualTo(3000000000L);

        assertThat(assertions.function("round", "-3000000000"))
                .isEqualTo(-3000000000L);

        assertThat(assertions.function("round", "3.0E0"))
                .isEqualTo(3.0);

        assertThat(assertions.function("round", "-3.0E0"))
                .isEqualTo(-3.0);

        assertThat(assertions.function("round", "3.499E0"))
                .isEqualTo(3.0);

        assertThat(assertions.function("round", "-3.499E0"))
                .isEqualTo(-3.0);

        assertThat(assertions.function("round", "3.5E0"))
                .isEqualTo(4.0);

        assertThat(assertions.function("round", "-3.5E0"))
                .isEqualTo(-4.0);

        assertThat(assertions.function("round", "-3.5001E0"))
                .isEqualTo(-4.0);

        assertThat(assertions.function("round", "-3.99E0"))
                .isEqualTo(-4.0);

        assertThat(assertions.function("round", "REAL '3.0'"))
                .isEqualTo(3.0f);

        assertThat(assertions.function("round", "REAL '-3.0'"))
                .isEqualTo(-3.0f);

        assertThat(assertions.function("round", "REAL '3.499'"))
                .isEqualTo(3.0f);

        assertThat(assertions.function("round", "REAL '-3.499'"))
                .isEqualTo(-3.0f);

        assertThat(assertions.function("round", "REAL '3.5'"))
                .isEqualTo(4.0f);

        assertThat(assertions.function("round", "REAL '-3.5'"))
                .isEqualTo(-4.0f);

        assertThat(assertions.function("round", "REAL '-3.5001'"))
                .isEqualTo(-4.0f);

        assertThat(assertions.function("round", "REAL '-3.99'"))
                .isEqualTo(-4.0f);

        assertThat(assertions.function("round", "CAST(NULL as DOUBLE)"))
                .isNull(DOUBLE);

        assertThat(assertions.function("round", "DOUBLE '%s'".formatted(GREATEST_DOUBLE_LESS_THAN_HALF)))
                .isEqualTo(0.0);

        assertThat(assertions.function("round", "DOUBLE '-" + 0x1p-1 + "'"))
                .isEqualTo(-1.0); // -0.5

        assertThat(assertions.function("round", "DOUBLE '-" + GREATEST_DOUBLE_LESS_THAN_HALF + "'"))
                .isEqualTo(-0.0);

        assertThat(assertions.function("round", "TINYINT '3'", "TINYINT '0'"))
                .isEqualTo((byte) 3);

        assertThat(assertions.function("round", "TINYINT '3'", "0"))
                .isEqualTo((byte) 3);

        assertThat(assertions.function("round", "SMALLINT '3'", "SMALLINT '0'"))
                .isEqualTo((short) 3);

        assertThat(assertions.function("round", "SMALLINT '3'", "0"))
                .isEqualTo((short) 3);

        assertThat(assertions.function("round", "3", "0"))
                .isEqualTo(3);

        assertThat(assertions.function("round", "-3", "0"))
                .isEqualTo(-3);

        assertThat(assertions.function("round", "-3", "INTEGER '0'"))
                .isEqualTo(-3);

        assertThat(assertions.function("round", "BIGINT '3'", "0"))
                .isEqualTo(3L);

        assertThat(assertions.function("round", " 3000000000", "0"))
                .isEqualTo(3000000000L);

        assertThat(assertions.function("round", "-3000000000", "0"))
                .isEqualTo(-3000000000L);

        assertThat(assertions.function("round", "3.0E0", "0"))
                .isEqualTo(3.0);

        assertThat(assertions.function("round", "-3.0E0", "0"))
                .isEqualTo(-3.0);

        assertThat(assertions.function("round", "3.499E0", "0"))
                .isEqualTo(3.0);

        assertThat(assertions.function("round", "-3.499E0", "0"))
                .isEqualTo(-3.0);

        assertThat(assertions.function("round", "3.5E0", "0"))
                .isEqualTo(4.0);

        assertThat(assertions.function("round", "-3.5E0", "0"))
                .isEqualTo(-4.0);

        assertThat(assertions.function("round", "-3.5001E0", "0"))
                .isEqualTo(-4.0);

        assertThat(assertions.function("round", "-3.99E0", "0"))
                .isEqualTo(-4.0);

        assertThat(assertions.function("round", "DOUBLE '%s'".formatted(GREATEST_DOUBLE_LESS_THAN_HALF), "0"))
                .isEqualTo(0.0);

        assertThat(assertions.function("round", "DOUBLE '-" + 0x1p-1 + "'"))
                .isEqualTo(-1.0); // -0.5

        assertThat(assertions.function("round", "DOUBLE '-" + GREATEST_DOUBLE_LESS_THAN_HALF + "'", "0"))
                .isEqualTo(-0.0);

        assertThat(assertions.function("round", "0.3E0"))
                .isEqualTo(0.0);

        assertThat(assertions.function("round", "-0.3E0"))
                .isEqualTo(-0.0);

        // 923 * 10^16 exceeds Long.MAX_VALUE
        assertThat(assertions.function("round", "923e0", "16"))
                .isEqualTo(923.0);

        // 3000 * 10^16 exceeds Long.MAX_VALUE. Note that value has limited precision even before round is invoked
        assertThat(assertions.function("round", "DOUBLE '3000.1234567890123456789'", "16"))
                .isEqualTo(3000.1234567890124);

        assertThat(assertions.function("round", "TINYINT '3'", "TINYINT '1'"))
                .isEqualTo((byte) 3);

        assertThat(assertions.function("round", "TINYINT '3'", "1"))
                .isEqualTo((byte) 3);

        assertThat(assertions.function("round", "SMALLINT '3'", "SMALLINT '1'"))
                .isEqualTo((short) 3);

        assertThat(assertions.function("round", "SMALLINT '3'", "1"))
                .isEqualTo((short) 3);

        assertThat(assertions.function("round", "REAL '3.0'", "0"))
                .isEqualTo(3.0f);

        assertThat(assertions.function("round", "REAL '-3.0'", "0"))
                .isEqualTo(-3.0f);

        assertThat(assertions.function("round", "REAL '3.499'", "0"))
                .isEqualTo(3.0f);

        assertThat(assertions.function("round", "REAL '-3.499'", "0"))
                .isEqualTo(-3.0f);

        assertThat(assertions.function("round", "REAL '3.5'", "0"))
                .isEqualTo(4.0f);

        assertThat(assertions.function("round", "REAL '-3.5'", "0"))
                .isEqualTo(-4.0f);

        assertThat(assertions.function("round", "REAL '-3.5001'", "0"))
                .isEqualTo(-4.0f);

        assertThat(assertions.function("round", "REAL '-3.99'", "0"))
                .isEqualTo(-4.0f);

        // 923 * 10^16 exceeds Long.MAX_VALUE
        assertThat(assertions.function("round", "REAL '923'", "16"))
                .isEqualTo(923f);

        // 3000 * 10^16 exceeds Long.MAX_VALUE. Note that value has limited precision even before round is invoked
        assertThat(assertions.function("round", "REAL '3000.1234567890123456789'", "16"))
                .isEqualTo(3000.1235f);

        assertThat(assertions.function("round", "3", "1"))
                .isEqualTo(3);

        assertThat(assertions.function("round", "-3", "1"))
                .isEqualTo(-3);

        assertThat(assertions.function("round", "-3", "INTEGER '1'"))
                .isEqualTo(-3);

        assertThat(assertions.function("round", "-3", "CAST(NULL as INTEGER)"))
                .isNull(INTEGER);

        assertThat(assertions.function("round", "BIGINT '3'", "1"))
                .isEqualTo(3L);

        assertThat(assertions.function("round", " 3000000000", "1"))
                .isEqualTo(3000000000L);

        assertThat(assertions.function("round", "-3000000000", "1"))
                .isEqualTo(-3000000000L);

        assertThat(assertions.function("round", "CAST(NULL as BIGINT)", "CAST(NULL as INTEGER)"))
                .isNull(BIGINT);

        assertThat(assertions.function("round", "CAST(NULL as BIGINT)", "1"))
                .isNull(BIGINT);

        assertThat(assertions.function("round", "3.0E0", "1"))
                .isEqualTo(3.0);

        assertThat(assertions.function("round", "-3.0E0", "1"))
                .isEqualTo(-3.0);

        assertThat(assertions.function("round", "3.499E0", "1"))
                .isEqualTo(3.5);

        assertThat(assertions.function("round", "-3.499E0", "1"))
                .isEqualTo(-3.5);

        assertThat(assertions.function("round", "3.5E0", "1"))
                .isEqualTo(3.5);

        assertThat(assertions.function("round", "-3.5E0", "1"))
                .isEqualTo(-3.5);

        assertThat(assertions.function("round", "-3.5001E0", "1"))
                .isEqualTo(-3.5);

        assertThat(assertions.function("round", "-3.99E0", "1"))
                .isEqualTo(-4.0);

        assertThat(assertions.function("round", "REAL '3.0'", "1"))
                .isEqualTo(3.0f);

        assertThat(assertions.function("round", "REAL '-3.0'", "1"))
                .isEqualTo(-3.0f);

        assertThat(assertions.function("round", "REAL '3.499'", "1"))
                .isEqualTo(3.5f);

        assertThat(assertions.function("round", "REAL '-3.499'", "1"))
                .isEqualTo(-3.5f);

        assertThat(assertions.function("round", "REAL '3.5'", "1"))
                .isEqualTo(3.5f);

        assertThat(assertions.function("round", "REAL '-3.5'", "1"))
                .isEqualTo(-3.5f);

        assertThat(assertions.function("round", "REAL '-3.5001'", "1"))
                .isEqualTo(-3.5f);

        assertThat(assertions.function("round", "REAL '-3.99'", "1"))
                .isEqualTo(-4.0f);

        // ROUND negative DECIMAL
        assertThat(assertions.function("round", "TINYINT '9'", "-1"))
                .isEqualTo((byte) 10);

        assertThat(assertions.function("round", "TINYINT '-9'", "-1"))
                .isEqualTo((byte) -10);

        assertThat(assertions.function("round", "TINYINT '5'", "-1"))
                .isEqualTo((byte) 10);

        assertThat(assertions.function("round", "TINYINT '-5'", "-1"))
                .isEqualTo((byte) -10);

        assertThat(assertions.function("round", "TINYINT '-14'", "-1"))
                .isEqualTo((byte) -10);

        assertThat(assertions.function("round", "TINYINT '12'", "-1"))
                .isEqualTo((byte) 10);

        assertThat(assertions.function("round", "TINYINT '18'", "-1"))
                .isEqualTo((byte) 20);

        assertThat(assertions.function("round", "TINYINT '18'", "-2"))
                .isEqualTo((byte) 0);

        assertThat(assertions.function("round", "TINYINT '18'", "-3"))
                .isEqualTo((byte) 0);

        assertThat(assertions.function("round", "TINYINT '127'", "-2"))
                .isEqualTo((byte) 100);

        assertThat(assertions.function("round", "TINYINT '127'", "-3"))
                .isEqualTo((byte) 0);

        assertThat(assertions.function("round", "TINYINT '-128'", "-2"))
                .isEqualTo((byte) -100);

        assertThat(assertions.function("round", "TINYINT '-128'", "-3"))
                .isEqualTo((byte) 0);

        assertThat(assertions.function("round", "SMALLINT '99'", "-1"))
                .isEqualTo((short) 100);

        assertThat(assertions.function("round", "SMALLINT '99'", "-2"))
                .isEqualTo((short) 100);

        assertThat(assertions.function("round", "SMALLINT '99'", "-3"))
                .isEqualTo((short) 0);

        assertThat(assertions.function("round", "SMALLINT '-99'", "-1"))
                .isEqualTo((short) -100);

        assertThat(assertions.function("round", "SMALLINT '-99'", "-2"))
                .isEqualTo((short) -100);

        assertThat(assertions.function("round", "SMALLINT '-99'", "-3"))
                .isEqualTo((short) 0);

        assertThat(assertions.function("round", "SMALLINT '32767'", "-4"))
                .isEqualTo((short) 30000);

        assertThat(assertions.function("round", "SMALLINT '32767'", "-5"))
                .isEqualTo((short) 0);

        assertThat(assertions.function("round", "SMALLINT '-32768'", "-4"))
                .isEqualTo((short) -30000);

        assertThat(assertions.function("round", "SMALLINT '-32768'", "-5"))
                .isEqualTo((short) 0);

        assertThat(assertions.function("round", "99", "-1"))
                .isEqualTo(100);

        assertThat(assertions.function("round", "-99", "-1"))
                .isEqualTo(-100);

        assertThat(assertions.function("round", "99", "INTEGER '-1'"))
                .isEqualTo(100);

        assertThat(assertions.function("round", "-99", "INTEGER '-1'"))
                .isEqualTo(-100);

        assertThat(assertions.function("round", "12355", "-2"))
                .isEqualTo(12400);

        assertThat(assertions.function("round", "12345", "-2"))
                .isEqualTo(12300);

        assertThat(assertions.function("round", "2147483647", "-9"))
                .isEqualTo(2000000000);

        assertThat(assertions.function("round", "2147483647", "-10"))
                .isEqualTo(0);

        assertThat(assertions.function("round", " 3999999999", "-1"))
                .isEqualTo(4000000000L);

        assertThat(assertions.function("round", "-3999999999", "-1"))
                .isEqualTo(-4000000000L);

        assertThat(assertions.function("round", "9223372036854775807", "-2"))
                .isEqualTo(9223372036854775800L);

        assertThat(assertions.function("round", "9223372036854775807", "-17"))
                .isEqualTo(9200000000000000000L);

        assertThat(assertions.function("round", "9223372036854775807", "-18"))
                .isEqualTo(9000000000000000000L);

        assertThat(assertions.function("round", "-9223372036854775807", "-17"))
                .isEqualTo(-9200000000000000000L);

        assertThat(assertions.function("round", "-9223372036854775807", "-18"))
                .isEqualTo(-9000000000000000000L);

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "TINYINT '127'", "-1").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "TINYINT '-128'", "-1").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "SMALLINT '32767'", "-1").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "SMALLINT '32767'", "-3").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "SMALLINT '-32768'", "-1").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "SMALLINT '-32768'", "-3").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "2147483647", "-100").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "2147483647", "-2147483648").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "9223372036854775807", "-1").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "9223372036854775807", "-3").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "9223372036854775807", "-19").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "-9223372036854775807", "-20").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "-9223372036854775807", "-2147483648").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        // ROUND short DECIMAL -> short DECIMAL
        assertThat(assertions.function("round", "DECIMAL '0'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.function("round", "DECIMAL '0.1'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.function("round", "DECIMAL '-0.1'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.function("round", "DECIMAL '3'"))
                .isEqualTo(decimal("3", createDecimalType(1)));

        assertThat(assertions.function("round", "DECIMAL '-3'"))
                .isEqualTo(decimal("-3", createDecimalType(1)));

        assertThat(assertions.function("round", "DECIMAL '3.0'"))
                .isEqualTo(decimal("3", createDecimalType(2)));

        assertThat(assertions.function("round", "DECIMAL '-3.0'"))
                .isEqualTo(decimal("-3", createDecimalType(2)));

        assertThat(assertions.function("round", "DECIMAL '3.49'"))
                .isEqualTo(decimal("3", createDecimalType(2)));

        assertThat(assertions.function("round", "DECIMAL '-3.49'"))
                .isEqualTo(decimal("-3", createDecimalType(2)));

        assertThat(assertions.function("round", "DECIMAL '3.50'"))
                .isEqualTo(decimal("4", createDecimalType(2)));

        assertThat(assertions.function("round", "DECIMAL '-3.50'"))
                .isEqualTo(decimal("-4", createDecimalType(2)));

        assertThat(assertions.function("round", "DECIMAL '3.99'"))
                .isEqualTo(decimal("4", createDecimalType(2)));

        assertThat(assertions.function("round", "DECIMAL '-3.99'"))
                .isEqualTo(decimal("-4", createDecimalType(2)));

        assertThat(assertions.function("round", "DECIMAL '9.99'"))
                .isEqualTo(decimal("10", createDecimalType(2)));

        assertThat(assertions.function("round", "DECIMAL '-9.99'"))
                .isEqualTo(decimal("-10", createDecimalType(2)));

        assertThat(assertions.function("round", "DECIMAL '9999.9'"))
                .isEqualTo(decimal("10000", createDecimalType(5)));

        assertThat(assertions.function("round", "DECIMAL '-9999.9'"))
                .isEqualTo(decimal("-10000", createDecimalType(5)));

        assertThat(assertions.function("round", "DECIMAL '1000000000000.9999'"))
                .isEqualTo(decimal("1000000000001", createDecimalType(14)));

        assertThat(assertions.function("round", "DECIMAL '-1000000000000.9999'"))
                .isEqualTo(decimal("-1000000000001", createDecimalType(14)));

        assertThat(assertions.function("round", "DECIMAL '10000000000000000'"))
                .isEqualTo(decimal("10000000000000000", createDecimalType(17)));

        assertThat(assertions.function("round", "DECIMAL '-10000000000000000'"))
                .isEqualTo(decimal("-10000000000000000", createDecimalType(17)));

        assertThat(assertions.function("round", "DECIMAL '9999999999999999.99'"))
                .isEqualTo(decimal("10000000000000000", createDecimalType(17)));

        assertThat(assertions.function("round", "DECIMAL '99999999999999999.9'"))
                .isEqualTo(decimal("100000000000000000", createDecimalType(18)));

        // ROUND long DECIMAL -> long DECIMAL
        assertThat(assertions.function("round", "CAST(0 AS DECIMAL(18,0))"))
                .isEqualTo(decimal("0", createDecimalType(18)));

        assertThat(assertions.function("round", "CAST(0 AS DECIMAL(18,1))"))
                .isEqualTo(decimal("0", createDecimalType(18)));

        assertThat(assertions.function("round", "CAST(0 AS DECIMAL(18,2))"))
                .isEqualTo(decimal("0", createDecimalType(17)));

        assertThat(assertions.function("round", "CAST(DECIMAL '0.1' AS DECIMAL(18,1))"))
                .isEqualTo(decimal("0", createDecimalType(18)));

        assertThat(assertions.function("round", "CAST(DECIMAL '-0.1' AS DECIMAL(18,1))"))
                .isEqualTo(decimal("0", createDecimalType(18)));

        assertThat(assertions.function("round", "DECIMAL '3000000000000000000000'"))
                .isEqualTo(decimal("3000000000000000000000", createDecimalType(22)));

        assertThat(assertions.function("round", "DECIMAL '-3000000000000000000000'"))
                .isEqualTo(decimal("-3000000000000000000000", createDecimalType(22)));

        assertThat(assertions.function("round", "DECIMAL '3000000000000000000000.0'"))
                .isEqualTo(decimal("3000000000000000000000", createDecimalType(23)));

        assertThat(assertions.function("round", "DECIMAL '-3000000000000000000000.0'"))
                .isEqualTo(decimal("-3000000000000000000000", createDecimalType(23)));

        assertThat(assertions.function("round", "DECIMAL '3000000000000000000000.49'"))
                .isEqualTo(decimal("3000000000000000000000", createDecimalType(23)));

        assertThat(assertions.function("round", "DECIMAL '-3000000000000000000000.49'"))
                .isEqualTo(decimal("-3000000000000000000000", createDecimalType(23)));

        assertThat(assertions.function("round", "DECIMAL '3000000000000000000000.50'"))
                .isEqualTo(decimal("3000000000000000000001", createDecimalType(23)));

        assertThat(assertions.function("round", "DECIMAL '-3000000000000000000000.50'"))
                .isEqualTo(decimal("-3000000000000000000001", createDecimalType(23)));

        assertThat(assertions.function("round", "DECIMAL '3000000000000000000000.99'"))
                .isEqualTo(decimal("3000000000000000000001", createDecimalType(23)));

        assertThat(assertions.function("round", "DECIMAL '-3000000000000000000000.99'"))
                .isEqualTo(decimal("-3000000000000000000001", createDecimalType(23)));

        assertThat(assertions.function("round", "DECIMAL '9999999999999999999999.99'"))
                .isEqualTo(decimal("10000000000000000000000", createDecimalType(23)));

        assertThat(assertions.function("round", "DECIMAL '-9999999999999999999999.99'"))
                .isEqualTo(decimal("-10000000000000000000000", createDecimalType(23)));

        assertThat(assertions.function("round", "DECIMAL '1000000000000000000000000000000000.9999'"))
                .isEqualTo(decimal("1000000000000000000000000000000001", createDecimalType(35)));

        assertThat(assertions.function("round", "DECIMAL '-1000000000000000000000000000000000.9999'"))
                .isEqualTo(decimal("-1000000000000000000000000000000001", createDecimalType(35)));

        assertThat(assertions.function("round", "DECIMAL '10000000000000000000000000000000000000'"))
                .isEqualTo(decimal("10000000000000000000000000000000000000", createDecimalType(38)));

        assertThat(assertions.function("round", "DECIMAL '-10000000000000000000000000000000000000'"))
                .isEqualTo(decimal("-10000000000000000000000000000000000000", createDecimalType(38)));

        // ROUND long DECIMAL -> short DECIMAL
        assertThat(assertions.function("round", "DECIMAL '3000000000000000.000000'"))
                .isEqualTo(decimal("3000000000000000", createDecimalType(17)));

        assertThat(assertions.function("round", "DECIMAL '-3000000000000000.000000'"))
                .isEqualTo(decimal("-3000000000000000", createDecimalType(17)));

        assertThat(assertions.function("round", "DECIMAL '3000000000000000.499999'"))
                .isEqualTo(decimal("3000000000000000", createDecimalType(17)));

        assertThat(assertions.function("round", "DECIMAL '-3000000000000000.499999'"))
                .isEqualTo(decimal("-3000000000000000", createDecimalType(17)));

        assertThat(assertions.function("round", "DECIMAL '3000000000000000.500000'"))
                .isEqualTo(decimal("3000000000000001", createDecimalType(17)));

        assertThat(assertions.function("round", "DECIMAL '-3000000000000000.500000'"))
                .isEqualTo(decimal("-3000000000000001", createDecimalType(17)));

        assertThat(assertions.function("round", "DECIMAL '3000000000000000.999999'"))
                .isEqualTo(decimal("3000000000000001", createDecimalType(17)));

        assertThat(assertions.function("round", "DECIMAL '-3000000000000000.999999'"))
                .isEqualTo(decimal("-3000000000000001", createDecimalType(17)));

        assertThat(assertions.function("round", "DECIMAL '9999999999999999.999999'"))
                .isEqualTo(decimal("10000000000000000", createDecimalType(17)));

        assertThat(assertions.function("round", "DECIMAL '-9999999999999999.999999'"))
                .isEqualTo(decimal("-10000000000000000", createDecimalType(17)));

        // ROUND_N short DECIMAL -> short DECIMAL
        assertThat(assertions.function("round", "DECIMAL '3'", "1"))
                .isEqualTo(decimal("3", createDecimalType(2)));

        assertThat(assertions.function("round", "DECIMAL '-3'", "1"))
                .isEqualTo(decimal("-3", createDecimalType(2)));

        assertThat(assertions.function("round", "DECIMAL '3.0'", "1"))
                .isEqualTo(decimal("3.0", createDecimalType(3, 1)));

        assertThat(assertions.function("round", "DECIMAL '-3.0'", "1"))
                .isEqualTo(decimal("-3.0", createDecimalType(3, 1)));

        assertThat(assertions.function("round", "DECIMAL '3.449'", "1"))
                .isEqualTo(decimal("3.400", createDecimalType(5, 3)));

        assertThat(assertions.function("round", "DECIMAL '-3.449'", "1"))
                .isEqualTo(decimal("-3.400", createDecimalType(5, 3)));

        assertThat(assertions.function("round", "DECIMAL '3.450'", "1"))
                .isEqualTo(decimal("3.500", createDecimalType(5, 3)));

        assertThat(assertions.function("round", "DECIMAL '-3.450'", "1"))
                .isEqualTo(decimal("-3.500", createDecimalType(5, 3)));

        assertThat(assertions.function("round", "DECIMAL '3.99'", "1"))
                .isEqualTo(decimal("4.00", createDecimalType(4, 2)));

        assertThat(assertions.function("round", "DECIMAL '-3.99'", "1"))
                .isEqualTo(decimal("-4.00", createDecimalType(4, 2)));

        assertThat(assertions.function("round", "DECIMAL '9.99'", "1"))
                .isEqualTo(decimal("10.00", createDecimalType(4, 2)));

        assertThat(assertions.function("round", "DECIMAL '-9.99'", "1"))
                .isEqualTo(decimal("-10.00", createDecimalType(4, 2)));

        assertThat(assertions.function("round", "DECIMAL '0.3'", "0"))
                .isEqualTo(decimal("0.0", createDecimalType(2, 1)));

        assertThat(assertions.function("round", "DECIMAL '0.7'", "0"))
                .isEqualTo(decimal("1.0", createDecimalType(2, 1)));

        assertThat(assertions.function("round", "DECIMAL '1.7'", "0"))
                .isEqualTo(decimal("2.0", createDecimalType(3, 1)));

        assertThat(assertions.function("round", "DECIMAL '-0.3'", "0"))
                .isEqualTo(decimal("0.0", createDecimalType(2, 1)));

        assertThat(assertions.function("round", "DECIMAL '-0.7'", "0"))
                .isEqualTo(decimal("-1.0", createDecimalType(2, 1)));

        assertThat(assertions.function("round", "DECIMAL '-1.7'", "0"))
                .isEqualTo(decimal("-2.0", createDecimalType(3, 1)));

        assertThat(assertions.function("round", "DECIMAL '0.7'", "-1"))
                .isEqualTo(decimal("0.0", createDecimalType(2, 1)));

        assertThat(assertions.function("round", "DECIMAL '1.7'", "-1"))
                .isEqualTo(decimal("0.0", createDecimalType(3, 1)));

        assertThat(assertions.function("round", "DECIMAL '7.1'", "-1"))
                .isEqualTo(decimal("10.0", createDecimalType(3, 1)));

        assertThat(assertions.function("round", "DECIMAL '0.3'", "-1"))
                .isEqualTo(decimal("0.0", createDecimalType(2, 1)));

        assertThat(assertions.function("round", "DECIMAL '33.3'", "-2"))
                .isEqualTo(decimal("0.0", createDecimalType(4, 1)));

        assertThat(assertions.function("round", "CAST(DECIMAL '0.7' AS decimal(20, 1))", "-19"))
                .isEqualTo(decimal("0.0", createDecimalType(21, 1)));

        assertThat(assertions.function("round", "DECIMAL '0.00'", "1"))
                .isEqualTo(decimal("0.00", createDecimalType(3, 2)));

        assertThat(assertions.function("round", "DECIMAL '1234'", "7"))
                .isEqualTo(decimal("1234", createDecimalType(5)));

        assertThat(assertions.function("round", "DECIMAL '-1234'", "7"))
                .isEqualTo(decimal("-1234", createDecimalType(5)));

        assertThat(assertions.function("round", "DECIMAL '1234'", "-7"))
                .isEqualTo(decimal("0", createDecimalType(5)));

        assertThat(assertions.function("round", "DECIMAL '-1234'", "-7"))
                .isEqualTo(decimal("0", createDecimalType(5)));

        assertThat(assertions.function("round", "DECIMAL '1234.5678'", "7"))
                .isEqualTo(decimal("1234.5678", createDecimalType(9, 4)));

        assertThat(assertions.function("round", "DECIMAL '-1234.5678'", "7"))
                .isEqualTo(decimal("-1234.5678", createDecimalType(9, 4)));

        assertThat(assertions.function("round", "DECIMAL '1234.5678'", "-2"))
                .isEqualTo(decimal("1200.0000", createDecimalType(9, 4)));

        assertThat(assertions.function("round", "DECIMAL '-1234.5678'", "-2"))
                .isEqualTo(decimal("-1200.0000", createDecimalType(9, 4)));

        assertThat(assertions.function("round", "DECIMAL '1254.5678'", "-2"))
                .isEqualTo(decimal("1300.0000", createDecimalType(9, 4)));

        assertThat(assertions.function("round", "DECIMAL '-1254.5678'", "-2"))
                .isEqualTo(decimal("-1300.0000", createDecimalType(9, 4)));

        assertThat(assertions.function("round", "DECIMAL '1234.5678'", "-7"))
                .isEqualTo(decimal("0.0000", createDecimalType(9, 4)));

        assertThat(assertions.function("round", "DECIMAL '-1234.5678'", "-7"))
                .isEqualTo(decimal("0.0000", createDecimalType(9, 4)));

        assertThat(assertions.function("round", "DECIMAL '99'", "-1"))
                .isEqualTo(decimal("100", createDecimalType(3)));

        // ROUND_N long DECIMAL -> long DECIMAL
        assertThat(assertions.function("round", "DECIMAL '1234567890123456789'", "1"))
                .isEqualTo(decimal("1234567890123456789", createDecimalType(20)));

        assertThat(assertions.function("round", "DECIMAL '-1234567890123456789'", "1"))
                .isEqualTo(decimal("-1234567890123456789", createDecimalType(20)));

        assertThat(assertions.function("round", "DECIMAL '123456789012345678.0'", "1"))
                .isEqualTo(decimal("123456789012345678.0", createDecimalType(20, 1)));

        assertThat(assertions.function("round", "DECIMAL '-123456789012345678.0'", "1"))
                .isEqualTo(decimal("-123456789012345678.0", createDecimalType(20, 1)));

        assertThat(assertions.function("round", "DECIMAL '123456789012345678.449'", "1"))
                .isEqualTo(decimal("123456789012345678.400", createDecimalType(22, 3)));

        assertThat(assertions.function("round", "DECIMAL '-123456789012345678.449'", "1"))
                .isEqualTo(decimal("-123456789012345678.400", createDecimalType(22, 3)));

        assertThat(assertions.function("round", "DECIMAL '123456789012345678.45'", "1"))
                .isEqualTo(decimal("123456789012345678.50", createDecimalType(21, 2)));

        assertThat(assertions.function("round", "DECIMAL '-123456789012345678.45'", "1"))
                .isEqualTo(decimal("-123456789012345678.50", createDecimalType(21, 2)));

        assertThat(assertions.function("round", "DECIMAL '123456789012345678.501'", "1"))
                .isEqualTo(decimal("123456789012345678.500", createDecimalType(22, 3)));

        assertThat(assertions.function("round", "DECIMAL '-123456789012345678.501'", "1"))
                .isEqualTo(decimal("-123456789012345678.500", createDecimalType(22, 3)));

        assertThat(assertions.function("round", "DECIMAL '999999999999999999.99'", "1"))
                .isEqualTo(decimal("1000000000000000000.00", createDecimalType(21, 2)));

        assertThat(assertions.function("round", "DECIMAL '-999999999999999999.99'", "1"))
                .isEqualTo(decimal("-1000000000000000000.00", createDecimalType(21, 2)));

        assertThat(assertions.function("round", "DECIMAL '1234567890123456789'", "7"))
                .isEqualTo(decimal("1234567890123456789", createDecimalType(20)));

        assertThat(assertions.function("round", "DECIMAL '-1234567890123456789'", "7"))
                .isEqualTo(decimal("-1234567890123456789", createDecimalType(20)));

        assertThat(assertions.function("round", "DECIMAL '123456789012345678.99'", "7"))
                .isEqualTo(decimal("123456789012345678.99", createDecimalType(21, 2)));

        assertThat(assertions.function("round", "DECIMAL '-123456789012345678.99'", "7"))
                .isEqualTo(decimal("-123456789012345678.99", createDecimalType(21, 2)));

        assertThat(assertions.function("round", "DECIMAL '123456789012345611.99'", "-2"))
                .isEqualTo(decimal("123456789012345600.00", createDecimalType(21, 2)));

        assertThat(assertions.function("round", "DECIMAL '-123456789012345611.99'", "-2"))
                .isEqualTo(decimal("-123456789012345600.00", createDecimalType(21, 2)));

        assertThat(assertions.function("round", "DECIMAL '123456789012345678.99'", "-2"))
                .isEqualTo(decimal("123456789012345700.00", createDecimalType(21, 2)));

        assertThat(assertions.function("round", "DECIMAL '-123456789012345678.99'", "-2"))
                .isEqualTo(decimal("-123456789012345700.00", createDecimalType(21, 2)));

        assertThat(assertions.function("round", "DECIMAL '123456789012345678.99'", "-30"))
                .isEqualTo(decimal("0.00", createDecimalType(21, 2)));

        assertThat(assertions.function("round", "DECIMAL '-123456789012345678.99'", "-30"))
                .isEqualTo(decimal("0.00", createDecimalType(21, 2)));

        assertThat(assertions.function("round", "DECIMAL '9999999999999999999999999999999999999.9'", "1"))
                .isEqualTo(decimal("9999999999999999999999999999999999999.9", createDecimalType(38, 1)));

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "DECIMAL '9999999999999999999999999999999999999.9'", "0").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(() -> assertions.function("round", "DECIMAL '9999999999999999999999999999999999999.9'", "-1").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertThat(assertions.function("round", "DECIMAL  '1329123201320737513'", "-3"))
                .isEqualTo(decimal("1329123201320738000", createDecimalType(20)));

        assertThat(assertions.function("round", "DECIMAL '-1329123201320737513'", "-3"))
                .isEqualTo(decimal("-1329123201320738000", createDecimalType(20)));

        assertThat(assertions.function("round", "DECIMAL  '1329123201320739513'", "-3"))
                .isEqualTo(decimal("1329123201320740000", createDecimalType(20)));

        assertThat(assertions.function("round", "DECIMAL '-1329123201320739513'", "-3"))
                .isEqualTo(decimal("-1329123201320740000", createDecimalType(20)));

        assertThat(assertions.function("round", "DECIMAL  '9999999999999999999'", "-3"))
                .isEqualTo(decimal("10000000000000000000", createDecimalType(20)));

        assertThat(assertions.function("round", "DECIMAL '-9999999999999999999'", "-3"))
                .isEqualTo(decimal("-10000000000000000000", createDecimalType(20)));

        // ROUND_N short DECIMAL -> long DECIMAL

        assertThat(assertions.function("round", "DECIMAL '9999999999999999.99'", "1"))
                .isEqualTo(decimal("10000000000000000.00", createDecimalType(19, 2)));

        assertThat(assertions.function("round", "DECIMAL '-9999999999999999.99'", "1"))
                .isEqualTo(decimal("-10000000000000000.00", createDecimalType(19, 2)));

        assertThat(assertions.function("round", "DECIMAL '9999999999999999.99'", "-1"))
                .isEqualTo(decimal("10000000000000000.00", createDecimalType(19, 2)));

        assertThat(assertions.function("round", "DECIMAL '-9999999999999999.99'", "-1"))
                .isEqualTo(decimal("-10000000000000000.00", createDecimalType(19, 2)));

        assertThat(assertions.function("round", "DECIMAL '9999999999999999.99'", "2"))
                .isEqualTo(decimal("9999999999999999.99", createDecimalType(19, 2)));

        assertThat(assertions.function("round", "DECIMAL '-9999999999999999.99'", "2"))
                .isEqualTo(decimal("-9999999999999999.99", createDecimalType(19, 2)));

        assertThat(assertions.function("round", "DECIMAL '329123201320737513'", "-3"))
                .isEqualTo(decimal("329123201320738000", createDecimalType(19)));

        assertThat(assertions.function("round", "DECIMAL '-329123201320737513'", "-3"))
                .isEqualTo(decimal("-329123201320738000", createDecimalType(19)));

        assertThat(assertions.function("round", "DECIMAL '329123201320739513'", "-3"))
                .isEqualTo(decimal("329123201320740000", createDecimalType(19)));

        assertThat(assertions.function("round", "DECIMAL '-329123201320739513'", "-3"))
                .isEqualTo(decimal("-329123201320740000", createDecimalType(19)));

        assertThat(assertions.function("round", "DECIMAL '999999999999999999'", "-3"))
                .isEqualTo(decimal("1000000000000000000", createDecimalType(19)));

        assertThat(assertions.function("round", "DECIMAL '-999999999999999999'", "-3"))
                .isEqualTo(decimal("-1000000000000000000", createDecimalType(19)));

        // NULL

        assertThat(assertions.function("round", "CAST(NULL as DOUBLE)", "CAST(NULL as INTEGER)"))
                .isNull(DOUBLE);

        assertThat(assertions.function("round", "-3.0E0", "CAST(NULL as INTEGER)"))
                .isNull(DOUBLE);

        assertThat(assertions.function("round", "CAST(NULL as DOUBLE)", "1"))
                .isNull(DOUBLE);

        assertThat(assertions.function("round", "CAST(NULL as DECIMAL(1,0))", "CAST(NULL as INTEGER)"))
                .isNull(createDecimalType(2, 0));

        assertThat(assertions.function("round", "DECIMAL '-3.0'", "CAST(NULL as INTEGER)"))
                .isNull(createDecimalType(3, 1));

        assertThat(assertions.function("round", "CAST(NULL as DECIMAL(1,0))", "1"))
                .isNull(createDecimalType(2, 0));

        assertThat(assertions.function("round", "CAST(NULL as DECIMAL(17,2))", "1"))
                .isNull(createDecimalType(18, 2));

        assertThat(assertions.function("round", "CAST(NULL as DECIMAL(20,2))", "1"))
                .isNull(createDecimalType(21, 2));

        // NaN

        assertThat(assertions.function("round", "nan()", "2"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.function("round", "1.0E0 / 0", "2"))
                .isEqualTo(Double.POSITIVE_INFINITY);

        assertThat(assertions.function("round", "-1.0E0 / 0", "2"))
                .isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testSign()
    {
        //retains type for NULL values
        assertThat(assertions.function("sign", "CAST(NULL as TINYINT)"))
                .isNull(TINYINT);

        assertThat(assertions.function("sign", "CAST(NULL as SMALLINT)"))
                .isNull(SMALLINT);

        assertThat(assertions.function("sign", "CAST(NULL as INTEGER)"))
                .isNull(INTEGER);

        assertThat(assertions.function("sign", "CAST(NULL as BIGINT)"))
                .isNull(BIGINT);

        assertThat(assertions.function("sign", "CAST(NULL as DOUBLE)"))
                .isNull(DOUBLE);

        assertThat(assertions.function("sign", "CAST(NULL as DECIMAL(2,1))"))
                .isNull(createDecimalType(1, 0));

        assertThat(assertions.function("sign", "CAST(NULL as DECIMAL(38,0))"))
                .isNull(createDecimalType(1, 0));

        //tinyint
        for (int intValue : intLefts) {
            Float signum = Math.signum(intValue);

            assertThat(assertions.function("sign", "TINYINT '%s'".formatted(intValue)))
                    .isEqualTo(signum.byteValue());
        }

        //smallint
        for (int intValue : intLefts) {
            Float signum = Math.signum(intValue);

            assertThat(assertions.function("sign", "SMALLINT '%s'".formatted(intValue)))
                    .isEqualTo(signum.shortValue());
        }

        //integer
        for (int intValue : intLefts) {
            Float signum = Math.signum(intValue);

            assertThat(assertions.function("sign", "INTEGER '%s'".formatted(intValue)))
                    .isEqualTo(signum.intValue());
        }

        //bigint
        for (int intValue : intLefts) {
            Float signum = Math.signum(intValue);

            assertThat(assertions.function("sign", "BIGINT '%s'".formatted(intValue)))
                    .isEqualTo(signum.longValue());
        }

        //double and float
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("sign", "DOUBLE '%s'".formatted(doubleValue)))
                    .isEqualTo(Math.signum(doubleValue));

            assertThat(assertions.function("sign", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.signum(((float) doubleValue)));
        }

        //returns NaN for NaN input
        assertThat(assertions.function("sign", "DOUBLE 'NaN'"))
                .isEqualTo(Double.NaN);

        //returns proper sign for +/-Infinity input
        assertThat(assertions.function("sign", "DOUBLE '+Infinity'"))
                .isEqualTo(1.0);

        assertThat(assertions.function("sign", "DOUBLE '-Infinity'"))
                .isEqualTo(-1.0);

        //short decimal
        assertThat(assertions.function("sign", "DECIMAL '0'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.function("sign", "DECIMAL '123'"))
                .isEqualTo(decimal("1", createDecimalType(1)));

        assertThat(assertions.function("sign", "DECIMAL '-123'"))
                .isEqualTo(decimal("-1", createDecimalType(1)));

        assertThat(assertions.function("sign", "DECIMAL '123.000000000000000'"))
                .isEqualTo(decimal("1", createDecimalType(1)));

        assertThat(assertions.function("sign", "DECIMAL '-123.000000000000000'"))
                .isEqualTo(decimal("-1", createDecimalType(1)));

        //long decimal
        assertThat(assertions.function("sign", "DECIMAL '0.000000000000000000'"))
                .isEqualTo(decimal("0", createDecimalType(1)));

        assertThat(assertions.function("sign", "DECIMAL '1230.000000000000000'"))
                .isEqualTo(decimal("1", createDecimalType(1)));

        assertThat(assertions.function("sign", "DECIMAL '-1230.000000000000000'"))
                .isEqualTo(decimal("-1", createDecimalType(1)));
    }

    @Test
    public void testSin()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("sin", Double.toString(doubleValue)))
                    .isEqualTo(Math.sin(doubleValue));

            assertThat(assertions.function("sin", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.sin((float) doubleValue));
        }

        assertThat(assertions.function("sin", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testSinh()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("sinh", Double.toString(doubleValue)))
                    .isEqualTo(Math.sinh(doubleValue));

            assertThat(assertions.function("sinh", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.sinh((float) doubleValue));
        }

        assertThat(assertions.function("sinh", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testSqrt()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("sqrt", Double.toString(doubleValue)))
                    .isEqualTo(Math.sqrt(doubleValue));

            assertThat(assertions.function("sqrt", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.sqrt((float) doubleValue));
        }

        assertThat(assertions.function("sqrt", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testTan()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("tan", Double.toString(doubleValue)))
                    .isEqualTo(Math.tan(doubleValue));

            assertThat(assertions.function("tan", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.tan((float) doubleValue));
        }

        assertThat(assertions.function("tan", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testTanh()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertThat(assertions.function("tanh", Double.toString(doubleValue)))
                    .isEqualTo(Math.tanh(doubleValue));

            assertThat(assertions.function("tanh", "REAL '%s'".formatted((float) doubleValue)))
                    .isEqualTo(Math.tanh((float) doubleValue));
        }

        assertThat(assertions.function("tanh", "NULL"))
                .isNull(DOUBLE);
    }

    @Test
    public void testGreatest()
    {
        // tinyint
        assertThat(assertions.function("greatest", "TINYINT '1'", "TINYINT '2'"))
                .isEqualTo((byte) 2);

        assertThat(assertions.function("greatest", "TINYINT '-1'", "TINYINT '-2'"))
                .isEqualTo((byte) -1);

        assertThat(assertions.function("greatest", "TINYINT '5'", "TINYINT '4'", "TINYINT '3'", "TINYINT '2'", "TINYINT '1'", "TINYINT '2'", "TINYINT '3'", "TINYINT '4'", "TINYINT '1'", "TINYINT '5'"))
                .isEqualTo((byte) 5);

        assertThat(assertions.function("greatest", "TINYINT '-1'"))
                .isEqualTo((byte) -1);

        assertThat(assertions.function("greatest", "TINYINT '5'", "TINYINT '4'", "CAST(NULL AS TINYINT)", "TINYINT '3'"))
                .isNull(TINYINT);

        // smallint
        assertThat(assertions.function("greatest", "SMALLINT '1'", "SMALLINT '2'"))
                .isEqualTo((short) 2);

        assertThat(assertions.function("greatest", "SMALLINT '-1'", "SMALLINT '-2'"))
                .isEqualTo((short) -1);

        assertThat(assertions.function("greatest", "SMALLINT '5'", "SMALLINT '4'", "SMALLINT '3'", "SMALLINT '2'", "SMALLINT '1'", "SMALLINT '2'", "SMALLINT '3'", "SMALLINT '4'", "SMALLINT '1'", "SMALLINT '5'"))
                .isEqualTo((short) 5);

        assertThat(assertions.function("greatest", "SMALLINT '-1'"))
                .isEqualTo((short) -1);

        assertThat(assertions.function("greatest", "SMALLINT '5'", "SMALLINT '4'", "CAST(NULL AS SMALLINT)", "SMALLINT '3'"))
                .isNull(SMALLINT);

        // integer
        assertThat(assertions.function("greatest", "1", "2"))
                .isEqualTo(2);

        assertThat(assertions.function("greatest", "-1", "-2"))
                .isEqualTo(-1);

        assertThat(assertions.function("greatest", "5", "4", "3", "2", "1", "2", "3", "4", "1", "5"))
                .isEqualTo(5);

        assertThat(assertions.function("greatest", "-1"))
                .isEqualTo(-1);

        assertThat(assertions.function("greatest", "5", "4", "CAST(NULL AS INTEGER)", "3"))
                .isNull(INTEGER);

        // bigint
        assertThat(assertions.function("greatest", "10000000000", "20000000000"))
                .isEqualTo(20000000000L);

        assertThat(assertions.function("greatest", "-10000000000", "-20000000000"))
                .isEqualTo(-10000000000L);

        assertThat(assertions.function("greatest", "5000000000", "4", "3", "2", "1000000000", "2", "3", "4", "1", "5000000000"))
                .isEqualTo(5000000000L);

        assertThat(assertions.function("greatest", "-10000000000"))
                .isEqualTo(-10000000000L);

        assertThat(assertions.function("greatest", "5000000000", "4000000000", "CAST(NULL as BIGINT)", "3000000000"))
                .isNull(BIGINT);

        // double
        assertThat(assertions.function("greatest", "1.5E0", "2.3E0"))
                .isEqualTo(2.3);

        assertThat(assertions.function("greatest", "-1.5E0", "-2.3E0"))
                .isEqualTo(-1.5);

        assertThat(assertions.function("greatest", "-1.5E0", "-2.3E0", "-5/3"))
                .isEqualTo(-1.0);

        assertThat(assertions.function("greatest", "1.5E0", "-infinity()", "infinity()"))
                .isEqualTo(Double.POSITIVE_INFINITY);

        assertThat(assertions.function("greatest", "5", "4", "CAST(NULL as DOUBLE)", "3"))
                .isNull(DOUBLE);

        assertThat(assertions.function("greatest", "NaN()", "5", "4", "3"))
                .isEqualTo(5.0);

        assertThat(assertions.function("greatest", "5", "4", "NaN()", "3"))
                .isEqualTo(5.0);

        assertThat(assertions.function("greatest", "5", "4", "3", "NaN()"))
                .isEqualTo(5.0);

        assertThat(assertions.function("greatest", "NaN()"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.function("greatest", "NaN()", "NaN()", "NaN()"))
                .isEqualTo(Double.NaN);

        // real
        assertThat(assertions.function("greatest", "REAL '1.5'", "REAL '2.3'"))
                .isEqualTo(2.3f);

        assertThat(assertions.function("greatest", "REAL '-1.5'", "REAL '-2.3'"))
                .isEqualTo(-1.5f);

        assertThat(assertions.function("greatest", "REAL '-1.5'", "REAL '-2.3'", "CAST(-5/3 AS REAL)"))
                .isEqualTo(-1.0f);

        assertThat(assertions.function("greatest", "REAL '1.5'", "CAST(infinity() AS REAL)"))
                .isEqualTo(Float.POSITIVE_INFINITY);

        assertThat(assertions.function("greatest", "REAL '5'", "REAL '4'", "CAST(NULL as REAL)", "REAL '3'"))
                .isNull(REAL);

        assertThat(assertions.function("greatest", "CAST(NaN() as REAL)", "REAL '5'", "REAL '4'", "REAL '3'"))
                .isEqualTo(5.0f);

        assertThat(assertions.function("greatest", "REAL '5'", "REAL '4'", "CAST(NaN() as REAL)", "REAL '3'"))
                .isEqualTo(5.0f);

        assertThat(assertions.function("greatest", "REAL '5'", "REAL '4'", "REAL '3'", "CAST(NaN() as REAL)"))
                .isEqualTo(5.0f);

        assertThat(assertions.function("greatest", "CAST(NaN() as REAL)"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.function("greatest", "CAST(NaN() as REAL)", "CAST(NaN() as REAL)", "CAST(NaN() as REAL)"))
                .isEqualTo(Float.NaN);

        // decimal
        assertThat(assertions.function("greatest", "1.0", "2.0"))
                .isEqualTo(decimal("2.0", createDecimalType(2, 1)));

        assertThat(assertions.function("greatest", "1.0", "-2.0"))
                .isEqualTo(decimal("1.0", createDecimalType(2, 1)));

        assertThat(assertions.function("greatest", "1.0", "1.1", "1.2", "1.3"))
                .isEqualTo(decimal("1.3", createDecimalType(2, 1)));

        // mixed
        assertThat(assertions.function("greatest", "1", "20000000000"))
                .isEqualTo(20000000000L);

        assertThat(assertions.function("greatest", "1", "BIGINT '2'"))
                .isEqualTo(2L);

        assertThat(assertions.function("greatest", "1.0E0", "2"))
                .isEqualTo(2.0);

        assertThat(assertions.function("greatest", "1", "2.0E0"))
                .isEqualTo(2.0);

        assertThat(assertions.function("greatest", "1.0E0", "2"))
                .isEqualTo(2.0);

        assertThat(assertions.function("greatest", "5.0E0", "4", "CAST(NULL as DOUBLE)", "3"))
                .isNull(DOUBLE);

        assertThat(assertions.function("greatest", "5.0E0", "4", "CAST(NULL as BIGINT)", "3"))
                .isNull(DOUBLE);

        assertThat(assertions.function("greatest", "1.0", "2.0E0"))
                .isEqualTo(2.0);

        assertThat(assertions.function("greatest", "5", "4", "3.0", "2"))
                .isEqualTo(decimal("0000000005.0", createDecimalType(11, 1)));

        // argument count limit
        assertThat(assertions.function("greatest", nCopies(127, "1E0")))
                .hasType(DOUBLE);

        assertTrinoExceptionThrownBy(() -> assertions.function("greatest", nCopies(128, "rand()")).evaluate())
                .hasErrorCode(TOO_MANY_ARGUMENTS)
                .hasMessage("line 1:8: Too many arguments for function call greatest()");

        // row(double)
        assertThat(assertions.function("greatest", "ROW(1.5E0)", "ROW(2.3E0)"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(2.3));

        assertThat(assertions.function("greatest", "ROW(-1.5E0)", "ROW(-2.3E0)"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(-1.5));

        assertThat(assertions.function("greatest", "ROW(-1.5E0)", "ROW(-2.3E0)", "ROW(-5/3)"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(-1.0));

        assertThat(assertions.function("greatest", "ROW(1.5E0)", "ROW(-infinity())", "ROW(infinity())"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(Double.POSITIVE_INFINITY));

        assertThat(assertions.function("greatest", "ROW(5)", "ROW(4)", "CAST(NULL as ROW(DOUBLE))", "ROW(3)"))
                .hasType(anonymousRow(DOUBLE))
                .isNull(anonymousRow(DOUBLE));

        assertThat(assertions.function("greatest", "ROW(NaN())", "ROW(5)", "ROW(4)", "ROW(3)"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(5.0));

        assertThat(assertions.function("greatest", "ROW(5)", "ROW(4)", "ROW(NaN())", "ROW(3)"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(5.0));

        assertThat(assertions.function("greatest", "ROW(5)", "ROW(4)", "ROW(3)", "ROW(NaN())"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(5.0));

        assertThat(assertions.function("greatest", "ROW(NaN())"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(Double.NaN));

        assertThat(assertions.function("greatest", "ROW(NaN())", "ROW(NaN())", "ROW(NaN())"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(Double.NaN));

        // row(real)
        assertThat(assertions.function("greatest", "ROW(REAL '1.5')", "ROW(REAL '2.3')"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(2.3f));

        assertThat(assertions.function("greatest", "ROW(REAL '-1.5')", "ROW(REAL '-2.3')"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(-1.5f));

        assertThat(assertions.function("greatest", "ROW(REAL '-1.5')", "ROW(REAL '-2.3')", "ROW(CAST(-5/3 AS REAL))"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(-1.0f));

        assertThat(assertions.function("greatest", "ROW(REAL '1.5')", "ROW(CAST(infinity() AS REAL))"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(Float.POSITIVE_INFINITY));

        assertThat(assertions.function("greatest", "ROW(REAL '5')", "ROW(REAL '4')", "CAST(NULL as ROW(REAL))", "ROW(REAL '3')"))
                .isNull(anonymousRow(REAL));

        assertThat(assertions.function("greatest", "ROW(CAST(NaN() as REAL))", "ROW(REAL '5')", "ROW(REAL '4')", "ROW(REAL '3')"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(5.0f));

        assertThat(assertions.function("greatest", "ROW(REAL '5')", "ROW(REAL '4')", "ROW(CAST(NaN() as REAL))", "ROW(REAL '3')"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(5.0f));

        assertThat(assertions.function("greatest", "ROW(REAL '5')", "ROW(REAL '4')", "ROW(REAL '3')", "ROW(CAST(NaN() as REAL))"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(5.0f));

        assertThat(assertions.function("greatest", "ROW(CAST(NaN() as REAL))"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(Float.NaN));

        assertThat(assertions.function("greatest", "ROW(CAST(NaN() as REAL))", "ROW(CAST(NaN() as REAL))", "ROW(CAST(NaN() as REAL))"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(Float.NaN));

        // array(double)
        assertThat(assertions.function("greatest", "ARRAY[1.5E0]", "ARRAY[2.3E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(2.3));

        assertThat(assertions.function("greatest", "ARRAY[-1.5E0]", "ARRAY[-2.3E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(-1.5));

        assertThat(assertions.function("greatest", "ARRAY[-1.5E0]", "ARRAY[-2.3E0]", "ARRAY[-5/3]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(-1.0));

        assertThat(assertions.function("greatest", "ARRAY[1.5E0]", "ARRAY[-infinity()]", "ARRAY[infinity()]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(Double.POSITIVE_INFINITY));

        assertThat(assertions.function("greatest", "ARRAY[5]", "ARRAY[4]", "CAST(NULL as ARRAY(DOUBLE))", "ARRAY[3]"))
                .isNull(new ArrayType(DOUBLE));

        assertThat(assertions.function("greatest", "ARRAY[NaN()]", "ARRAY[5]", "ARRAY[4]", "ARRAY[3]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(5.0));

        assertThat(assertions.function("greatest", "ARRAY[5]", "ARRAY[4]", "ARRAY[NaN()]", "ARRAY[3]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(5.0));

        assertThat(assertions.function("greatest", "ARRAY[5]", "ARRAY[4]", "ARRAY[3]", "ARRAY[NaN()]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(5.0));

        assertThat(assertions.function("greatest", "ARRAY[NaN()]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(Double.NaN));

        assertThat(assertions.function("greatest", "ARRAY[NaN()]", "ARRAY[NaN()]", "ARRAY[NaN()]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(Double.NaN));

        // array(real)
        assertThat(assertions.function("greatest", "ARRAY[REAL '1.5']", "ARRAY[REAL '2.3']"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(2.3f));

        assertThat(assertions.function("greatest", "ARRAY[REAL '-1.5']", "ARRAY[REAL '-2.3']"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(-1.5f));

        assertThat(assertions.function("greatest", "ARRAY[REAL '-1.5']", "ARRAY[REAL '-2.3']", "ARRAY[CAST(-5/3 AS REAL)]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(-1.0f));

        assertThat(assertions.function("greatest", "ARRAY[REAL '1.5']", "ARRAY[CAST(infinity() AS REAL)]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(Float.POSITIVE_INFINITY));

        assertThat(assertions.function("greatest", "ARRAY[REAL '5']", "ARRAY[REAL '4']", "CAST(NULL as ARRAY(REAL))", "ARRAY[REAL '3']"))
                .isNull(new ArrayType(REAL));

        assertThat(assertions.function("greatest", "ARRAY[CAST(NaN() as REAL)]", "ARRAY[REAL '5']", "ARRAY[REAL '4']", "ARRAY[REAL '3']"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(5.0f));

        assertThat(assertions.function("greatest", "ARRAY[REAL '5']", "ARRAY[REAL '4']", "ARRAY[CAST(NaN() as REAL)]", "ARRAY[REAL '3']"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(5.0f));

        assertThat(assertions.function("greatest", "ARRAY[REAL '5']", "ARRAY[REAL '4']", "ARRAY[REAL '3']", "ARRAY[CAST(NaN() as REAL)]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(5.0f));

        assertThat(assertions.function("greatest", "ARRAY[CAST(NaN() as REAL)]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(Float.NaN));

        assertThat(assertions.function("greatest", "ARRAY[CAST(NaN() as REAL)]", "ARRAY[CAST(NaN() as REAL)]", "ARRAY[CAST(NaN() as REAL)]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(Float.NaN));
    }

    @Test
    public void testLeast()
    {
        // integer
        assertThat(assertions.function("least", "TINYINT '1'", "TINYINT '2'"))
                .isEqualTo((byte) 1);

        assertThat(assertions.function("least", "TINYINT '-1'", "TINYINT '-2'"))
                .isEqualTo((byte) -2);

        assertThat(assertions.function("least", "TINYINT '5'", "TINYINT '4'", "TINYINT '3'", "TINYINT '2'", "TINYINT '1'", "TINYINT '2'", "TINYINT '3'", "TINYINT '4'", "TINYINT '1'", "TINYINT '5'"))
                .isEqualTo((byte) 1);

        assertThat(assertions.function("least", "TINYINT '-1'"))
                .isEqualTo((byte) -1);

        assertThat(assertions.function("least", "TINYINT '5'", "TINYINT '4'", "CAST(NULL AS TINYINT)", "TINYINT '3'"))
                .isNull(TINYINT);

        // integer
        assertThat(assertions.function("least", "SMALLINT '1'", "SMALLINT '2'"))
                .isEqualTo((short) 1);

        assertThat(assertions.function("least", "SMALLINT '-1'", "SMALLINT '-2'"))
                .isEqualTo((short) -2);

        assertThat(assertions.function("least", "SMALLINT '5'", "SMALLINT '4'", "SMALLINT '3'", "SMALLINT '2'", "SMALLINT '1'", "SMALLINT '2'", "SMALLINT '3'", "SMALLINT '4'", "SMALLINT '1'", "SMALLINT '5'"))
                .isEqualTo((short) 1);

        assertThat(assertions.function("least", "SMALLINT '-1'"))
                .isEqualTo((short) -1);

        assertThat(assertions.function("least", "SMALLINT '5'", "SMALLINT '4'", "CAST(NULL AS SMALLINT)", "SMALLINT '3'"))
                .isNull(SMALLINT);

        // integer
        assertThat(assertions.function("least", "1", "2"))
                .isEqualTo(1);

        assertThat(assertions.function("least", "-1", "-2"))
                .isEqualTo(-2);

        assertThat(assertions.function("least", "5", "4", "3", "2", "1", "2", "3", "4", "1", "5"))
                .isEqualTo(1);

        assertThat(assertions.function("least", "-1"))
                .isEqualTo(-1);

        assertThat(assertions.function("least", "5", "4", "CAST(NULL AS INTEGER)", "3"))
                .isNull(INTEGER);

        // bigint
        assertThat(assertions.function("least", "10000000000", "20000000000"))
                .isEqualTo(10000000000L);

        assertThat(assertions.function("least", "-10000000000", "-20000000000"))
                .isEqualTo(-20000000000L);

        assertThat(assertions.function("least", "50000000000", "40000000000", "30000000000", "20000000000", "50000000000"))
                .isEqualTo(20000000000L);

        assertThat(assertions.function("least", "-10000000000"))
                .isEqualTo(-10000000000L);

        assertThat(assertions.function("least", "500000000", "400000000", "CAST(NULL as BIGINT)", "300000000"))
                .isNull(BIGINT);

        // double
        assertThat(assertions.function("least", "1.5E0", "2.3E0"))
                .isEqualTo(1.5);

        assertThat(assertions.function("least", "-1.5E0", "-2.3E0"))
                .isEqualTo(-2.3);

        assertThat(assertions.function("least", "-1.5E0", "-2.3E0", "-5/3"))
                .isEqualTo(-2.3);

        assertThat(assertions.function("least", "1.5E0", "-infinity()", "infinity()"))
                .isEqualTo(Double.NEGATIVE_INFINITY);

        assertThat(assertions.function("least", "5", "4", "CAST(NULL as DOUBLE)", "3"))
                .isNull(DOUBLE);

        assertThat(assertions.function("least", "NaN()", "5", "4", "3"))
                .isEqualTo(3.0);

        assertThat(assertions.function("least", "5", "4", "NaN()", "3"))
                .isEqualTo(3.0);

        assertThat(assertions.function("least", "5", "4", "3", "NaN()"))
                .isEqualTo(3.0);

        assertThat(assertions.function("least", "NaN()"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.function("least", "NaN()", "NaN()", "NaN()"))
                .isEqualTo(Double.NaN);

        // real
        assertThat(assertions.function("least", "REAL '1.5'", "REAL '2.3'"))
                .isEqualTo(1.5f);

        assertThat(assertions.function("least", "REAL '-1.5'", "REAL '-2.3'"))
                .isEqualTo(-2.3f);

        assertThat(assertions.function("least", "REAL '-1.5'", "REAL '-2.3'", "CAST(-5/3 AS REAL)"))
                .isEqualTo(-2.3f);

        assertThat(assertions.function("least", "REAL '1.5'", "CAST(-infinity() AS REAL)"))
                .isEqualTo(Float.NEGATIVE_INFINITY);

        assertThat(assertions.function("least", "REAL '5'", "REAL '4'", "CAST(NULL as REAL)", "REAL '3'"))
                .isNull(REAL);

        assertThat(assertions.function("least", "CAST(NaN() as REAL)", "REAL '5'", "REAL '4'", "REAL '3'"))
                .isEqualTo(3.0f);

        assertThat(assertions.function("least", "REAL '5'", "REAL '4'", "CAST(NaN() as REAL)", "REAL '3'"))
                .isEqualTo(3.0f);

        assertThat(assertions.function("least", "REAL '5'", "REAL '4'", "REAL '3'", "CAST(NaN() as REAL)"))
                .isEqualTo(3.0f);

        assertThat(assertions.function("least", "CAST(NaN() as REAL)"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.function("least", "CAST(NaN() as REAL)", "CAST(NaN() as REAL)", "CAST(NaN() as REAL)"))
                .isEqualTo(Float.NaN);

        // decimal
        assertThat(assertions.function("least", "1.0", "2.0"))
                .isEqualTo(decimal("1.0", createDecimalType(2, 1)));

        assertThat(assertions.function("least", "1.0", "-2.0"))
                .isEqualTo(decimal("-2.0", createDecimalType(2, 1)));

        assertThat(assertions.function("least", "1.0", "1.1", "1.2", "1.3"))
                .isEqualTo(decimal("1.0", createDecimalType(2, 1)));

        // mixed
        assertThat(assertions.function("least", "1", "20000000000"))
                .isEqualTo(1L);

        assertThat(assertions.function("least", "1", "BIGINT '2'"))
                .isEqualTo(1L);

        assertThat(assertions.function("least", "1.0E0", "2"))
                .isEqualTo(1.0);

        assertThat(assertions.function("least", "1", "2.0E0"))
                .isEqualTo(1.0);

        assertThat(assertions.function("least", "1.0E0", "2"))
                .isEqualTo(1.0);

        assertThat(assertions.function("least", "5.0E0", "4", "CAST(NULL as DOUBLE)", "3"))
                .isNull(DOUBLE);

        assertThat(assertions.function("least", "5.0E0", "4", "CAST(NULL as BIGINT)", "3"))
                .isNull(DOUBLE);

        assertThat(assertions.function("least", "1.0", "2.0E0"))
                .isEqualTo(1.0);

        assertThat(assertions.function("least", "5", "4", "3.0", "2"))
                .isEqualTo(decimal("0000000002.0", createDecimalType(11, 1)));

        // row(double)
        assertThat(assertions.function("least", "ROW(1.5E0)", "ROW(2.3E0)"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(1.5));

        assertThat(assertions.function("least", "ROW(-1.5E0)", "ROW(-2.3E0)"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(-2.3));

        assertThat(assertions.function("least", "ROW(-1.5E0)", "ROW(-2.3E0)", "ROW(-5/3)"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(-2.3));

        assertThat(assertions.function("least", "ROW(1.5E0)", "ROW(-infinity())", "ROW(infinity())"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(Double.NEGATIVE_INFINITY));

        assertThat(assertions.function("least", "ROW(5)", "ROW(4)", "CAST(NULL as ROW(DOUBLE))", "ROW(3)"))
                .isNull(anonymousRow(DOUBLE));

        assertThat(assertions.function("least", "ROW(NaN())", "ROW(5)", "ROW(4)", "ROW(3)"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(3.0));

        assertThat(assertions.function("least", "ROW(5)", "ROW(4)", "ROW(NaN())", "ROW(3)"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(3.0));

        assertThat(assertions.function("least", "ROW(5)", "ROW(4)", "ROW(3)", "ROW(NaN())"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(3.0));

        assertThat(assertions.function("least", "ROW(NaN())"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(Double.NaN));

        assertThat(assertions.function("least", "ROW(NaN())", "ROW(NaN())", "ROW(NaN())"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(Double.NaN));

        // row(real)
        assertThat(assertions.function("least", "ROW(REAL '1.5')", "ROW(REAL '2.3')"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(1.5f));

        assertThat(assertions.function("least", "ROW(REAL '-1.5')", "ROW(REAL '-2.3')"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(-2.3f));

        assertThat(assertions.function("least", "ROW(REAL '-1.5')", "ROW(REAL '-2.3')", "ROW(CAST(-5/3 AS REAL))"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(-2.3f));

        assertThat(assertions.function("least", "ROW(REAL '1.5')", "ROW(CAST(-infinity() AS REAL))", "ROW(CAST(infinity() AS REAL))"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(Float.NEGATIVE_INFINITY));

        assertThat(assertions.function("least", "ROW(REAL '5')", "ROW(REAL '4')", "CAST(NULL as ROW(REAL))", "ROW(REAL '3')"))
                .isNull(anonymousRow(REAL));

        assertThat(assertions.function("least", "ROW(CAST(NaN() as REAL))", "ROW(REAL '5')", "ROW(REAL '4')", "ROW(REAL '3')"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(3.0f));

        assertThat(assertions.function("least", "ROW(REAL '5')", "ROW(REAL '4')", "ROW(CAST(NaN() as REAL))", "ROW(REAL '3')"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(3.0f));

        assertThat(assertions.function("least", "ROW(REAL '5')", "ROW(REAL '4')", "ROW(REAL '3')", "ROW(CAST(NaN() as REAL))"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(3.0f));

        assertThat(assertions.function("least", "ROW(CAST(NaN() as REAL))"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(Float.NaN));

        assertThat(assertions.function("least", "ROW(CAST(NaN() as REAL))", "ROW(CAST(NaN() as REAL))", "ROW(CAST(NaN() as REAL))"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(Float.NaN));

        // array(double)
        assertThat(assertions.function("least", "ARRAY[1.5E0]", "ARRAY[2.3E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.5));

        assertThat(assertions.function("least", "ARRAY[-1.5E0]", "ARRAY[-2.3E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(-2.3));

        assertThat(assertions.function("least", "ARRAY[-1.5E0]", "ARRAY[-2.3E0]", "ARRAY[-5/3]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(-2.3));

        assertThat(assertions.function("least", "ARRAY[1.5E0]", "ARRAY[-infinity()]", "ARRAY[infinity()]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(Double.NEGATIVE_INFINITY));

        assertThat(assertions.function("least", "ARRAY[5]", "ARRAY[4]", "CAST(NULL as ARRAY(DOUBLE))", "ARRAY[3]"))
                .isNull(new ArrayType(DOUBLE));

        assertThat(assertions.function("least", "ARRAY[NaN()]", "ARRAY[5]", "ARRAY[4]", "ARRAY[3]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(3.0));

        assertThat(assertions.function("least", "ARRAY[5]", "ARRAY[4]", "ARRAY[NaN()]", "ARRAY[3]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(3.0));

        assertThat(assertions.function("least", "ARRAY[5]", "ARRAY[4]", "ARRAY[3]", "ARRAY[NaN()]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(3.0));

        assertThat(assertions.function("least", "ARRAY[NaN()]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(Double.NaN));

        assertThat(assertions.function("least", "ARRAY[NaN()]", "ARRAY[NaN()]", "ARRAY[NaN()]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(Double.NaN));

        // array(real)
        assertThat(assertions.function("least", "ARRAY[REAL '1.5']", "ARRAY[REAL '2.3']"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(1.5f));

        assertThat(assertions.function("least", "ARRAY[REAL '-1.5']", "ARRAY[REAL '-2.3']"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(-2.3f));

        assertThat(assertions.function("least", "ARRAY[REAL '-1.5']", "ARRAY[REAL '-2.3']", "ARRAY[CAST(-5/3 AS REAL)]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(-2.3f));

        assertThat(assertions.function("least", "ARRAY[REAL '1.5']", "ARRAY[CAST(-infinity() AS REAL)]", "ARRAY[CAST(infinity() AS REAL)]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(Float.NEGATIVE_INFINITY));

        assertThat(assertions.function("least", "ARRAY[REAL '5']", "ARRAY[REAL '4']", "CAST(NULL as ARRAY(REAL))", "ARRAY[REAL '3']"))
                .isNull(new ArrayType(REAL));

        assertThat(assertions.function("least", "ARRAY[CAST(NaN() as REAL)]", "ARRAY[REAL '5']", "ARRAY[REAL '4']", "ARRAY[REAL '3']"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(3.0f));

        assertThat(assertions.function("least", "ARRAY[REAL '5']", "ARRAY[REAL '4']", "ARRAY[CAST(NaN() as REAL)]", "ARRAY[REAL '3']"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(3.0f));

        assertThat(assertions.function("least", "ARRAY[REAL '5']", "ARRAY[REAL '4']", "ARRAY[REAL '3']", "ARRAY[CAST(NaN() as REAL)]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(3.0f));

        assertThat(assertions.function("least", "ARRAY[CAST(NaN() as REAL)]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(Float.NaN));

        assertThat(assertions.function("least", "ARRAY[CAST(NaN() as REAL)]", "ARRAY[CAST(NaN() as REAL)]", "ARRAY[CAST(NaN() as REAL)]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(Float.NaN));
    }

    @Test
    public void testToBase()
    {
        assertThat(assertions.function("to_base", "2147483648", "16"))
                .isEqualTo("80000000");

        assertThat(assertions.function("to_base", "255", "2"))
                .isEqualTo("11111111");

        assertThat(assertions.function("to_base", "-2147483647", "16"))
                .isEqualTo("-7fffffff");

        assertThat(assertions.function("to_base", "NULL", "16"))
                .isNull(createVarcharType(64));

        assertThat(assertions.function("to_base", "-2147483647", "NULL"))
                .isNull(createVarcharType(64));

        assertThat(assertions.function("to_base", "NULL", "NULL"))
                .isNull(createVarcharType(64));

        assertTrinoExceptionThrownBy(() -> assertions.function("to_base", "255", "1").evaluate())
                .hasMessage("Radix must be between 2 and 36");
    }

    @Test
    public void testFromBase()
    {
        assertThat(assertions.function("from_base", "'80000000'", "16"))
                .isEqualTo(2147483648L);

        assertThat(assertions.function("from_base", "'11111111'", "2"))
                .isEqualTo(255L);

        assertThat(assertions.function("from_base", "'-7fffffff'", "16"))
                .isEqualTo(-2147483647L);

        assertThat(assertions.function("from_base", "'9223372036854775807'", "10"))
                .isEqualTo(9223372036854775807L);

        assertThat(assertions.function("from_base", "'-9223372036854775808'", "10"))
                .isEqualTo(-9223372036854775808L);

        assertThat(assertions.function("from_base", "NULL", "10"))
                .isNull(BIGINT);

        assertThat(assertions.function("from_base", "'-9223372036854775808'", "NULL"))
                .isNull(BIGINT);

        assertThat(assertions.function("from_base", "NULL", "NULL"))
                .isNull(BIGINT);

        assertTrinoExceptionThrownBy(() -> assertions.function("from_base", "'Z'", "37").evaluate())
                .hasMessage("Radix must be between 2 and 36");

        assertTrinoExceptionThrownBy(() -> assertions.function("from_base", "'Z'", "35").evaluate())
                .hasMessage("Not a valid base-35 number: Z");

        assertTrinoExceptionThrownBy(() -> assertions.function("from_base", "'9223372036854775808'", "10").evaluate())
                .hasMessage("Not a valid base-10 number: 9223372036854775808");

        assertTrinoExceptionThrownBy(() -> assertions.function("from_base", "'Z'", "37").evaluate())
                .hasMessage("Radix must be between 2 and 36");

        assertTrinoExceptionThrownBy(() -> assertions.function("from_base", "'Z'", "35").evaluate())
                .hasMessage("Not a valid base-35 number: Z");

        assertTrinoExceptionThrownBy(() -> assertions.function("from_base", "'9223372036854775808'", "10").evaluate())
                .hasMessage("Not a valid base-10 number: 9223372036854775808");
    }

    @Test
    public void testWidthBucket()
    {
        assertThat(assertions.function("width_bucket", "3.14E0", "0", "4", "3"))
                .isEqualTo(3L);

        assertThat(assertions.function("width_bucket", "2", "0", "4", "3"))
                .isEqualTo(2L);

        assertThat(assertions.function("width_bucket", "infinity()", "0", "4", "3"))
                .isEqualTo(4L);

        assertThat(assertions.function("width_bucket", "-1", "0", "3.2E0", "4"))
                .isEqualTo(0L);

        // bound1 > bound2 is not symmetric with bound2 > bound1
        assertThat(assertions.function("width_bucket", "3.14E0", "4", "0", "3"))
                .isEqualTo(1L);

        assertThat(assertions.function("width_bucket", "2", "4", "0", "3"))
                .isEqualTo(2L);

        assertThat(assertions.function("width_bucket", "infinity()", "4", "0", "3"))
                .isEqualTo(0L);

        assertThat(assertions.function("width_bucket", "-1", "3.2E0", "0", "4"))
                .isEqualTo(5L);

        // failure modes
        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "3.14E0", "0", "4", "0").evaluate())
                .hasMessage("bucketCount must be greater than 0");

        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "3.14E0", "0", "4", "-1").evaluate())
                .hasMessage("bucketCount must be greater than 0");

        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "nan()", "0", "4", "3").evaluate())
                .hasMessage("operand must not be NaN");

        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "3.14E0", "-1", "-1", "3").evaluate())
                .hasMessage("bounds cannot equal each other");

        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "3.14E0", "nan()", "-1", "3").evaluate())
                .hasMessage("first bound must be finite");

        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "3.14E0", "-1", "nan()", "3").evaluate())
                .hasMessage("second bound must be finite");

        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "3.14E0", "infinity()", "-1", "3").evaluate())
                .hasMessage("first bound must be finite");

        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "3.14E0", "-1", "infinity()", "3").evaluate())
                .hasMessage("second bound must be finite");
    }

    @Test
    public void testWidthBucketOverflowAscending()
    {
        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "infinity()", "0", "4", Long.toString(Long.MAX_VALUE)).evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Bucket for value Infinity is out of range");

        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "CAST(infinity() as REAL)", "0", "4", Long.toString(Long.MAX_VALUE)).evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Bucket for value Infinity is out of range");
    }

    @Test
    public void testWidthBucketOverflowDescending()
    {
        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "infinity()", "4", "0", Long.toString(Long.MAX_VALUE)).evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Bucket for value Infinity is out of range");

        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "CAST(infinity() as REAL)", "4", "0", Long.toString(Long.MAX_VALUE)).evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Bucket for value Infinity is out of range");
    }

    @Test
    public void testWidthBucketArray()
    {
        assertThat(assertions.function("width_bucket", "3.14E0", "array[0.0E0, 2.0E0, 4.0E0]"))
                .isEqualTo(2L);

        assertThat(assertions.function("width_bucket", "infinity()", "array[0.0E0, 2.0E0, 4.0E0]"))
                .isEqualTo(3L);

        assertThat(assertions.function("width_bucket", "-1", "array[0.0E0, 1.2E0, 3.3E0, 4.5E0]"))
                .isEqualTo(0L);

        // edge case of only a single bin
        assertThat(assertions.function("width_bucket", "3.145E0", "array[0.0E0]"))
                .isEqualTo(1L);

        assertThat(assertions.function("width_bucket", "-3.145E0", "array[0.0E0]"))
                .isEqualTo(0L);

        // failure modes
        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "3.14E0", "array[]").evaluate())
                .hasMessage("Bins cannot be an empty array");

        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "nan()", "array[1.0E0, 2.0E0, 3.0E0]").evaluate())
                .hasMessage("Operand cannot be NaN");

        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "3.14E0", "array[0.0E0, infinity()]").evaluate())
                .hasMessage("Bin value must be finite, got Infinity");

        // fail if we aren't sorted
        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "3.145E0", "array[1.0E0, 0.0E0]").evaluate())
                .hasMessage("Bin values are not sorted in ascending order");

        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "3.145E0", "array[1.0E0, 0.0E0, -1.0E0]").evaluate())
                .hasMessage("Bin values are not sorted in ascending order");

        assertTrinoExceptionThrownBy(() -> assertions.function("width_bucket", "3.145E0", "array[1.0E0, 0.3E0, 0.0E0, -1.0E0]").evaluate())
                .hasMessage("Bin values are not sorted in ascending order");

        // this is a case that we can't catch because we are using binary search to bisect the bins array
        assertThat(assertions.function("width_bucket", "1.5E0", "array[1.0E0, 2.3E0, 2.0E0]"))
                .isEqualTo(1L);
    }

    @Test
    public void testCosineSimilarity()
    {
        assertThat(assertions.function("cosine_similarity", "map(ARRAY['a', 'b'], ARRAY[1.0E0, 2.0E0])", "map(ARRAY['c', 'b'], ARRAY[1.0E0, 3.0E0])"))
                .isEqualTo(2 * 3 / (Math.sqrt(5) * Math.sqrt(10)));

        assertThat(assertions.function("cosine_similarity", "map(ARRAY['a', 'b', 'c'], ARRAY[1.0E0, 2.0E0, -1.0E0])", "map(ARRAY['c', 'b'], ARRAY[1.0E0, 3.0E0])"))
                .isEqualTo((2 * 3 + (-1) * 1) / (Math.sqrt(1 + 4 + 1) * Math.sqrt(1 + 9)));

        assertThat(assertions.function("cosine_similarity", "map(ARRAY['a', 'b', 'c'], ARRAY[1.0E0, 2.0E0, -1.0E0])", "map(ARRAY['d', 'e'], ARRAY[1.0E0, 3.0E0])"))
                .isEqualTo(0.0);

        assertThat(assertions.function("cosine_similarity", "null", "map(ARRAY['c', 'b'], ARRAY[1.0E0, 3.0E0])"))
                .isNull(DOUBLE);

        assertThat(assertions.function("cosine_similarity", "map(ARRAY['a', 'b'], ARRAY[1.0E0, null])", "map(ARRAY['c', 'b'], ARRAY[1.0E0, 3.0E0])"))
                .isNull(DOUBLE);
    }

    @Test
    public void testInverseNormalCdf()
    {
        assertThat(assertions.function("inverse_normal_cdf", "0", "1", "0.3"))
                .isEqualTo(-0.52440051270804089);

        assertThat(assertions.function("inverse_normal_cdf", "10", "9", "0.9"))
                .isEqualTo(21.533964089901406);

        assertThat(assertions.function("inverse_normal_cdf", "0.5", "0.25", "0.65"))
                .isEqualTo(0.59633011660189195);

        assertTrinoExceptionThrownBy(() -> assertions.function("inverse_normal_cdf", "4", "48", "0").evaluate())
                .hasMessage("p must be 0 > p > 1");

        assertTrinoExceptionThrownBy(() -> assertions.function("inverse_normal_cdf", "4", "48", "1").evaluate())
                .hasMessage("p must be 0 > p > 1");

        assertTrinoExceptionThrownBy(() -> assertions.function("inverse_normal_cdf", "4", "0", "0.4").evaluate())
                .hasMessage("sd must be > 0");
    }

    @Test
    public void testNormalCdf()
    {
        assertThat(assertions.function("normal_cdf", "0", "1", "1.96"))
                .isEqualTo(0.9750021048517796);

        assertThat(assertions.function("normal_cdf", "10", "9", "10"))
                .isEqualTo(0.5);

        assertThat(assertions.function("normal_cdf", "-1.5", "2.1", "-7.8"))
                .isEqualTo(0.0013498980316301035);

        assertThat(assertions.function("normal_cdf", "0", "1", "infinity()"))
                .isEqualTo(1.0);

        assertThat(assertions.function("normal_cdf", "0", "1", "-infinity()"))
                .isEqualTo(0.0);

        assertThat(assertions.function("normal_cdf", "infinity()", "1", "0"))
                .isEqualTo(0.0);

        assertThat(assertions.function("normal_cdf", "-infinity()", "1", "0"))
                .isEqualTo(1.0);

        assertThat(assertions.function("normal_cdf", "0", "infinity()", "0"))
                .isEqualTo(0.5);

        assertThat(assertions.function("normal_cdf", "nan()", "1", "0"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.function("normal_cdf", "0", "1", "nan()"))
                .isEqualTo(Double.NaN);

        assertTrinoExceptionThrownBy(() -> assertions.function("normal_cdf", "0", "0", "0.1985").evaluate())
                .hasMessage("standardDeviation must be > 0");

        assertTrinoExceptionThrownBy(() -> assertions.function("normal_cdf", "0", "nan()", "0.1985").evaluate())
                .hasMessage("standardDeviation must be > 0");
    }

    @Test
    public void testInverseBetaCdf()
    {
        assertThat(assertions.function("inverse_beta_cdf", "3", "3.6", "0.0"))
                .isEqualTo(0.0);

        assertThat(assertions.function("inverse_beta_cdf", "3", "3.6", "1.0"))
                .isEqualTo(1.0);

        assertThat(assertions.function("inverse_beta_cdf", "3", "3.6", "0.3"))
                .isEqualTo(0.3469675485440618);

        assertThat(assertions.function("inverse_beta_cdf", "3", "3.6", "0.95"))
                .isEqualTo(0.7600272463100223);

        assertTrinoExceptionThrownBy(() -> assertions.function("inverse_beta_cdf", "0", "3", "0.5").evaluate())
                .hasMessage("a, b must be > 0");

        assertTrinoExceptionThrownBy(() -> assertions.function("inverse_beta_cdf", "3", "0", "0.5").evaluate())
                .hasMessage("a, b must be > 0");

        assertTrinoExceptionThrownBy(() -> assertions.function("inverse_beta_cdf", "3", "5", "-0.1").evaluate())
                .hasMessage("p must be 0 >= p >= 1");

        assertTrinoExceptionThrownBy(() -> assertions.function("inverse_beta_cdf", "3", "5", "1.1").evaluate())
                .hasMessage("p must be 0 >= p >= 1");
    }

    @Test
    public void testBetaCdf()
    {
        assertThat(assertions.function("beta_cdf", "3", "3.6", "0.0"))
                .isEqualTo(0.0);

        assertThat(assertions.function("beta_cdf", "3", "3.6", "1.0"))
                .isEqualTo(1.0);

        assertThat(assertions.function("beta_cdf", "3", "3.6", "0.3"))
                .isEqualTo(0.21764809997679938);

        assertThat(assertions.function("beta_cdf", "3", "3.6", "0.9"))
                .isEqualTo(0.9972502881611551);

        assertTrinoExceptionThrownBy(() -> assertions.function("beta_cdf", "0", "3", "0.5").evaluate())
                .hasMessage("a, b must be > 0");

        assertTrinoExceptionThrownBy(() -> assertions.function("beta_cdf", "3", "0", "0.5").evaluate())
                .hasMessage("a, b must be > 0");

        assertTrinoExceptionThrownBy(() -> assertions.function("beta_cdf", "3", "5", "-0.1").evaluate())
                .hasMessage("value must be 0 >= v >= 1");

        assertTrinoExceptionThrownBy(() -> assertions.function("beta_cdf", "3", "5", "1.1").evaluate())
                .hasMessage("value must be 0 >= v >= 1");
    }

    @Test
    public void testWilsonInterval()
    {
        assertTrinoExceptionThrownBy(() -> assertions.function("wilson_interval_lower", "-1", "100", "2.575").evaluate())
                .hasMessage("number of successes must not be negative");

        assertTrinoExceptionThrownBy(() -> assertions.function("wilson_interval_lower", "0", "0", "2.575").evaluate())
                .hasMessage("number of trials must be positive");

        assertTrinoExceptionThrownBy(() -> assertions.function("wilson_interval_lower", "10", "5", "2.575").evaluate())
                .hasMessage("number of successes must not be larger than number of trials");

        assertTrinoExceptionThrownBy(() -> assertions.function("wilson_interval_lower", "0", "100", "-1").evaluate())
                .hasMessage("z-score must not be negative");

        assertThat(assertions.function("wilson_interval_lower", "1250", "1310", "1.96e0"))
                .isEqualTo(0.9414883725395894);

        assertTrinoExceptionThrownBy(() -> assertions.function("wilson_interval_upper", "-1", "100", "2.575").evaluate())
                .hasMessage("number of successes must not be negative");

        assertTrinoExceptionThrownBy(() -> assertions.function("wilson_interval_upper", "0", "0", "2.575").evaluate())
                .hasMessage("number of trials must be positive");

        assertTrinoExceptionThrownBy(() -> assertions.function("wilson_interval_upper", "10", "5", "2.575").evaluate())
                .hasMessage("number of successes must not be larger than number of trials");

        assertTrinoExceptionThrownBy(() -> assertions.function("wilson_interval_upper", "0", "100", "-1").evaluate())
                .hasMessage("z-score must not be negative");

        assertThat(assertions.function("wilson_interval_upper", "1250", "1310", "1.96e0"))
                .isEqualTo(0.9642524717143908);
    }
}
