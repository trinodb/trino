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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.VarcharType;
import org.testng.annotations.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
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
import static java.lang.String.format;
import static java.util.Collections.nCopies;

public class TestMathFunctions
        extends AbstractTestFunctions
{
    private static final double[] DOUBLE_VALUES = {123, -123, 123.45, -123.45, 0};
    private static final int[] intLefts = {9, 10, 11, -9, -10, -11, 0};
    private static final int[] intRights = {3, -3};
    private static final double[] doubleLefts = {9, 10, 11, -9, -10, -11, 9.1, 10.1, 11.1, -9.1, -10.1, -11.1};
    private static final double[] doubleRights = {3, -3, 3.1, -3.1};
    private static final double GREATEST_DOUBLE_LESS_THAN_HALF = 0x1.fffffffffffffp-2;

    @Test
    public void testAbs()
    {
        assertFunction("abs(TINYINT'123')", TINYINT, (byte) 123);
        assertFunction("abs(TINYINT'-123')", TINYINT, (byte) 123);
        assertFunction("abs(CAST(NULL AS TINYINT))", TINYINT, null);
        assertFunction("abs(SMALLINT'123')", SMALLINT, (short) 123);
        assertFunction("abs(SMALLINT'-123')", SMALLINT, (short) 123);
        assertFunction("abs(CAST(NULL AS SMALLINT))", SMALLINT, null);
        assertFunction("abs(123)", INTEGER, 123);
        assertFunction("abs(-123)", INTEGER, 123);
        assertFunction("abs(CAST(NULL AS INTEGER))", INTEGER, null);
        assertFunction("abs(BIGINT '123')", BIGINT, 123L);
        assertFunction("abs(BIGINT '-123')", BIGINT, 123L);
        assertFunction("abs(12300000000)", BIGINT, 12300000000L);
        assertFunction("abs(-12300000000)", BIGINT, 12300000000L);
        assertFunction("abs(CAST(NULL AS BIGINT))", BIGINT, null);
        assertFunction("abs(123.0E0)", DOUBLE, 123.0);
        assertFunction("abs(-123.0E0)", DOUBLE, 123.0);
        assertFunction("abs(123.45E0)", DOUBLE, 123.45);
        assertFunction("abs(-123.45E0)", DOUBLE, 123.45);
        assertFunction("abs(CAST(NULL AS DOUBLE))", DOUBLE, null);
        assertFunction("abs(REAL '-754.1985')", REAL, 754.1985f);
        assertInvalidFunction("abs(TINYINT'" + Byte.MIN_VALUE + "')", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("abs(SMALLINT'" + Short.MIN_VALUE + "')", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("abs(INTEGER'" + Integer.MIN_VALUE + "')", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("abs(-9223372036854775807 - if(rand() < 10, 1, 1))", NUMERIC_VALUE_OUT_OF_RANGE);
        assertFunction("abs(DECIMAL '123.45')", createDecimalType(5, 2), decimal("12345", createDecimalType(5, 2)));
        assertFunction("abs(DECIMAL '-123.45')", createDecimalType(5, 2), decimal("12345", createDecimalType(5, 2)));
        assertFunction("abs(DECIMAL '1234567890123456.78')", createDecimalType(18, 2), decimal("123456789012345678", createDecimalType(18, 2)));
        assertFunction("abs(DECIMAL '-1234567890123456.78')", createDecimalType(18, 2), decimal("123456789012345678", createDecimalType(18, 2)));
        assertFunction("abs(DECIMAL '12345678901234560.78')", createDecimalType(19, 2), decimal("1234567890123456078", createDecimalType(19, 2)));
        assertFunction("abs(DECIMAL '-12345678901234560.78')", createDecimalType(19, 2), decimal("1234567890123456078", createDecimalType(19, 2)));
        assertFunction("abs(CAST(NULL AS DECIMAL(1,0)))", createDecimalType(1, 0), null);
    }

    @Test
    public void testAcos()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("acos(" + doubleValue + ")", DOUBLE, Math.acos(doubleValue));
            assertFunction("acos(REAL '" + (float) doubleValue + "')", DOUBLE, Math.acos((float) doubleValue));
        }
        assertFunction("acos(NULL)", DOUBLE, null);
    }

    @Test
    public void testAsin()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("asin(" + doubleValue + ")", DOUBLE, Math.asin(doubleValue));
            assertFunction("asin(REAL '" + (float) doubleValue + "')", DOUBLE, Math.asin((float) doubleValue));
        }
        assertFunction("asin(NULL)", DOUBLE, null);
    }

    @Test
    public void testAtan()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("atan(" + doubleValue + ")", DOUBLE, Math.atan(doubleValue));
            assertFunction("atan(REAL '" + (float) doubleValue + "')", DOUBLE, Math.atan((float) doubleValue));
        }
        assertFunction("atan(NULL)", DOUBLE, null);
    }

    @Test
    public void testAtan2()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("atan2(" + doubleValue + ", " + doubleValue + ")", DOUBLE, Math.atan2(doubleValue, doubleValue));
            assertFunction("atan2(REAL '" + (float) doubleValue + "', REAL '" + (float) doubleValue + "')", DOUBLE, Math.atan2((float) doubleValue, (float) doubleValue));
        }
        assertFunction("atan2(NULL, NULL)", DOUBLE, null);
        assertFunction("atan2(1.0E0, NULL)", DOUBLE, null);
        assertFunction("atan2(NULL, 1.0E0)", DOUBLE, null);
    }

    @Test
    public void testCbrt()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cbrt(" + doubleValue + ")", DOUBLE, Math.cbrt(doubleValue));
            assertFunction("cbrt(REAL '" + (float) doubleValue + "')", DOUBLE, Math.cbrt((float) doubleValue));
        }
        assertFunction("cbrt(NULL)", DOUBLE, null);
    }

    @Test
    public void testCeil()
    {
        assertFunction("ceil(TINYINT'123')", TINYINT, (byte) 123);
        assertFunction("ceil(TINYINT'-123')", TINYINT, (byte) -123);
        assertFunction("ceil(CAST(NULL AS TINYINT))", TINYINT, null);
        assertFunction("ceil(SMALLINT'123')", SMALLINT, (short) 123);
        assertFunction("ceil(SMALLINT'-123')", SMALLINT, (short) -123);
        assertFunction("ceil(CAST(NULL AS SMALLINT))", SMALLINT, null);
        assertFunction("ceil(123)", INTEGER, 123);
        assertFunction("ceil(-123)", INTEGER, -123);
        assertFunction("ceil(CAST(NULL AS INTEGER))", INTEGER, null);
        assertFunction("ceil(BIGINT '123')", BIGINT, 123L);
        assertFunction("ceil(BIGINT '-123')", BIGINT, -123L);
        assertFunction("ceil(12300000000)", BIGINT, 12300000000L);
        assertFunction("ceil(-12300000000)", BIGINT, -12300000000L);
        assertFunction("ceil(CAST(NULL as BIGINT))", BIGINT, null);
        assertFunction("ceil(123.0E0)", DOUBLE, 123.0);
        assertFunction("ceil(-123.0E0)", DOUBLE, -123.0);
        assertFunction("ceil(123.45E0)", DOUBLE, 124.0);
        assertFunction("ceil(-123.45E0)", DOUBLE, -123.0);
        assertFunction("ceil(CAST(NULL as DOUBLE))", DOUBLE, null);
        assertFunction("ceil(REAL '123.0')", REAL, 123.0f);
        assertFunction("ceil(REAL '-123.0')", REAL, -123.0f);
        assertFunction("ceil(REAL '123.45')", REAL, 124.0f);
        assertFunction("ceil(REAL '-123.45')", REAL, -123.0f);

        assertFunction("ceiling(12300000000)", BIGINT, 12300000000L);
        assertFunction("ceiling(-12300000000)", BIGINT, -12300000000L);
        assertFunction("ceiling(CAST(NULL AS BIGINT))", BIGINT, null);
        assertFunction("ceiling(123.0E0)", DOUBLE, 123.0);
        assertFunction("ceiling(-123.0E0)", DOUBLE, -123.0);
        assertFunction("ceiling(123.45E0)", DOUBLE, 124.0);
        assertFunction("ceiling(-123.45E0)", DOUBLE, -123.0);
        assertFunction("ceiling(REAL '123.0')", REAL, 123.0f);
        assertFunction("ceiling(REAL '-123.0')", REAL, -123.0f);
        assertFunction("ceiling(REAL '123.45')", REAL, 124.0f);
        assertFunction("ceiling(REAL '-123.45')", REAL, -123.0f);

        // short DECIMAL -> short DECIMAL
        assertFunction("ceiling(DECIMAL '0')", createDecimalType(1), decimal("0", createDecimalType(1)));
        assertFunction("ceiling(CAST(DECIMAL '0.00' AS DECIMAL(3,2)))", createDecimalType(2), decimal("0", createDecimalType(2)));
        assertFunction("ceiling(CAST(DECIMAL '0.00' AS DECIMAL(3,2)))", createDecimalType(2), decimal("0", createDecimalType(2)));
        assertFunction("ceiling(CAST(DECIMAL '0.01' AS DECIMAL(3,2)))", createDecimalType(2), decimal("1", createDecimalType(2)));
        assertFunction("ceiling(CAST(DECIMAL '-0.01' AS DECIMAL(3,2)))", createDecimalType(2), decimal("0", createDecimalType(2)));
        assertFunction("ceiling(CAST(DECIMAL '0.49' AS DECIMAL(3,2)))", createDecimalType(2), decimal("1", createDecimalType(2)));
        assertFunction("ceiling(CAST(DECIMAL '-0.49' AS DECIMAL(3,2)))", createDecimalType(2), decimal("0", createDecimalType(2)));
        assertFunction("ceiling(CAST(DECIMAL '0.50' AS DECIMAL(3,2)))", createDecimalType(2), decimal("1", createDecimalType(2)));
        assertFunction("ceiling(CAST(DECIMAL '-0.50' AS DECIMAL(3,2)))", createDecimalType(2), decimal("0", createDecimalType(2)));
        assertFunction("ceiling(CAST(DECIMAL '0.99' AS DECIMAL(3,2)))", createDecimalType(2), decimal("1", createDecimalType(2)));
        assertFunction("ceiling(CAST(DECIMAL '-0.99' AS DECIMAL(3,2)))", createDecimalType(2), decimal("0", createDecimalType(2)));
        assertFunction("ceiling(DECIMAL '123')", createDecimalType(3), decimal("123", createDecimalType(3)));
        assertFunction("ceiling(DECIMAL '-123')", createDecimalType(3), decimal("-123", createDecimalType(3)));
        assertFunction("ceiling(DECIMAL '123.00')", createDecimalType(4), decimal("123", createDecimalType(4)));
        assertFunction("ceiling(DECIMAL '-123.00')", createDecimalType(4), decimal("-123", createDecimalType(4)));
        assertFunction("ceiling(DECIMAL '123.01')", createDecimalType(4), decimal("124", createDecimalType(4)));
        assertFunction("ceiling(DECIMAL '-123.01')", createDecimalType(4), decimal("-123", createDecimalType(4)));
        assertFunction("ceiling(DECIMAL '123.45')", createDecimalType(4), decimal("124", createDecimalType(4)));
        assertFunction("ceiling(DECIMAL '-123.45')", createDecimalType(4), decimal("-123", createDecimalType(4)));
        assertFunction("ceiling(DECIMAL '123.49')", createDecimalType(4), decimal("124", createDecimalType(4)));
        assertFunction("ceiling(DECIMAL '-123.49')", createDecimalType(4), decimal("-123", createDecimalType(4)));
        assertFunction("ceiling(DECIMAL '123.50')", createDecimalType(4), decimal("124", createDecimalType(4)));
        assertFunction("ceiling(DECIMAL '-123.50')", createDecimalType(4), decimal("-123", createDecimalType(4)));
        assertFunction("ceiling(DECIMAL '123.99')", createDecimalType(4), decimal("124", createDecimalType(4)));
        assertFunction("ceiling(DECIMAL '-123.99')", createDecimalType(4), decimal("-123", createDecimalType(4)));
        assertFunction("ceiling(DECIMAL '999.9')", createDecimalType(4), decimal("1000", createDecimalType(4)));

        // long DECIMAL -> long DECIMAL
        assertFunction("ceiling(CAST(DECIMAL '0000000000000000000' AS DECIMAL(19,0)))", createDecimalType(19), decimal("0", createDecimalType(19)));
        assertFunction("ceiling(CAST(DECIMAL '000000000000000000.00' AS DECIMAL(20,2)))", createDecimalType(19), decimal("0", createDecimalType(19)));
        assertFunction("ceiling(CAST(DECIMAL '000000000000000000.01' AS DECIMAL(20,2)))", createDecimalType(19), decimal("1", createDecimalType(19)));
        assertFunction("ceiling(CAST(DECIMAL '-000000000000000000.01' AS DECIMAL(20,2)))", createDecimalType(19), decimal("0", createDecimalType(19)));
        assertFunction("ceiling(CAST(DECIMAL '000000000000000000.49' AS DECIMAL(20,2)))", createDecimalType(19), decimal("1", createDecimalType(19)));
        assertFunction("ceiling(CAST(DECIMAL '-000000000000000000.49' AS DECIMAL(20,2)))", createDecimalType(19), decimal("0", createDecimalType(19)));
        assertFunction("ceiling(CAST(DECIMAL '000000000000000000.50' AS DECIMAL(20,2)))", createDecimalType(19), decimal("1", createDecimalType(19)));
        assertFunction("ceiling(CAST(DECIMAL '-000000000000000000.50' AS DECIMAL(20,2)))", createDecimalType(19), decimal("0", createDecimalType(19)));
        assertFunction("ceiling(CAST(DECIMAL '000000000000000000.99' AS DECIMAL(20,2)))", createDecimalType(19), decimal("1", createDecimalType(19)));
        assertFunction("ceiling(CAST(DECIMAL '-000000000000000000.99' AS DECIMAL(20,2)))", createDecimalType(19), decimal("0", createDecimalType(19)));
        assertFunction("ceiling(DECIMAL '123456789012345678')", createDecimalType(18), decimal("123456789012345678", createDecimalType(18)));
        assertFunction("ceiling(DECIMAL '-123456789012345678')", createDecimalType(18), decimal("-123456789012345678", createDecimalType(18)));
        assertFunction("ceiling(DECIMAL '123456789012345678.00')", createDecimalType(19), decimal("123456789012345678", createDecimalType(19)));
        assertFunction("ceiling(DECIMAL '-123456789012345678.00')", createDecimalType(19), decimal("-123456789012345678", createDecimalType(19)));
        assertFunction("ceiling(DECIMAL '123456789012345678.01')", createDecimalType(19), decimal("123456789012345679", createDecimalType(19)));
        assertFunction("ceiling(DECIMAL '-123456789012345678.01')", createDecimalType(19), decimal("-123456789012345678", createDecimalType(19)));
        assertFunction("ceiling(DECIMAL '123456789012345678.99')", createDecimalType(19), decimal("123456789012345679", createDecimalType(19)));
        assertFunction("ceiling(DECIMAL '-123456789012345678.99')", createDecimalType(19), decimal("-123456789012345678", createDecimalType(19)));
        assertFunction("ceiling(DECIMAL '123456789012345678.49')", createDecimalType(19), decimal("123456789012345679", createDecimalType(19)));
        assertFunction("ceiling(DECIMAL '-123456789012345678.49')", createDecimalType(19), decimal("-123456789012345678", createDecimalType(19)));
        assertFunction("ceiling(DECIMAL '123456789012345678.50')", createDecimalType(19), decimal("123456789012345679", createDecimalType(19)));
        assertFunction("ceiling(DECIMAL '-123456789012345678.50')", createDecimalType(19), decimal("-123456789012345678", createDecimalType(19)));
        assertFunction("ceiling(DECIMAL '999999999999999999.9')", createDecimalType(19), decimal("1000000000000000000", createDecimalType(19)));

        // long DECIMAL -> short DECIMAL
        assertFunction("ceiling(DECIMAL '1234567890123456.78')", createDecimalType(17), decimal("1234567890123457", createDecimalType(17)));
        assertFunction("ceiling(DECIMAL '-1234567890123456.78')", createDecimalType(17), decimal("-1234567890123456", createDecimalType(17)));

        assertFunction("ceiling(CAST(NULL AS DOUBLE))", DOUBLE, null);
        assertFunction("ceiling(CAST(NULL AS REAL))", REAL, null);
        assertFunction("ceiling(CAST(NULL AS DECIMAL(1,0)))", createDecimalType(1), null);
        assertFunction("ceiling(CAST(NULL AS DECIMAL(25,5)))", createDecimalType(21), null);
    }

    @Test
    public void testTruncate()
    {
        // DOUBLE
        String maxDouble = Double.toString(Double.MAX_VALUE);
        String minDouble = Double.toString(-Double.MAX_VALUE);
        assertFunction("truncate(17.18E0)", DOUBLE, 17.0);
        assertFunction("truncate(-17.18E0)", DOUBLE, -17.0);
        assertFunction("truncate(17.88E0)", DOUBLE, 17.0);
        assertFunction("truncate(-17.88E0)", DOUBLE, -17.0);
        assertFunction("truncate(REAL '17.18')", REAL, 17.0f);
        assertFunction("truncate(REAL '-17.18')", REAL, -17.0f);
        assertFunction("truncate(REAL '17.88')", REAL, 17.0f);
        assertFunction("truncate(REAL '-17.88')", REAL, -17.0f);
        assertFunction("truncate(DOUBLE '" + maxDouble + "')", DOUBLE, Double.MAX_VALUE);
        assertFunction("truncate(DOUBLE '" + minDouble + "')", DOUBLE, -Double.MAX_VALUE);

        // TRUNCATE short DECIMAL -> short DECIMAL
        assertFunction("truncate(DECIMAL '1234')", createDecimalType(4, 0), decimal("1234", createDecimalType(4)));
        assertFunction("truncate(DECIMAL '-1234')", createDecimalType(4, 0), decimal("-1234", createDecimalType(4)));
        assertFunction("truncate(DECIMAL '1234.56')", createDecimalType(4, 0), decimal("1234", createDecimalType(4)));
        assertFunction("truncate(DECIMAL '-1234.56')", createDecimalType(4, 0), decimal("-1234", createDecimalType(4)));
        assertFunction("truncate(DECIMAL '123456789123456.999')", createDecimalType(15, 0), decimal("123456789123456", createDecimalType(15)));
        assertFunction("truncate(DECIMAL '-123456789123456.999')", createDecimalType(15, 0), decimal("-123456789123456", createDecimalType(15)));

        // TRUNCATE long DECIMAL -> short DECIMAL
        assertFunction("truncate(DECIMAL '1.99999999999999999999999999')", createDecimalType(1, 0), decimal("1", createDecimalType(1)));
        assertFunction("truncate(DECIMAL '-1.99999999999999999999999999')", createDecimalType(1, 0), decimal("-1", createDecimalType(1)));

        // TRUNCATE long DECIMAL -> long DECIMAL
        assertFunction("truncate(DECIMAL '1234567890123456789012')", createDecimalType(22, 0), decimal("1234567890123456789012", createDecimalType(22)));
        assertFunction("truncate(DECIMAL '-1234567890123456789012')", createDecimalType(22, 0), decimal("-1234567890123456789012", createDecimalType(22)));
        assertFunction("truncate(DECIMAL '1234567890123456789012.999')", createDecimalType(22, 0), decimal("1234567890123456789012", createDecimalType(22)));
        assertFunction("truncate(DECIMAL '-1234567890123456789012.999')", createDecimalType(22, 0), decimal("-1234567890123456789012", createDecimalType(22)));

        // TRUNCATE_N short DECIMAL -> short DECIMAL
        assertFunction("truncate(DECIMAL '1234', 1)", createDecimalType(4, 0), decimal("1234", createDecimalType(4)));
        assertFunction("truncate(DECIMAL '1234', -1)", createDecimalType(4, 0), decimal("1230", createDecimalType(4)));
        assertFunction("truncate(DECIMAL '1234.56', 1)", createDecimalType(6, 2), decimal("1234.50", createDecimalType(6, 2)));
        assertFunction("truncate(DECIMAL '1234.56', -1)", createDecimalType(6, 2), decimal("1230.00", createDecimalType(6, 2)));
        assertFunction("truncate(DECIMAL '-1234.56', 1)", createDecimalType(6, 2), decimal("-1234.50", createDecimalType(6, 2)));
        assertFunction("truncate(DECIMAL '1239.99', 1)", createDecimalType(6, 2), decimal("1239.90", createDecimalType(6, 2)));
        assertFunction("truncate(DECIMAL '-1239.99', 1)", createDecimalType(6, 2), decimal("-1239.90", createDecimalType(6, 2)));
        assertFunction("truncate(DECIMAL '1239.999', 2)", createDecimalType(7, 3), decimal("1239.990", createDecimalType(7, 3)));
        assertFunction("truncate(DECIMAL '1239.999', -2)", createDecimalType(7, 3), decimal("1200.000", createDecimalType(7, 3)));
        assertFunction("truncate(DECIMAL '123456789123456.999', 2)", createDecimalType(18, 3), decimal("123456789123456.990", createDecimalType(18, 3)));
        assertFunction("truncate(DECIMAL '123456789123456.999', -2)", createDecimalType(18, 3), decimal("123456789123400.000", createDecimalType(18, 3)));

        assertFunction("truncate(DECIMAL '1234', -4)", createDecimalType(4, 0), decimal("0000", createDecimalType(4)));
        assertFunction("truncate(DECIMAL '1234.56', -4)", createDecimalType(6, 2), decimal("0000.00", createDecimalType(6, 2)));
        assertFunction("truncate(DECIMAL '-1234.56', -4)", createDecimalType(6, 2), decimal("0000.00", createDecimalType(6, 2)));
        assertFunction("truncate(DECIMAL '1234.56', 3)", createDecimalType(6, 2), decimal("1234.56", createDecimalType(6, 2)));
        assertFunction("truncate(DECIMAL '-1234.56', 3)", createDecimalType(6, 2), decimal("-1234.56", createDecimalType(6, 2)));

        // TRUNCATE_N long DECIMAL -> long DECIMAL
        assertFunction("truncate(DECIMAL '1234567890123456789012', 1)", createDecimalType(22, 0), decimal("1234567890123456789012", createDecimalType(22)));
        assertFunction("truncate(DECIMAL '1234567890123456789012', -1)", createDecimalType(22, 0), decimal("1234567890123456789010", createDecimalType(22)));
        assertFunction("truncate(DECIMAL '1234567890123456789012.23', 1)", createDecimalType(24, 2), decimal("1234567890123456789012.20", createDecimalType(24, 2)));
        assertFunction("truncate(DECIMAL '1234567890123456789012.23', -1)", createDecimalType(24, 2), decimal("1234567890123456789010.00", createDecimalType(24, 2)));
        assertFunction("truncate(DECIMAL '123456789012345678999.99', -1)", createDecimalType(23, 2), decimal("123456789012345678990.00", createDecimalType(23, 2)));
        assertFunction("truncate(DECIMAL '-123456789012345678999.99', -1)", createDecimalType(23, 2), decimal("-123456789012345678990.00", createDecimalType(23, 2)));
        assertFunction("truncate(DECIMAL '123456789012345678999.999', 2)", createDecimalType(24, 3), decimal("123456789012345678999.990", createDecimalType(24, 3)));
        assertFunction("truncate(DECIMAL '123456789012345678999.999', -2)", createDecimalType(24, 3), decimal("123456789012345678900.000", createDecimalType(24, 3)));
        assertFunction("truncate(DECIMAL '123456789012345678901', -21)", createDecimalType(21, 0), decimal("000000000000000000000", createDecimalType(21)));
        assertFunction("truncate(DECIMAL '123456789012345678901.23', -21)", createDecimalType(23, 2), decimal("000000000000000000000.00", createDecimalType(23, 2)));
        assertFunction("truncate(DECIMAL '123456789012345678901.23', 3)", createDecimalType(23, 2), decimal("123456789012345678901.23", createDecimalType(23, 2)));
        assertFunction("truncate(DECIMAL '-123456789012345678901.23', 3)", createDecimalType(23, 2), decimal("-123456789012345678901.23", createDecimalType(23, 2)));

        // NULL
        assertFunction("truncate(CAST(NULL AS DOUBLE))", DOUBLE, null);
        assertFunction("truncate(CAST(NULL AS DECIMAL(1,0)), -1)", createDecimalType(1, 0), null);
        assertFunction("truncate(CAST(NULL AS DECIMAL(1,0)))", createDecimalType(1, 0), null);
        assertFunction("truncate(CAST(NULL AS DECIMAL(18,5)))", createDecimalType(13, 0), null);
        assertFunction("truncate(CAST(NULL AS DECIMAL(25,2)))", createDecimalType(23, 0), null);
        assertFunction("truncate(NULL, NULL)", createDecimalType(1, 0), null);
    }

    @Test
    public void testCos()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cos(" + doubleValue + ")", DOUBLE, Math.cos(doubleValue));
            assertFunction("cos(REAL '" + (float) doubleValue + "')", DOUBLE, Math.cos((float) doubleValue));
        }
        assertFunction("cos(NULL)", DOUBLE, null);
    }

    @Test
    public void testCosh()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cosh(" + doubleValue + ")", DOUBLE, Math.cosh(doubleValue));
            assertFunction("cosh(REAL '" + (float) doubleValue + "')", DOUBLE, Math.cosh((float) doubleValue));
        }
        assertFunction("cosh(NULL)", DOUBLE, null);
    }

    @Test
    public void testDegrees()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction(format("degrees(%s)", doubleValue), DOUBLE, Math.toDegrees(doubleValue));
            assertFunction(format("degrees(REAL '%s')", (float) doubleValue), DOUBLE, Math.toDegrees((float) doubleValue));
        }
        assertFunction("degrees(NULL)", DOUBLE, null);
    }

    @Test
    public void testE()
    {
        assertFunction("e()", DOUBLE, Math.E);
    }

    @Test
    public void testExp()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("exp(" + doubleValue + ")", DOUBLE, Math.exp(doubleValue));
            assertFunction("exp(REAL '" + (float) doubleValue + "')", DOUBLE, Math.exp((float) doubleValue));
        }
        assertFunction("exp(NULL)", DOUBLE, null);
    }

    @Test
    public void testFloor()
    {
        assertFunction("floor(TINYINT'123')", TINYINT, (byte) 123);
        assertFunction("floor(TINYINT'-123')", TINYINT, (byte) -123);
        assertFunction("floor(CAST(NULL AS TINYINT))", TINYINT, null);
        assertFunction("floor(SMALLINT'123')", SMALLINT, (short) 123);
        assertFunction("floor(SMALLINT'-123')", SMALLINT, (short) -123);
        assertFunction("floor(CAST(NULL AS SMALLINT))", SMALLINT, null);
        assertFunction("floor(123)", INTEGER, 123);
        assertFunction("floor(-123)", INTEGER, -123);
        assertFunction("floor(CAST(NULL AS INTEGER))", INTEGER, null);
        assertFunction("floor(BIGINT '123')", BIGINT, 123L);
        assertFunction("floor(BIGINT '-123')", BIGINT, -123L);
        assertFunction("floor(12300000000)", BIGINT, 12300000000L);
        assertFunction("floor(-12300000000)", BIGINT, -12300000000L);
        assertFunction("floor(CAST(NULL as BIGINT))", BIGINT, null);
        assertFunction("floor(123.0E0)", DOUBLE, 123.0);
        assertFunction("floor(-123.0E0)", DOUBLE, -123.0);
        assertFunction("floor(123.45E0)", DOUBLE, 123.0);
        assertFunction("floor(-123.45E0)", DOUBLE, -124.0);

        assertFunction("floor(REAL '123.0')", REAL, 123.0f);
        assertFunction("floor(REAL '-123.0')", REAL, -123.0f);
        assertFunction("floor(REAL '123.45')", REAL, 123.0f);
        assertFunction("floor(REAL '-123.45')", REAL, -124.0f);

        // short DECIMAL -> short DECIMAL
        assertFunction("floor(DECIMAL '0')", createDecimalType(1), decimal("0", createDecimalType(1)));
        assertFunction("floor(CAST(DECIMAL '0.00' AS DECIMAL(3,2)))", createDecimalType(2), decimal("0", createDecimalType(2)));
        assertFunction("floor(CAST(DECIMAL '0.00' AS DECIMAL(3,2)))", createDecimalType(2), decimal("0", createDecimalType(2)));
        assertFunction("floor(CAST(DECIMAL '0.01' AS DECIMAL(3,2)))", createDecimalType(2), decimal("0", createDecimalType(2)));
        assertFunction("floor(CAST(DECIMAL '-0.01' AS DECIMAL(3,2)))", createDecimalType(2), decimal("-1", createDecimalType(2)));
        assertFunction("floor(CAST(DECIMAL '0.49' AS DECIMAL(3,2)))", createDecimalType(2), decimal("0", createDecimalType(2)));
        assertFunction("floor(CAST(DECIMAL '-0.49' AS DECIMAL(3,2)))", createDecimalType(2), decimal("-1", createDecimalType(2)));
        assertFunction("floor(CAST(DECIMAL '0.50' AS DECIMAL(3,2)))", createDecimalType(2), decimal("0", createDecimalType(2)));
        assertFunction("floor(CAST(DECIMAL '-0.50' AS DECIMAL(3,2)))", createDecimalType(2), decimal("-1", createDecimalType(2)));
        assertFunction("floor(CAST(DECIMAL '0.99' AS DECIMAL(3,2)))", createDecimalType(2), decimal("0", createDecimalType(2)));
        assertFunction("floor(CAST(DECIMAL '-0.99' AS DECIMAL(3,2)))", createDecimalType(2), decimal("-1", createDecimalType(2)));
        assertFunction("floor(DECIMAL '123')", createDecimalType(3), decimal("123", createDecimalType(3)));
        assertFunction("floor(DECIMAL '-123')", createDecimalType(3), decimal("-123", createDecimalType(3)));
        assertFunction("floor(DECIMAL '123.00')", createDecimalType(4), decimal("123", createDecimalType(4)));
        assertFunction("floor(DECIMAL '-123.00')", createDecimalType(4), decimal("-123", createDecimalType(4)));
        assertFunction("floor(DECIMAL '123.01')", createDecimalType(4), decimal("123", createDecimalType(4)));
        assertFunction("floor(DECIMAL '-123.01')", createDecimalType(4), decimal("-124", createDecimalType(4)));
        assertFunction("floor(DECIMAL '123.45')", createDecimalType(4), decimal("123", createDecimalType(4)));
        assertFunction("floor(DECIMAL '-123.45')", createDecimalType(4), decimal("-124", createDecimalType(4)));
        assertFunction("floor(DECIMAL '123.49')", createDecimalType(4), decimal("123", createDecimalType(4)));
        assertFunction("floor(DECIMAL '-123.49')", createDecimalType(4), decimal("-124", createDecimalType(4)));
        assertFunction("floor(DECIMAL '123.50')", createDecimalType(4), decimal("123", createDecimalType(4)));
        assertFunction("floor(DECIMAL '-123.50')", createDecimalType(4), decimal("-124", createDecimalType(4)));
        assertFunction("floor(DECIMAL '123.99')", createDecimalType(4), decimal("123", createDecimalType(4)));
        assertFunction("floor(DECIMAL '-123.99')", createDecimalType(4), decimal("-124", createDecimalType(4)));
        assertFunction("floor(DECIMAL '-999.9')", createDecimalType(4), decimal("-1000", createDecimalType(4)));

        // long DECIMAL -> long DECIMAL
        assertFunction("floor(CAST(DECIMAL '0000000000000000000' AS DECIMAL(19,0)))", createDecimalType(19), decimal("0", createDecimalType(19)));
        assertFunction("floor(CAST(DECIMAL '000000000000000000.00' AS DECIMAL(20,2)))", createDecimalType(19), decimal("0", createDecimalType(19)));
        assertFunction("floor(CAST(DECIMAL '000000000000000000.01' AS DECIMAL(20,2)))", createDecimalType(19), decimal("0", createDecimalType(19)));
        assertFunction("floor(CAST(DECIMAL '-000000000000000000.01' AS DECIMAL(20,2)))", createDecimalType(19), decimal("-1", createDecimalType(19)));
        assertFunction("floor(CAST(DECIMAL '000000000000000000.49' AS DECIMAL(20,2)))", createDecimalType(19), decimal("0", createDecimalType(19)));
        assertFunction("floor(CAST(DECIMAL '-000000000000000000.49' AS DECIMAL(20,2)))", createDecimalType(19), decimal("-1", createDecimalType(19)));
        assertFunction("floor(CAST(DECIMAL '000000000000000000.50' AS DECIMAL(20,2)))", createDecimalType(19), decimal("0", createDecimalType(19)));
        assertFunction("floor(CAST(DECIMAL '-000000000000000000.50' AS DECIMAL(20,2)))", createDecimalType(19), decimal("-1", createDecimalType(19)));
        assertFunction("floor(CAST(DECIMAL '000000000000000000.99' AS DECIMAL(20,2)))", createDecimalType(19), decimal("0", createDecimalType(19)));
        assertFunction("floor(CAST(DECIMAL '-000000000000000000.99' AS DECIMAL(20,2)))", createDecimalType(19), decimal("-1", createDecimalType(19)));
        assertFunction("floor(DECIMAL '123456789012345678')", createDecimalType(18), decimal("123456789012345678", createDecimalType(18)));
        assertFunction("floor(DECIMAL '-123456789012345678')", createDecimalType(18), decimal("-123456789012345678", createDecimalType(18)));
        assertFunction("floor(DECIMAL '123456789012345678.00')", createDecimalType(19), decimal("123456789012345678", createDecimalType(19)));
        assertFunction("floor(DECIMAL '-123456789012345678.00')", createDecimalType(19), decimal("-123456789012345678", createDecimalType(19)));
        assertFunction("floor(DECIMAL '123456789012345678.01')", createDecimalType(19), decimal("123456789012345678", createDecimalType(19)));
        assertFunction("floor(DECIMAL '-123456789012345678.01')", createDecimalType(19), decimal("-123456789012345679", createDecimalType(19)));
        assertFunction("floor(DECIMAL '123456789012345678.99')", createDecimalType(19), decimal("123456789012345678", createDecimalType(19)));
        assertFunction("floor(DECIMAL '-123456789012345678.49')", createDecimalType(19), decimal("-123456789012345679", createDecimalType(19)));
        assertFunction("floor(DECIMAL '123456789012345678.49')", createDecimalType(19), decimal("123456789012345678", createDecimalType(19)));
        assertFunction("floor(DECIMAL '-123456789012345678.50')", createDecimalType(19), decimal("-123456789012345679", createDecimalType(19)));
        assertFunction("floor(DECIMAL '123456789012345678.50')", createDecimalType(19), decimal("123456789012345678", createDecimalType(19)));
        assertFunction("floor(DECIMAL '-123456789012345678.99')", createDecimalType(19), decimal("-123456789012345679", createDecimalType(19)));
        assertFunction("floor(DECIMAL '-999999999999999999.9')", createDecimalType(19), decimal("-1000000000000000000", createDecimalType(19)));

        // long DECIMAL -> short DECIMAL
        assertFunction("floor(DECIMAL '1234567890123456.78')", createDecimalType(17), decimal("1234567890123456", createDecimalType(17)));
        assertFunction("floor(DECIMAL '-1234567890123456.78')", createDecimalType(17), decimal("-1234567890123457", createDecimalType(17)));

        assertFunction("floor(CAST(NULL as REAL))", REAL, null);
        assertFunction("floor(CAST(NULL as DOUBLE))", DOUBLE, null);
        assertFunction("floor(CAST(NULL as DECIMAL(1,0)))", createDecimalType(1), null);
        assertFunction("floor(CAST(NULL as DECIMAL(25,5)))", createDecimalType(21), null);
    }

    @Test
    public void testLn()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("ln(" + doubleValue + ")", DOUBLE, Math.log(doubleValue));
        }
        assertFunction("ln(NULL)", DOUBLE, null);
    }

    @Test
    public void testLog2()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("log2(" + doubleValue + ")", DOUBLE, Math.log(doubleValue) / Math.log(2));
        }
        assertFunction("log2(NULL)", DOUBLE, null);
    }

    @Test
    public void testLog10()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("log10(" + doubleValue + ")", DOUBLE, Math.log10(doubleValue));
        }
        assertFunction("log10(NULL)", DOUBLE, null);
    }

    @Test
    public void testLog()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            for (double base : DOUBLE_VALUES) {
                assertFunction("log(" + base + ", " + doubleValue + ")", DOUBLE, Math.log(doubleValue) / Math.log(base));
                assertFunction("log(REAL '" + (float) base + "', REAL'" + (float) doubleValue + "')", DOUBLE, Math.log((float) doubleValue) / Math.log((float) base));
            }
        }
        assertFunction("log(NULL, NULL)", DOUBLE, null);
        assertFunction("log(5.0E0, NULL)", DOUBLE, null);
        assertFunction("log(NULL, 5.0E0)", DOUBLE, null);
    }

    @Test
    public void testMod()
    {
        for (int left : intLefts) {
            for (int right : intRights) {
                assertFunction("mod(" + left + ", " + right + ")", INTEGER, (left % right));
            }
        }

        for (int left : intLefts) {
            for (int right : intRights) {
                assertFunction("mod( BIGINT '" + left + "' , BIGINT '" + right + "')", BIGINT, (long) (left % right));
            }
        }

        for (long left : intLefts) {
            for (long right : intRights) {
                assertFunction("mod(" + left * 10000000000L + ", " + right * 10000000000L + ")", BIGINT, (left * 10000000000L) % (right * 10000000000L));
            }
        }

        for (int left : intLefts) {
            for (double right : doubleRights) {
                assertFunction("mod(" + left + ", DOUBLE '" + right + "')", DOUBLE, left % right);
            }
        }

        for (int left : intLefts) {
            for (double right : doubleRights) {
                assertFunction("mod(" + left + ", REAL '" + (float) right + "')", REAL, left % (float) right);
            }
        }

        for (double left : doubleLefts) {
            for (long right : intRights) {
                assertFunction("mod(DOUBLE '" + left + "', " + right + ")", DOUBLE, left % right);
            }
        }

        for (double left : doubleLefts) {
            for (long right : intRights) {
                assertFunction("mod(REAL '" + (float) left + "', " + right + ")", REAL, (float) left % right);
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertFunction("mod(DOUBLE '" + left + "', DOUBLE '" + right + "')", DOUBLE, left % right);
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertFunction("mod(REAL '" + (float) left + "', REAL '" + (float) right + "')", REAL, (float) left % (float) right);
            }
        }

        assertFunction("mod(5.0E0, NULL)", DOUBLE, null);
        assertFunction("mod(NULL, 5.0E0)", DOUBLE, null);

        assertFunction("mod(DECIMAL '0.0', DECIMAL '2.0')", createDecimalType(1, 1), decimal("0", createDecimalType(1, 1)));
        assertFunction("mod(DECIMAL '13.0', DECIMAL '5.0')", createDecimalType(2, 1), decimal("3.0", createDecimalType(2, 1)));
        assertFunction("mod(DECIMAL '-13.0', DECIMAL '5.0')", createDecimalType(2, 1), decimal("-3.0", createDecimalType(2, 1)));
        assertFunction("mod(DECIMAL '13.0', DECIMAL '-5.0')", createDecimalType(2, 1), decimal("3.0", createDecimalType(2, 1)));
        assertFunction("mod(DECIMAL '-13.0', DECIMAL '-5.0')", createDecimalType(2, 1), decimal("-3.0", createDecimalType(2, 1)));
        assertFunction("mod(DECIMAL '5.0', DECIMAL '2.5')", createDecimalType(2, 1), decimal("0.0", createDecimalType(2, 1)));
        assertFunction("mod(DECIMAL '5.0', DECIMAL '2.05')", createDecimalType(3, 2), decimal("0.90", createDecimalType(3, 2)));
        assertFunction("mod(DECIMAL '5.0', DECIMAL '2.55')", createDecimalType(3, 2), decimal("2.45", createDecimalType(3, 2)));
        assertFunction("mod(DECIMAL '5.0001', DECIMAL '2.55')", createDecimalType(5, 4), decimal("2.4501", createDecimalType(5, 4)));
        assertFunction("mod(DECIMAL '123456789012345670', DECIMAL '123456789012345669')", createDecimalType(18, 0), decimal("0.01", createDecimalType(18)));
        assertFunction("mod(DECIMAL '12345678901234567.90', DECIMAL '12345678901234567.89')", createDecimalType(19, 2), decimal("0.01", createDecimalType(19, 2)));
        assertFunction("mod(DECIMAL '5.0', CAST(NULL as DECIMAL(1,0)))", createDecimalType(2, 1), null);
        assertFunction("mod(CAST(NULL as DECIMAL(1,0)), DECIMAL '5.0')", createDecimalType(2, 1), null);
        assertInvalidFunction("mod(DECIMAL '5.0', DECIMAL '0')", DIVISION_BY_ZERO);
    }

    @Test
    public void testPi()
    {
        assertFunction("pi()", DOUBLE, Math.PI);
    }

    @Test
    public void testNaN()
    {
        assertFunction("nan()", DOUBLE, Double.NaN);
        assertFunction("0.0E0 / 0.0E0", DOUBLE, Double.NaN);
    }

    @Test
    public void testInfinity()
    {
        assertFunction("infinity()", DOUBLE, Double.POSITIVE_INFINITY);
        assertFunction("-rand() / 0.0", DOUBLE, Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testIsInfinite()
    {
        assertFunction("is_infinite(1.0E0 / 0.0E0)", BOOLEAN, true);
        assertFunction("is_infinite(0.0E0 / 0.0E0)", BOOLEAN, false);
        assertFunction("is_infinite(1.0E0 / 1.0E0)", BOOLEAN, false);
        assertFunction("is_infinite(REAL '1.0' / REAL '0.0')", BOOLEAN, true);
        assertFunction("is_infinite(REAL '0.0' / REAL '0.0')", BOOLEAN, false);
        assertFunction("is_infinite(REAL '1.0' / REAL '1.0')", BOOLEAN, false);
        assertFunction("is_infinite(NULL)", BOOLEAN, null);
    }

    @Test
    public void testIsFinite()
    {
        assertFunction("is_finite(100000)", BOOLEAN, true);
        assertFunction("is_finite(rand() / 0.0E0)", BOOLEAN, false);
        assertFunction("is_finite(REAL '754.2008E0')", BOOLEAN, true);
        assertFunction("is_finite(rand() / REAL '0.0E0')", BOOLEAN, false);
        assertFunction("is_finite(NULL)", BOOLEAN, null);
    }

    @Test
    public void testIsNaN()
    {
        assertFunction("is_nan(0.0E0 / 0.0E0)", BOOLEAN, true);
        assertFunction("is_nan(0.0E0 / 1.0E0)", BOOLEAN, false);
        assertFunction("is_nan(infinity() / infinity())", BOOLEAN, true);
        assertFunction("is_nan(nan())", BOOLEAN, true);
        assertFunction("is_nan(REAL '0.0' / REAL '0.0')", BOOLEAN, true);
        assertFunction("is_nan(REAL '0.0' / 1.0E0)", BOOLEAN, false);
        assertFunction("is_nan(infinity() / infinity())", BOOLEAN, true);
        assertFunction("is_nan(nan())", BOOLEAN, true);
        assertFunction("is_nan(NULL)", BOOLEAN, null);
    }

    @Test
    public void testPower()
    {
        for (long left : intLefts) {
            for (long right : intRights) {
                assertFunction("power(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
            }
        }

        for (int left : intLefts) {
            for (int right : intRights) {
                assertFunction("power( BIGINT '" + left + "' , BIGINT '" + right + "')", DOUBLE, Math.pow(left, right));
            }
        }

        for (long left : intLefts) {
            for (long right : intRights) {
                assertFunction("power(" + left * 10000000000L + ", " + right + ")", DOUBLE, Math.pow(left * 10000000000L, right));
            }
        }

        for (long left : intLefts) {
            for (double right : doubleRights) {
                assertFunction("power(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
                assertFunction("power(" + left + ", REAL '" + (float) right + "')", DOUBLE, Math.pow(left, (float) right));
            }
        }

        for (double left : doubleLefts) {
            for (long right : intRights) {
                assertFunction("power(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
                assertFunction("power(REAL '" + (float) left + "', " + right + ")", DOUBLE, Math.pow((float) left, right));
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertFunction("power(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
                assertFunction("power(REAL '" + left + "', REAL '" + right + "')", DOUBLE, Math.pow((float) left, (float) right));
            }
        }

        assertFunction("power(NULL, NULL)", DOUBLE, null);
        assertFunction("power(5.0E0, NULL)", DOUBLE, null);
        assertFunction("power(NULL, 5.0E0)", DOUBLE, null);

        // test alias
        assertFunction("pow(5.0E0, 2.0E0)", DOUBLE, 25.0);
    }

    @Test
    public void testRadians()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction(format("radians(%s)", doubleValue), DOUBLE, Math.toRadians(doubleValue));
            assertFunction(format("radians(REAL '%s')", (float) doubleValue), DOUBLE, Math.toRadians((float) doubleValue));
        }
        assertFunction("radians(NULL)", DOUBLE, null);
    }

    @Test
    public void testRandom()
    {
        // random is non-deterministic
        functionAssertions.tryEvaluateWithAll("rand()", DOUBLE, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random()", DOUBLE, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("rand(1000)", INTEGER, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(TINYINT '3', TINYINT '5')", TINYINT, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(TINYINT '-3', TINYINT '-1')", TINYINT, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(TINYINT '-3', TINYINT '5')", TINYINT, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(SMALLINT '20000', SMALLINT '30000')", SMALLINT, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(SMALLINT '-20000', SMALLINT '-10000')", SMALLINT, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(SMALLINT '-20000', SMALLINT '30000')", SMALLINT, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(1000, 2000)", INTEGER, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(-10, -5)", INTEGER, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(-10, 10)", INTEGER, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(2000)", INTEGER, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(3000000000)", BIGINT, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(3000000000, 5000000000)", BIGINT, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(-3000000000, -2000000000)", BIGINT, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(-3000000000, 5000000000)", BIGINT, TEST_SESSION);

        assertInvalidFunction("rand(-1)", "bound must be positive");
        assertInvalidFunction("rand(-3000000000)", "bound must be positive");
        assertInvalidFunction("random(TINYINT '5', TINYINT '3')", "start value must be less than stop value");
        assertInvalidFunction("random(TINYINT '5', TINYINT '5')", "start value must be less than stop value");
        assertInvalidFunction("random(TINYINT '-5', TINYINT '-10')", "start value must be less than stop value");
        assertInvalidFunction("random(TINYINT '-5', TINYINT '-5')", "start value must be less than stop value");
        assertInvalidFunction("random(SMALLINT '30000', SMALLINT '10000')", "start value must be less than stop value");
        assertInvalidFunction("random(SMALLINT '30000', SMALLINT '30000')", "start value must be less than stop value");
        assertInvalidFunction("random(SMALLINT '-30000', SMALLINT '-31000')", "start value must be less than stop value");
        assertInvalidFunction("random(SMALLINT '-30000', SMALLINT '-30000')", "start value must be less than stop value");
        assertInvalidFunction("random(1000, 500)", "start value must be less than stop value");
        assertInvalidFunction("random(500, 500)", "start value must be less than stop value");
        assertInvalidFunction("random(-500, -600)", "start value must be less than stop value");
        assertInvalidFunction("random(-500, -500)", "start value must be less than stop value");
        assertInvalidFunction("random(3000000000, 1000000000)", "start value must be less than stop value");
        assertInvalidFunction("random(3000000000, 3000000000)", "start value must be less than stop value");
        assertInvalidFunction("random(-3000000000, -4000000000)", "start value must be less than stop value");
        assertInvalidFunction("random(-3000000000, -3000000000)", "start value must be less than stop value");
    }

    @Test
    public void testRound()
    {
        assertFunction("round(TINYINT '3')", TINYINT, (byte) 3);
        assertFunction("round(TINYINT '-3')", TINYINT, (byte) -3);
        assertFunction("round(CAST(NULL as TINYINT))", TINYINT, null);
        assertFunction("round(SMALLINT '3')", SMALLINT, (short) 3);
        assertFunction("round(SMALLINT '-3')", SMALLINT, (short) -3);
        assertFunction("round(CAST(NULL as SMALLINT))", SMALLINT, null);
        assertFunction("round(3)", INTEGER, 3);
        assertFunction("round(-3)", INTEGER, -3);
        assertFunction("round(CAST(NULL as INTEGER))", INTEGER, null);
        assertFunction("round(BIGINT '3')", BIGINT, 3L);
        assertFunction("round(BIGINT '-3')", BIGINT, -3L);
        assertFunction("round(CAST(NULL as BIGINT))", BIGINT, null);
        assertFunction("round( 3000000000)", BIGINT, 3000000000L);
        assertFunction("round(-3000000000)", BIGINT, -3000000000L);
        assertFunction("round(3.0E0)", DOUBLE, 3.0);
        assertFunction("round(-3.0E0)", DOUBLE, -3.0);
        assertFunction("round(3.499E0)", DOUBLE, 3.0);
        assertFunction("round(-3.499E0)", DOUBLE, -3.0);
        assertFunction("round(3.5E0)", DOUBLE, 4.0);
        assertFunction("round(-3.5E0)", DOUBLE, -4.0);
        assertFunction("round(-3.5001E0)", DOUBLE, -4.0);
        assertFunction("round(-3.99E0)", DOUBLE, -4.0);
        assertFunction("round(REAL '3.0')", REAL, 3.0f);
        assertFunction("round(REAL '-3.0')", REAL, -3.0f);
        assertFunction("round(REAL '3.499')", REAL, 3.0f);
        assertFunction("round(REAL '-3.499')", REAL, -3.0f);
        assertFunction("round(REAL '3.5')", REAL, 4.0f);
        assertFunction("round(REAL '-3.5')", REAL, -4.0f);
        assertFunction("round(REAL '-3.5001')", REAL, -4.0f);
        assertFunction("round(REAL '-3.99')", REAL, -4.0f);
        assertFunction("round(CAST(NULL as DOUBLE))", DOUBLE, null);
        assertFunction("round(DOUBLE '" + GREATEST_DOUBLE_LESS_THAN_HALF + "')", DOUBLE, 0.0);
        assertFunction("round(DOUBLE '-" + 0x1p-1 + "')", DOUBLE, -1.0); // -0.5
        assertFunction("round(DOUBLE '-" + GREATEST_DOUBLE_LESS_THAN_HALF + "')", DOUBLE, -0.0);

        assertFunction("round(TINYINT '3', TINYINT '0')", TINYINT, (byte) 3);
        assertFunction("round(TINYINT '3', 0)", TINYINT, (byte) 3);
        assertFunction("round(SMALLINT '3', SMALLINT '0')", SMALLINT, (short) 3);
        assertFunction("round(SMALLINT '3', 0)", SMALLINT, (short) 3);
        assertFunction("round(3, 0)", INTEGER, 3);
        assertFunction("round(-3, 0)", INTEGER, -3);
        assertFunction("round(-3, INTEGER '0')", INTEGER, -3);
        assertFunction("round(BIGINT '3', 0)", BIGINT, 3L);
        assertFunction("round( 3000000000, 0)", BIGINT, 3000000000L);
        assertFunction("round(-3000000000, 0)", BIGINT, -3000000000L);
        assertFunction("round(3.0E0, 0)", DOUBLE, 3.0);
        assertFunction("round(-3.0E0, 0)", DOUBLE, -3.0);
        assertFunction("round(3.499E0, 0)", DOUBLE, 3.0);
        assertFunction("round(-3.499E0, 0)", DOUBLE, -3.0);
        assertFunction("round(3.5E0, 0)", DOUBLE, 4.0);
        assertFunction("round(-3.5E0, 0)", DOUBLE, -4.0);
        assertFunction("round(-3.5001E0, 0)", DOUBLE, -4.0);
        assertFunction("round(-3.99E0, 0)", DOUBLE, -4.0);
        assertFunction("round(DOUBLE '" + GREATEST_DOUBLE_LESS_THAN_HALF + "', 0)", DOUBLE, 0.0);
        assertFunction("round(DOUBLE '-" + 0x1p-1 + "')", DOUBLE, -1.0); // -0.5
        assertFunction("round(DOUBLE '-" + GREATEST_DOUBLE_LESS_THAN_HALF + "', 0)", DOUBLE, -0.0);
        assertFunction("round(0.3E0)", DOUBLE, 0.0);
        assertFunction("round(-0.3E0)", DOUBLE, -0.0);

        assertFunction("round(TINYINT '3', TINYINT '1')", TINYINT, (byte) 3);
        assertFunction("round(TINYINT '3', 1)", TINYINT, (byte) 3);
        assertFunction("round(SMALLINT '3', SMALLINT '1')", SMALLINT, (short) 3);
        assertFunction("round(SMALLINT '3', 1)", SMALLINT, (short) 3);
        assertFunction("round(REAL '3.0', 0)", REAL, 3.0f);
        assertFunction("round(REAL '-3.0', 0)", REAL, -3.0f);
        assertFunction("round(REAL '3.499', 0)", REAL, 3.0f);
        assertFunction("round(REAL '-3.499', 0)", REAL, -3.0f);
        assertFunction("round(REAL '3.5', 0)", REAL, 4.0f);
        assertFunction("round(REAL '-3.5', 0)", REAL, -4.0f);
        assertFunction("round(REAL '-3.5001', 0)", REAL, -4.0f);
        assertFunction("round(REAL '-3.99', 0)", REAL, -4.0f);
        assertFunction("round(3, 1)", INTEGER, 3);
        assertFunction("round(-3, 1)", INTEGER, -3);
        assertFunction("round(-3, INTEGER '1')", INTEGER, -3);
        assertFunction("round(-3, CAST(NULL as INTEGER))", INTEGER, null);
        assertFunction("round(BIGINT '3', 1)", BIGINT, 3L);
        assertFunction("round( 3000000000, 1)", BIGINT, 3000000000L);
        assertFunction("round(-3000000000, 1)", BIGINT, -3000000000L);
        assertFunction("round(CAST(NULL as BIGINT), CAST(NULL as INTEGER))", BIGINT, null);
        assertFunction("round(CAST(NULL as BIGINT), 1)", BIGINT, null);
        assertFunction("round(3.0E0, 1)", DOUBLE, 3.0);
        assertFunction("round(-3.0E0, 1)", DOUBLE, -3.0);
        assertFunction("round(3.499E0, 1)", DOUBLE, 3.5);
        assertFunction("round(-3.499E0, 1)", DOUBLE, -3.5);
        assertFunction("round(3.5E0, 1)", DOUBLE, 3.5);
        assertFunction("round(-3.5E0, 1)", DOUBLE, -3.5);
        assertFunction("round(-3.5001E0, 1)", DOUBLE, -3.5);
        assertFunction("round(-3.99E0, 1)", DOUBLE, -4.0);
        assertFunction("round(REAL '3.0', 1)", REAL, 3.0f);
        assertFunction("round(REAL '-3.0', 1)", REAL, -3.0f);
        assertFunction("round(REAL '3.499', 1)", REAL, 3.5f);
        assertFunction("round(REAL '-3.499', 1)", REAL, -3.5f);
        assertFunction("round(REAL '3.5', 1)", REAL, 3.5f);
        assertFunction("round(REAL '-3.5', 1)", REAL, -3.5f);
        assertFunction("round(REAL '-3.5001', 1)", REAL, -3.5f);
        assertFunction("round(REAL '-3.99', 1)", REAL, -4.0f);

        // ROUND negative DECIMAL
        assertFunction("round(TINYINT '9', -1)", TINYINT, (byte) 10);
        assertFunction("round(TINYINT '-9', -1)", TINYINT, (byte) -10);
        assertFunction("round(TINYINT '5', -1)", TINYINT, (byte) 10);
        assertFunction("round(TINYINT '-5', -1)", TINYINT, (byte) -10);
        assertFunction("round(TINYINT '-14', -1)", TINYINT, (byte) -10);
        assertFunction("round(TINYINT '12', -1)", TINYINT, (byte) 10);
        assertFunction("round(TINYINT '18', -1)", TINYINT, (byte) 20);
        assertFunction("round(TINYINT '18', -2)", TINYINT, (byte) 0);
        assertFunction("round(TINYINT '18', -3)", TINYINT, (byte) 0);
        assertFunction("round(TINYINT '127', -2)", TINYINT, (byte) 100);
        assertFunction("round(TINYINT '127', -3)", TINYINT, (byte) 0);
        assertFunction("round(TINYINT '-128', -2)", TINYINT, (byte) -100);
        assertFunction("round(TINYINT '-128', -3)", TINYINT, (byte) 0);
        assertFunction("round(SMALLINT '99', -1)", SMALLINT, (short) 100);
        assertFunction("round(SMALLINT '99', -2)", SMALLINT, (short) 100);
        assertFunction("round(SMALLINT '99', -3)", SMALLINT, (short) 0);
        assertFunction("round(SMALLINT '-99', -1)", SMALLINT, (short) -100);
        assertFunction("round(SMALLINT '-99', -2)", SMALLINT, (short) -100);
        assertFunction("round(SMALLINT '-99', -3)", SMALLINT, (short) 0);
        assertFunction("round(SMALLINT '32767', -4)", SMALLINT, (short) 30000);
        assertFunction("round(SMALLINT '32767', -5)", SMALLINT, (short) 0);
        assertFunction("round(SMALLINT '-32768', -4)", SMALLINT, (short) -30000);
        assertFunction("round(SMALLINT '-32768', -5)", SMALLINT, (short) 0);
        assertFunction("round(99, -1)", INTEGER, 100);
        assertFunction("round(-99, -1)", INTEGER, -100);
        assertFunction("round(99, INTEGER '-1')", INTEGER, 100);
        assertFunction("round(-99, INTEGER '-1')", INTEGER, -100);
        assertFunction("round(12355, -2)", INTEGER, 12400);
        assertFunction("round(12345, -2)", INTEGER, 12300);
        assertFunction("round(2147483647, -9)", INTEGER, 2000000000);
        assertFunction("round(2147483647, -10)", INTEGER, 0);
        assertFunction("round( 3999999999, -1)", BIGINT, 4000000000L);
        assertFunction("round(-3999999999, -1)", BIGINT, -4000000000L);
        assertFunction("round(9223372036854775807, -2)", BIGINT, 9223372036854775800L);
        assertFunction("round(9223372036854775807, -17)", BIGINT, 9200000000000000000L);
        assertFunction("round(9223372036854775807, -18)", BIGINT, 9000000000000000000L);
        assertFunction("round(-9223372036854775807, -17)", BIGINT, -9200000000000000000L);
        assertFunction("round(-9223372036854775807, -18)", BIGINT, -9000000000000000000L);

        assertInvalidFunction("round(TINYINT '127', -1)", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("round(TINYINT '-128', -1)", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("round(SMALLINT '32767', -1)", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("round(SMALLINT '32767', -3)", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("round(SMALLINT '-32768', -1)", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("round(SMALLINT '-32768', -3)", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("round(2147483647, -100)", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("round(2147483647, -2147483648)", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("round(9223372036854775807, -1)", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("round(9223372036854775807, -3)", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("round(9223372036854775807, -19)", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("round(-9223372036854775807, -20)", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("round(-9223372036854775807, -2147483648)", NUMERIC_VALUE_OUT_OF_RANGE);

        // ROUND short DECIMAL -> short DECIMAL
        assertFunction("round(DECIMAL '0')", createDecimalType(1, 0), decimal("0", createDecimalType(1)));
        assertFunction("round(DECIMAL '0.1')", createDecimalType(1, 0), decimal("0", createDecimalType(1)));
        assertFunction("round(DECIMAL '-0.1')", createDecimalType(1, 0), decimal("0", createDecimalType(1)));
        assertFunction("round(DECIMAL '3')", createDecimalType(1, 0), decimal("3", createDecimalType(1)));
        assertFunction("round(DECIMAL '-3')", createDecimalType(1, 0), decimal("-3", createDecimalType(1)));
        assertFunction("round(DECIMAL '3.0')", createDecimalType(2, 0), decimal("3", createDecimalType(2)));
        assertFunction("round(DECIMAL '-3.0')", createDecimalType(2, 0), decimal("-3", createDecimalType(2)));
        assertFunction("round(DECIMAL '3.49')", createDecimalType(2, 0), decimal("3", createDecimalType(2)));
        assertFunction("round(DECIMAL '-3.49')", createDecimalType(2, 0), decimal("-3", createDecimalType(2)));
        assertFunction("round(DECIMAL '3.50')", createDecimalType(2, 0), decimal("4", createDecimalType(2)));
        assertFunction("round(DECIMAL '-3.50')", createDecimalType(2, 0), decimal("-4", createDecimalType(2)));
        assertFunction("round(DECIMAL '3.99')", createDecimalType(2, 0), decimal("4", createDecimalType(2)));
        assertFunction("round(DECIMAL '-3.99')", createDecimalType(2, 0), decimal("-4", createDecimalType(2)));
        assertFunction("round(DECIMAL '9.99')", createDecimalType(2, 0), decimal("10", createDecimalType(2)));
        assertFunction("round(DECIMAL '-9.99')", createDecimalType(2, 0), decimal("-10", createDecimalType(2)));
        assertFunction("round(DECIMAL '9999.9')", createDecimalType(5, 0), decimal("10000", createDecimalType(5)));

        assertFunction("round(DECIMAL '-9999.9')", createDecimalType(5, 0), decimal("-10000", createDecimalType(5)));
        assertFunction("round(DECIMAL '1000000000000.9999')", createDecimalType(14, 0), decimal("1000000000001", createDecimalType(14)));
        assertFunction("round(DECIMAL '-1000000000000.9999')", createDecimalType(14, 0), decimal("-1000000000001", createDecimalType(14)));
        assertFunction("round(DECIMAL '10000000000000000')", createDecimalType(17, 0), decimal("10000000000000000", createDecimalType(17)));
        assertFunction("round(DECIMAL '-10000000000000000')", createDecimalType(17, 0), decimal("-10000000000000000", createDecimalType(17)));
        assertFunction("round(DECIMAL '9999999999999999.99')", createDecimalType(17, 0), decimal("10000000000000000", createDecimalType(17)));
        assertFunction("round(DECIMAL '99999999999999999.9')", createDecimalType(18, 0), decimal("100000000000000000", createDecimalType(18)));

        // ROUND long DECIMAL -> long DECIMAL
        assertFunction("round(CAST(0 AS DECIMAL(18,0)))", createDecimalType(18, 0), decimal("0", createDecimalType(18)));
        assertFunction("round(CAST(0 AS DECIMAL(18,1)))", createDecimalType(18, 0), decimal("0", createDecimalType(18)));
        assertFunction("round(CAST(0 AS DECIMAL(18,2)))", createDecimalType(17, 0), decimal("0", createDecimalType(17)));
        assertFunction("round(CAST(DECIMAL '0.1' AS DECIMAL(18,1)))", createDecimalType(18, 0), decimal("0", createDecimalType(18)));
        assertFunction("round(CAST(DECIMAL '-0.1' AS DECIMAL(18,1)))", createDecimalType(18, 0), decimal("0", createDecimalType(18)));
        assertFunction("round(DECIMAL '3000000000000000000000')", createDecimalType(22, 0), decimal("3000000000000000000000", createDecimalType(22)));
        assertFunction("round(DECIMAL '-3000000000000000000000')", createDecimalType(22, 0), decimal("-3000000000000000000000", createDecimalType(22)));
        assertFunction("round(DECIMAL '3000000000000000000000.0')", createDecimalType(23, 0), decimal("3000000000000000000000", createDecimalType(23)));
        assertFunction("round(DECIMAL '-3000000000000000000000.0')", createDecimalType(23, 0), decimal("-3000000000000000000000", createDecimalType(23)));
        assertFunction("round(DECIMAL '3000000000000000000000.49')", createDecimalType(23, 0), decimal("3000000000000000000000", createDecimalType(23)));
        assertFunction("round(DECIMAL '-3000000000000000000000.49')", createDecimalType(23, 0), decimal("-3000000000000000000000", createDecimalType(23)));
        assertFunction("round(DECIMAL '3000000000000000000000.50')", createDecimalType(23, 0), decimal("3000000000000000000001", createDecimalType(23)));
        assertFunction("round(DECIMAL '-3000000000000000000000.50')", createDecimalType(23, 0), decimal("-3000000000000000000001", createDecimalType(23)));
        assertFunction("round(DECIMAL '3000000000000000000000.99')", createDecimalType(23, 0), decimal("3000000000000000000001", createDecimalType(23)));
        assertFunction("round(DECIMAL '-3000000000000000000000.99')", createDecimalType(23, 0), decimal("-3000000000000000000001", createDecimalType(23)));
        assertFunction("round(DECIMAL '9999999999999999999999.99')", createDecimalType(23, 0), decimal("10000000000000000000000", createDecimalType(23)));
        assertFunction("round(DECIMAL '-9999999999999999999999.99')", createDecimalType(23, 0), decimal("-10000000000000000000000", createDecimalType(23)));
        assertFunction("round(DECIMAL '1000000000000000000000000000000000.9999')", createDecimalType(35, 0), decimal("1000000000000000000000000000000001", createDecimalType(35)));
        assertFunction("round(DECIMAL '-1000000000000000000000000000000000.9999')", createDecimalType(35, 0), decimal("-1000000000000000000000000000000001", createDecimalType(35)));
        assertFunction("round(DECIMAL '10000000000000000000000000000000000000')", createDecimalType(38, 0), decimal("10000000000000000000000000000000000000", createDecimalType(38)));
        assertFunction("round(DECIMAL '-10000000000000000000000000000000000000')", createDecimalType(38, 0), decimal("-10000000000000000000000000000000000000", createDecimalType(38)));

        // ROUND long DECIMAL -> short DECIMAL
        assertFunction("round(DECIMAL '3000000000000000.000000')", createDecimalType(17, 0), decimal("3000000000000000", createDecimalType(17)));
        assertFunction("round(DECIMAL '-3000000000000000.000000')", createDecimalType(17, 0), decimal("-3000000000000000", createDecimalType(17)));
        assertFunction("round(DECIMAL '3000000000000000.499999')", createDecimalType(17, 0), decimal("3000000000000000", createDecimalType(17)));
        assertFunction("round(DECIMAL '-3000000000000000.499999')", createDecimalType(17, 0), decimal("-3000000000000000", createDecimalType(17)));
        assertFunction("round(DECIMAL '3000000000000000.500000')", createDecimalType(17, 0), decimal("3000000000000001", createDecimalType(17)));
        assertFunction("round(DECIMAL '-3000000000000000.500000')", createDecimalType(17, 0), decimal("-3000000000000001", createDecimalType(17)));
        assertFunction("round(DECIMAL '3000000000000000.999999')", createDecimalType(17, 0), decimal("3000000000000001", createDecimalType(17)));
        assertFunction("round(DECIMAL '-3000000000000000.999999')", createDecimalType(17, 0), decimal("-3000000000000001", createDecimalType(17)));
        assertFunction("round(DECIMAL '9999999999999999.999999')", createDecimalType(17, 0), decimal("10000000000000000", createDecimalType(17)));
        assertFunction("round(DECIMAL '-9999999999999999.999999')", createDecimalType(17, 0), decimal("-10000000000000000", createDecimalType(17)));

        // ROUND_N short DECIMAL -> short DECIMAL
        assertFunction("round(DECIMAL '3', 1)", createDecimalType(2, 0), decimal("3", createDecimalType(2)));
        assertFunction("round(DECIMAL '-3', 1)", createDecimalType(2, 0), decimal("-3", createDecimalType(2)));
        assertFunction("round(DECIMAL '3.0', 1)", createDecimalType(3, 1), decimal("3.0", createDecimalType(3, 1)));
        assertFunction("round(DECIMAL '-3.0', 1)", createDecimalType(3, 1), decimal("-3.0", createDecimalType(3, 1)));
        assertFunction("round(DECIMAL '3.449', 1)", createDecimalType(5, 3), decimal("3.400", createDecimalType(5, 3)));
        assertFunction("round(DECIMAL '-3.449', 1)", createDecimalType(5, 3), decimal("-3.400", createDecimalType(5, 3)));
        assertFunction("round(DECIMAL '3.450', 1)", createDecimalType(5, 3), decimal("3.500", createDecimalType(5, 3)));
        assertFunction("round(DECIMAL '-3.450', 1)", createDecimalType(5, 3), decimal("-3.500", createDecimalType(5, 3)));
        assertFunction("round(DECIMAL '3.99', 1)", createDecimalType(4, 2), decimal("4.00", createDecimalType(4, 2)));
        assertFunction("round(DECIMAL '-3.99', 1)", createDecimalType(4, 2), decimal("-4.00", createDecimalType(4, 2)));
        assertFunction("round(DECIMAL '9.99', 1)", createDecimalType(4, 2), decimal("10.00", createDecimalType(4, 2)));
        assertFunction("round(DECIMAL '-9.99', 1)", createDecimalType(4, 2), decimal("-10.00", createDecimalType(4, 2)));

        assertFunction("round(DECIMAL '0.3', 0)", createDecimalType(2, 1), decimal("0.0", createDecimalType(2, 1)));
        assertFunction("round(DECIMAL '0.7', 0)", createDecimalType(2, 1), decimal("1.0", createDecimalType(2, 1)));
        assertFunction("round(DECIMAL '1.7', 0)", createDecimalType(3, 1), decimal("2.0", createDecimalType(3, 1)));
        assertFunction("round(DECIMAL '-0.3', 0)", createDecimalType(2, 1), decimal("0.0", createDecimalType(2, 1)));
        assertFunction("round(DECIMAL '-0.7', 0)", createDecimalType(2, 1), decimal("-1.0", createDecimalType(2, 1)));
        assertFunction("round(DECIMAL '-1.7', 0)", createDecimalType(3, 1), decimal("-2.0", createDecimalType(3, 1)));
        assertFunction("round(DECIMAL '0.7', -1)", createDecimalType(2, 1), decimal("0.0", createDecimalType(2, 1)));
        assertFunction("round(DECIMAL '1.7', -1)", createDecimalType(3, 1), decimal("0.0", createDecimalType(3, 1)));
        assertFunction("round(DECIMAL '7.1', -1)", createDecimalType(3, 1), decimal("10.0", createDecimalType(3, 1)));
        assertFunction("round(DECIMAL '0.3', -1)", createDecimalType(2, 1), decimal("0.0", createDecimalType(2, 1)));
        assertFunction("round(DECIMAL '33.3', -2)", createDecimalType(4, 1), decimal("0.0", createDecimalType(4, 1)));
        assertFunction("round(CAST(DECIMAL '0.7' AS decimal(20, 1)), -19)", createDecimalType(21, 1), decimal("0.0", createDecimalType(21, 1)));
        assertFunction("round(DECIMAL '0.00', 1)", createDecimalType(3, 2), decimal("0.00", createDecimalType(3, 2)));
        assertFunction("round(DECIMAL '1234', 7)", createDecimalType(5, 0), decimal("1234", createDecimalType(5)));
        assertFunction("round(DECIMAL '-1234', 7)", createDecimalType(5, 0), decimal("-1234", createDecimalType(5)));
        assertFunction("round(DECIMAL '1234', -7)", createDecimalType(5, 0), decimal("0", createDecimalType(5)));
        assertFunction("round(DECIMAL '-1234', -7)", createDecimalType(5, 0), decimal("0", createDecimalType(5)));
        assertFunction("round(DECIMAL '1234.5678', 7)", createDecimalType(9, 4), decimal("1234.5678", createDecimalType(9, 4)));
        assertFunction("round(DECIMAL '-1234.5678', 7)", createDecimalType(9, 4), decimal("-1234.5678", createDecimalType(9, 4)));
        assertFunction("round(DECIMAL '1234.5678', -2)", createDecimalType(9, 4), decimal("1200.0000", createDecimalType(9, 4)));
        assertFunction("round(DECIMAL '-1234.5678', -2)", createDecimalType(9, 4), decimal("-1200.0000", createDecimalType(9, 4)));
        assertFunction("round(DECIMAL '1254.5678', -2)", createDecimalType(9, 4), decimal("1300.0000", createDecimalType(9, 4)));
        assertFunction("round(DECIMAL '-1254.5678', -2)", createDecimalType(9, 4), decimal("-1300.0000", createDecimalType(9, 4)));
        assertFunction("round(DECIMAL '1234.5678', -7)", createDecimalType(9, 4), decimal("0.0000", createDecimalType(9, 4)));
        assertFunction("round(DECIMAL '-1234.5678', -7)", createDecimalType(9, 4), decimal("0.0000", createDecimalType(9, 4)));
        assertFunction("round(DECIMAL '99', -1)", createDecimalType(3, 0), decimal("100", createDecimalType(3)));

        // ROUND_N long DECIMAL -> long DECIMAL
        assertFunction("round(DECIMAL '1234567890123456789', 1)", createDecimalType(20, 0), decimal("1234567890123456789", createDecimalType(20)));
        assertFunction("round(DECIMAL '-1234567890123456789', 1)", createDecimalType(20, 0), decimal("-1234567890123456789", createDecimalType(20)));
        assertFunction("round(DECIMAL '123456789012345678.0', 1)", createDecimalType(20, 1), decimal("123456789012345678.0", createDecimalType(20, 1)));
        assertFunction("round(DECIMAL '-123456789012345678.0', 1)", createDecimalType(20, 1), decimal("-123456789012345678.0", createDecimalType(20, 1)));
        assertFunction("round(DECIMAL '123456789012345678.449', 1)", createDecimalType(22, 3), decimal("123456789012345678.400", createDecimalType(22, 3)));
        assertFunction("round(DECIMAL '-123456789012345678.449', 1)", createDecimalType(22, 3), decimal("-123456789012345678.400", createDecimalType(22, 3)));
        assertFunction("round(DECIMAL '123456789012345678.45', 1)", createDecimalType(21, 2), decimal("123456789012345678.50", createDecimalType(21, 2)));
        assertFunction("round(DECIMAL '-123456789012345678.45', 1)", createDecimalType(21, 2), decimal("-123456789012345678.50", createDecimalType(21, 2)));
        assertFunction("round(DECIMAL '123456789012345678.501', 1)", createDecimalType(22, 3), decimal("123456789012345678.500", createDecimalType(22, 3)));
        assertFunction("round(DECIMAL '-123456789012345678.501', 1)", createDecimalType(22, 3), decimal("-123456789012345678.500", createDecimalType(22, 3)));
        assertFunction("round(DECIMAL '999999999999999999.99', 1)", createDecimalType(21, 2), decimal("1000000000000000000.00", createDecimalType(21, 2)));
        assertFunction("round(DECIMAL '-999999999999999999.99', 1)", createDecimalType(21, 2), decimal("-1000000000000000000.00", createDecimalType(21, 2)));
        assertFunction("round(DECIMAL '1234567890123456789', 7)", createDecimalType(20, 0), decimal("1234567890123456789", createDecimalType(20)));
        assertFunction("round(DECIMAL '-1234567890123456789', 7)", createDecimalType(20, 0), decimal("-1234567890123456789", createDecimalType(20)));
        assertFunction("round(DECIMAL '123456789012345678.99', 7)", createDecimalType(21, 2), decimal("123456789012345678.99", createDecimalType(21, 2)));
        assertFunction("round(DECIMAL '-123456789012345678.99', 7)", createDecimalType(21, 2), decimal("-123456789012345678.99", createDecimalType(21, 2)));
        assertFunction("round(DECIMAL '123456789012345611.99', -2)", createDecimalType(21, 2), decimal("123456789012345600.00", createDecimalType(21, 2)));
        assertFunction("round(DECIMAL '-123456789012345611.99', -2)", createDecimalType(21, 2), decimal("-123456789012345600.00", createDecimalType(21, 2)));
        assertFunction("round(DECIMAL '123456789012345678.99', -2)", createDecimalType(21, 2), decimal("123456789012345700.00", createDecimalType(21, 2)));
        assertFunction("round(DECIMAL '-123456789012345678.99', -2)", createDecimalType(21, 2), decimal("-123456789012345700.00", createDecimalType(21, 2)));
        assertFunction("round(DECIMAL '123456789012345678.99', -30)", createDecimalType(21, 2), decimal("0.00", createDecimalType(21, 2)));
        assertFunction("round(DECIMAL '-123456789012345678.99', -30)", createDecimalType(21, 2), decimal("0.00", createDecimalType(21, 2)));
        assertFunction("round(DECIMAL '9999999999999999999999999999999999999.9', 1)", createDecimalType(38, 1), decimal("9999999999999999999999999999999999999.9", createDecimalType(38, 1)));
        assertInvalidFunction("round(DECIMAL '9999999999999999999999999999999999999.9', 0)", NUMERIC_VALUE_OUT_OF_RANGE);
        assertInvalidFunction("round(DECIMAL '9999999999999999999999999999999999999.9', -1)", NUMERIC_VALUE_OUT_OF_RANGE);
        assertFunction("round(DECIMAL  '1329123201320737513', -3)", createDecimalType(20, 0), decimal("1329123201320738000", createDecimalType(20)));
        assertFunction("round(DECIMAL '-1329123201320737513', -3)", createDecimalType(20, 0), decimal("-1329123201320738000", createDecimalType(20)));
        assertFunction("round(DECIMAL  '1329123201320739513', -3)", createDecimalType(20, 0), decimal("1329123201320740000", createDecimalType(20)));
        assertFunction("round(DECIMAL '-1329123201320739513', -3)", createDecimalType(20, 0), decimal("-1329123201320740000", createDecimalType(20)));
        assertFunction("round(DECIMAL  '9999999999999999999', -3)", createDecimalType(20, 0), decimal("10000000000000000000", createDecimalType(20)));
        assertFunction("round(DECIMAL '-9999999999999999999', -3)", createDecimalType(20, 0), decimal("-10000000000000000000", createDecimalType(20)));

        // ROUND_N short DECIMAL -> long DECIMAL
        assertFunction("round(DECIMAL '9999999999999999.99', 1)", createDecimalType(19, 2), decimal("10000000000000000.00", createDecimalType(19, 2)));
        assertFunction("round(DECIMAL '-9999999999999999.99', 1)", createDecimalType(19, 2), decimal("-10000000000000000.00", createDecimalType(19, 2)));
        assertFunction("round(DECIMAL '9999999999999999.99', -1)", createDecimalType(19, 2), decimal("10000000000000000.00", createDecimalType(19, 2)));
        assertFunction("round(DECIMAL '-9999999999999999.99', -1)", createDecimalType(19, 2), decimal("-10000000000000000.00", createDecimalType(19, 2)));
        assertFunction("round(DECIMAL '9999999999999999.99', 2)", createDecimalType(19, 2), decimal("9999999999999999.99", createDecimalType(19, 2)));
        assertFunction("round(DECIMAL '-9999999999999999.99', 2)", createDecimalType(19, 2), decimal("-9999999999999999.99", createDecimalType(19, 2)));
        assertFunction("round(DECIMAL '329123201320737513', -3)", createDecimalType(19, 0), decimal("329123201320738000", createDecimalType(19)));
        assertFunction("round(DECIMAL '-329123201320737513', -3)", createDecimalType(19, 0), decimal("-329123201320738000", createDecimalType(19)));
        assertFunction("round(DECIMAL '329123201320739513', -3)", createDecimalType(19, 0), decimal("329123201320740000", createDecimalType(19)));
        assertFunction("round(DECIMAL '-329123201320739513', -3)", createDecimalType(19, 0), decimal("-329123201320740000", createDecimalType(19)));
        assertFunction("round(DECIMAL '999999999999999999', -3)", createDecimalType(19, 0), decimal("1000000000000000000", createDecimalType(19)));
        assertFunction("round(DECIMAL '-999999999999999999', -3)", createDecimalType(19, 0), decimal("-1000000000000000000", createDecimalType(19)));

        // NULL
        assertFunction("round(CAST(NULL as DOUBLE), CAST(NULL as INTEGER))", DOUBLE, null);
        assertFunction("round(-3.0E0, CAST(NULL as INTEGER))", DOUBLE, null);
        assertFunction("round(CAST(NULL as DOUBLE), 1)", DOUBLE, null);
        assertFunction("round(CAST(NULL as DECIMAL(1,0)), CAST(NULL as INTEGER))", createDecimalType(2, 0), null);
        assertFunction("round(DECIMAL '-3.0', CAST(NULL as INTEGER))", createDecimalType(3, 1), null);
        assertFunction("round(CAST(NULL as DECIMAL(1,0)), 1)", createDecimalType(2, 0), null);
        assertFunction("round(CAST(NULL as DECIMAL(17,2)), 1)", createDecimalType(18, 2), null);
        assertFunction("round(CAST(NULL as DECIMAL(20,2)), 1)", createDecimalType(21, 2), null);

        // NaN
        assertFunction("round(nan(), 2)", DOUBLE, Double.NaN);
        assertFunction("round(1.0E0 / 0, 2)", DOUBLE, Double.POSITIVE_INFINITY);
        assertFunction("round(-1.0E0 / 0, 2)", DOUBLE, Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testSign()
    {
        DecimalType expectedDecimalReturnType = createDecimalType(1, 0);

        //retains type for NULL values
        assertFunction("sign(CAST(NULL as TINYINT))", TINYINT, null);
        assertFunction("sign(CAST(NULL as SMALLINT))", SMALLINT, null);
        assertFunction("sign(CAST(NULL as INTEGER))", INTEGER, null);
        assertFunction("sign(CAST(NULL as BIGINT))", BIGINT, null);
        assertFunction("sign(CAST(NULL as DOUBLE))", DOUBLE, null);
        assertFunction("sign(CAST(NULL as DECIMAL(2,1)))", expectedDecimalReturnType, null);
        assertFunction("sign(CAST(NULL as DECIMAL(38,0)))", expectedDecimalReturnType, null);

        //tinyint
        for (int intValue : intLefts) {
            Float signum = Math.signum(intValue);
            assertFunction("sign(TINYINT '" + intValue + "')", TINYINT, signum.byteValue());
        }

        //smallint
        for (int intValue : intLefts) {
            Float signum = Math.signum(intValue);
            assertFunction("sign(SMALLINT '" + intValue + "')", SMALLINT, signum.shortValue());
        }

        //integer
        for (int intValue : intLefts) {
            Float signum = Math.signum(intValue);
            assertFunction("sign(INTEGER '" + intValue + "')", INTEGER, signum.intValue());
        }

        //bigint
        for (int intValue : intLefts) {
            Float signum = Math.signum(intValue);
            assertFunction("sign(BIGINT '" + intValue + "')", BIGINT, signum.longValue());
        }

        //double and float
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("sign(DOUBLE '" + doubleValue + "')", DOUBLE, Math.signum(doubleValue));
            assertFunction("sign(REAL '" + (float) doubleValue + "')", REAL, Math.signum(((float) doubleValue)));
        }

        //returns NaN for NaN input
        assertFunction("sign(DOUBLE 'NaN')", DOUBLE, Double.NaN);

        //returns proper sign for +/-Infinity input
        assertFunction("sign(DOUBLE '+Infinity')", DOUBLE, 1.0);
        assertFunction("sign(DOUBLE '-Infinity')", DOUBLE, -1.0);

        //short decimal
        assertFunction("sign(DECIMAL '0')", expectedDecimalReturnType, decimal("0", createDecimalType(1)));
        assertFunction("sign(DECIMAL '123')", expectedDecimalReturnType, decimal("1", createDecimalType(1)));
        assertFunction("sign(DECIMAL '-123')", expectedDecimalReturnType, decimal("-1", createDecimalType(1)));
        assertFunction("sign(DECIMAL '123.000000000000000')", expectedDecimalReturnType, decimal("1", createDecimalType(1)));
        assertFunction("sign(DECIMAL '-123.000000000000000')", expectedDecimalReturnType, decimal("-1", createDecimalType(1)));

        //long decimal
        assertFunction("sign(DECIMAL '0.000000000000000000')", expectedDecimalReturnType, decimal("0", createDecimalType(1)));
        assertFunction("sign(DECIMAL '1230.000000000000000')", expectedDecimalReturnType, decimal("1", createDecimalType(1)));
        assertFunction("sign(DECIMAL '-1230.000000000000000')", expectedDecimalReturnType, decimal("-1", createDecimalType(1)));
    }

    @Test
    public void testSin()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("sin(" + doubleValue + ")", DOUBLE, Math.sin(doubleValue));
            assertFunction("sin(REAL '" + (float) doubleValue + "')", DOUBLE, Math.sin((float) doubleValue));
        }
        assertFunction("sin(NULL)", DOUBLE, null);
    }

    @Test
    public void testSqrt()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("sqrt(" + doubleValue + ")", DOUBLE, Math.sqrt(doubleValue));
            assertFunction("sqrt(REAL '" + doubleValue + "')", DOUBLE, Math.sqrt((float) doubleValue));
        }
        assertFunction("sqrt(NULL)", DOUBLE, null);
    }

    @Test
    public void testTan()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("tan(" + doubleValue + ")", DOUBLE, Math.tan(doubleValue));
            assertFunction("tan(REAL '" + (float) doubleValue + "')", DOUBLE, Math.tan((float) doubleValue));
        }
        assertFunction("tan(NULL)", DOUBLE, null);
    }

    @Test
    public void testTanh()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("tanh(" + doubleValue + ")", DOUBLE, Math.tanh(doubleValue));
            assertFunction("tanh(REAL '" + doubleValue + "')", DOUBLE, Math.tanh((float) doubleValue));
        }
        assertFunction("tanh(NULL)", DOUBLE, null);
    }

    @Test
    public void testGreatest()
    {
        // tinyint
        assertFunction("greatest(TINYINT'1', TINYINT'2')", TINYINT, (byte) 2);
        assertFunction("greatest(TINYINT'-1', TINYINT'-2')", TINYINT, (byte) -1);
        assertFunction("greatest(TINYINT'5', TINYINT'4', TINYINT'3', TINYINT'2', TINYINT'1', TINYINT'2', TINYINT'3', TINYINT'4', TINYINT'1', TINYINT'5')", TINYINT, (byte) 5);
        assertFunction("greatest(TINYINT'-1')", TINYINT, (byte) -1);
        assertFunction("greatest(TINYINT'5', TINYINT'4', CAST(NULL AS TINYINT), TINYINT'3')", TINYINT, null);

        // smallint
        assertFunction("greatest(SMALLINT'1', SMALLINT'2')", SMALLINT, (short) 2);
        assertFunction("greatest(SMALLINT'-1', SMALLINT'-2')", SMALLINT, (short) -1);
        assertFunction("greatest(SMALLINT'5', SMALLINT'4', SMALLINT'3', SMALLINT'2', SMALLINT'1', SMALLINT'2', SMALLINT'3', SMALLINT'4', SMALLINT'1', SMALLINT'5')", SMALLINT, (short) 5);
        assertFunction("greatest(SMALLINT'-1')", SMALLINT, (short) -1);
        assertFunction("greatest(SMALLINT'5', SMALLINT'4', CAST(NULL AS SMALLINT), SMALLINT'3')", SMALLINT, null);

        // integer
        assertFunction("greatest(1, 2)", INTEGER, 2);
        assertFunction("greatest(-1, -2)", INTEGER, -1);
        assertFunction("greatest(5, 4, 3, 2, 1, 2, 3, 4, 1, 5)", INTEGER, 5);
        assertFunction("greatest(-1)", INTEGER, -1);
        assertFunction("greatest(5, 4, CAST(NULL AS INTEGER), 3)", INTEGER, null);

        // bigint
        assertFunction("greatest(10000000000, 20000000000)", BIGINT, 20000000000L);
        assertFunction("greatest(-10000000000, -20000000000)", BIGINT, -10000000000L);
        assertFunction("greatest(5000000000, 4, 3, 2, 1000000000, 2, 3, 4, 1, 5000000000)", BIGINT, 5000000000L);
        assertFunction("greatest(-10000000000)", BIGINT, -10000000000L);
        assertFunction("greatest(5000000000, 4000000000, CAST(NULL as BIGINT), 3000000000)", BIGINT, null);

        // double
        assertFunction("greatest(1.5E0, 2.3E0)", DOUBLE, 2.3);
        assertFunction("greatest(-1.5E0, -2.3E0)", DOUBLE, -1.5);
        assertFunction("greatest(-1.5E0, -2.3E0, -5/3)", DOUBLE, -1.0);
        assertFunction("greatest(1.5E0, -infinity(), infinity())", DOUBLE, Double.POSITIVE_INFINITY);
        assertFunction("greatest(5, 4, CAST(NULL as DOUBLE), 3)", DOUBLE, null);
        assertFunction("greatest(NaN(), 5, 4, 3)", DOUBLE, 5.0);
        assertFunction("greatest(5, 4, NaN(), 3)", DOUBLE, 5.0);
        assertFunction("greatest(5, 4, 3, NaN())", DOUBLE, 5.0);
        assertFunction("greatest(NaN())", DOUBLE, Double.NaN);
        assertFunction("greatest(NaN(), NaN(), NaN())", DOUBLE, Double.NaN);

        // real
        assertFunction("greatest(REAL '1.5', REAL '2.3')", REAL, 2.3f);
        assertFunction("greatest(REAL '-1.5', REAL '-2.3')", REAL, -1.5f);
        assertFunction("greatest(REAL '-1.5', REAL '-2.3', CAST(-5/3 AS REAL))", REAL, -1.0f);
        assertFunction("greatest(REAL '1.5', CAST(infinity() AS REAL))", REAL, Float.POSITIVE_INFINITY);
        assertFunction("greatest(REAL '5', REAL '4', CAST(NULL as REAL), REAL '3')", REAL, null);
        assertFunction("greatest(CAST(NaN() as REAL), REAL '5', REAL '4', REAL '3')", REAL, 5.0f);
        assertFunction("greatest(REAL '5', REAL '4', CAST(NaN() as REAL), REAL '3')", REAL, 5.0f);
        assertFunction("greatest(REAL '5', REAL '4', REAL '3', CAST(NaN() as REAL))", REAL, 5.0f);
        assertFunction("greatest(CAST(NaN() as REAL))", REAL, Float.NaN);
        assertFunction("greatest(CAST(NaN() as REAL), CAST(NaN() as REAL), CAST(NaN() as REAL))", REAL, Float.NaN);

        // decimal
        assertDecimalFunction("greatest(1.0, 2.0)", decimal("2.0", createDecimalType(2, 1)));
        assertDecimalFunction("greatest(1.0, -2.0)", decimal("1.0", createDecimalType(2, 1)));
        assertDecimalFunction("greatest(1.0, 1.1, 1.2, 1.3)", decimal("1.3", createDecimalType(2, 1)));

        // mixed
        assertFunction("greatest(1, 20000000000)", BIGINT, 20000000000L);
        assertFunction("greatest(1, BIGINT '2')", BIGINT, 2L);
        assertFunction("greatest(1.0E0, 2)", DOUBLE, 2.0);
        assertFunction("greatest(1, 2.0E0)", DOUBLE, 2.0);
        assertFunction("greatest(1.0E0, 2)", DOUBLE, 2.0);
        assertFunction("greatest(5.0E0, 4, CAST(NULL as DOUBLE), 3)", DOUBLE, null);
        assertFunction("greatest(5.0E0, 4, CAST(NULL as BIGINT), 3)", DOUBLE, null);
        assertFunction("greatest(1.0, 2.0E0)", DOUBLE, 2.0);
        assertDecimalFunction("greatest(5, 4, 3.0, 2)", decimal("0000000005.0", createDecimalType(11, 1)));

        // argument count limit
        tryEvaluateWithAll("greatest(" + Joiner.on(", ").join(nCopies(127, "rand()")) + ")", DOUBLE);
        assertInvalidFunction(
                "greatest(" + Joiner.on(", ").join(nCopies(128, "rand()")) + ")",
                TOO_MANY_ARGUMENTS,
                "line 1:1: Too many arguments for function call greatest()");

        // row(double)
        assertFunction("greatest(ROW(1.5E0), ROW(2.3E0))", anonymousRow(DOUBLE), ImmutableList.of(2.3));
        assertFunction("greatest(ROW(-1.5E0), ROW(-2.3E0))", anonymousRow(DOUBLE), ImmutableList.of(-1.5));
        assertFunction("greatest(ROW(-1.5E0), ROW(-2.3E0), ROW(-5/3))", anonymousRow(DOUBLE), ImmutableList.of(-1.0));
        assertFunction("greatest(ROW(1.5E0), ROW(-infinity()), ROW(infinity()))", anonymousRow(DOUBLE), ImmutableList.of(Double.POSITIVE_INFINITY));
        assertFunction("greatest(ROW(5), ROW(4), CAST(NULL as ROW(DOUBLE)), ROW(3))", anonymousRow(DOUBLE), null);
        assertFunction("greatest(ROW(NaN()), ROW(5), ROW(4), ROW(3))", anonymousRow(DOUBLE), ImmutableList.of(5.0));
        assertFunction("greatest(ROW(5), ROW(4), ROW(NaN()), ROW(3))", anonymousRow(DOUBLE), ImmutableList.of(5.0));
        assertFunction("greatest(ROW(5), ROW(4), ROW(3), ROW(NaN()))", anonymousRow(DOUBLE), ImmutableList.of(5.0));
        assertFunction("greatest(ROW(NaN()))", anonymousRow(DOUBLE), ImmutableList.of(Double.NaN));
        assertFunction("greatest(ROW(NaN()), ROW(NaN()), ROW(NaN()))", anonymousRow(DOUBLE), ImmutableList.of(Double.NaN));

        // row(real)
        assertFunction("greatest(ROW(REAL '1.5'), ROW(REAL '2.3'))", anonymousRow(REAL), ImmutableList.of(2.3f));
        assertFunction("greatest(ROW(REAL '-1.5'), ROW(REAL '-2.3'))", anonymousRow(REAL), ImmutableList.of(-1.5f));
        assertFunction("greatest(ROW(REAL '-1.5'), ROW(REAL '-2.3'), ROW(CAST(-5/3 AS REAL)))", anonymousRow(REAL), ImmutableList.of(-1.0f));
        assertFunction("greatest(ROW(REAL '1.5'), ROW(CAST(infinity() AS REAL)))", anonymousRow(REAL), ImmutableList.of(Float.POSITIVE_INFINITY));
        assertFunction("greatest(ROW(REAL '5'), ROW(REAL '4'), CAST(NULL as ROW(REAL)), ROW(REAL '3'))", anonymousRow(REAL), null);
        assertFunction("greatest(ROW(CAST(NaN() as REAL)), ROW(REAL '5'), ROW(REAL '4'), ROW(REAL '3'))", anonymousRow(REAL), ImmutableList.of(5.0f));
        assertFunction("greatest(ROW(REAL '5'), ROW(REAL '4'), ROW(CAST(NaN() as REAL)), ROW(REAL '3'))", anonymousRow(REAL), ImmutableList.of(5.0f));
        assertFunction("greatest(ROW(REAL '5'), ROW(REAL '4'), ROW(REAL '3'), ROW(CAST(NaN() as REAL)))", anonymousRow(REAL), ImmutableList.of(5.0f));
        assertFunction("greatest(ROW(CAST(NaN() as REAL)))", anonymousRow(REAL), ImmutableList.of(Float.NaN));
        assertFunction("greatest(ROW(CAST(NaN() as REAL)), ROW(CAST(NaN() as REAL)), ROW(CAST(NaN() as REAL)))", anonymousRow(REAL), ImmutableList.of(Float.NaN));

        // row(double)
        assertFunction("greatest(ARRAY[1.5E0], ARRAY[2.3E0])", new ArrayType(DOUBLE), ImmutableList.of(2.3));
        assertFunction("greatest(ARRAY[-1.5E0], ARRAY[-2.3E0])", new ArrayType(DOUBLE), ImmutableList.of(-1.5));
        assertFunction("greatest(ARRAY[-1.5E0], ARRAY[-2.3E0], ARRAY[-5/3])", new ArrayType(DOUBLE), ImmutableList.of(-1.0));
        assertFunction("greatest(ARRAY[1.5E0], ARRAY[-infinity()], ARRAY[infinity()])", new ArrayType(DOUBLE), ImmutableList.of(Double.POSITIVE_INFINITY));
        assertFunction("greatest(ARRAY[5], ARRAY[4], CAST(NULL as ARRAY(DOUBLE)), ARRAY[3])", new ArrayType(DOUBLE), null);
        assertFunction("greatest(ARRAY[NaN()], ARRAY[5], ARRAY[4], ARRAY[3])", new ArrayType(DOUBLE), ImmutableList.of(5.0));
        assertFunction("greatest(ARRAY[5], ARRAY[4], ARRAY[NaN()], ARRAY[3])", new ArrayType(DOUBLE), ImmutableList.of(5.0));
        assertFunction("greatest(ARRAY[5], ARRAY[4], ARRAY[3], ARRAY[NaN()])", new ArrayType(DOUBLE), ImmutableList.of(5.0));
        assertFunction("greatest(ARRAY[NaN()])", new ArrayType(DOUBLE), ImmutableList.of(Double.NaN));
        assertFunction("greatest(ARRAY[NaN()], ARRAY[NaN()], ARRAY[NaN()])", new ArrayType(DOUBLE), ImmutableList.of(Double.NaN));

        // row(real)
        assertFunction("greatest(ARRAY[REAL '1.5'], ARRAY[REAL '2.3'])", new ArrayType(REAL), ImmutableList.of(2.3f));
        assertFunction("greatest(ARRAY[REAL '-1.5'], ARRAY[REAL '-2.3'])", new ArrayType(REAL), ImmutableList.of(-1.5f));
        assertFunction("greatest(ARRAY[REAL '-1.5'], ARRAY[REAL '-2.3'], ARRAY[CAST(-5/3 AS REAL)])", new ArrayType(REAL), ImmutableList.of(-1.0f));
        assertFunction("greatest(ARRAY[REAL '1.5'], ARRAY[CAST(infinity() AS REAL)])", new ArrayType(REAL), ImmutableList.of(Float.POSITIVE_INFINITY));
        assertFunction("greatest(ARRAY[REAL '5'], ARRAY[REAL '4'], CAST(NULL as ARRAY(REAL)), ARRAY[REAL '3'])", new ArrayType(REAL), null);
        assertFunction("greatest(ARRAY[CAST(NaN() as REAL)], ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[REAL '3'])", new ArrayType(REAL), ImmutableList.of(5.0f));
        assertFunction("greatest(ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[CAST(NaN() as REAL)], ARRAY[REAL '3'])", new ArrayType(REAL), ImmutableList.of(5.0f));
        assertFunction("greatest(ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[REAL '3'], ARRAY[CAST(NaN() as REAL)])", new ArrayType(REAL), ImmutableList.of(5.0f));
        assertFunction("greatest(ARRAY[CAST(NaN() as REAL)])", new ArrayType(REAL), ImmutableList.of(Float.NaN));
        assertFunction("greatest(ARRAY[CAST(NaN() as REAL)], ARRAY[CAST(NaN() as REAL)], ARRAY[CAST(NaN() as REAL)])", new ArrayType(REAL), ImmutableList.of(Float.NaN));
    }

    @Test
    public void testLeast()
    {
        // integer
        assertFunction("least(TINYINT'1', TINYINT'2')", TINYINT, (byte) 1);
        assertFunction("least(TINYINT'-1', TINYINT'-2')", TINYINT, (byte) -2);
        assertFunction("least(TINYINT'5', TINYINT'4', TINYINT'3', TINYINT'2', TINYINT'1', TINYINT'2', TINYINT'3', TINYINT'4', TINYINT'1', TINYINT'5')", TINYINT, (byte) 1);
        assertFunction("least(TINYINT'-1')", TINYINT, (byte) -1);
        assertFunction("least(TINYINT'5', TINYINT'4', CAST(NULL AS TINYINT), TINYINT'3')", TINYINT, null);

        // integer
        assertFunction("least(SMALLINT'1', SMALLINT'2')", SMALLINT, (short) 1);
        assertFunction("least(SMALLINT'-1', SMALLINT'-2')", SMALLINT, (short) -2);
        assertFunction("least(SMALLINT'5', SMALLINT'4', SMALLINT'3', SMALLINT'2', SMALLINT'1', SMALLINT'2', SMALLINT'3', SMALLINT'4', SMALLINT'1', SMALLINT'5')", SMALLINT, (short) 1);
        assertFunction("least(SMALLINT'-1')", SMALLINT, (short) -1);
        assertFunction("least(SMALLINT'5', SMALLINT'4', CAST(NULL AS SMALLINT), SMALLINT'3')", SMALLINT, null);

        // integer
        assertFunction("least(1, 2)", INTEGER, 1);
        assertFunction("least(-1, -2)", INTEGER, -2);
        assertFunction("least(5, 4, 3, 2, 1, 2, 3, 4, 1, 5)", INTEGER, 1);
        assertFunction("least(-1)", INTEGER, -1);
        assertFunction("least(5, 4, CAST(NULL AS INTEGER), 3)", INTEGER, null);

        // bigint
        assertFunction("least(10000000000, 20000000000)", BIGINT, 10000000000L);
        assertFunction("least(-10000000000, -20000000000)", BIGINT, -20000000000L);
        assertFunction("least(50000000000, 40000000000, 30000000000, 20000000000, 50000000000)", BIGINT, 20000000000L);
        assertFunction("least(-10000000000)", BIGINT, -10000000000L);
        assertFunction("least(500000000, 400000000, CAST(NULL as BIGINT), 300000000)", BIGINT, null);

        // double
        assertFunction("least(1.5E0, 2.3E0)", DOUBLE, 1.5);
        assertFunction("least(-1.5E0, -2.3E0)", DOUBLE, -2.3);
        assertFunction("least(-1.5E0, -2.3E0, -5/3)", DOUBLE, -2.3);
        assertFunction("least(1.5E0, -infinity(), infinity())", DOUBLE, Double.NEGATIVE_INFINITY);
        assertFunction("least(5, 4, CAST(NULL as DOUBLE), 3)", DOUBLE, null);
        assertFunction("least(NaN(), 5, 4, 3)", DOUBLE, 3.0);
        assertFunction("least(5, 4, NaN(), 3)", DOUBLE, 3.0);
        assertFunction("least(5, 4, 3, NaN())", DOUBLE, 3.0);
        assertFunction("least(NaN())", DOUBLE, Double.NaN);
        assertFunction("least(NaN(), NaN(), NaN())", DOUBLE, Double.NaN);

        // real
        assertFunction("least(REAL '1.5', REAL '2.3')", REAL, 1.5f);
        assertFunction("least(REAL '-1.5', REAL '-2.3')", REAL, -2.3f);
        assertFunction("least(REAL '-1.5', REAL '-2.3', CAST(-5/3 AS REAL))", REAL, -2.3f);
        assertFunction("least(REAL '1.5', CAST(-infinity() AS REAL))", REAL, Float.NEGATIVE_INFINITY);
        assertFunction("least(REAL '5', REAL '4', CAST(NULL as REAL), REAL '3')", REAL, null);
        assertFunction("least(CAST(NaN() as REAL), REAL '5', REAL '4', REAL '3')", REAL, 3.0f);
        assertFunction("least(REAL '5', REAL '4', CAST(NaN() as REAL), REAL '3')", REAL, 3.0f);
        assertFunction("least(REAL '5', REAL '4', REAL '3', CAST(NaN() as REAL))", REAL, 3.0f);
        assertFunction("least(CAST(NaN() as REAL))", REAL, Float.NaN);
        assertFunction("least(CAST(NaN() as REAL), CAST(NaN() as REAL), CAST(NaN() as REAL))", REAL, Float.NaN);

        // decimal
        assertDecimalFunction("least(1.0, 2.0)", decimal("1.0", createDecimalType(2, 1)));
        assertDecimalFunction("least(1.0, -2.0)", decimal("-2.0", createDecimalType(2, 1)));
        assertDecimalFunction("least(1.0, 1.1, 1.2, 1.3)", decimal("1.0", createDecimalType(2, 1)));

        // mixed
        assertFunction("least(1, 20000000000)", BIGINT, 1L);
        assertFunction("least(1, BIGINT '2')", BIGINT, 1L);
        assertFunction("least(1.0E0, 2)", DOUBLE, 1.0);
        assertFunction("least(1, 2.0E0)", DOUBLE, 1.0);
        assertFunction("least(1.0E0, 2)", DOUBLE, 1.0);
        assertFunction("least(5.0E0, 4, CAST(NULL as DOUBLE), 3)", DOUBLE, null);
        assertFunction("least(5.0E0, 4, CAST(NULL as BIGINT), 3)", DOUBLE, null);
        assertFunction("least(1.0, 2.0E0)", DOUBLE, 1.0);
        assertDecimalFunction("least(5, 4, 3.0, 2)", decimal("0000000002.0", createDecimalType(11, 1)));

        // row(double)
        assertFunction("least(ROW(1.5E0), ROW(2.3E0))", anonymousRow(DOUBLE), ImmutableList.of(1.5));
        assertFunction("least(ROW(-1.5E0), ROW(-2.3E0))", anonymousRow(DOUBLE), ImmutableList.of(-2.3));
        assertFunction("least(ROW(-1.5E0), ROW(-2.3E0), ROW(-5/3))", anonymousRow(DOUBLE), ImmutableList.of(-2.3));
        assertFunction("least(ROW(1.5E0), ROW(-infinity()), ROW(infinity()))", anonymousRow(DOUBLE), ImmutableList.of(Double.NEGATIVE_INFINITY));
        assertFunction("least(ROW(5), ROW(4), CAST(NULL as ROW(DOUBLE)), ROW(3))", anonymousRow(DOUBLE), null);
        assertFunction("least(ROW(NaN()), ROW(5), ROW(4), ROW(3))", anonymousRow(DOUBLE), ImmutableList.of(3.0));
        assertFunction("least(ROW(5), ROW(4), ROW(NaN()), ROW(3))", anonymousRow(DOUBLE), ImmutableList.of(3.0));
        assertFunction("least(ROW(5), ROW(4), ROW(3), ROW(NaN()))", anonymousRow(DOUBLE), ImmutableList.of(3.0));
        assertFunction("least(ROW(NaN()))", anonymousRow(DOUBLE), ImmutableList.of(Double.NaN));
        assertFunction("least(ROW(NaN()), ROW(NaN()), ROW(NaN()))", anonymousRow(DOUBLE), ImmutableList.of(Double.NaN));

        // row(real)
        assertFunction("least(ROW(REAL '1.5'), ROW(REAL '2.3'))", anonymousRow(REAL), ImmutableList.of(1.5f));
        assertFunction("least(ROW(REAL '-1.5'), ROW(REAL '-2.3'))", anonymousRow(REAL), ImmutableList.of(-2.3f));
        assertFunction("least(ROW(REAL '-1.5'), ROW(REAL '-2.3'), ROW(CAST(-5/3 AS REAL)))", anonymousRow(REAL), ImmutableList.of(-2.3f));
        assertFunction("least(ROW(REAL '1.5'), ROW(CAST(-infinity() AS REAL)), ROW(CAST(infinity() AS REAL)))", anonymousRow(REAL), ImmutableList.of(Float.NEGATIVE_INFINITY));
        assertFunction("least(ROW(REAL '5'), ROW(REAL '4'), CAST(NULL as ROW(REAL)), ROW(REAL '3'))", anonymousRow(REAL), null);
        assertFunction("least(ROW(CAST(NaN() as REAL)), ROW(REAL '5'), ROW(REAL '4'), ROW(REAL '3'))", anonymousRow(REAL), ImmutableList.of(3.0f));
        assertFunction("least(ROW(REAL '5'), ROW(REAL '4'), ROW(CAST(NaN() as REAL)), ROW(REAL '3'))", anonymousRow(REAL), ImmutableList.of(3.0f));
        assertFunction("least(ROW(REAL '5'), ROW(REAL '4'), ROW(REAL '3'), ROW(CAST(NaN() as REAL)))", anonymousRow(REAL), ImmutableList.of(3.0f));
        assertFunction("least(ROW(CAST(NaN() as REAL)))", anonymousRow(REAL), ImmutableList.of(Float.NaN));
        assertFunction("least(ROW(CAST(NaN() as REAL)), ROW(CAST(NaN() as REAL)), ROW(CAST(NaN() as REAL)))", anonymousRow(REAL), ImmutableList.of(Float.NaN));

        // row(double)
        assertFunction("least(ARRAY[1.5E0], ARRAY[2.3E0])", new ArrayType(DOUBLE), ImmutableList.of(1.5));
        assertFunction("least(ARRAY[-1.5E0], ARRAY[-2.3E0])", new ArrayType(DOUBLE), ImmutableList.of(-2.3));
        assertFunction("least(ARRAY[-1.5E0], ARRAY[-2.3E0], ARRAY[-5/3])", new ArrayType(DOUBLE), ImmutableList.of(-2.3));
        assertFunction("least(ARRAY[1.5E0], ARRAY[-infinity()], ARRAY[infinity()])", new ArrayType(DOUBLE), ImmutableList.of(Double.NEGATIVE_INFINITY));
        assertFunction("least(ARRAY[5], ARRAY[4], CAST(NULL as ARRAY(DOUBLE)), ARRAY[3])", new ArrayType(DOUBLE), null);
        assertFunction("least(ARRAY[NaN()], ARRAY[5], ARRAY[4], ARRAY[3])", new ArrayType(DOUBLE), ImmutableList.of(3.0));
        assertFunction("least(ARRAY[5], ARRAY[4], ARRAY[NaN()], ARRAY[3])", new ArrayType(DOUBLE), ImmutableList.of(3.0));
        assertFunction("least(ARRAY[5], ARRAY[4], ARRAY[3], ARRAY[NaN()])", new ArrayType(DOUBLE), ImmutableList.of(3.0));
        assertFunction("least(ARRAY[NaN()])", new ArrayType(DOUBLE), ImmutableList.of(Double.NaN));
        assertFunction("least(ARRAY[NaN()], ARRAY[NaN()], ARRAY[NaN()])", new ArrayType(DOUBLE), ImmutableList.of(Double.NaN));

        // row(real)
        assertFunction("least(ARRAY[REAL '1.5'], ARRAY[REAL '2.3'])", new ArrayType(REAL), ImmutableList.of(1.5f));
        assertFunction("least(ARRAY[REAL '-1.5'], ARRAY[REAL '-2.3'])", new ArrayType(REAL), ImmutableList.of(-2.3f));
        assertFunction("least(ARRAY[REAL '-1.5'], ARRAY[REAL '-2.3'], ARRAY[CAST(-5/3 AS REAL)])", new ArrayType(REAL), ImmutableList.of(-2.3f));
        assertFunction("least(ARRAY[REAL '1.5'], ARRAY[CAST(-infinity() AS REAL)], ARRAY[CAST(infinity() AS REAL)])", new ArrayType(REAL), ImmutableList.of(Float.NEGATIVE_INFINITY));
        assertFunction("least(ARRAY[REAL '5'], ARRAY[REAL '4'], CAST(NULL as ARRAY(REAL)), ARRAY[REAL '3'])", new ArrayType(REAL), null);
        assertFunction("least(ARRAY[CAST(NaN() as REAL)], ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[REAL '3'])", new ArrayType(REAL), ImmutableList.of(3.0f));
        assertFunction("least(ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[CAST(NaN() as REAL)], ARRAY[REAL '3'])", new ArrayType(REAL), ImmutableList.of(3.0f));
        assertFunction("least(ARRAY[REAL '5'], ARRAY[REAL '4'], ARRAY[REAL '3'], ARRAY[CAST(NaN() as REAL)])", new ArrayType(REAL), ImmutableList.of(3.0f));
        assertFunction("least(ARRAY[CAST(NaN() as REAL)])", new ArrayType(REAL), ImmutableList.of(Float.NaN));
        assertFunction("least(ARRAY[CAST(NaN() as REAL)], ARRAY[CAST(NaN() as REAL)], ARRAY[CAST(NaN() as REAL)])", new ArrayType(REAL), ImmutableList.of(Float.NaN));
    }

    @Test
    public void testToBase()
    {
        VarcharType toBaseReturnType = VarcharType.createVarcharType(64);
        assertFunction("to_base(2147483648, 16)", toBaseReturnType, "80000000");
        assertFunction("to_base(255, 2)", toBaseReturnType, "11111111");
        assertFunction("to_base(-2147483647, 16)", toBaseReturnType, "-7fffffff");
        assertFunction("to_base(NULL, 16)", toBaseReturnType, null);
        assertFunction("to_base(-2147483647, NULL)", toBaseReturnType, null);
        assertFunction("to_base(NULL, NULL)", toBaseReturnType, null);
        assertInvalidFunction("to_base(255, 1)", "Radix must be between 2 and 36");
    }

    @Test
    public void testFromBase()
    {
        assertFunction("from_base('80000000', 16)", BIGINT, 2147483648L);
        assertFunction("from_base('11111111', 2)", BIGINT, 255L);
        assertFunction("from_base('-7fffffff', 16)", BIGINT, -2147483647L);
        assertFunction("from_base('9223372036854775807', 10)", BIGINT, 9223372036854775807L);
        assertFunction("from_base('-9223372036854775808', 10)", BIGINT, -9223372036854775808L);
        assertFunction("from_base(NULL, 10)", BIGINT, null);
        assertFunction("from_base('-9223372036854775808', NULL)", BIGINT, null);
        assertFunction("from_base(NULL, NULL)", BIGINT, null);
        assertInvalidFunction("from_base('Z', 37)", "Radix must be between 2 and 36");
        assertInvalidFunction("from_base('Z', 35)", "Not a valid base-35 number: Z");
        assertInvalidFunction("from_base('9223372036854775808', 10)", "Not a valid base-10 number: 9223372036854775808");
        assertInvalidFunction("from_base('Z', 37)", "Radix must be between 2 and 36");
        assertInvalidFunction("from_base('Z', 35)", "Not a valid base-35 number: Z");
        assertInvalidFunction("from_base('9223372036854775808', 10)", "Not a valid base-10 number: 9223372036854775808");
    }

    @Test
    public void testWidthBucket()
    {
        assertFunction("width_bucket(3.14E0, 0, 4, 3)", BIGINT, 3L);
        assertFunction("width_bucket(2, 0, 4, 3)", BIGINT, 2L);
        assertFunction("width_bucket(infinity(), 0, 4, 3)", BIGINT, 4L);
        assertFunction("width_bucket(-1, 0, 3.2E0, 4)", BIGINT, 0L);

        // bound1 > bound2 is not symmetric with bound2 > bound1
        assertFunction("width_bucket(3.14E0, 4, 0, 3)", BIGINT, 1L);
        assertFunction("width_bucket(2, 4, 0, 3)", BIGINT, 2L);
        assertFunction("width_bucket(infinity(), 4, 0, 3)", BIGINT, 0L);
        assertFunction("width_bucket(-1, 3.2E0, 0, 4)", BIGINT, 5L);

        // failure modes
        assertInvalidFunction("width_bucket(3.14E0, 0, 4, 0)", "bucketCount must be greater than 0");
        assertInvalidFunction("width_bucket(3.14E0, 0, 4, -1)", "bucketCount must be greater than 0");
        assertInvalidFunction("width_bucket(nan(), 0, 4, 3)", "operand must not be NaN");
        assertInvalidFunction("width_bucket(3.14E0, -1, -1, 3)", "bounds cannot equal each other");
        assertInvalidFunction("width_bucket(3.14E0, nan(), -1, 3)", "first bound must be finite");
        assertInvalidFunction("width_bucket(3.14E0, -1, nan(), 3)", "second bound must be finite");
        assertInvalidFunction("width_bucket(3.14E0, infinity(), -1, 3)", "first bound must be finite");
        assertInvalidFunction("width_bucket(3.14E0, -1, infinity(), 3)", "second bound must be finite");
    }

    @Test
    public void testWidthBucketOverflowAscending()
    {
        assertInvalidFunction("width_bucket(infinity(), 0, 4, " + Long.MAX_VALUE + ")", NUMERIC_VALUE_OUT_OF_RANGE, "Bucket for value Infinity is out of range");
        assertInvalidFunction("width_bucket(CAST(infinity() as REAL), 0, 4, " + Long.MAX_VALUE + ")", NUMERIC_VALUE_OUT_OF_RANGE, "Bucket for value Infinity is out of range");
    }

    @Test
    public void testWidthBucketOverflowDescending()
    {
        assertInvalidFunction("width_bucket(infinity(), 4, 0, " + Long.MAX_VALUE + ")", NUMERIC_VALUE_OUT_OF_RANGE, "Bucket for value Infinity is out of range");
        assertInvalidFunction("width_bucket(CAST(infinity() as REAL), 4, 0, " + Long.MAX_VALUE + ")", NUMERIC_VALUE_OUT_OF_RANGE, "Bucket for value Infinity is out of range");
    }

    @Test
    public void testWidthBucketArray()
    {
        assertFunction("width_bucket(3.14E0, array[0.0E0, 2.0E0, 4.0E0])", BIGINT, 2L);
        assertFunction("width_bucket(infinity(), array[0.0E0, 2.0E0, 4.0E0])", BIGINT, 3L);
        assertFunction("width_bucket(-1, array[0.0E0, 1.2E0, 3.3E0, 4.5E0])", BIGINT, 0L);

        // edge case of only a single bin
        assertFunction("width_bucket(3.145E0, array[0.0E0])", BIGINT, 1L);
        assertFunction("width_bucket(-3.145E0, array[0.0E0])", BIGINT, 0L);

        // failure modes
        assertInvalidFunction("width_bucket(3.14E0, array[])", "Bins cannot be an empty array");
        assertInvalidFunction("width_bucket(nan(), array[1.0E0, 2.0E0, 3.0E0])", "Operand cannot be NaN");
        assertInvalidFunction("width_bucket(3.14E0, array[0.0E0, infinity()])", "Bin value must be finite, got Infinity");

        // fail if we aren't sorted
        assertInvalidFunction("width_bucket(3.145E0, array[1.0E0, 0.0E0])", "Bin values are not sorted in ascending order");
        assertInvalidFunction("width_bucket(3.145E0, array[1.0E0, 0.0E0, -1.0E0])", "Bin values are not sorted in ascending order");
        assertInvalidFunction("width_bucket(3.145E0, array[1.0E0, 0.3E0, 0.0E0, -1.0E0])", "Bin values are not sorted in ascending order");

        // this is a case that we can't catch because we are using binary search to bisect the bins array
        assertFunction("width_bucket(1.5E0, array[1.0E0, 2.3E0, 2.0E0])", BIGINT, 1L);
    }

    @Test
    public void testCosineSimilarity()
    {
        assertFunction("cosine_similarity(map(array ['a', 'b'], array [1.0E0, 2.0E0]), map(array ['c', 'b'], array [1.0E0, 3.0E0]))",
                DOUBLE,
                2 * 3 / (Math.sqrt(5) * Math.sqrt(10)));

        assertFunction("cosine_similarity(map(array ['a', 'b', 'c'], array [1.0E0, 2.0E0, -1.0E0]), map(array ['c', 'b'], array [1.0E0, 3.0E0]))",
                DOUBLE,
                (2 * 3 + (-1) * 1) / (Math.sqrt(1 + 4 + 1) * Math.sqrt(1 + 9)));

        assertFunction("cosine_similarity(map(array ['a', 'b', 'c'], array [1.0E0, 2.0E0, -1.0E0]), map(array ['d', 'e'], array [1.0E0, 3.0E0]))",
                DOUBLE,
                0.0);

        assertFunction("cosine_similarity(null, map(array ['c', 'b'], array [1.0E0, 3.0E0]))",
                DOUBLE,
                null);

        assertFunction("cosine_similarity(map(array ['a', 'b'], array [1.0E0, null]), map(array ['c', 'b'], array [1.0E0, 3.0E0]))",
                DOUBLE,
                null);
    }

    @Test
    public void testInverseNormalCdf()
    {
        assertFunction("inverse_normal_cdf(0, 1, 0.3)", DOUBLE, -0.52440051270804089);
        assertFunction("inverse_normal_cdf(10, 9, 0.9)", DOUBLE, 21.533964089901406);
        assertFunction("inverse_normal_cdf(0.5, 0.25, 0.65)", DOUBLE, 0.59633011660189195);
        assertInvalidFunction("inverse_normal_cdf(4, 48, 0)", "p must be 0 > p > 1");
        assertInvalidFunction("inverse_normal_cdf(4, 48, 1)", "p must be 0 > p > 1");
        assertInvalidFunction("inverse_normal_cdf(4, 0, 0.4)", "sd must be > 0");
    }

    @Test
    public void testNormalCdf()
    {
        assertFunction("normal_cdf(0, 1, 1.96)", DOUBLE, 0.9750021048517796);
        assertFunction("normal_cdf(10, 9, 10)", DOUBLE, 0.5);
        assertFunction("normal_cdf(-1.5, 2.1, -7.8)", DOUBLE, 0.0013498980316301035);
        assertFunction("normal_cdf(0, 1, infinity())", DOUBLE, 1.0);
        assertFunction("normal_cdf(0, 1, -infinity())", DOUBLE, 0.0);
        assertFunction("normal_cdf(infinity(), 1, 0)", DOUBLE, 0.0);
        assertFunction("normal_cdf(-infinity(), 1, 0)", DOUBLE, 1.0);
        assertFunction("normal_cdf(0, infinity(), 0)", DOUBLE, 0.5);
        assertFunction("normal_cdf(nan(), 1, 0)", DOUBLE, Double.NaN);
        assertFunction("normal_cdf(0, 1, nan())", DOUBLE, Double.NaN);

        assertInvalidFunction("normal_cdf(0, 0, 0.1985)", "standardDeviation must be > 0");
        assertInvalidFunction("normal_cdf(0, nan(), 0.1985)", "standardDeviation must be > 0");
    }

    @Test
    public void testInverseBetaCdf()
    {
        assertFunction("inverse_beta_cdf(3, 3.6, 0.0)", DOUBLE, 0.0);
        assertFunction("inverse_beta_cdf(3, 3.6, 1.0)", DOUBLE, 1.0);
        assertFunction("inverse_beta_cdf(3, 3.6, 0.3)", DOUBLE, 0.3469675485440618);
        assertFunction("inverse_beta_cdf(3, 3.6, 0.95)", DOUBLE, 0.7600272463100223);

        assertInvalidFunction("inverse_beta_cdf(0, 3, 0.5)", "a, b must be > 0");
        assertInvalidFunction("inverse_beta_cdf(3, 0, 0.5)", "a, b must be > 0");
        assertInvalidFunction("inverse_beta_cdf(3, 5, -0.1)", "p must be 0 >= p >= 1");
        assertInvalidFunction("inverse_beta_cdf(3, 5, 1.1)", "p must be 0 >= p >= 1");
    }

    @Test
    public void testBetaCdf()
    {
        assertFunction("beta_cdf(3, 3.6, 0.0)", DOUBLE, 0.0);
        assertFunction("beta_cdf(3, 3.6, 1.0)", DOUBLE, 1.0);
        assertFunction("beta_cdf(3, 3.6, 0.3)", DOUBLE, 0.21764809997679938);
        assertFunction("beta_cdf(3, 3.6, 0.9)", DOUBLE, 0.9972502881611551);

        assertInvalidFunction("beta_cdf(0, 3, 0.5)", "a, b must be > 0");
        assertInvalidFunction("beta_cdf(3, 0, 0.5)", "a, b must be > 0");
        assertInvalidFunction("beta_cdf(3, 5, -0.1)", "value must be 0 >= v >= 1");
        assertInvalidFunction("beta_cdf(3, 5, 1.1)", "value must be 0 >= v >= 1");
    }

    @Test
    public void testWilsonInterval()
    {
        assertInvalidFunction("wilson_interval_lower(-1, 100, 2.575)", "number of successes must not be negative");
        assertInvalidFunction("wilson_interval_lower(0, 0, 2.575)", "number of trials must be positive");
        assertInvalidFunction("wilson_interval_lower(10, 5, 2.575)", "number of successes must not be larger than number of trials");
        assertInvalidFunction("wilson_interval_lower(0, 100, -1)", "z-score must not be negative");

        assertFunction("wilson_interval_lower(1250, 1310, 1.96e0)", DOUBLE, 0.9414883725395894);

        assertInvalidFunction("wilson_interval_upper(-1, 100, 2.575)", "number of successes must not be negative");
        assertInvalidFunction("wilson_interval_upper(0, 0, 2.575)", "number of trials must be positive");
        assertInvalidFunction("wilson_interval_upper(10, 5, 2.575)", "number of successes must not be larger than number of trials");
        assertInvalidFunction("wilson_interval_upper(0, 100, -1)", "z-score must not be negative");

        assertFunction("wilson_interval_upper(1250, 1310, 1.96e0)", DOUBLE, 0.9642524717143908);
    }
}
