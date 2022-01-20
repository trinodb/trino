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
package io.trino.type;

import io.trino.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Float.isNaN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRealOperators
        extends AbstractTestFunctions
{
    @Test
    public void testTypeConstructor()
    {
        assertFunction("REAL '12.2'", REAL, 12.2f);
        assertFunction("REAL '-17.76'", REAL, -17.76f);
        assertFunction("REAL 'NaN'", REAL, Float.NaN);
        assertFunction("REAL '-NaN'", REAL, Float.NaN);
        assertFunction("REAL 'Infinity'", REAL, Float.POSITIVE_INFINITY);
        assertFunction("REAL '-Infinity'", REAL, Float.NEGATIVE_INFINITY);
    }

    @Test
    public void testAdd()
    {
        assertFunction("REAL '12.34' + REAL '56.78'", REAL, 12.34f + 56.78f);
        assertFunction("REAL '-17.34' + REAL '-22.891'", REAL, -17.34f + -22.891f);
        assertFunction("REAL '-89.123' + REAL '754.0'", REAL, -89.123f + 754.0f);
        assertFunction("REAL '-0.0' + REAL '0.0'", REAL, -0.0f + 0.0f);
        assertFunction("REAL 'NaN' + REAL '1.23'", REAL, Float.NaN);
        assertFunction("REAL '1.23' + REAL 'NaN'", REAL, Float.NaN);
        assertFunction("REAL 'NaN' + REAL '-NaN'", REAL, Float.NaN);
    }

    @Test
    public void testSubtract()
    {
        assertFunction("REAL '12.34' - REAL '56.78'", REAL, 12.34f - 56.78f);
        assertFunction("REAL '-17.34' - REAL '-22.891'", REAL, -17.34f - -22.891f);
        assertFunction("REAL '-89.123' - REAL '754.0'", REAL, -89.123f - 754.0f);
        assertFunction("REAL '-0.0' - REAL '0.0'", REAL, -0.0f - 0.0f);
        assertFunction("REAL 'NaN' - REAL '1.23'", REAL, Float.NaN);
        assertFunction("REAL '1.23' - REAL 'NaN'", REAL, Float.NaN);
        assertFunction("REAL 'NaN' - REAL 'NaN'", REAL, Float.NaN);
    }

    @Test
    public void testMultiply()
    {
        assertFunction("REAL '12.34' * REAL '56.78'", REAL, 12.34f * 56.78f);
        assertFunction("REAL '-17.34' * REAL '-22.891'", REAL, -17.34f * -22.891f);
        assertFunction("REAL '-89.123' * REAL '754.0'", REAL, -89.123f * 754.0f);
        assertFunction("REAL '-0.0' * REAL '0.0'", REAL, -0.0f * 0.0f);
        assertFunction("REAL '-17.71' * REAL '-1.0'", REAL, -17.71f * -1.0f);
        assertFunction("REAL 'NaN' * REAL '1.23'", REAL, Float.NaN);
        assertFunction("REAL '1.23' * REAL 'NaN'", REAL, Float.NaN);
        assertFunction("REAL 'NaN' * REAL '-NaN'", REAL, Float.NaN);
    }

    @Test
    public void testDivide()
    {
        assertFunction("REAL '12.34' / REAL '56.78'", REAL, 12.34f / 56.78f);
        assertFunction("REAL '-17.34' / REAL '-22.891'", REAL, -17.34f / -22.891f);
        assertFunction("REAL '-89.123' / REAL '754.0'", REAL, -89.123f / 754.0f);
        assertFunction("REAL '-0.0' / REAL '0.0'", REAL, -0.0f / 0.0f);
        assertFunction("REAL '-17.71' / REAL '-1.0'", REAL, -17.71f / -1.0f);
        assertFunction("REAL 'NaN' / REAL '1.23'", REAL, Float.NaN);
        assertFunction("REAL '1.23' / REAL 'NaN'", REAL, Float.NaN);
        assertFunction("REAL 'NaN' / REAL '-NaN'", REAL, Float.NaN);
    }

    @Test
    public void testModulus()
    {
        assertFunction("REAL '12.34' % REAL '56.78'", REAL, 12.34f % 56.78f);
        assertFunction("REAL '-17.34' % REAL '-22.891'", REAL, -17.34f % -22.891f);
        assertFunction("REAL '-89.123' % REAL '754.0'", REAL, -89.123f % 754.0f);
        assertFunction("REAL '-0.0' % REAL '0.0'", REAL, -0.0f % 0.0f);
        assertFunction("REAL '-17.71' % REAL '-1.0'", REAL, -17.71f % -1.0f);
        assertFunction("REAL 'NaN' % REAL '1.23'", REAL, Float.NaN);
        assertFunction("REAL '1.23' % REAL 'NaN'", REAL, Float.NaN);
        assertFunction("REAL 'NaN' % REAL 'NaN'", REAL, Float.NaN);
    }

    @Test
    public void testNegation()
    {
        assertFunction("-REAL '12.34'", REAL, -12.34f);
        assertFunction("-REAL '-17.34'", REAL, 17.34f);
        assertFunction("-REAL '-0.0'", REAL, -(-0.0f));
        assertFunction("-REAL 'NaN'", REAL, Float.NaN);
        assertFunction("-REAL '-NaN'", REAL, Float.NaN);
    }

    @Test
    public void testEqual()
    {
        assertFunction("REAL '12.34' = REAL '12.34'", BOOLEAN, true);
        assertFunction("REAL '12.340' = REAL '12.34'", BOOLEAN, true);
        assertFunction("REAL '-17.34' = REAL '-17.34'", BOOLEAN, true);
        assertFunction("REAL '71.17' = REAL '23.45'", BOOLEAN, false);
        assertFunction("REAL '-0.0' = REAL '0.0'", BOOLEAN, true);
        assertFunction("REAL 'NaN' = REAL '1.23'", BOOLEAN, false);
        assertFunction("REAL '1.23' = REAL 'NaN'", BOOLEAN, false);
        assertFunction("REAL 'NaN' = REAL 'NaN'", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("REAL '12.34' <> REAL '12.34'", BOOLEAN, false);
        assertFunction("REAL '12.34' <> REAL '12.340'", BOOLEAN, false);
        assertFunction("REAL '-17.34' <> REAL '-17.34'", BOOLEAN, false);
        assertFunction("REAL '71.17' <> REAL '23.45'", BOOLEAN, true);
        assertFunction("REAL '-0.0' <> REAL '0.0'", BOOLEAN, false);
        assertFunction("REAL 'NaN' <> REAL '1.23'", BOOLEAN, true);
        assertFunction("REAL '1.23' <> REAL 'NaN'", BOOLEAN, true);
        assertFunction("REAL 'NaN' <> REAL 'NaN'", BOOLEAN, true);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("REAL '12.34' < REAL '754.123'", BOOLEAN, true);
        assertFunction("REAL '-17.34' < REAL '-16.34'", BOOLEAN, true);
        assertFunction("REAL '71.17' < REAL '23.45'", BOOLEAN, false);
        assertFunction("REAL '-0.0' < REAL '0.0'", BOOLEAN, false);
        assertFunction("REAL 'NaN' < REAL '1.23'", BOOLEAN, false);
        assertFunction("REAL '1.23' < REAL 'NaN'", BOOLEAN, false);
        assertFunction("REAL 'NaN' < REAL 'NaN'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("REAL '12.34' <= REAL '754.123'", BOOLEAN, true);
        assertFunction("REAL '-17.34' <= REAL '-17.34'", BOOLEAN, true);
        assertFunction("REAL '71.17' <= REAL '23.45'", BOOLEAN, false);
        assertFunction("REAL '-0.0' <= REAL '0.0'", BOOLEAN, true);
        assertFunction("REAL 'NaN' <= REAL '1.23'", BOOLEAN, false);
        assertFunction("REAL '1.23' <= REAL 'NaN'", BOOLEAN, false);
        assertFunction("REAL 'NaN' <= REAL 'NaN'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("REAL '12.34' > REAL '754.123'", BOOLEAN, false);
        assertFunction("REAL '-17.34' > REAL '-17.34'", BOOLEAN, false);
        assertFunction("REAL '71.17' > REAL '23.45'", BOOLEAN, true);
        assertFunction("REAL '-0.0' > REAL '0.0'", BOOLEAN, false);
        assertFunction("REAL 'NaN' > REAL '1.23'", BOOLEAN, false);
        assertFunction("REAL '1.23' > REAL 'NaN'", BOOLEAN, false);
        assertFunction("REAL 'NaN' > REAL 'NaN'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("REAL '12.34' >= REAL '754.123'", BOOLEAN, false);
        assertFunction("REAL '-17.34' >= REAL '-17.34'", BOOLEAN, true);
        assertFunction("REAL '71.17' >= REAL '23.45'", BOOLEAN, true);
        assertFunction("REAL '-0.0' >= REAL '0.0'", BOOLEAN, true);
        assertFunction("REAL 'NaN' >= REAL '1.23'", BOOLEAN, false);
        assertFunction("REAL '1.23' >= REAL 'NaN'", BOOLEAN, false);
        assertFunction("REAL 'NaN' >= REAL 'NaN'", BOOLEAN, false);
    }

    @Test
    public void testBetween()
    {
        assertFunction("REAL '12.34' BETWEEN REAL '9.12' AND REAL '25.89'", BOOLEAN, true);
        assertFunction("REAL '-17.34' BETWEEN REAL '-17.34' AND REAL '-16.57'", BOOLEAN, true);
        assertFunction("REAL '-17.34' BETWEEN REAL '-18.98' AND REAL '-17.34'", BOOLEAN, true);
        assertFunction("REAL '0.0' BETWEEN REAL '-1.2' AND REAL '2.3'", BOOLEAN, true);
        assertFunction("REAL '56.78' BETWEEN REAL '12.34' AND REAL '34.56'", BOOLEAN, false);
        assertFunction("REAL '56.78' BETWEEN REAL '78.89' AND REAL '98.765'", BOOLEAN, false);
        assertFunction("REAL 'NaN' BETWEEN REAL '-1.2' AND REAL '2.3'", BOOLEAN, false);
        assertFunction("REAL '56.78' BETWEEN REAL '-NaN' AND REAL 'NaN'", BOOLEAN, false);
        assertFunction("REAL '56.78' BETWEEN REAL 'NaN' AND REAL '-NaN'", BOOLEAN, false);
        assertFunction("REAL '56.78' BETWEEN REAL '56.78' AND REAL 'NaN'", BOOLEAN, false);
        assertFunction("REAL 'NaN' BETWEEN REAL 'NaN' AND REAL 'NaN'", BOOLEAN, false);
    }

    @Test
    public void testCastToVarchar()
    {
        assertFunction("CAST(REAL '754.1985' as VARCHAR)", VARCHAR, "7.541985E2");
        assertFunction("CAST(REAL '-754.2008' as VARCHAR)", VARCHAR, "-7.542008E2");
        assertFunction("CAST(REAL 'Infinity' as VARCHAR)", VARCHAR, "Infinity");
        assertFunction("CAST(REAL '0.0' / REAL '0.0' as VARCHAR)", VARCHAR, "NaN");
        assertFunction("cast(REAL '12e2' as varchar(6))", createVarcharType(6), "1.2E3");
        assertFunction("cast(REAL '12e2' as varchar(50))", createVarcharType(50), "1.2E3");
        assertFunction("cast(REAL '12345678.9e0' as varchar(50))", createVarcharType(50), "1.234568E7");
        assertFunction("cast(REAL 'NaN' as varchar(3))", createVarcharType(3), "NaN");
        assertFunction("cast(REAL 'Infinity' as varchar(50))", createVarcharType(50), "Infinity");
        assertFunction("cast(REAL '12e2' as varchar(5))", createVarcharType(5), "1.2E3");
        assertInvalidCast("cast(REAL '12e2' as varchar(4))", "Value 1200.0 (1.2E3) cannot be represented as varchar(4)");
        assertInvalidCast("cast(REAL '0e0' as varchar(2))", "Value 0.0 (0E0) cannot be represented as varchar(2)");
        assertInvalidCast("cast(REAL '-0e0' as varchar(3))", "Value -0.0 (-0E0) cannot be represented as varchar(3)");
        assertInvalidCast("cast(REAL '0e0' / REAL '0e0' as varchar(2))", "Value NaN (NaN) cannot be represented as varchar(2)");
        assertInvalidCast("cast(REAL 'Infinity' as varchar(7))", "Value Infinity (Infinity) cannot be represented as varchar(7)");
    }

    @Test
    public void testCastToBigInt()
    {
        assertFunction("CAST(REAL '754.1985' as BIGINT)", BIGINT, 754L);
        assertFunction("CAST(REAL '-754.2008' as BIGINT)", BIGINT, -754L);
        assertFunction("CAST(REAL '1.98' as BIGINT)", BIGINT, 2L);
        assertFunction("CAST(REAL '-0.0' as BIGINT)", BIGINT, 0L);
        assertInvalidFunction("CAST(REAL 'NaN' as BIGINT)", INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToInteger()
    {
        assertFunction("CAST(REAL '754.2008' AS INTEGER)", INTEGER, 754);
        assertFunction("CAST(REAL '-754.1985' AS INTEGER)", INTEGER, -754);
        assertFunction("CAST(REAL '9.99' AS INTEGER)", INTEGER, 10);
        assertFunction("CAST(REAL '-0.0' AS INTEGER)", INTEGER, 0);
        assertInvalidFunction("CAST(REAL 'NaN' AS INTEGER)", INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToSmallint()
    {
        assertFunction("CAST(REAL '754.2008' AS SMALLINT)", SMALLINT, (short) 754);
        assertFunction("CAST(REAL '-754.1985' AS SMALLINT)", SMALLINT, (short) -754);
        assertFunction("CAST(REAL '9.99' AS SMALLINT)", SMALLINT, (short) 10);
        assertFunction("CAST(REAL '-0.0' AS SMALLINT)", SMALLINT, (short) 0);
        assertInvalidFunction("CAST(REAL 'NaN' AS SMALLINT)", INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToTinyint()
    {
        assertFunction("CAST(REAL '127.45' AS TINYINT)", TINYINT, (byte) 127);
        assertFunction("CAST(REAL '-128.234' AS TINYINT)", TINYINT, (byte) -128);
        assertFunction("CAST(REAL '9.99' AS TINYINT)", TINYINT, (byte) 10);
        assertFunction("CAST(REAL '-0.0' AS TINYINT)", TINYINT, (byte) 0);
        assertInvalidFunction("CAST(REAL 'NaN' AS TINYINT)", INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToDouble()
    {
        assertFunction("CAST(REAL '754.1985' AS DOUBLE)", DOUBLE, (double) 754.1985f);
        assertFunction("CAST(REAL '-754.2008' AS DOUBLE)", DOUBLE, (double) -754.2008f);
        assertFunction("CAST(REAL '0.0' AS DOUBLE)", DOUBLE, (double) 0.0f);
        assertFunction("CAST(REAL '-0.0' AS DOUBLE)", DOUBLE, (double) -0.0f);
        assertFunction("CAST(CAST(REAL '754.1985' AS DOUBLE) AS REAL)", REAL, 754.1985f);
        assertFunction("CAST(REAL 'NaN' AS DOUBLE)", DOUBLE, Double.NaN);
    }

    @Test
    public void testCastToBoolean()
    {
        assertFunction("CAST(REAL '754.1985' AS BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(REAL '0.0' AS BOOLEAN)", BOOLEAN, false);
        assertFunction("CAST(REAL '-0.0' AS BOOLEAN)", BOOLEAN, false);
        assertFunction("CAST(REAL 'NaN' AS BOOLEAN)", BOOLEAN, true);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertFunction("CAST(NULL AS REAL) IS DISTINCT FROM CAST(NULL AS REAL)", BOOLEAN, false);
        assertFunction("REAL '37.7' IS DISTINCT FROM REAL '37.7'", BOOLEAN, false);
        assertFunction("REAL '37.7' IS DISTINCT FROM REAL '37.8'", BOOLEAN, true);
        assertFunction("NULL IS DISTINCT FROM REAL '37.7'", BOOLEAN, true);
        assertFunction("REAL '37.7' IS DISTINCT FROM NULL", BOOLEAN, true);
        assertFunction("CAST(nan() AS REAL) IS DISTINCT FROM CAST(nan() AS REAL)", BOOLEAN, false);
        assertFunction("REAL 'NaN' IS DISTINCT FROM REAL '37.8'", BOOLEAN, true);
        assertFunction("REAL '37.8' IS DISTINCT FROM REAL 'NaN'", BOOLEAN, true);
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as real)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "cast(-1.2 as real)", BOOLEAN, false);
        assertOperator(INDETERMINATE, "cast(1.2 as real)", BOOLEAN, false);
        assertOperator(INDETERMINATE, "cast(123 as real)", BOOLEAN, false);
        assertOperator(INDETERMINATE, "REAL 'NaN'", BOOLEAN, false);
    }

    @Test
    public void testNanHash()
            throws Throwable
    {
        int[] nanRepresentations = {floatToIntBits(Float.NaN), 0xffc00000, 0x7fc00000, 0x7fc01234, 0xffc01234};
        for (int nanRepresentation : nanRepresentations) {
            assertTrue(isNaN(intBitsToFloat(nanRepresentation)));
            assertEquals(executeHashOperator(nanRepresentation), executeHashOperator(nanRepresentations[0]));
            assertEquals(executeXxHas64hOperator(nanRepresentation), executeXxHas64hOperator(nanRepresentations[0]));
        }
    }

    @Test
    public void testZeroHash()
            throws Throwable
    {
        int[] zeroes = {floatToIntBits(0.0f), floatToIntBits(-0.0f)};
        for (int zero : zeroes) {
            //noinspection SimplifiedTestNGAssertion
            assertTrue(intBitsToFloat(zero) == 0f);
            assertEquals(executeHashOperator(zero), executeHashOperator(zeroes[0]));
            assertEquals(executeXxHas64hOperator(zero), executeXxHas64hOperator(zeroes[0]));
        }
    }

    private long executeHashOperator(long value)
            throws Throwable
    {
        MethodHandle hashCodeOperator = functionAssertions.getTypeOperators().getHashCodeOperator(REAL, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
        return (long) hashCodeOperator.invokeExact((long) intBitsToFloat((int) value));
    }

    private long executeXxHas64hOperator(long value)
            throws Throwable
    {
        MethodHandle xxHash64Operator = functionAssertions.getTypeOperators().getXxHash64Operator(REAL, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
        return (long) xxHash64Operator.invokeExact((long) intBitsToFloat((int) value));
    }
}
